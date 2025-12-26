use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::db::PageReader;
use crate::error::Result;
use crate::wal::Commit;

/// Page cache that tracks current page state across WAL commits
#[derive(Debug)]
pub struct PageCache {
    /// Path to the database file (kept for debugging)
    #[allow(dead_code)]
    db_path: PathBuf,
    /// Page size in bytes
    page_size: u32,
    /// Total pages in base database
    db_page_count: u32,
    /// WAL overlay: page number -> (page data, frame index that last modified it)
    overlay: HashMap<u32, (Vec<u8>, u64)>,
    /// Page reader for base database
    page_reader: PageReader,
}

impl PageCache {
    /// Create a new page cache from a database file
    pub fn new(db_path: &Path, page_size: u32, page_count: u32) -> Self {
        PageCache {
            db_path: db_path.to_path_buf(),
            page_size,
            db_page_count: page_count,
            overlay: HashMap::new(),
            page_reader: PageReader::new(db_path, page_size, page_count),
        }
    }

    /// Get a page, checking WAL overlay first, then base database
    pub fn get_page(&mut self, page_num: u32) -> Result<Vec<u8>> {
        // Check WAL overlay first
        if let Some((page, _frame_idx)) = self.overlay.get(&page_num) {
            return Ok(page.clone());
        }

        // Fall back to base database
        match self.page_reader.read_page(page_num) {
            Ok(page) => Ok(page),
            Err(crate::error::WalValidatorError::PageNotFound { .. }) => {
                // Page doesn't exist in base DB file (may only exist in WAL)
                // Return a zeroed page
                Ok(vec![0u8; self.page_size as usize])
            }
            Err(e) => Err(e),
        }
    }

    /// Get the frame index that last modified a page (None if from base DB)
    pub fn get_frame_index(&self, page_num: u32) -> Option<u64> {
        self.overlay.get(&page_num).map(|(_, frame_idx)| *frame_idx)
    }

    /// Apply a commit's frames to the overlay
    pub fn apply_commit(&mut self, commit: &Commit) {
        for frame in &commit.frames {
            self.overlay.insert(
                frame.header.page_number,
                (frame.page_data.clone(), frame.frame_index),
            );
        }
    }

    /// Reset the overlay (for re-validation)
    pub fn reset(&mut self) {
        self.overlay.clear();
    }

    /// Get the page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Get the current effective page count (base + WAL extensions)
    pub fn effective_page_count(&self) -> u32 {
        let max_overlay = self.overlay.keys().max().copied().unwrap_or(0);
        std::cmp::max(self.db_page_count, max_overlay)
    }
}
