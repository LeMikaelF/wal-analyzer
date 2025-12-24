use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Result, WalValidatorError};

/// Reads pages from a SQLite database file
#[derive(Debug)]
pub struct PageReader {
    path: PathBuf,
    page_size: u32,
    page_count: u32,
}

impl PageReader {
    /// Create a new page reader for the given database file
    pub fn new(path: &Path, page_size: u32, page_count: u32) -> Self {
        PageReader {
            path: path.to_path_buf(),
            page_size,
            page_count,
        }
    }

    /// Read a page from the database file (1-indexed)
    pub fn read_page(&self, page_num: u32) -> Result<Vec<u8>> {
        if page_num == 0 || page_num > self.page_count {
            return Err(WalValidatorError::PageNotFound { page_num });
        }

        let mut file = File::open(&self.path)?;
        let offset = (page_num as u64 - 1) * self.page_size as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; self.page_size as usize];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Get the page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Get the page count
    pub fn page_count(&self) -> u32 {
        self.page_count
    }
}
