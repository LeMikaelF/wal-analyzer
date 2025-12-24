use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Result, WalValidatorError};
use crate::wal::{Frame, FrameHeader, WalHeader};

/// A commit consisting of one or more frames
#[derive(Debug)]
pub struct Commit {
    /// Commit index (0-indexed sequence number)
    pub index: u64,
    /// All frames in this commit
    pub frames: Vec<Frame>,
    /// Database size after this commit
    pub db_size: u32,
}

/// Iterator that yields commits from a WAL file
pub struct CommitIterator {
    file: File,
    wal_header: WalHeader,
    page_size: u32,
    current_frame_index: u64,
    current_commit_index: u64,
    pending_frames: Vec<Frame>,
    current_checksum: (u32, u32),
    finished: bool,
}

impl CommitIterator {
    /// Create a new commit iterator for a WAL file
    pub fn new(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read and parse WAL header
        let mut header_bytes = [0u8; 32];
        file.read_exact(&mut header_bytes)?;
        let wal_header = WalHeader::parse(&header_bytes)?;

        // Initial checksum is from the header (first 24 bytes)
        let initial_checksum = wal_header.checksum(&header_bytes[0..24], (0, 0));

        Ok(CommitIterator {
            file,
            page_size: wal_header.page_size,
            wal_header,
            current_frame_index: 0,
            current_commit_index: 0,
            pending_frames: Vec::new(),
            current_checksum: initial_checksum,
            finished: false,
        })
    }

    /// Get a reference to the WAL header
    pub fn wal_header(&self) -> &WalHeader {
        &self.wal_header
    }

    /// Try to read the next frame from the WAL file
    fn read_frame(&mut self) -> Result<Option<Frame>> {
        // Calculate frame offset: header (32 bytes) + frame_index * (24 + page_size)
        let frame_size = 24 + self.page_size as u64;
        let offset = 32 + self.current_frame_index * frame_size;

        // Seek to frame position
        if self.file.seek(SeekFrom::Start(offset)).is_err() {
            return Ok(None);
        }

        // Read frame header (24 bytes)
        let mut header_bytes = [0u8; 24];
        match self.file.read_exact(&mut header_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let frame_header = FrameHeader::parse(&header_bytes)?;

        // Validate salt values match WAL header
        if frame_header.salt1 != self.wal_header.salt1
            || frame_header.salt2 != self.wal_header.salt2
        {
            // Salt mismatch indicates end of valid frames or corruption
            return Ok(None);
        }

        // Read page data
        let mut page_data = vec![0u8; self.page_size as usize];
        match self.file.read_exact(&mut page_data) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        // Verify checksum
        let checksum = self.wal_header.checksum(&header_bytes[0..8], self.current_checksum);
        let checksum = self.wal_header.checksum(&page_data, checksum);

        if checksum.0 != frame_header.checksum1 || checksum.1 != frame_header.checksum2 {
            return Err(WalValidatorError::ChecksumMismatch {
                frame_index: self.current_frame_index,
            });
        }

        // Update running checksum
        self.current_checksum = checksum;

        let frame = Frame {
            header: frame_header,
            page_data,
            frame_index: self.current_frame_index,
        };

        self.current_frame_index += 1;

        Ok(Some(frame))
    }
}

impl Iterator for CommitIterator {
    type Item = Result<Commit>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            match self.read_frame() {
                Ok(Some(frame)) => {
                    let is_commit = frame.header.is_commit();
                    let db_size = frame.header.db_size_after_commit;
                    self.pending_frames.push(frame);

                    if is_commit {
                        let commit = Commit {
                            index: self.current_commit_index,
                            frames: std::mem::take(&mut self.pending_frames),
                            db_size,
                        };
                        self.current_commit_index += 1;
                        return Some(Ok(commit));
                    }
                }
                Ok(None) => {
                    // EOF reached
                    self.finished = true;
                    if !self.pending_frames.is_empty() {
                        // Incomplete commit - could be in-progress transaction
                        // We'll silently ignore it rather than error
                    }
                    return None;
                }
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }
    }
}
