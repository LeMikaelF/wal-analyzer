use byteorder::{BigEndian, ByteOrder};

use crate::error::{Result, WalValidatorError};

/// WAL frame header (24 bytes)
#[derive(Debug, Clone)]
pub struct FrameHeader {
    /// Page number (1-indexed)
    pub page_number: u32,
    /// Database size after commit (0 if not a commit frame)
    pub db_size_after_commit: u32,
    /// Salt value 1 (must match WAL header)
    pub salt1: u32,
    /// Salt value 2 (must match WAL header)
    pub salt2: u32,
    /// Checksum part 1
    pub checksum1: u32,
    /// Checksum part 2
    pub checksum2: u32,
}

impl FrameHeader {
    /// Parse a frame header from bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(WalValidatorError::UnexpectedEof);
        }

        Ok(FrameHeader {
            page_number: BigEndian::read_u32(&data[0..4]),
            db_size_after_commit: BigEndian::read_u32(&data[4..8]),
            salt1: BigEndian::read_u32(&data[8..12]),
            salt2: BigEndian::read_u32(&data[12..16]),
            checksum1: BigEndian::read_u32(&data[16..20]),
            checksum2: BigEndian::read_u32(&data[20..24]),
        })
    }

    /// Returns true if this frame marks a commit boundary
    pub fn is_commit(&self) -> bool {
        self.db_size_after_commit != 0
    }
}

/// A complete WAL frame with header and page data
#[derive(Debug, Clone)]
pub struct Frame {
    /// Frame header
    pub header: FrameHeader,
    /// Page data
    pub page_data: Vec<u8>,
    /// Frame index in the WAL file (0-indexed)
    pub frame_index: u64,
}
