use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::{Result, WalValidatorError};

/// WAL magic number for big-endian checksums
pub const WAL_MAGIC_BE: u32 = 0x377f0682;
/// WAL magic number for little-endian checksums
pub const WAL_MAGIC_LE: u32 = 0x377f0683;

/// SQLite WAL file header (32 bytes)
#[derive(Debug, Clone)]
pub struct WalHeader {
    /// Magic number (determines checksum byte order)
    pub magic: u32,
    /// WAL format version (3007000)
    pub format_version: u32,
    /// Database page size
    pub page_size: u32,
    /// Checkpoint sequence number
    pub checkpoint_seq: u32,
    /// Salt value 1
    pub salt1: u32,
    /// Salt value 2
    pub salt2: u32,
    /// Checksum part 1
    pub checksum1: u32,
    /// Checksum part 2
    pub checksum2: u32,
    /// Whether checksums use big-endian byte order
    pub big_endian_checksums: bool,
}

impl WalHeader {
    /// Parse the WAL header from a file
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;
        Self::parse(&header)
    }

    /// Parse the WAL header from bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 32 {
            return Err(WalValidatorError::UnexpectedEof);
        }

        // First 4 bytes are magic - determines endianness
        let magic = BigEndian::read_u32(&data[0..4]);
        let big_endian_checksums = match magic {
            WAL_MAGIC_BE => true,
            WAL_MAGIC_LE => false,
            _ => return Err(WalValidatorError::InvalidWalMagic(magic)),
        };

        // Remaining fields are always big-endian
        let format_version = BigEndian::read_u32(&data[4..8]);
        let page_size = BigEndian::read_u32(&data[8..12]);
        let checkpoint_seq = BigEndian::read_u32(&data[12..16]);
        let salt1 = BigEndian::read_u32(&data[16..20]);
        let salt2 = BigEndian::read_u32(&data[20..24]);
        let checksum1 = BigEndian::read_u32(&data[24..28]);
        let checksum2 = BigEndian::read_u32(&data[28..32]);

        Ok(WalHeader {
            magic,
            format_version,
            page_size,
            checkpoint_seq,
            salt1,
            salt2,
            checksum1,
            checksum2,
            big_endian_checksums,
        })
    }

    /// Calculate the WAL checksum for a block of data
    /// Returns (checksum1, checksum2)
    pub fn checksum(&self, data: &[u8], initial: (u32, u32)) -> (u32, u32) {
        let (mut s0, mut s1) = initial;

        // Process data in 8-byte chunks
        for chunk in data.chunks(8) {
            if chunk.len() == 8 {
                let (v0, v1) = if self.big_endian_checksums {
                    (
                        BigEndian::read_u32(&chunk[0..4]),
                        BigEndian::read_u32(&chunk[4..8]),
                    )
                } else {
                    (
                        LittleEndian::read_u32(&chunk[0..4]),
                        LittleEndian::read_u32(&chunk[4..8]),
                    )
                };
                s0 = s0.wrapping_add(v0).wrapping_add(s1);
                s1 = s1.wrapping_add(v1).wrapping_add(s0);
            }
        }

        (s0, s1)
    }
}
