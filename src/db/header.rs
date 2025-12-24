use byteorder::{BigEndian, ByteOrder};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::{Result, WalValidatorError};

/// SQLite database file header (first 100 bytes of page 1)
#[derive(Debug, Clone)]
pub struct DbHeader {
    /// Database page size in bytes
    pub page_size: u32,
    /// Size of the database in pages
    pub page_count: u32,
    /// Schema cookie (incremented on schema changes)
    pub schema_cookie: u32,
    /// Text encoding (1=UTF-8, 2=UTF-16le, 3=UTF-16be)
    pub text_encoding: u32,
}

/// SQLite database header magic bytes
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

impl DbHeader {
    /// Parse the database header from a file
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut header = [0u8; 100];
        file.read_exact(&mut header)?;
        Self::parse(&header)
    }

    /// Parse the database header from bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 100 {
            return Err(WalValidatorError::UnexpectedEof);
        }

        // Validate magic bytes (first 16 bytes)
        if &data[0..16] != SQLITE_MAGIC {
            return Err(WalValidatorError::InvalidDbMagic);
        }

        // Page size at offset 16-17 (big-endian)
        // A value of 1 means 65536
        let raw_page_size = BigEndian::read_u16(&data[16..18]) as u32;
        let page_size = if raw_page_size == 1 {
            65536
        } else {
            raw_page_size
        };

        // Validate page size is a power of 2 between 512 and 65536
        if !page_size.is_power_of_two() || page_size < 512 || page_size > 65536 {
            return Err(WalValidatorError::InvalidPageSize(page_size));
        }

        // Database size in pages at offset 28-31
        let page_count = BigEndian::read_u32(&data[28..32]);

        // Schema cookie at offset 40-43
        let schema_cookie = BigEndian::read_u32(&data[40..44]);

        // Text encoding at offset 56-59
        let text_encoding = BigEndian::read_u32(&data[56..60]);

        Ok(DbHeader {
            page_size,
            page_count,
            schema_cookie,
            text_encoding,
        })
    }
}
