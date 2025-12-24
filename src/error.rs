use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalValidatorError {
    // I/O Errors
    #[error("Failed to read file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database file not found: {}", .0.display())]
    DatabaseNotFound(PathBuf),

    #[error("WAL file not found: {}", .0.display())]
    WalNotFound(PathBuf),

    // Database Format Errors
    #[error("Invalid SQLite database header magic bytes")]
    InvalidDbMagic,

    #[error("Invalid database page size: {0}")]
    InvalidPageSize(u32),

    // WAL Format Errors
    #[error("Invalid WAL header magic: expected 0x377f0682 or 0x377f0683, got {0:#x}")]
    InvalidWalMagic(u32),

    #[error("Page size mismatch: database says {db_size}, WAL says {wal_size}")]
    PageSizeMismatch { db_size: u32, wal_size: u32 },

    #[error("WAL checksum mismatch at frame {frame_index}")]
    ChecksumMismatch { frame_index: u64 },

    #[error("Incomplete commit at end of WAL (started at frame {start_frame})")]
    IncompleteCommit { start_frame: u64 },

    // B-tree Errors
    #[error("Invalid B-tree page type: {0:#x} at page {1}")]
    InvalidPageType(u8, u32),

    #[error("Invalid varint encoding")]
    InvalidVarint,

    #[error("Page {page_num} referenced but not found")]
    PageNotFound { page_num: u32 },

    #[error("Cell pointer out of bounds at page {page_num}")]
    CellPointerOutOfBounds { page_num: u32 },

    #[error("Unexpected end of data while parsing")]
    UnexpectedEof,
}

pub type Result<T> = std::result::Result<T, WalValidatorError>;
