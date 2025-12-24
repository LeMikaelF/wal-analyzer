use byteorder::{BigEndian, ByteOrder};

use crate::error::{Result, WalValidatorError};

/// B-tree page types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreePageType {
    /// Interior page of a table B-tree (0x05)
    TableInterior,
    /// Leaf page of a table B-tree (0x0D)
    TableLeaf,
    /// Interior page of an index B-tree (0x02)
    IndexInterior,
    /// Leaf page of an index B-tree (0x0A)
    IndexLeaf,
}

impl BTreePageType {
    /// Parse a page type byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(BTreePageType::IndexInterior),
            0x05 => Some(BTreePageType::TableInterior),
            0x0A => Some(BTreePageType::IndexLeaf),
            0x0D => Some(BTreePageType::TableLeaf),
            _ => None,
        }
    }

    /// Returns true if this is an interior (non-leaf) page
    pub fn is_interior(&self) -> bool {
        matches!(self, BTreePageType::TableInterior | BTreePageType::IndexInterior)
    }

    /// Returns true if this is a table (not index) page
    pub fn is_table(&self) -> bool {
        matches!(self, BTreePageType::TableInterior | BTreePageType::TableLeaf)
    }
}

/// B-tree page header
#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// Page type
    pub page_type: BTreePageType,
    /// Offset to first freeblock (0 if none)
    pub first_freeblock: u16,
    /// Number of cells on this page
    pub cell_count: u16,
    /// Offset to start of cell content area
    pub cell_content_offset: u16,
    /// Number of fragmented free bytes
    pub fragmented_bytes: u8,
    /// Right-most child pointer (only for interior pages)
    pub right_child: Option<u32>,
}

impl BTreePageHeader {
    /// Parse a B-tree page header
    /// `is_page_one` should be true for page 1 (which has 100-byte DB header prefix)
    pub fn parse(data: &[u8], page_num: u32) -> Result<(Self, usize)> {
        // Page 1 has 100-byte database header before B-tree header
        let offset = if page_num == 1 { 100 } else { 0 };

        if data.len() < offset + 8 {
            return Err(WalValidatorError::UnexpectedEof);
        }

        let page_type_byte = data[offset];
        let page_type = BTreePageType::from_byte(page_type_byte)
            .ok_or(WalValidatorError::InvalidPageType(page_type_byte, page_num))?;

        let first_freeblock = BigEndian::read_u16(&data[offset + 1..offset + 3]);
        let cell_count = BigEndian::read_u16(&data[offset + 3..offset + 5]);
        let cell_content_offset = BigEndian::read_u16(&data[offset + 5..offset + 7]);
        let fragmented_bytes = data[offset + 7];

        let (right_child, header_size) = if page_type.is_interior() {
            if data.len() < offset + 12 {
                return Err(WalValidatorError::UnexpectedEof);
            }
            let right_child = BigEndian::read_u32(&data[offset + 8..offset + 12]);
            (Some(right_child), offset + 12)
        } else {
            (None, offset + 8)
        };

        Ok((
            BTreePageHeader {
                page_type,
                first_freeblock,
                cell_count,
                cell_content_offset,
                fragmented_bytes,
                right_child,
            },
            header_size,
        ))
    }

    /// Get cell pointers from the page
    pub fn get_cell_pointers(&self, data: &[u8], page_num: u32) -> Result<Vec<u16>> {
        let header_offset = if page_num == 1 { 100 } else { 0 };
        let header_size = if self.page_type.is_interior() { 12 } else { 8 };
        let pointers_start = header_offset + header_size;

        let mut pointers = Vec::with_capacity(self.cell_count as usize);

        for i in 0..self.cell_count as usize {
            let ptr_offset = pointers_start + i * 2;
            if ptr_offset + 2 > data.len() {
                return Err(WalValidatorError::CellPointerOutOfBounds { page_num });
            }
            let ptr = BigEndian::read_u16(&data[ptr_offset..ptr_offset + 2]);
            pointers.push(ptr);
        }

        Ok(pointers)
    }
}
