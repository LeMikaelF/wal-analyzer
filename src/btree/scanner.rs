use byteorder::{BigEndian, ByteOrder};

use crate::btree::cell::{extract_index_key, extract_index_rowid, parse_varint, IndexKey};
use crate::btree::page::{BTreePageHeader, BTreePageType};
use crate::error::{Result, WalValidatorError};
use crate::validator::PageCache;

/// Information about a B-tree
#[derive(Debug, Clone)]
pub struct BTreeInfo {
    /// Root page number
    pub root_page: u32,
    /// Table or index name (if known)
    pub name: Option<String>,
    /// For indexes: the table this index belongs to
    pub tbl_name: Option<String>,
    /// The SQL statement that created this object (for indexes: used to detect partial/expression indexes)
    pub sql: Option<String>,
    /// True if this is a table, false if index
    pub is_table: bool,
    /// True if this is a unique index (only relevant for indexes)
    pub is_unique: bool,
}

/// Location of a rowid or key within a B-tree
#[derive(Debug, Clone, PartialEq)]
pub struct RowidLocation {
    /// Page number where this rowid was found
    pub page_number: u32,
    /// Cell index within the page
    pub cell_index: u16,
    /// Frame index that last modified this page (None if from base DB)
    pub frame_index: Option<u64>,
}

/// Scanner for traversing B-trees and collecting rowids/keys
pub struct BTreeScanner<'a> {
    page_cache: &'a mut PageCache,
}

impl<'a> BTreeScanner<'a> {
    pub fn new(page_cache: &'a mut PageCache) -> Self {
        BTreeScanner { page_cache }
    }

    /// Discover all B-trees by reading sqlite_master (page 1)
    pub fn discover_btrees(&mut self) -> Result<Vec<BTreeInfo>> {
        let mut btrees = Vec::new();

        self.scan_sqlite_master(1, &mut btrees)?;

        Ok(btrees)
    }

    /// Scan sqlite_master pages to find all tables and indexes
    fn scan_sqlite_master(&mut self, root_page: u32, btrees: &mut Vec<BTreeInfo>) -> Result<()> {
        let mut stack = vec![root_page];

        while let Some(page_num) = stack.pop() {
            let page_data = self.page_cache.get_page(page_num)?;
            let (header, _) = BTreePageHeader::parse(&page_data, page_num)?;

            match header.page_type {
                BTreePageType::TableLeaf => {
                    // Parse cells to find tables and indexes
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for cell_ptr in cell_pointers {
                        if let Ok(btree_info) =
                            self.parse_sqlite_master_cell(&page_data, cell_ptr as usize)
                        {
                            if let Some(info) = btree_info {
                                btrees.push(info);
                            }
                        }
                    }
                }
                BTreePageType::TableInterior => {
                    // Push child pages
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;
                    for cell_ptr in cell_pointers {
                        let ptr = cell_ptr as usize;
                        if ptr + 4 <= page_data.len() {
                            let child = BigEndian::read_u32(&page_data[ptr..ptr + 4]);
                            stack.push(child);
                        }
                    }
                    if let Some(right_child) = header.right_child {
                        stack.push(right_child);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Parse a single sqlite_master cell to extract table/index info
    fn parse_sqlite_master_cell(
        &self,
        page_data: &[u8],
        cell_offset: usize,
    ) -> Result<Option<BTreeInfo>> {
        if cell_offset >= page_data.len() {
            return Err(WalValidatorError::CellPointerOutOfBounds { page_num: 1 });
        }

        let cell_data = &page_data[cell_offset..];

        // Parse payload size
        let (payload_size, mut offset) = parse_varint(cell_data)?;
        // Parse rowid
        let (_rowid, rowid_len) = parse_varint(&cell_data[offset..])?;
        offset += rowid_len;

        // Now we have the payload
        let payload_size = payload_size as usize;
        if offset + payload_size > cell_data.len() {
            // Could be overflow, skip this cell
            return Ok(None);
        }

        let payload = &cell_data[offset..offset + payload_size];

        // Parse record header
        let (header_size, header_varint_len) = parse_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len() {
            return Ok(None);
        }

        // Parse serial types from header
        let mut serial_types = Vec::new();
        let mut hdr_offset = header_varint_len;
        while hdr_offset < header_size {
            let (st, len) = parse_varint(&payload[hdr_offset..])?;
            serial_types.push(st);
            hdr_offset += len;
        }

        // sqlite_master schema: type, name, tbl_name, rootpage, sql
        // We need at least 4 columns
        if serial_types.len() < 4 {
            return Ok(None);
        }

        // Calculate offsets for each column
        let mut data_offset = header_size;
        let mut column_offsets = Vec::new();
        for &st in &serial_types {
            column_offsets.push(data_offset);
            data_offset += serial_type_content_size(st);
        }

        // Column 0: type (TEXT)
        let type_col = self.read_text_column(payload, &serial_types, &column_offsets, 0)?;

        // Column 1: name (TEXT)
        let name_col = self.read_text_column(payload, &serial_types, &column_offsets, 1)?;

        // Column 2: tbl_name (TEXT) - for indexes, this is the table they belong to
        let tbl_name_col = self.read_text_column(payload, &serial_types, &column_offsets, 2)?;

        // Column 3: rootpage (INTEGER)
        let rootpage = self.read_int_column(payload, &serial_types, &column_offsets, 3)?;

        // Column 4: sql (TEXT) - used to determine if index is unique, partial, or expression-based
        let sql_col = self.read_text_column(payload, &serial_types, &column_offsets, 4)?;

        if let (Some(obj_type), Some(ref name), Some(root_page)) = (type_col, name_col, rootpage) {
            let root_page = root_page as u32;
            if root_page > 0 && (obj_type == "table" || obj_type == "index") {
                // Determine if index is unique:
                // - Autoindexes (created for PRIMARY KEY/UNIQUE constraints) are always unique
                // - For explicit indexes, check if SQL contains "UNIQUE"
                let is_unique = if obj_type == "index" {
                    if name.starts_with("sqlite_autoindex_") {
                        // Autoindexes are created for PRIMARY KEY and UNIQUE constraints
                        true
                    } else if let Some(ref sql) = sql_col {
                        // Check if the CREATE INDEX statement includes UNIQUE
                        sql.to_uppercase().contains("UNIQUE")
                    } else {
                        // No SQL available, assume not unique to be safe
                        false
                    }
                } else {
                    // Tables: is_unique doesn't apply, set to false
                    false
                };

                return Ok(Some(BTreeInfo {
                    root_page,
                    name: Some(name.clone()),
                    tbl_name: tbl_name_col,
                    sql: sql_col,
                    is_table: obj_type == "table",
                    is_unique,
                }));
            }
        }

        Ok(None)
    }

    fn read_text_column(
        &self,
        payload: &[u8],
        serial_types: &[u64],
        offsets: &[usize],
        col: usize,
    ) -> Result<Option<String>> {
        if col >= serial_types.len() {
            return Ok(None);
        }

        let st = serial_types[col];
        if st < 13 || st % 2 == 0 {
            return Ok(None); // Not text
        }

        let size = ((st - 13) / 2) as usize;
        let offset = offsets[col];

        if offset + size > payload.len() {
            return Ok(None);
        }

        Ok(String::from_utf8(payload[offset..offset + size].to_vec()).ok())
    }

    fn read_int_column(
        &self,
        payload: &[u8],
        serial_types: &[u64],
        offsets: &[usize],
        col: usize,
    ) -> Result<Option<i64>> {
        if col >= serial_types.len() {
            return Ok(None);
        }

        let st = serial_types[col];
        let offset = offsets[col];

        let value = match st {
            0 => return Ok(None), // NULL
            1 => {
                if offset >= payload.len() {
                    return Ok(None);
                }
                payload[offset] as i8 as i64
            }
            2 => {
                if offset + 2 > payload.len() {
                    return Ok(None);
                }
                BigEndian::read_i16(&payload[offset..offset + 2]) as i64
            }
            3 => {
                if offset + 3 > payload.len() {
                    return Ok(None);
                }
                let b = &payload[offset..offset + 3];
                ((b[0] as i32) << 16 | (b[1] as i32) << 8 | b[2] as i32) as i64
            }
            4 => {
                if offset + 4 > payload.len() {
                    return Ok(None);
                }
                BigEndian::read_i32(&payload[offset..offset + 4]) as i64
            }
            5 => {
                if offset + 6 > payload.len() {
                    return Ok(None);
                }
                let mut buf = [0u8; 8];
                buf[2..8].copy_from_slice(&payload[offset..offset + 6]);
                i64::from_be_bytes(buf) >> 16
            }
            6 => {
                if offset + 8 > payload.len() {
                    return Ok(None);
                }
                BigEndian::read_i64(&payload[offset..offset + 8])
            }
            8 => 0,
            9 => 1,
            _ => return Ok(None),
        };

        Ok(Some(value))
    }

    /// Collect all rowids from a table B-tree
    pub fn collect_table_rowids(
        &mut self,
        root_page: u32,
    ) -> Result<Vec<(i64, RowidLocation)>> {
        let mut rowids = Vec::new();
        let mut stack = vec![root_page];

        while let Some(page_num) = stack.pop() {
            let frame_index = self.page_cache.get_frame_index(page_num);
            let page_data = self.page_cache.get_page(page_num)?;
            let (header, _) = BTreePageHeader::parse(&page_data, page_num)?;

            match header.page_type {
                BTreePageType::TableLeaf => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for (cell_idx, &cell_ptr) in cell_pointers.iter().enumerate() {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset >= page_data.len() {
                            continue;
                        }

                        let cell_data = &page_data[cell_offset..];

                        // Parse payload size varint
                        let (_, payload_len) = parse_varint(cell_data)?;
                        // Parse rowid varint
                        let (rowid, _) = parse_varint(&cell_data[payload_len..])?;

                        rowids.push((
                            rowid as i64,
                            RowidLocation {
                                page_number: page_num,
                                cell_index: cell_idx as u16,
                                frame_index,
                            },
                        ));
                    }
                }
                BTreePageType::TableInterior => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for &cell_ptr in &cell_pointers {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset + 4 > page_data.len() {
                            continue;
                        }

                        // First 4 bytes are left child pointer
                        let left_child = BigEndian::read_u32(&page_data[cell_offset..cell_offset + 4]);
                        stack.push(left_child);
                    }

                    // Don't forget the rightmost child
                    if let Some(right_child) = header.right_child {
                        stack.push(right_child);
                    }
                }
                _ => {
                    // Index pages in a table B-tree shouldn't happen
                }
            }
        }

        Ok(rowids)
    }

    /// Collect all keys from an index B-tree
    pub fn collect_index_keys(
        &mut self,
        root_page: u32,
    ) -> Result<Vec<(IndexKey, RowidLocation)>> {
        let mut keys = Vec::new();
        let mut stack = vec![root_page];

        while let Some(page_num) = stack.pop() {
            let frame_index = self.page_cache.get_frame_index(page_num);
            let page_data = self.page_cache.get_page(page_num)?;
            let (header, _) = BTreePageHeader::parse(&page_data, page_num)?;

            match header.page_type {
                BTreePageType::IndexLeaf => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for (cell_idx, &cell_ptr) in cell_pointers.iter().enumerate() {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset >= page_data.len() {
                            continue;
                        }

                        let cell_data = &page_data[cell_offset..];

                        // Parse payload size varint
                        let (payload_size, payload_len) = parse_varint(cell_data)?;
                        let payload_size = payload_size as usize;

                        // The payload starts right after the payload size
                        if payload_len + payload_size > cell_data.len() {
                            continue; // Overflow, skip
                        }

                        let payload = &cell_data[payload_len..payload_len + payload_size];
                        if let Ok(key) = extract_index_key(payload) {
                            keys.push((
                                key,
                                RowidLocation {
                                    page_number: page_num,
                                    cell_index: cell_idx as u16,
                                    frame_index,
                                },
                            ));
                        }
                    }
                }
                BTreePageType::IndexInterior => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for &cell_ptr in &cell_pointers {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset + 4 > page_data.len() {
                            continue;
                        }

                        // First 4 bytes are left child pointer
                        let left_child = BigEndian::read_u32(&page_data[cell_offset..cell_offset + 4]);
                        stack.push(left_child);
                    }

                    if let Some(right_child) = header.right_child {
                        stack.push(right_child);
                    }
                }
                _ => {}
            }
        }

        Ok(keys)
    }

    /// Collect all rowids referenced by an index B-tree
    /// Returns the rowids that the index entries point to (the last column in each index entry)
    pub fn collect_index_rowids(&mut self, root_page: u32) -> Result<Vec<i64>> {
        let mut rowids = Vec::new();
        let mut stack = vec![root_page];

        while let Some(page_num) = stack.pop() {
            let page_data = self.page_cache.get_page(page_num)?;
            let (header, _) = BTreePageHeader::parse(&page_data, page_num)?;

            match header.page_type {
                BTreePageType::IndexLeaf => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for &cell_ptr in &cell_pointers {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset >= page_data.len() {
                            continue;
                        }

                        let cell_data = &page_data[cell_offset..];

                        // Parse payload size varint
                        let (payload_size, payload_len) = parse_varint(cell_data)?;
                        let payload_size = payload_size as usize;

                        // The payload starts right after the payload size
                        if payload_len + payload_size > cell_data.len() {
                            continue; // Overflow, skip
                        }

                        let payload = &cell_data[payload_len..payload_len + payload_size];
                        if let Ok(rowid) = extract_index_rowid(payload) {
                            rowids.push(rowid);
                        }
                    }
                }
                BTreePageType::IndexInterior => {
                    let cell_pointers = header.get_cell_pointers(&page_data, page_num)?;

                    for &cell_ptr in &cell_pointers {
                        let cell_offset = cell_ptr as usize;
                        if cell_offset + 4 > page_data.len() {
                            continue;
                        }

                        // First 4 bytes are left child pointer
                        let left_child =
                            BigEndian::read_u32(&page_data[cell_offset..cell_offset + 4]);
                        stack.push(left_child);
                    }

                    if let Some(right_child) = header.right_child {
                        stack.push(right_child);
                    }
                }
                _ => {}
            }
        }

        Ok(rowids)
    }
}

/// Get the content size for a serial type
fn serial_type_content_size(serial_type: u64) -> usize {
    match serial_type {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8,
        8 | 9 => 0,
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize,
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize,
        _ => 0,
    }
}
