use crate::error::{Result, WalValidatorError};

/// Parse a SQLite varint (1-9 bytes)
/// Returns (value, bytes_consumed)
pub fn parse_varint(data: &[u8]) -> Result<(u64, usize)> {
    if data.is_empty() {
        return Err(WalValidatorError::UnexpectedEof);
    }

    let mut value: u64 = 0;
    let mut bytes_read = 0;

    for (i, &byte) in data.iter().take(9).enumerate() {
        if i == 8 {
            // 9th byte uses all 8 bits
            value = (value << 8) | (byte as u64);
            bytes_read = 9;
            break;
        } else {
            value = (value << 7) | ((byte & 0x7F) as u64);
            bytes_read = i + 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
    }

    Ok((value, bytes_read))
}

/// Parse a signed varint (interpreting the u64 as i64)
pub fn parse_signed_varint(data: &[u8]) -> Result<(i64, usize)> {
    let (value, len) = parse_varint(data)?;
    Ok((value as i64, len))
}

/// Represents an index key (the first few columns of an index entry)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    /// Raw bytes of the key for comparison
    pub raw: Vec<u8>,
}

impl std::fmt::Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Try to interpret as UTF-8 text, fall back to hex
        if let Ok(s) = std::str::from_utf8(&self.raw) {
            if s.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') {
                return write!(f, "\"{}\"", s);
            }
        }
        write!(f, "0x{}", hex_encode(&self.raw))
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse a record header to get serial types
/// Returns (serial_types, header_bytes_consumed)
pub fn parse_record_header(data: &[u8]) -> Result<(Vec<u64>, usize)> {
    if data.is_empty() {
        return Err(WalValidatorError::UnexpectedEof);
    }

    // First varint is header size (including itself)
    let (header_size, first_varint_len) = parse_varint(data)?;
    let header_size = header_size as usize;

    if header_size > data.len() {
        return Err(WalValidatorError::UnexpectedEof);
    }

    let mut serial_types = Vec::new();
    let mut offset = first_varint_len;

    while offset < header_size {
        let (serial_type, len) = parse_varint(&data[offset..])?;
        serial_types.push(serial_type);
        offset += len;
    }

    Ok((serial_types, header_size))
}

/// Get the size of a value given its serial type
pub fn serial_type_size(serial_type: u64) -> usize {
    match serial_type {
        0 => 0,           // NULL
        1 => 1,           // 8-bit signed int
        2 => 2,           // 16-bit signed int
        3 => 3,           // 24-bit signed int
        4 => 4,           // 32-bit signed int
        5 => 6,           // 48-bit signed int
        6 => 8,           // 64-bit signed int
        7 => 8,           // IEEE 754 float
        8 => 0,           // Integer constant 0
        9 => 0,           // Integer constant 1
        10 | 11 => 0,     // Reserved
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // TEXT
        _ => 0,
    }
}

/// Extract the key portion of an index cell payload
/// For indexes, the key is everything except the last column (which is the rowid)
pub fn extract_index_key(payload: &[u8]) -> Result<IndexKey> {
    // Parse the record header to find column boundaries
    let (serial_types, header_size) = parse_record_header(payload)?;

    if serial_types.is_empty() {
        return Ok(IndexKey { raw: Vec::new() });
    }

    // Calculate total size of all columns except the last (rowid)
    let key_columns = if serial_types.len() > 1 {
        &serial_types[..serial_types.len() - 1]
    } else {
        &serial_types[..]
    };

    let key_size: usize = key_columns.iter().map(|&st| serial_type_size(st)).sum();

    // Extract the key bytes (header + key column data)
    let key_end = header_size + key_size;
    if key_end > payload.len() {
        return Err(WalValidatorError::UnexpectedEof);
    }

    Ok(IndexKey {
        raw: payload[..key_end].to_vec(),
    })
}
