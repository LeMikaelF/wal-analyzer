# wal-validator

A Rust CLI tool that validates SQLite databases and WAL (Write-Ahead Log) files for duplicate rowids and index keys in B-trees.

## Overview

This tool parses a SQLite database file and its associated WAL file, iterating through each commit to detect B-tree corruption in the form of duplicate entries:

- **Duplicate rowids** in table B-trees
- **Duplicate keys** in index B-trees (experimental, opt-in)

Duplicates can occur either within a single page (intra-page) or across multiple pages (inter-page) of the same B-tree.

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/wal-validator`.

## Usage

```bash
wal-validator --database <path-to-db> [--wal <path-to-wal>]
```

### Options

| Option | Description |
|--------|-------------|
| `-d, --database <PATH>` | Path to the SQLite database file (.db) |
| `-w, --wal <PATH>` | Path to the WAL file (defaults to `<database>-wal`) |
| `--check-indexes` | Also check index B-trees for duplicate keys (experimental) |
| `-h, --help` | Print help |
| `-V, --version` | Print version |

### Examples

```bash
# Validate a database with its default WAL location
wal-validator --database /path/to/mydb.db

# Specify a custom WAL file path
wal-validator --database /path/to/mydb.db --wal /path/to/mydb.db-wal
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No duplicates found - database is valid |
| 1 | Error occurred during validation |
| 2 | Duplicates were detected |

## Output

The tool produces a human-readable report:

```
================================================================================
SQLite WAL Validator Report
================================================================================
Database: /path/to/test.db
WAL File: /path/to/test.db-wal
Page Size: 4096 bytes

--------------------------------------------------------------------------------
DUPLICATE FOUND in Base Database State
--------------------------------------------------------------------------------
Table: users (root page 3)

  Rowid 42:
    - Page 5, Cell 12
    - Page 8, Cell 3

--------------------------------------------------------------------------------
DUPLICATE FOUND in Commit #7
--------------------------------------------------------------------------------
Index: idx_users_email (root page 9)

  Key: "user@example.com"
    - Page 10, Cell 5
    - Page 10, Cell 6  [Intra-page]

================================================================================
Summary: 2 duplicates found (1 in base DB, 1 in WAL commits)
================================================================================
```

## How It Works

1. **Parse database header** - Validates the SQLite magic bytes and extracts page size
2. **Initialize page cache** - Loads base pages from the database file
3. **Discover B-trees** - Parses `sqlite_master` to find all tables and indexes
4. **Check base state** - Scans all B-trees for duplicates before any WAL commits
5. **Process WAL commits** - For each commit:
   - Applies frame pages to the page cache (overlay)
   - Re-discovers B-trees (schema may have changed)
   - Scans all table B-trees for duplicate rowids
   - Optionally scans index B-trees for duplicate keys (if `--check-indexes`)
6. **Report findings** - Outputs any duplicates with their locations

## Technical Details

### Supported Formats

- SQLite database format (page sizes 512 to 65536 bytes)
- WAL format version 3007000
- Both big-endian and little-endian WAL checksums

### B-tree Page Types

| Type | Code | Description |
|------|------|-------------|
| Table Interior | 0x05 | Non-leaf table page |
| Table Leaf | 0x0D | Leaf table page with rowids |
| Index Interior | 0x02 | Non-leaf index page |
| Index Leaf | 0x0A | Leaf index page with keys |

### Limitations

- Does not handle overflow pages (large payloads are skipped)
- Does not validate WITHOUT ROWID tables
- Assumes valid page structure (may panic on severely corrupted data)
- **Index checking is experimental** and may produce false positives due to incomplete key parsing (disabled by default, enable with `--check-indexes`)

## Development

### Running Tests

```bash
cargo test
```

### Project Structure

```
src/
├── main.rs              # CLI entry point
├── lib.rs               # Library with validate() function
├── error.rs             # Error types
├── report.rs            # Human-readable output formatting
├── db/
│   ├── header.rs        # SQLite DB header parsing
│   └── page.rs          # Base page reading
├── wal/
│   ├── header.rs        # WAL header parsing
│   ├── frame.rs         # Frame parsing
│   └── iterator.rs      # CommitIterator
├── btree/
│   ├── page.rs          # B-tree page header
│   ├── cell.rs          # Cell/varint parsing
│   └── scanner.rs       # B-tree traversal
└── validator/
    ├── page_cache.rs    # Page state management
    └── duplicate.rs     # Duplicate detection
```

## License

MIT
