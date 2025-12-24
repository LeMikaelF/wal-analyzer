pub mod btree;
pub mod db;
pub mod error;
pub mod report;
pub mod validator;
pub mod wal;

use std::path::Path;

use crate::btree::BTreeScanner;
use crate::db::DbHeader;
use crate::error::{Result, WalValidatorError};
use crate::validator::{
    find_duplicates, DuplicateReport, DuplicateType, PageCache,
};
use crate::wal::CommitIterator;

/// Validate a SQLite database and WAL file for duplicate rowids/keys
pub fn validate(db_path: &Path, wal_path: &Path) -> Result<(Vec<DuplicateReport>, u64)> {
    // Verify files exist
    if !db_path.exists() {
        return Err(WalValidatorError::DatabaseNotFound(db_path.to_path_buf()));
    }
    if !wal_path.exists() {
        return Err(WalValidatorError::WalNotFound(wal_path.to_path_buf()));
    }

    // Parse database header
    let db_header = DbHeader::from_file(db_path)?;

    // Initialize page cache
    let mut page_cache = PageCache::new(db_path, db_header.page_size, db_header.page_count);

    let mut all_reports = Vec::new();
    let mut total_commits = 0u64;

    // Check base database state first
    {
        let mut scanner = BTreeScanner::new(&mut page_cache);
        let btrees = scanner.discover_btrees()?;

        for btree in btrees {
            let report = if btree.is_table {
                scan_table_for_duplicates(&mut scanner, &btree, None)?
            } else {
                scan_index_for_duplicates(&mut scanner, &btree, None)?
            };

            if report.has_duplicates() {
                all_reports.push(report);
            }
        }
    }

    // Open WAL and iterate through commits
    let commit_iter = CommitIterator::new(wal_path)?;

    // Verify page sizes match
    if commit_iter.wal_header().page_size != db_header.page_size {
        return Err(WalValidatorError::PageSizeMismatch {
            db_size: db_header.page_size,
            wal_size: commit_iter.wal_header().page_size,
        });
    }

    for commit_result in commit_iter {
        let commit = commit_result?;
        total_commits += 1;

        // Apply commit to page cache
        page_cache.apply_commit(&commit);

        // Re-scan for duplicates
        let mut scanner = BTreeScanner::new(&mut page_cache);
        let btrees = scanner.discover_btrees()?;

        for btree in btrees {
            let report = if btree.is_table {
                scan_table_for_duplicates(&mut scanner, &btree, Some(commit.index))?
            } else {
                scan_index_for_duplicates(&mut scanner, &btree, Some(commit.index))?
            };

            if report.has_duplicates() {
                all_reports.push(report);
            }
        }
    }

    Ok((all_reports, total_commits))
}

fn scan_table_for_duplicates(
    scanner: &mut BTreeScanner,
    btree: &btree::BTreeInfo,
    commit_index: Option<u64>,
) -> Result<DuplicateReport> {
    let rowids = scanner.collect_table_rowids(btree.root_page)?;
    let duplicates = find_duplicates(rowids);

    Ok(DuplicateReport {
        commit_index,
        btree_root: btree.root_page,
        name: btree.name.clone(),
        duplicate_type: DuplicateType::TableRowid,
        rowid_duplicates: duplicates,
        key_duplicates: Vec::new(),
    })
}

fn scan_index_for_duplicates(
    scanner: &mut BTreeScanner,
    btree: &btree::BTreeInfo,
    commit_index: Option<u64>,
) -> Result<DuplicateReport> {
    let keys = scanner.collect_index_keys(btree.root_page)?;
    let duplicates = find_duplicates(keys);

    Ok(DuplicateReport {
        commit_index,
        btree_root: btree.root_page,
        name: btree.name.clone(),
        duplicate_type: DuplicateType::IndexKey,
        rowid_duplicates: Vec::new(),
        key_duplicates: duplicates,
    })
}
