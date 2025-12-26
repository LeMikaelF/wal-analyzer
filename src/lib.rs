pub mod btree;
pub mod db;
pub mod error;
pub mod report;
pub mod validator;
pub mod validators;

use std::path::Path;

use crate::db::DbHeader;
use crate::error::{Result, WalValidatorError};
use crate::validator::PageCache;
use crate::validators::{
    enabled_validators, ValidationContext, ValidationIssue, ValidatorConfig,
};
use crate::wal::CommitIterator;

pub mod wal;

/// Validate a SQLite database and WAL file.
///
/// Runs all enabled validators against the base database state and each
/// WAL commit, returning any issues found.
///
/// # Arguments
///
/// * `db_path` - Path to the SQLite database file
/// * `wal_path` - Path to the WAL file
/// * `config` - Validator configuration
///
/// # Returns
///
/// A tuple of (issues found, total commits processed).
pub fn validate(
    db_path: &Path,
    wal_path: &Path,
    config: &ValidatorConfig,
) -> Result<(Vec<ValidationIssue>, u64)> {
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

    // Get enabled validators
    let mut validators = enabled_validators(config);

    let mut all_issues = Vec::new();
    let mut total_commits = 0u64;

    // Check base database state first
    {
        let mut ctx = ValidationContext::new(&mut page_cache, None, config);
        for validator in &mut validators {
            let issues = validator.validate(&mut ctx)?;
            all_issues.extend(issues);
        }
    }

    // Open WAL and iterate through commits
    if let Some(commit_iter) = CommitIterator::new(wal_path)? {
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

            // Run all validators
            let mut ctx = ValidationContext::new(&mut page_cache, Some(commit.index), config);
            for validator in &mut validators {
                let issues = validator.validate(&mut ctx)?;
                all_issues.extend(issues);
            }
        }
    }

    Ok((all_issues, total_commits))
}
