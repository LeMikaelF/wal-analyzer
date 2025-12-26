//! Validator for detecting duplicate keys in index B-trees.

use crate::error::Result;

use super::duplicate_rowid::find_duplicates;
use super::{ValidationContext, ValidationIssue, Validator};

/// Validator that detects duplicate keys in unique index B-trees.
///
/// Duplicate keys in a unique index indicate corruption, as each key should
/// appear only once.
///
/// Note: This validator only checks unique indexes. Duplicate keys are valid
/// in non-unique indexes.
pub struct DuplicateIndexKeyValidator;

impl DuplicateIndexKeyValidator {
    /// Create a new duplicate index key validator.
    pub fn new() -> Self {
        Self
    }
}

impl Default for DuplicateIndexKeyValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for DuplicateIndexKeyValidator {
    fn name(&self) -> &'static str {
        "duplicate-index-key"
    }

    fn validate(&mut self, ctx: &mut ValidationContext) -> Result<Vec<ValidationIssue>> {
        let mut issues = Vec::new();
        let commit_index = ctx.commit_index;

        // Discover all B-trees
        let mut scanner = ctx.scanner();
        let btrees = scanner.discover_btrees()?;

        // Check each index B-tree
        for btree in btrees {
            // Skip tables
            if btree.is_table {
                continue;
            }

            // Skip non-unique indexes (duplicate keys are valid in non-unique indexes)
            if !btree.is_unique {
                continue;
            }

            let keys = scanner.collect_index_keys(btree.root_page)?;
            let duplicates = find_duplicates(keys);

            if !duplicates.is_empty() {
                issues.push(ValidationIssue::duplicate_index_keys(
                    self.name(),
                    btree.name.clone(),
                    btree.root_page,
                    commit_index,
                    duplicates,
                ));
            }
        }

        Ok(issues)
    }
}
