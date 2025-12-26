//! Validator for detecting duplicate rowids in table B-trees.

use std::collections::HashMap;
use std::hash::Hash;

use crate::btree::RowidLocation;
use crate::error::Result;

use super::{DuplicateEntry, ValidationContext, ValidationIssue, Validator};

/// Validator that detects duplicate rowids in table B-trees.
///
/// Duplicate rowids indicate B-tree corruption, as each rowid should be unique
/// within a table.
pub struct DuplicateRowidValidator;

impl DuplicateRowidValidator {
    /// Create a new duplicate rowid validator.
    pub fn new() -> Self {
        Self
    }
}

impl Default for DuplicateRowidValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for DuplicateRowidValidator {
    fn name(&self) -> &'static str {
        "duplicate-rowid"
    }

    fn validate(&mut self, ctx: &mut ValidationContext) -> Result<Vec<ValidationIssue>> {
        let mut issues = Vec::new();
        let commit_index = ctx.commit_index;

        // Discover all B-trees
        let mut scanner = ctx.scanner();
        let btrees = scanner.discover_btrees()?;

        // Check each table B-tree
        for btree in btrees {
            if !btree.is_table {
                continue;
            }

            let rowids = scanner.collect_table_rowids(btree.root_page)?;
            let duplicates = find_duplicates(rowids);

            if !duplicates.is_empty() {
                issues.push(ValidationIssue::duplicate_rowids(
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

/// Find duplicate entries in a list of (key, location) pairs.
pub fn find_duplicates<K: Eq + Hash + Clone>(
    entries: Vec<(K, RowidLocation)>,
) -> Vec<DuplicateEntry<K>> {
    // Group by key
    let mut groups: HashMap<K, Vec<RowidLocation>> = HashMap::new();

    for (key, location) in entries {
        groups.entry(key).or_default().push(location);
    }

    // Filter to only those with multiple occurrences
    groups
        .into_iter()
        .filter(|(_, locations)| locations.len() > 1)
        .map(|(key, locations)| DuplicateEntry { key, locations })
        .collect()
}
