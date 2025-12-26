//! Validator for checking index integrity against tables.
//!
//! This validator checks that:
//! 1. All rows in a table are present in its indexes (no missing entries)
//! 2. All entries in an index reference existing rows in the table (no dangling entries)
//!
//! Partial indexes (with WHERE clause) and expression indexes are skipped as they
//! intentionally contain a subset of rows.

use std::collections::HashSet;

use crate::btree::BTreeInfo;
use crate::error::Result;

use super::{IssueLocation, Severity, ValidationContext, ValidationIssue, Validator};

/// Validator that checks index integrity against tables.
pub struct IndexIntegrityValidator;

impl IndexIntegrityValidator {
    /// Create a new index integrity validator.
    pub fn new() -> Self {
        Self
    }

    /// Check if an index should be skipped (partial or expression index).
    fn should_skip_index(index: &BTreeInfo) -> bool {
        if let Some(ref sql) = index.sql {
            let sql_upper = sql.to_uppercase();

            // Skip partial indexes (have WHERE clause)
            if sql_upper.contains(" WHERE ") {
                return true;
            }

            // Skip expression indexes (have parentheses in column list after ON table_name)
            // Expression indexes look like: CREATE INDEX idx ON table ((expr))
            // We detect this by looking for (( pattern or function calls in the index definition
            if sql_upper.contains("((") {
                return true;
            }

            // Also check for common SQL functions that indicate expression indexes
            let expr_patterns = [
                "LOWER(", "UPPER(", "SUBSTR(", "LENGTH(", "ABS(",
                "COALESCE(", "IFNULL(", "NULLIF(", "TYPEOF(",
                "CAST(", "DATE(", "TIME(", "DATETIME(", "JULIANDAY(",
                "JSON_EXTRACT(", "JSON(",
            ];
            for pattern in expr_patterns {
                if sql_upper.contains(pattern) {
                    return true;
                }
            }
        } else {
            // Autoindexes (no SQL) are created by SQLite for PRIMARY KEY / UNIQUE constraints
            // These should be checked, so don't skip
        }

        false
    }
}

impl Default for IndexIntegrityValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for IndexIntegrityValidator {
    fn name(&self) -> &'static str {
        "index-integrity"
    }

    fn validate(&mut self, ctx: &mut ValidationContext) -> Result<Vec<ValidationIssue>> {
        let mut issues = Vec::new();
        let commit_index = ctx.commit_index;

        // Discover all B-trees
        let mut scanner = ctx.scanner();
        let btrees = scanner.discover_btrees()?;

        // Build a map of table name -> root page for quick lookup
        let table_map: std::collections::HashMap<String, u32> = btrees
            .iter()
            .filter(|b| b.is_table)
            .filter_map(|b| b.name.clone().map(|name| (name, b.root_page)))
            .collect();

        // Check each index
        for index in &btrees {
            // Skip tables
            if index.is_table {
                continue;
            }

            // Skip partial and expression indexes
            if Self::should_skip_index(index) {
                continue;
            }

            // Get the table this index belongs to
            let tbl_name = match &index.tbl_name {
                Some(name) => name,
                None => continue, // Can't validate without knowing the table
            };

            // Find the table's root page
            let table_root = match table_map.get(tbl_name) {
                Some(&root) => root,
                None => continue, // Table not found (shouldn't happen in valid DB)
            };

            // Collect rowids from the table
            let table_rowids: HashSet<i64> = scanner
                .collect_table_rowids(table_root)?
                .into_iter()
                .map(|(rowid, _)| rowid)
                .collect();

            // Collect rowids referenced by the index
            let index_rowids: HashSet<i64> = scanner
                .collect_index_rowids(index.root_page)?
                .into_iter()
                .collect();

            // Find missing entries (in table but not in index)
            let missing: Vec<i64> = table_rowids
                .difference(&index_rowids)
                .copied()
                .collect();

            // Find dangling entries (in index but not in table)
            let dangling: Vec<i64> = index_rowids
                .difference(&table_rowids)
                .copied()
                .collect();

            // Report missing entries
            if !missing.is_empty() {
                issues.push(ValidationIssue::new(
                    self.name(),
                    Severity::Error,
                    format!(
                        "Index is missing {} row(s) that exist in table '{}': {:?}",
                        missing.len(),
                        tbl_name,
                        if missing.len() <= 10 {
                            missing.clone()
                        } else {
                            let mut preview = missing[..10].to_vec();
                            preview.push(-1); // Sentinel to indicate truncation
                            preview
                        }
                    ),
                    IssueLocation::Index {
                        name: index.name.clone(),
                        root_page: index.root_page,
                    },
                    commit_index,
                ));
            }

            // Report dangling entries
            if !dangling.is_empty() {
                issues.push(ValidationIssue::new(
                    self.name(),
                    Severity::Error,
                    format!(
                        "Index has {} dangling entry(ies) referencing non-existent rows: {:?}",
                        dangling.len(),
                        if dangling.len() <= 10 {
                            dangling.clone()
                        } else {
                            let mut preview = dangling[..10].to_vec();
                            preview.push(-1); // Sentinel to indicate truncation
                            preview
                        }
                    ),
                    IssueLocation::Index {
                        name: index.name.clone(),
                        root_page: index.root_page,
                    },
                    commit_index,
                ));
            }
        }

        Ok(issues)
    }
}
