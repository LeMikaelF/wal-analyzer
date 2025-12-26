//! Validation issue types for reporting problems found by validators.

use std::fmt;

use crate::btree::{IndexKey, RowidLocation};

/// Severity of a validation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Critical issue that indicates corruption
    Error,
    /// Potential issue that may indicate problems
    Warning,
    /// Informational message
    Info,
}

/// Location where an issue was found.
#[derive(Debug, Clone)]
pub enum IssueLocation {
    /// Issue in a table B-tree
    Table {
        name: Option<String>,
        root_page: u32,
    },
    /// Issue in an index B-tree
    Index {
        name: Option<String>,
        root_page: u32,
    },
    /// Issue at a specific page
    Page { page_number: u32 },
    /// Global database issue
    Database,
}

impl fmt::Display for IssueLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IssueLocation::Table { name, root_page } => {
                let name_str = name.as_deref().unwrap_or("<unknown>");
                write!(f, "table {} (root page {})", name_str, root_page)
            }
            IssueLocation::Index { name, root_page } => {
                let name_str = name.as_deref().unwrap_or("<unknown>");
                write!(f, "index {} (root page {})", name_str, root_page)
            }
            IssueLocation::Page { page_number } => {
                write!(f, "page {}", page_number)
            }
            IssueLocation::Database => {
                write!(f, "database")
            }
        }
    }
}

/// A single duplicate entry (one rowid/key that appears multiple times).
#[derive(Debug, Clone)]
pub struct DuplicateEntry<K> {
    /// The duplicated key or rowid
    pub key: K,
    /// All locations where this key appears
    pub locations: Vec<RowidLocation>,
}

impl<K> DuplicateEntry<K> {
    /// Returns true if all occurrences are on the same page (intra-page duplicate).
    pub fn is_intra_page(&self) -> bool {
        if self.locations.len() < 2 {
            return false;
        }
        let first_page = self.locations[0].page_number;
        self.locations.iter().all(|loc| loc.page_number == first_page)
    }
}

/// Details about a duplicate issue.
#[derive(Debug, Clone)]
pub enum DuplicateDetails {
    /// Duplicate rowids in a table
    Rowid(Vec<DuplicateEntry<i64>>),
    /// Duplicate keys in an index
    IndexKey(Vec<DuplicateEntry<IndexKey>>),
}

/// A validation issue found by a validator.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    /// Name of the validator that found this issue
    pub validator: &'static str,
    /// Severity of the issue
    pub severity: Severity,
    /// Human-readable description
    pub message: String,
    /// Location where the issue was found
    pub location: IssueLocation,
    /// Commit index where detected (None = base database)
    pub commit_index: Option<u64>,
    /// Additional details for duplicate issues
    pub duplicate_details: Option<DuplicateDetails>,
}

impl ValidationIssue {
    /// Create a new validation issue.
    pub fn new(
        validator: &'static str,
        severity: Severity,
        message: impl Into<String>,
        location: IssueLocation,
        commit_index: Option<u64>,
    ) -> Self {
        Self {
            validator,
            severity,
            message: message.into(),
            location,
            commit_index,
            duplicate_details: None,
        }
    }

    /// Create a new issue for duplicate rowids.
    pub fn duplicate_rowids(
        validator: &'static str,
        name: Option<String>,
        root_page: u32,
        commit_index: Option<u64>,
        duplicates: Vec<DuplicateEntry<i64>>,
    ) -> Self {
        let count = duplicates.len();
        Self {
            validator,
            severity: Severity::Error,
            message: format!("Found {} duplicate rowid(s)", count),
            location: IssueLocation::Table { name, root_page },
            commit_index,
            duplicate_details: Some(DuplicateDetails::Rowid(duplicates)),
        }
    }

    /// Create a new issue for duplicate index keys.
    pub fn duplicate_index_keys(
        validator: &'static str,
        name: Option<String>,
        root_page: u32,
        commit_index: Option<u64>,
        duplicates: Vec<DuplicateEntry<IndexKey>>,
    ) -> Self {
        let count = duplicates.len();
        Self {
            validator,
            severity: Severity::Error,
            message: format!("Found {} duplicate key(s)", count),
            location: IssueLocation::Index { name, root_page },
            commit_index,
            duplicate_details: Some(DuplicateDetails::IndexKey(duplicates)),
        }
    }

    /// Returns true if this is a duplicate issue.
    pub fn is_duplicate(&self) -> bool {
        self.duplicate_details.is_some()
    }

    /// Returns the total count of duplicates (for duplicate issues).
    pub fn duplicate_count(&self) -> usize {
        match &self.duplicate_details {
            Some(DuplicateDetails::Rowid(dups)) => dups.len(),
            Some(DuplicateDetails::IndexKey(dups)) => dups.len(),
            None => 0,
        }
    }
}
