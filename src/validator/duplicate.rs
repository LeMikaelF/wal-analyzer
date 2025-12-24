use std::collections::HashMap;
use std::hash::Hash;

use crate::btree::{IndexKey, RowidLocation};

/// Type of duplicate (table rowid or index key)
#[derive(Debug, Clone)]
pub enum DuplicateType {
    /// Duplicate rowid in a table B-tree
    TableRowid,
    /// Duplicate key in an index B-tree
    IndexKey,
}

/// A single duplicate entry (one rowid/key that appears multiple times)
#[derive(Debug, Clone)]
pub struct DuplicateEntry<K> {
    /// The duplicated key or rowid
    pub key: K,
    /// All locations where this key appears
    pub locations: Vec<RowidLocation>,
}

impl<K> DuplicateEntry<K> {
    /// Returns true if all occurrences are on the same page (intra-page duplicate)
    pub fn is_intra_page(&self) -> bool {
        if self.locations.len() < 2 {
            return false;
        }
        let first_page = self.locations[0].page_number;
        self.locations.iter().all(|loc| loc.page_number == first_page)
    }
}

/// Report of duplicates found in a single state (base DB or after a commit)
#[derive(Debug)]
pub struct DuplicateReport {
    /// Commit index (None for base database state)
    pub commit_index: Option<u64>,
    /// Root page of the B-tree
    pub btree_root: u32,
    /// Name of the table or index (if known)
    pub name: Option<String>,
    /// Type of duplicate
    pub duplicate_type: DuplicateType,
    /// Duplicate rowids (for tables)
    pub rowid_duplicates: Vec<DuplicateEntry<i64>>,
    /// Duplicate keys (for indexes)
    pub key_duplicates: Vec<DuplicateEntry<IndexKey>>,
}

impl DuplicateReport {
    /// Returns true if this report contains any duplicates
    pub fn has_duplicates(&self) -> bool {
        !self.rowid_duplicates.is_empty() || !self.key_duplicates.is_empty()
    }

    /// Total number of duplicate entries
    pub fn duplicate_count(&self) -> usize {
        self.rowid_duplicates.len() + self.key_duplicates.len()
    }
}

/// Find duplicate entries in a list of (key, location) pairs
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
