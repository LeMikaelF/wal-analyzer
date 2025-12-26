//! Validator framework for extensible WAL validation.
//!
//! This module provides the infrastructure for creating and running validators.
//! Each validator is a self-contained module that implements the `Validator` trait.
//!
//! # Adding a new validator
//!
//! 1. Create a new file in `src/validators/` (e.g., `my_validator.rs`)
//! 2. Implement the `Validator` trait
//! 3. Add the module to this file
//! 4. Register it in `default_validators()`
//!
//! # Example
//!
//! ```ignore
//! pub struct MyValidator;
//!
//! impl Validator for MyValidator {
//!     fn name(&self) -> &'static str {
//!         "my-validator"
//!     }
//!
//!     fn validate(&mut self, ctx: &mut ValidationContext) -> Result<Vec<ValidationIssue>> {
//!         // Your validation logic here
//!         Ok(vec![])
//!     }
//! }
//! ```

pub mod duplicate_index_key;
pub mod duplicate_rowid;
pub mod issue;

pub use duplicate_index_key::DuplicateIndexKeyValidator;
pub use duplicate_rowid::DuplicateRowidValidator;
pub use issue::{DuplicateDetails, DuplicateEntry, IssueLocation, Severity, ValidationIssue};

use crate::btree::BTreeScanner;
use crate::error::Result;
use crate::validator::PageCache;

/// Configuration for validators.
#[derive(Debug, Clone, Default)]
pub struct ValidatorConfig {
    /// Whether to check index B-trees for duplicate keys.
    pub check_indexes: bool,
}

/// Context provided to validators during validation.
///
/// Provides access to the current database state (base or after WAL commit).
pub struct ValidationContext<'a> {
    /// Page cache for reading pages (base DB + WAL overlay)
    pub page_cache: &'a mut PageCache,
    /// Current commit index (None = base database state)
    pub commit_index: Option<u64>,
    /// Validator configuration
    pub config: &'a ValidatorConfig,
}

impl<'a> ValidationContext<'a> {
    /// Create a new validation context.
    pub fn new(
        page_cache: &'a mut PageCache,
        commit_index: Option<u64>,
        config: &'a ValidatorConfig,
    ) -> Self {
        Self {
            page_cache,
            commit_index,
            config,
        }
    }

    /// Create a B-tree scanner for this context.
    pub fn scanner(&mut self) -> BTreeScanner<'_> {
        BTreeScanner::new(self.page_cache)
    }
}

/// Trait for implementing validators.
///
/// Each validator is responsible for checking one aspect of the database
/// and reporting any issues found.
pub trait Validator: Send + Sync {
    /// Returns the unique name of this validator.
    fn name(&self) -> &'static str;

    /// Validate the current database state and return any issues found.
    ///
    /// The context provides access to:
    /// - `page_cache`: Read pages from base DB + WAL overlay
    /// - `commit_index`: Current state (None = base DB, Some = after commit)
    /// - `config`: Validator configuration
    fn validate(&mut self, ctx: &mut ValidationContext) -> Result<Vec<ValidationIssue>>;

    /// Returns whether this validator is enabled for the given configuration.
    ///
    /// Default implementation always returns true.
    fn is_enabled(&self, _config: &ValidatorConfig) -> bool {
        true
    }
}

/// Returns the default set of validators.
pub fn default_validators() -> Vec<Box<dyn Validator>> {
    vec![
        Box::new(DuplicateRowidValidator::new()),
        Box::new(DuplicateIndexKeyValidator::new()),
    ]
}

/// Returns all enabled validators for the given configuration.
pub fn enabled_validators(config: &ValidatorConfig) -> Vec<Box<dyn Validator>> {
    default_validators()
        .into_iter()
        .filter(|v| v.is_enabled(config))
        .collect()
}
