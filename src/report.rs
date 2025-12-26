use colored::Colorize;
use std::path::Path;

use crate::btree::{IndexKey, RowidLocation};
use crate::validators::{
    DuplicateDetails, DuplicateEntry, IssueLocation, Severity, ValidationIssue,
};

/// Print the report header.
pub fn print_header(db_path: &Path, wal_path: &Path, page_size: u32) {
    println!("{}", "=".repeat(80));
    println!("{}", "SQLite WAL Validator Report".bold());
    println!("{}", "=".repeat(80));
    println!("Database: {}", db_path.display());
    println!("WAL File: {}", wal_path.display());
    println!("Page Size: {} bytes", page_size);
    println!();
}

/// Print a validation issue.
pub fn print_issue(issue: &ValidationIssue) {
    println!("{}", "-".repeat(80));

    let location_str = match issue.commit_index {
        Some(idx) => format!("Commit #{}", idx),
        None => "Base Database State".to_string(),
    };

    let severity_str = match issue.severity {
        Severity::Error => "ERROR".red().bold(),
        Severity::Warning => "WARNING".yellow().bold(),
        Severity::Info => "INFO".blue().bold(),
    };

    println!("{} in {}", severity_str, location_str.yellow());
    println!("{}", "-".repeat(80));

    // Print location info
    match &issue.location {
        IssueLocation::Table { name, root_page } => {
            let name_str = name.as_deref().unwrap_or("<unknown>");
            println!("Table: {} (root page {})", name_str.cyan(), root_page);
        }
        IssueLocation::Index { name, root_page } => {
            let name_str = name.as_deref().unwrap_or("<unknown>");
            println!("Index: {} (root page {})", name_str.cyan(), root_page);
        }
        IssueLocation::Page { page_number } => {
            println!("Page: {}", page_number);
        }
        IssueLocation::Database => {
            println!("Location: Database-wide");
        }
    }

    println!("Validator: {}", issue.validator);

    // Print the issue message
    if !issue.message.is_empty() {
        println!("Message: {}", issue.message);
    }
    println!();

    // Print duplicate details if present
    if let Some(details) = &issue.duplicate_details {
        match details {
            DuplicateDetails::Rowid(dups) => {
                for dup in dups {
                    print_rowid_duplicate(dup);
                }
            }
            DuplicateDetails::IndexKey(dups) => {
                for dup in dups {
                    print_key_duplicate(dup);
                }
            }
        }
    }
}

fn format_location(loc: &RowidLocation, is_intra_page_last: bool) -> String {
    let frame_str = match loc.frame_index {
        Some(idx) => format!(" (frame {})", idx),
        None => " (base db)".to_string(),
    };
    let intra_page = if is_intra_page_last {
        "  [Intra-page]".yellow().to_string()
    } else {
        String::new()
    };
    format!(
        "    - Page {}, Cell {}{}{}",
        loc.page_number, loc.cell_index, frame_str, intra_page
    )
}

fn print_rowid_duplicate(dup: &DuplicateEntry<i64>) {
    println!("  Rowid {}:", format!("{}", dup.key).green());

    for loc in &dup.locations {
        let is_last = loc == dup.locations.last().unwrap();
        let is_intra_page_last = dup.is_intra_page() && is_last;
        println!("{}", format_location(loc, is_intra_page_last));
    }
    println!();
}

fn print_key_duplicate(dup: &DuplicateEntry<IndexKey>) {
    println!("  Key {}:", format!("{}", dup.key).green());

    for loc in &dup.locations {
        let is_last = loc == dup.locations.last().unwrap();
        let is_intra_page_last = dup.is_intra_page() && is_last;
        println!("{}", format_location(loc, is_intra_page_last));
    }
    println!();
}

/// Print the summary footer.
pub fn print_summary(issues: &[ValidationIssue], total_commits: u64) {
    println!("{}", "=".repeat(80));

    let total_issues = issues.len();
    let base_issues = issues
        .iter()
        .filter(|i| i.commit_index.is_none())
        .count();
    let wal_issues = total_issues - base_issues;

    if total_issues == 0 {
        println!(
            "{}",
            "No issues found - database appears valid!".green().bold()
        );
    } else {
        println!(
            "{}: {} issue(s) found",
            "Summary".bold(),
            total_issues.to_string().red()
        );

        if base_issues > 0 {
            println!("  - {} in base database", base_issues);
        }
        if wal_issues > 0 {
            println!("  - {} in WAL commits", wal_issues);
        }
    }

    println!("Total commits processed: {}", total_commits);
    println!("{}", "=".repeat(80));
}
