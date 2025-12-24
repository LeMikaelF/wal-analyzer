use colored::Colorize;
use std::path::Path;

use crate::btree::IndexKey;
use crate::validator::{DuplicateEntry, DuplicateReport, DuplicateType};

/// Print the report header
pub fn print_header(db_path: &Path, wal_path: &Path, page_size: u32) {
    println!("{}", "=".repeat(80));
    println!("{}", "SQLite WAL Validator Report".bold());
    println!("{}", "=".repeat(80));
    println!("Database: {}", db_path.display());
    println!("WAL File: {}", wal_path.display());
    println!("Page Size: {} bytes", page_size);
    println!();
}

/// Print a duplicate report
pub fn print_duplicate_report(report: &DuplicateReport) {
    println!("{}", "-".repeat(80));

    let location_str = match report.commit_index {
        Some(idx) => format!("Commit #{}", idx),
        None => "Base Database State".to_string(),
    };

    println!(
        "{} in {}",
        "DUPLICATE FOUND".red().bold(),
        location_str.yellow()
    );
    println!("{}", "-".repeat(80));

    let type_str = match report.duplicate_type {
        DuplicateType::TableRowid => "Table",
        DuplicateType::IndexKey => "Index",
    };

    let name = report
        .name
        .as_deref()
        .unwrap_or("<unknown>");

    println!(
        "{}: {} (root page {})",
        type_str,
        name.cyan(),
        report.btree_root
    );
    println!();

    // Print rowid duplicates
    for dup in &report.rowid_duplicates {
        print_rowid_duplicate(dup);
    }

    // Print key duplicates
    for dup in &report.key_duplicates {
        print_key_duplicate(dup);
    }
}

fn format_location(loc: &crate::btree::RowidLocation, is_intra_page_last: bool) -> String {
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

/// Print the summary footer
pub fn print_summary(reports: &[DuplicateReport], total_commits: u64) {
    println!("{}", "=".repeat(80));

    let total_duplicates: usize = reports.iter().map(|r| r.duplicate_count()).sum();
    let base_duplicates = reports
        .iter()
        .filter(|r| r.commit_index.is_none())
        .map(|r| r.duplicate_count())
        .sum::<usize>();
    let wal_duplicates = total_duplicates - base_duplicates;

    if total_duplicates == 0 {
        println!(
            "{}",
            "No duplicates found - database appears valid!".green().bold()
        );
    } else {
        println!(
            "{}: {} duplicate(s) found",
            "Summary".bold(),
            total_duplicates.to_string().red()
        );

        if base_duplicates > 0 {
            println!("  - {} in base database", base_duplicates);
        }
        if wal_duplicates > 0 {
            println!("  - {} in WAL commits", wal_duplicates);
        }
    }

    println!("Total commits processed: {}", total_commits);
    println!("{}", "=".repeat(80));
}
