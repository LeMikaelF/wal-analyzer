use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use wal_validator::db::DbHeader;
use wal_validator::report::{print_duplicate_report, print_header, print_summary};

#[derive(Parser, Debug)]
#[command(name = "wal-validator")]
#[command(about = "Validates SQLite WAL files for duplicate rowids and index keys")]
#[command(version)]
struct Cli {
    /// Path to the SQLite database file (.db)
    #[arg(short, long)]
    database: PathBuf,

    /// Path to the WAL file (defaults to <database>-wal)
    #[arg(short, long)]
    wal: Option<PathBuf>,

    /// Also check index B-trees for duplicate keys (experimental, may have false positives)
    #[arg(long, default_value = "false")]
    check_indexes: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Determine WAL path
    let wal_path = cli.wal.unwrap_or_else(|| {
        let mut wal = cli.database.clone();
        let filename = wal
            .file_name()
            .map(|f| format!("{}-wal", f.to_string_lossy()))
            .unwrap_or_else(|| "database-wal".to_string());
        wal.set_file_name(filename);
        wal
    });

    // Validate database exists
    if !cli.database.exists() {
        eprintln!("Error: Database file not found: {}", cli.database.display());
        return ExitCode::FAILURE;
    }

    // Validate WAL exists
    if !wal_path.exists() {
        eprintln!("Error: WAL file not found: {}", wal_path.display());
        return ExitCode::FAILURE;
    }

    // Get page size for header
    let page_size = match DbHeader::from_file(&cli.database) {
        Ok(header) => header.page_size,
        Err(e) => {
            eprintln!("Error reading database header: {}", e);
            return ExitCode::FAILURE;
        }
    };

    // Print header
    print_header(&cli.database, &wal_path, page_size);

    // Run validation
    match wal_validator::validate(&cli.database, &wal_path, cli.check_indexes) {
        Ok((reports, total_commits)) => {
            // Print each duplicate report
            for report in &reports {
                print_duplicate_report(report);
            }

            // Print summary
            print_summary(&reports, total_commits);

            // Exit with error code if duplicates were found
            if reports.is_empty() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2) // Duplicates found
            }
        }
        Err(e) => {
            eprintln!("Error during validation: {}", e);
            ExitCode::FAILURE
        }
    }
}
