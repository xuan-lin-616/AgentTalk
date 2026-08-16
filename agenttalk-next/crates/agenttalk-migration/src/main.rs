use agenttalk_migration::{dry_run, LegacyExport, MigrationStore};
use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "usage: agenttalk-migrate <export.json> <sqlite-path> [--dry-run] [--report=<path>]"
        );
        std::process::exit(2);
    }
    let export: LegacyExport = serde_json::from_str(&fs::read_to_string(&args[1])?)?;
    let report = if args.iter().any(|arg| arg == "--dry-run") {
        dry_run(&export)?
    } else {
        let mut store = MigrationStore::open(&args[2])?;
        store.apply(&export)?
    };
    if let Some(path) = args.iter().find_map(|arg| arg.strip_prefix("--report=")) {
        fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&report)?),
        )?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
