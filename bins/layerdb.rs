use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[path = "layerdb/commands_inspect.rs"]
mod commands_inspect;
#[path = "layerdb/commands_ops.rs"]
mod commands_ops;

#[derive(Debug, Parser)]
#[command(name = "layerdb")]
#[command(about = "LayerDB helper tools", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    ManifestDump {
        #[arg(long)]
        db: PathBuf,
    },
    SstDump {
        #[arg(long)]
        sst: PathBuf,
    },
    DbCheck {
        #[arg(long)]
        db: PathBuf,
    },
    Verify {
        #[arg(long)]
        db: PathBuf,
    },
    Scrub {
        #[arg(long)]
        db: PathBuf,
    },
    Bench {
        #[arg(long)]
        db: PathBuf,
        #[arg(long, default_value_t = 50_000)]
        keys: usize,
        #[arg(long, value_enum, default_value_t = BenchWorkload::Smoke)]
        workload: BenchWorkload,
    },
    CompactRange {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
    },
    CompactAuto {
        #[arg(long)]
        db: PathBuf,
    },
    IngestSst {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        sst: PathBuf,
    },
    RebalanceTiers {
        #[arg(long)]
        db: PathBuf,
    },
    FreezeLevel {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        level: u8,
        #[arg(long)]
        max_files: Option<usize>,
    },
    ThawLevel {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        level: u8,
        #[arg(long)]
        max_files: Option<usize>,
    },
    GcS3 {
        #[arg(long)]
        db: PathBuf,
    },
    GcLocal {
        #[arg(long)]
        db: PathBuf,
    },
    DropBranch {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        name: String,
    },
    CreateBranch {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long, conflicts_with = "from_seqno")]
        from_branch: Option<String>,
        #[arg(long, conflicts_with = "from_branch")]
        from_seqno: Option<u64>,
    },
    Checkout {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        name: String,
    },
    Branches {
        #[arg(long)]
        db: PathBuf,
    },
    FrozenObjects {
        #[arg(long)]
        db: PathBuf,
    },
    ArchiveBranch {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        out: PathBuf,
    },
    ListArchives {
        #[arg(long)]
        db: PathBuf,
    },
    DropArchive {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        archive_id: String,
    },
    GcArchives {
        #[arg(long)]
        db: PathBuf,
    },
    Get {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        branch: Option<String>,
    },
    Scan {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        branch: Option<String>,
    },
    Put {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value_t = true)]
        sync: bool,
    },
    Delete {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value_t = true)]
        sync: bool,
    },
    WriteBatch {
        #[arg(long)]
        db: PathBuf,
        #[arg(long = "op", required = true)]
        ops: Vec<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value_t = true)]
        sync: bool,
    },
    DeleteRange {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value_t = true)]
        sync: bool,
    },
    RetentionFloor {
        #[arg(long)]
        db: PathBuf,
    },
    Metrics {
        #[arg(long)]
        db: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum BenchWorkload {
    Smoke,
    Fill,
    ReadRandom,
    ReadSeq,
    Overwrite,
    DeleteHeavy,
    ScanHeavy,
    Compact,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::ManifestDump { db } => commands_inspect::manifest_dump(&db),
        Command::SstDump { sst } => commands_inspect::sst_dump(&sst),
        Command::DbCheck { db } => commands_inspect::db_check(&db),
        Command::Verify { db } => commands_inspect::verify(&db),
        Command::Scrub { db } => commands_inspect::scrub(&db),
        Command::Bench { db, keys, workload } => commands_ops::bench(&db, keys, workload),
        Command::CompactRange { db, start, end } => {
            commands_ops::compact_range_cmd(&db, start.as_deref(), end.as_deref())
        }
        Command::CompactAuto { db } => commands_ops::compact_auto_cmd(&db),
        Command::IngestSst { db, sst } => commands_ops::ingest_sst_cmd(&db, &sst),
        Command::RebalanceTiers { db } => commands_ops::rebalance_tiers(&db),
        Command::FreezeLevel {
            db,
            level,
            max_files,
        } => commands_ops::freeze_level(&db, level, max_files),
        Command::ThawLevel {
            db,
            level,
            max_files,
        } => commands_ops::thaw_level(&db, level, max_files),
        Command::GcS3 { db } => commands_ops::gc_s3(&db),
        Command::GcLocal { db } => commands_ops::gc_local(&db),
        Command::DropBranch { db, name } => commands_ops::drop_branch(&db, &name),
        Command::CreateBranch {
            db,
            name,
            from_branch,
            from_seqno,
        } => commands_ops::create_branch(&db, &name, from_branch.as_deref(), from_seqno),
        Command::Checkout { db, name } => commands_ops::checkout_branch_cmd(&db, &name),
        Command::Branches { db } => commands_ops::branches(&db),
        Command::FrozenObjects { db } => commands_ops::frozen_objects(&db),
        Command::ArchiveBranch { db, name, out } => {
            commands_ops::archive_branch_cmd(&db, &name, &out)
        }
        Command::ListArchives { db } => commands_ops::list_archives_cmd(&db),
        Command::DropArchive { db, archive_id } => commands_ops::drop_archive_cmd(&db, &archive_id),
        Command::GcArchives { db } => commands_ops::gc_archives_cmd(&db),
        Command::Get { db, key, branch } => commands_ops::get_cmd(&db, &key, branch.as_deref()),
        Command::Scan {
            db,
            start,
            end,
            branch,
        } => commands_ops::scan_cmd(&db, start.as_deref(), end.as_deref(), branch.as_deref()),
        Command::Put {
            db,
            key,
            value,
            branch,
            sync,
        } => commands_ops::put_cmd(&db, &key, &value, branch.as_deref(), sync),
        Command::Delete {
            db,
            key,
            branch,
            sync,
        } => commands_ops::delete_cmd(&db, &key, branch.as_deref(), sync),
        Command::WriteBatch {
            db,
            ops,
            branch,
            sync,
        } => commands_ops::write_batch_cmd(&db, &ops, branch.as_deref(), sync),
        Command::DeleteRange {
            db,
            start,
            end,
            branch,
            sync,
        } => commands_ops::delete_range_cmd(&db, &start, &end, branch.as_deref(), sync),
        Command::RetentionFloor { db } => commands_ops::retention_floor_cmd(&db),
        Command::Metrics { db } => commands_ops::metrics_cmd(&db),
    }
}
