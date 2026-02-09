use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use layerdb::internal_key::{InternalKey, KeyKind};
use rayon::prelude::*;
use serde::Deserialize;

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
        Command::ManifestDump { db } => manifest_dump(&db),
        Command::SstDump { sst } => sst_dump(&sst),
        Command::DbCheck { db } => db_check(&db),
        Command::Verify { db } => verify(&db),
        Command::Scrub { db } => scrub(&db),
        Command::Bench { db, keys, workload } => bench(&db, keys, workload),
        Command::CompactRange { db, start, end } => {
            compact_range_cmd(&db, start.as_deref(), end.as_deref())
        }
        Command::CompactAuto { db } => compact_auto_cmd(&db),
        Command::IngestSst { db, sst } => ingest_sst_cmd(&db, &sst),
        Command::RebalanceTiers { db } => rebalance_tiers(&db),
        Command::FreezeLevel {
            db,
            level,
            max_files,
        } => freeze_level(&db, level, max_files),
        Command::ThawLevel {
            db,
            level,
            max_files,
        } => thaw_level(&db, level, max_files),
        Command::GcS3 { db } => gc_s3(&db),
        Command::GcLocal { db } => gc_local(&db),
        Command::DropBranch { db, name } => drop_branch(&db, &name),
        Command::CreateBranch {
            db,
            name,
            from_branch,
            from_seqno,
        } => create_branch(&db, &name, from_branch.as_deref(), from_seqno),
        Command::Checkout { db, name } => checkout_branch_cmd(&db, &name),
        Command::Branches { db } => branches(&db),
        Command::FrozenObjects { db } => frozen_objects(&db),
        Command::ArchiveBranch { db, name, out } => archive_branch_cmd(&db, &name, &out),
        Command::ListArchives { db } => list_archives_cmd(&db),
        Command::DropArchive { db, archive_id } => drop_archive_cmd(&db, &archive_id),
        Command::GcArchives { db } => gc_archives_cmd(&db),
        Command::Get { db, key, branch } => get_cmd(&db, &key, branch.as_deref()),
        Command::Scan {
            db,
            start,
            end,
            branch,
        } => scan_cmd(&db, start.as_deref(), end.as_deref(), branch.as_deref()),
        Command::Put {
            db,
            key,
            value,
            branch,
            sync,
        } => put_cmd(&db, &key, &value, branch.as_deref(), sync),
        Command::Delete {
            db,
            key,
            branch,
            sync,
        } => delete_cmd(&db, &key, branch.as_deref(), sync),
        Command::WriteBatch {
            db,
            ops,
            branch,
            sync,
        } => write_batch_cmd(&db, &ops, branch.as_deref(), sync),
        Command::DeleteRange {
            db,
            start,
            end,
            branch,
            sync,
        } => delete_range_cmd(&db, &start, &end, branch.as_deref(), sync),
        Command::RetentionFloor { db } => retention_floor_cmd(&db),
        Command::Metrics { db } => metrics_cmd(&db),
    }
}

fn list_archives_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    for archive in db.list_archives() {
        println!(
            "archive id={} branch={} seqno={} path={}",
            archive.archive_id,
            archive.branch,
            archive.seqno,
            archive.archive_path
        );
    }
    Ok(())
}

fn drop_archive_cmd(db: &Path, archive_id: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.drop_archive(archive_id)?;
    println!("drop_archive id={archive_id}");
    Ok(())
}

fn gc_archives_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_archives()?;
    println!("gc_archives removed={removed}");
    Ok(())
}

fn manifest_dump(db: &Path) -> anyhow::Result<()> {
    let data = std::fs::read(db.join("MANIFEST"))?;
    let mut offset = 0usize;
    let mut idx = 0usize;
    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > data.len() {
            break;
        }
        let rec: layerdb::version::manifest::ManifestRecord =
            bincode::deserialize(&data[offset..(offset + len)])?;
        println!("#{idx}: {rec:?}");
        idx += 1;
        offset += len;
    }
    Ok(())
}

fn sst_dump(sst: &Path) -> anyhow::Result<()> {
    let reader = layerdb::sst::SstReader::open(sst)?;
    println!("file: {}", sst.display());
    println!("props: {:?}", reader.properties());

    let mut iter = reader.iter(u64::MAX)?;
    iter.seek_to_first();
    let mut count = 0u64;
    let mut first = None;
    let mut last = None;
    while let Some(next) = iter.next() {
        let (k, seq, kind, v) = next?;
        if first.is_none() {
            first = Some(k.clone());
        }
        last = Some(k.clone());
        count += 1;
        if count <= 16 {
            println!(
                "entry[{count:04}] key={:?} seq={} kind={:?} value_len={}",
                String::from_utf8_lossy(&k),
                seq,
                kind,
                v.len()
            );
        }
    }
    println!("entries={count} first={first:?} last={last:?}");
    Ok(())
}

fn db_check(db: &Path) -> anyhow::Result<()> {
    let manifest_path = db.join("MANIFEST");
    if !manifest_path.exists() {
        anyhow::bail!("missing MANIFEST: {}", manifest_path.display());
    }

    let records = manifest_files(db)?;
    let results: Vec<anyhow::Result<(u8, u64)>> = records
        .par_iter()
        .map(|(_, add, freeze)| {
            let path = resolve_sst_for_check(db, add, freeze.as_ref())?;
            if !path.exists() {
                anyhow::bail!("missing referenced sst: {}", path.display());
            }

            let reader = layerdb::sst::SstReader::open(&path)
                .with_context(|| format!("open sst {}", path.display()))?;
            let props = reader.properties();

            if props.table_root != add.table_root {
                anyhow::bail!(
                    "table root mismatch file_id={} path={} manifest={:?} file={:?}",
                    add.file_id,
                    path.display(),
                    add.table_root,
                    props.table_root
                );
            }
            if props.format_version != add.sst_format_version {
                anyhow::bail!(
                    "format version mismatch file_id={} path={} manifest={} file={}",
                    add.file_id,
                    path.display(),
                    add.sst_format_version,
                    props.format_version
                );
            }
            if props.smallest_user_key != add.smallest_user_key
                || props.largest_user_key != add.largest_user_key
            {
                anyhow::bail!(
                    "key range mismatch file_id={} path={}",
                    add.file_id,
                    path.display()
                );
            }
            if props.max_seqno != add.max_seqno {
                anyhow::bail!(
                    "max_seqno mismatch file_id={} path={} manifest={} file={}",
                    add.file_id,
                    path.display(),
                    add.max_seqno,
                    props.max_seqno
                );
            }

            let mut iter = reader.iter(u64::MAX)?;
            iter.seek_to_first();
            let mut entries = 0u64;
            while let Some(next) = iter.next() {
                let _ = next?;
                entries += 1;
            }
            Ok((add.level, entries))
        })
        .collect();

    let mut totals: BTreeMap<u8, u64> = BTreeMap::new();
    for result in results {
        match result {
            Ok((level, entries)) => {
                *totals.entry(level).or_default() += entries;
            }
            Err(err) => {
                eprintln!("db_check error: {err:#}");
                anyhow::bail!("db_check failed");
            }
        }
    }

    for (level, entries) in totals {
        println!("db_check: level={level} entries={entries}");
    }

    let db_handle = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let archives = db_handle.list_archives();
    for archive in &archives {
        let path = Path::new(&archive.archive_path);
        if !path.exists() {
            anyhow::bail!(
                "missing referenced archive: id={} branch={} path={}",
                archive.archive_id,
                archive.branch,
                path.display()
            );
        }
    }
    println!("db_check: archives={}", archives.len());

    println!("db_check ok: {} files referenced", records.len());
    Ok(())
}

fn verify(db: &Path) -> anyhow::Result<()> {
    db_check(db)
}

fn scrub(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let report = db.scrub_integrity()?;
    for (level, n) in report.entries_by_level {
        println!("scrub: level={level} entries={n}");
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct S3SuperblockMeta {
    id: u32,
    len: u32,
    hash: [u8; 32],
}

#[derive(Debug, Clone, Deserialize)]
struct S3ObjectMetaFile {
    file_id: u64,
    level: u8,
    superblock_bytes: u32,
    superblocks: Vec<S3SuperblockMeta>,
}

fn bench(db: &Path, keys: usize, workload: BenchWorkload) -> anyhow::Result<()> {
    let options = layerdb::DbOptions {
        fsync_writes: false,
        memtable_bytes: 8 * 1024 * 1024,
        wal_segment_bytes: 8 * 1024 * 1024,
        ..Default::default()
    };
    let db = layerdb::Db::open(db, options)?;

    let key = |i: usize| format!("k{:08}", i);
    let value = |i: usize| format!("v{:08}", i);
    let workload_name = match workload {
        BenchWorkload::Smoke => "smoke",
        BenchWorkload::Fill => "fill",
        BenchWorkload::ReadRandom => "readrandom",
        BenchWorkload::ReadSeq => "readseq",
        BenchWorkload::Overwrite => "overwrite",
        BenchWorkload::DeleteHeavy => "delete-heavy",
        BenchWorkload::ScanHeavy => "scan-heavy",
        BenchWorkload::Compact => "compact",
    };

    let pseudo_random = |i: usize| -> usize {
        if keys == 0 {
            return 0;
        }
        (((i as u64)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407))
            % keys as u64) as usize
    };

    let fill = |db: &layerdb::Db| -> anyhow::Result<()> {
        for i in 0..keys {
            db.put(key(i), value(i), layerdb::WriteOptions { sync: false })?;
        }
        Ok(())
    };

    let elapsed = match workload {
        BenchWorkload::Smoke => {
            let start = std::time::Instant::now();
            fill(&db)?;
            let write_elapsed = start.elapsed();

            let start = std::time::Instant::now();
            for i in 0..keys {
                let _ = db.get(key(i).as_bytes(), layerdb::ReadOptions::default())?;
            }
            let read_elapsed = start.elapsed();

            let write_qps = keys as f64 / write_elapsed.as_secs_f64();
            let read_qps = keys as f64 / read_elapsed.as_secs_f64();
            println!("bench workload={workload_name} keys={keys}");
            println!("write elapsed={write_elapsed:?} qps={write_qps:.0}");
            println!("read elapsed={read_elapsed:?} qps={read_qps:.0}");
            return Ok(());
        }
        BenchWorkload::Fill => {
            let start = std::time::Instant::now();
            fill(&db)?;
            start.elapsed()
        }
        BenchWorkload::ReadRandom => {
            fill(&db)?;
            let start = std::time::Instant::now();
            for i in 0..keys {
                let idx = pseudo_random(i);
                let _ = db.get(key(idx).as_bytes(), layerdb::ReadOptions::default())?;
            }
            start.elapsed()
        }
        BenchWorkload::ReadSeq => {
            fill(&db)?;
            let start = std::time::Instant::now();
            let mut iter = db.iter(
                layerdb::Range {
                    start: Bound::Included(bytes::Bytes::from_static(b"k")),
                    end: Bound::Unbounded,
                },
                layerdb::ReadOptions::default(),
            )?;
            iter.seek_to_first();
            while let Some(next) = iter.next() {
                let _ = next?;
            }
            start.elapsed()
        }
        BenchWorkload::Overwrite => {
            fill(&db)?;
            let start = std::time::Instant::now();
            for i in 0..keys {
                db.put(
                    key(i),
                    value(i + keys),
                    layerdb::WriteOptions { sync: false },
                )?;
            }
            start.elapsed()
        }
        BenchWorkload::DeleteHeavy => {
            fill(&db)?;
            let start = std::time::Instant::now();
            for i in 0..keys {
                let idx = pseudo_random(i);
                db.delete(key(idx), layerdb::WriteOptions { sync: false })?;
            }
            start.elapsed()
        }
        BenchWorkload::ScanHeavy => {
            fill(&db)?;
            let start = std::time::Instant::now();
            let scans = 10usize.max((keys / 10).min(100));
            for scan in 0..scans {
                let window = (keys / scans).max(1);
                let begin = scan * window;
                let end = ((scan + 1) * window).min(keys);

                let mut iter = db.iter(
                    layerdb::Range {
                        start: Bound::Included(bytes::Bytes::from(key(begin))),
                        end: Bound::Excluded(bytes::Bytes::from(key(end))),
                    },
                    layerdb::ReadOptions::default(),
                )?;
                iter.seek_to_first();
                while let Some(next) = iter.next() {
                    let _ = next?;
                }
            }
            start.elapsed()
        }
        BenchWorkload::Compact => {
            fill(&db)?;
            let start = std::time::Instant::now();
            db.compact_range(None)?;
            start.elapsed()
        }
    };

    let qps = if elapsed.is_zero() {
        f64::INFINITY
    } else {
        keys as f64 / elapsed.as_secs_f64()
    };
    println!("bench workload={workload_name} keys={keys}");
    println!("elapsed={elapsed:?} qps={qps:.0}");
    Ok(())
}

fn compact_range_cmd(db: &Path, start: Option<&str>, end: Option<&str>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;

    if let (Some(s), Some(e)) = (start, end) {
        if s.as_bytes() >= e.as_bytes() {
            anyhow::bail!("invalid range: start must be < end");
        }
    }

    let range = match (start, end) {
        (None, None) => None,
        (Some(s), None) => Some(layerdb::Range {
            start: Bound::Included(bytes::Bytes::copy_from_slice(s.as_bytes())),
            end: Bound::Unbounded,
        }),
        (None, Some(e)) => Some(layerdb::Range {
            start: Bound::Unbounded,
            end: Bound::Excluded(bytes::Bytes::copy_from_slice(e.as_bytes())),
        }),
        (Some(s), Some(e)) => Some(layerdb::Range {
            start: Bound::Included(bytes::Bytes::copy_from_slice(s.as_bytes())),
            end: Bound::Excluded(bytes::Bytes::copy_from_slice(e.as_bytes())),
        }),
    };

    db.compact_range(range)?;
    println!(
        "compact_range start={} end={}",
        start.unwrap_or("-"),
        end.unwrap_or("-")
    );
    Ok(())
}

fn compact_auto_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let compacted = db.compact_if_needed()?;
    println!("compact_auto compacted={compacted}");
    Ok(())
}

fn ingest_sst_cmd(db: &Path, sst: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.ingest_sst(sst)?;
    println!("ingest_sst source={}", sst.display());
    Ok(())
}

fn rebalance_tiers(db: &Path) -> anyhow::Result<()> {
    let options = layerdb::DbOptions {
        enable_hdd_tier: true,
        hot_levels_max: 0,
        ..Default::default()
    };
    let db = layerdb::Db::open(db, options)?;
    let moved = db.rebalance_tiers()?;
    println!("rebalance_tiers moved={moved}");
    Ok(())
}

fn freeze_level(db: &Path, level: u8, max_files: Option<usize>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let moved = db.freeze_level_to_s3(level, max_files)?;
    println!("freeze_level level={level} moved={moved}");
    Ok(())
}

fn thaw_level(db: &Path, level: u8, max_files: Option<usize>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let moved = db.thaw_level_from_s3(level, max_files)?;
    println!("thaw_level level={level} moved={moved}");
    Ok(())
}

fn gc_s3(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_orphaned_s3_files()?;
    println!("gc_s3 removed={removed}");
    Ok(())
}

fn gc_local(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_orphaned_local_files()?;
    println!("gc_local removed={removed}");
    Ok(())
}

fn drop_branch(db: &Path, name: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.drop_branch(name)?;
    println!("drop_branch name={name}");
    Ok(())
}

fn create_branch(
    db: &Path,
    name: &str,
    from_branch: Option<&str>,
    from_seqno: Option<u64>,
) -> anyhow::Result<()> {
    if from_branch.is_some() && from_seqno.is_some() {
        anyhow::bail!("--from-branch and --from-seqno are mutually exclusive");
    }

    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    match (from_branch, from_seqno) {
        (Some(source_branch), None) => {
            db.checkout(source_branch)?;
            db.create_branch(name, None)?;
            println!("create_branch name={name} from_branch={source_branch}");
        }
        (None, Some(seqno)) => {
            db.create_branch_at_seqno(name, seqno)?;
            println!("create_branch name={name} from_seqno={seqno}");
        }
        (None, None) => {
            db.create_branch(name, None)?;
            println!("create_branch name={name}");
        }
        (Some(_), Some(_)) => unreachable!("validated mutually exclusive options"),
    }
    Ok(())
}

fn checkout_branch_cmd(db: &Path, name: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.checkout(name)?;
    println!("checkout name={name}");
    Ok(())
}

fn branches(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let current = db.current_branch();
    for (name, seqno) in db.list_branches() {
        let marker = if name == current { "*" } else { " " };
        println!("{marker} {name} {seqno}");
    }
    Ok(())
}

fn frozen_objects(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    for frozen in db.frozen_objects() {
        println!(
            "file_id={} level={} object_id={} object_version={} superblock_bytes={}",
            frozen.file_id,
            frozen.level,
            frozen.object_id,
            frozen.object_version.as_deref().unwrap_or("-"),
            frozen.superblock_bytes,
        );
    }
    Ok(())
}

fn archive_branch_cmd(db: &Path, name: &str, out: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let previous_branch = db.current_branch();

    db.checkout(name)?;
    let snapshot = db.create_snapshot()?;
    let branch_seqno = db.metrics().current_branch_seqno;

    std::fs::create_dir_all(out)
        .with_context(|| format!("create archive dir {}", out.display()))?;

    let file_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_nanos() as u64;
    let mut builder = layerdb::sst::SstBuilder::create(out, file_id, 64 * 1024)?;

    let mut iter = db.iter(
        layerdb::Range::all(),
        layerdb::ReadOptions {
            snapshot: Some(snapshot),
        },
    )?;
    iter.seek_to_first();
    let mut entries = 0u64;
    while let Some(next) = iter.next() {
        let (key, value) = next?;
        if let Some(value) = value {
            builder.add(
                &InternalKey::new(key.clone(), branch_seqno, KeyKind::Put),
                &value,
            )?;
            entries += 1;
        }
    }

    db.release_snapshot(snapshot);
    if previous_branch != name {
        db.checkout(&previous_branch)?;
    }

    let props = builder.finish()?;
    let out_file = out.join(format!("sst_{file_id:016x}.sst"));
    let archive_id = format!("{name}-{branch_seqno}-{file_id:016x}");
    db.add_archive(
        archive_id.clone(),
        name,
        branch_seqno,
        out_file.to_string_lossy().into_owned(),
    )?;
    println!(
        "archive_branch name={name} archive_id={archive_id} seqno={branch_seqno} entries={entries} out={} table_root={:?}",
        out_file.display(),
        props.table_root,
    );

    Ok(())
}

fn get_cmd(db: &Path, key: &str, branch: Option<&str>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }

    match db.get(key.as_bytes(), layerdb::ReadOptions::default())? {
        Some(value) => println!("value={}", String::from_utf8_lossy(&value)),
        None => println!("not_found"),
    }

    Ok(())
}

fn scan_cmd(
    db: &Path,
    start: Option<&str>,
    end: Option<&str>,
    branch: Option<&str>,
) -> anyhow::Result<()> {
    if let (Some(s), Some(e)) = (start, end) {
        if s >= e {
            anyhow::bail!("scan requires start < end when both bounds are provided");
        }
    }

    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }

    let range = match (start, end) {
        (None, None) => layerdb::Range::all(),
        (Some(s), None) => layerdb::Range {
            start: Bound::Included(bytes::Bytes::copy_from_slice(s.as_bytes())),
            end: Bound::Unbounded,
        },
        (None, Some(e)) => layerdb::Range {
            start: Bound::Unbounded,
            end: Bound::Excluded(bytes::Bytes::copy_from_slice(e.as_bytes())),
        },
        (Some(s), Some(e)) => layerdb::Range {
            start: Bound::Included(bytes::Bytes::copy_from_slice(s.as_bytes())),
            end: Bound::Excluded(bytes::Bytes::copy_from_slice(e.as_bytes())),
        },
    };

    let mut iter = db.iter(range, layerdb::ReadOptions::default())?;
    iter.seek_to_first();
    while let Some(next) = iter.next() {
        let (key, value) = next?;
        if let Some(value) = value {
            println!(
                "{}={}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );
        }
    }

    Ok(())
}

fn put_cmd(
    db: &Path,
    key: &str,
    value: &str,
    branch: Option<&str>,
    sync: bool,
) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }
    db.put(
        key.to_string(),
        value.to_string(),
        layerdb::WriteOptions { sync },
    )?;
    println!("put key={key} value={value} sync={sync}");
    Ok(())
}

fn delete_cmd(db: &Path, key: &str, branch: Option<&str>, sync: bool) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }
    db.delete(key.to_string(), layerdb::WriteOptions { sync })?;
    println!("delete key={key} sync={sync}");
    Ok(())
}

fn parse_batch_op(spec: &str) -> anyhow::Result<layerdb::Op> {
    if let Some(rest) = spec.strip_prefix("put:") {
        let mut parts = rest.splitn(2, ':');
        let key = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid --op spec: {spec}"))?;
        let value = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid --op spec: {spec}"))?;
        if key.is_empty() {
            anyhow::bail!("invalid --op spec: {spec}");
        }
        return Ok(layerdb::Op::put(key.to_string(), value.to_string()));
    }

    if let Some(key) = spec.strip_prefix("del:") {
        if key.is_empty() {
            anyhow::bail!("invalid --op spec: {spec}");
        }
        return Ok(layerdb::Op::delete(key.to_string()));
    }

    if let Some(rest) = spec.strip_prefix("rdel:") {
        let mut parts = rest.splitn(2, ':');
        let start = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid --op spec: {spec}"))?;
        let end = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid --op spec: {spec}"))?;
        if start.is_empty() || end.is_empty() || start >= end {
            anyhow::bail!("invalid --op spec: {spec}");
        }
        return Ok(layerdb::Op::delete_range(
            start.to_string(),
            end.to_string(),
        ));
    }

    anyhow::bail!("invalid --op spec: {spec}")
}

fn write_batch_cmd(
    db: &Path,
    ops: &[String],
    branch: Option<&str>,
    sync: bool,
) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }

    let parsed: Vec<layerdb::Op> = ops
        .iter()
        .map(|spec| parse_batch_op(spec))
        .collect::<anyhow::Result<Vec<_>>>()?;

    db.write_batch(parsed.clone(), layerdb::WriteOptions { sync })?;
    println!("write_batch ops={} sync={sync}", parsed.len());
    Ok(())
}

fn delete_range_cmd(
    db: &Path,
    start: &str,
    end: &str,
    branch: Option<&str>,
    sync: bool,
) -> anyhow::Result<()> {
    if start >= end {
        anyhow::bail!("delete-range requires start < end");
    }

    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    if let Some(branch_name) = branch {
        db.checkout(branch_name)?;
    }

    db.delete_range(
        start.to_string(),
        end.to_string(),
        layerdb::WriteOptions { sync },
    )?;
    println!("delete_range start={start} end={end} sync={sync}");
    Ok(())
}

fn retention_floor_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    println!("retention_floor seqno={}", db.retention_floor_seqno());
    Ok(())
}

fn format_hit_rate(rate: Option<f64>) -> String {
    match rate {
        Some(rate) => format!("{:.4}", rate),
        None => "n/a".to_string(),
    }
}

fn metrics_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let metrics = db.metrics();

    println!(
        "metrics wal_last_durable_seqno={} wal_last_ack_seqno={} retention_floor_seqno={}",
        metrics.wal_last_durable_seqno, metrics.wal_last_ack_seqno, metrics.retention_floor_seqno
    );
    println!(
        "metrics branch current={} head_seqno={}",
        metrics.current_branch, metrics.current_branch_seqno
    );

    let reader = metrics.version.reader_cache;
    println!(
        "metrics cache=reader hits={} misses={} inserts={} len={} hit_rate={}",
        reader.hits,
        reader.misses,
        reader.inserts,
        reader.len,
        format_hit_rate(reader.hit_rate())
    );

    if let Some(data) = metrics.version.data_block_cache {
        println!(
            "metrics cache=data hits={} misses={} inserts={} len={} hit_rate={}",
            data.hits,
            data.misses,
            data.inserts,
            data.len,
            format_hit_rate(data.hit_rate())
        );
    } else {
        println!("metrics cache=data disabled");
    }

    for (level, level_metrics) in metrics.version.levels {
        println!(
            "metrics level={} files={} bytes={} overlap_bytes={}",
            level, level_metrics.file_count, level_metrics.bytes, level_metrics.overlap_bytes
        );

        let debt = metrics
            .version
            .compaction_debt_bytes_by_level
            .get(&level)
            .copied()
            .unwrap_or(0);
        println!("metrics debt level={} bytes={}", level, debt);
    }

    println!("metrics debt l0_files={}", metrics.version.l0_file_debt);

    match (
        metrics.version.compaction_candidate_level,
        metrics.version.compaction_candidate_score,
    ) {
        (Some(level), Some(score)) => {
            println!(
                "metrics compaction candidate_level={} score={:.4} should_compact={}",
                level, score, metrics.version.should_compact
            );
        }
        _ => {
            println!(
                "metrics compaction candidate_level=- score=n/a should_compact={}",
                metrics.version.should_compact
            );
        }
    }

    println!(
        "metrics frozen_s3_files={}",
        metrics.version.frozen_s3_files
    );
    println!(
        "metrics tier s3_gets={} s3_get_cache_hits={} s3_puts={} s3_deletes={}",
        metrics.version.tier.s3_gets,
        metrics.version.tier.s3_get_cache_hits,
        metrics.version.tier.s3_puts,
        metrics.version.tier.s3_deletes,
    );

    Ok(())
}

fn manifest_files(
    db: &Path,
) -> anyhow::Result<
    Vec<(
        usize,
        layerdb::version::manifest::AddFile,
        Option<layerdb::version::manifest::FreezeFile>,
    )>,
> {
    let data = std::fs::read(db.join("MANIFEST"))?;
    let mut offset = 0usize;
    let mut adds: BTreeMap<u64, layerdb::version::manifest::AddFile> = BTreeMap::new();
    let mut freezes: BTreeMap<u64, layerdb::version::manifest::FreezeFile> = BTreeMap::new();
    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > data.len() {
            break;
        }
        let rec: layerdb::version::manifest::ManifestRecord =
            bincode::deserialize(&data[offset..(offset + len)])?;

        match rec {
            layerdb::version::manifest::ManifestRecord::AddFile(add) => {
                let file_id = add.file_id;
                adds.insert(file_id, add);
                freezes.remove(&file_id);
            }
            layerdb::version::manifest::ManifestRecord::DeleteFile(del) => {
                adds.remove(&del.file_id);
                freezes.remove(&del.file_id);
            }
            layerdb::version::manifest::ManifestRecord::VersionEdit(edit) => {
                for add in edit.adds {
                    let file_id = add.file_id;
                    adds.insert(file_id, add);
                    freezes.remove(&file_id);
                }
                for del in edit.deletes {
                    adds.remove(&del.file_id);
                    freezes.remove(&del.file_id);
                }
            }
            layerdb::version::manifest::ManifestRecord::MoveFile(mv) => {
                if let Some(add) = adds.get_mut(&mv.file_id) {
                    add.tier = mv.tier;
                }
                if mv.tier != layerdb::tier::StorageTier::S3 {
                    freezes.remove(&mv.file_id);
                }
            }
            layerdb::version::manifest::ManifestRecord::FreezeFile(freeze) => {
                if let Some(add) = adds.get_mut(&freeze.file_id) {
                    add.tier = layerdb::tier::StorageTier::S3;
                }
                freezes.insert(freeze.file_id, freeze);
            }
            layerdb::version::manifest::ManifestRecord::BranchHead(_) => {}
            layerdb::version::manifest::ManifestRecord::DropBranch(_) => {}
            layerdb::version::manifest::ManifestRecord::BranchArchive(_) => {}
            layerdb::version::manifest::ManifestRecord::DropBranchArchive(_) => {}
        }
        offset += len;
    }

    Ok(adds
        .into_values()
        .map(|add| {
            let freeze = freezes.get(&add.file_id).cloned();
            (add, freeze)
        })
        .enumerate()
        .map(|(idx, (add, freeze))| (idx, add, freeze))
        .collect())
}

fn resolve_sst_for_check(
    db: &Path,
    add: &layerdb::version::manifest::AddFile,
    freeze: Option<&layerdb::version::manifest::FreezeFile>,
) -> anyhow::Result<PathBuf> {
    let cache_path = db
        .join("sst_cache")
        .join(format!("sst_{:016x}.sst", add.file_id));
    if cache_path.exists() {
        return Ok(cache_path);
    }

    let tier_dir = match add.tier {
        layerdb::tier::StorageTier::Nvme => "sst",
        layerdb::tier::StorageTier::Hdd => "sst_hdd",
        layerdb::tier::StorageTier::S3 => "sst_s3",
    };
    let legacy_path = db
        .join(tier_dir)
        .join(format!("sst_{:016x}.sst", add.file_id));
    if add.tier != layerdb::tier::StorageTier::S3 {
        return Ok(legacy_path);
    }
    if legacy_path.exists() {
        return Ok(legacy_path);
    }

    let object_id = freeze
        .map(|f| f.object_id.as_str())
        .unwrap_or_else(|| "");
    let object_id = if object_id.is_empty() {
        format!("L{}-{:016x}", add.level, add.file_id)
    } else {
        object_id.to_string()
    };

    let object_dir = db
        .join("sst_s3")
        .join(format!("L{}", add.level))
        .join(&object_id);
    let meta_path = object_dir.join("meta.bin");
    if !meta_path.exists() {
        anyhow::bail!("missing referenced s3 meta: {}", meta_path.display());
    }
    let meta_bytes = std::fs::read(&meta_path)
        .with_context(|| format!("read s3 meta {}", meta_path.display()))?;
    let meta: S3ObjectMetaFile =
        bincode::deserialize(&meta_bytes).context("decode s3 meta")?;
    if meta.file_id != add.file_id || meta.level != add.level {
        anyhow::bail!(
            "s3 meta mismatch for file_id={} expected_level={} got_file_id={} got_level={}",
            add.file_id,
            add.level,
            meta.file_id,
            meta.level
        );
    }

    if meta.superblock_bytes == 0 {
        anyhow::bail!("s3 meta has invalid superblock_bytes=0");
    }

    std::fs::create_dir_all(db.join("sst_cache"))?;
    let tmp = cache_path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);

    {
        let mut out = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&tmp)
            .with_context(|| format!("open cache tmp {}", tmp.display()))?;

        for sb in &meta.superblocks {
            let path = object_dir.join(format!("sb_{:08}.bin", sb.id));
            let data = std::fs::read(&path)
                .with_context(|| format!("read superblock {}", path.display()))?;
            if data.len() != sb.len as usize {
                anyhow::bail!(
                    "superblock length mismatch for file_id={} sb={} expected {} got {}",
                    add.file_id,
                    sb.id,
                    sb.len,
                    data.len()
                );
            }
            let hash = blake3::hash(&data);
            if hash.as_bytes() != sb.hash.as_slice() {
                anyhow::bail!(
                    "superblock hash mismatch for file_id={} sb={}",
                    add.file_id,
                    sb.id
                );
            }
            use std::io::Write;
            out.write_all(&data)
                .with_context(|| format!("write cache tmp {}", tmp.display()))?;
        }

        out.sync_data()
            .with_context(|| format!("sync cache tmp {}", tmp.display()))?;
    }

    std::fs::rename(&tmp, &cache_path).with_context(|| {
        format!(
            "rename cache tmp {} -> {}",
            tmp.display(),
            cache_path.display()
        )
    })?;

    let dir_fd = std::fs::File::open(db.join("sst_cache"))?;
    dir_fd.sync_all()?;

    Ok(cache_path)
}
