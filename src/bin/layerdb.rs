use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use rayon::prelude::*;

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
    Scrub {
        #[arg(long)]
        db: PathBuf,
    },
    Bench {
        #[arg(long)]
        db: PathBuf,
        #[arg(long, default_value_t = 50_000)]
        keys: usize,
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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::ManifestDump { db } => manifest_dump(&db),
        Command::SstDump { sst } => sst_dump(&sst),
        Command::DbCheck { db } => db_check(&db),
        Command::Scrub { db } => scrub(&db),
        Command::Bench { db, keys } => bench(&db, keys),
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
    }
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

    let records = manifest_adds(db)?;
    let results: Vec<anyhow::Result<(u8, u64)>> = records
        .par_iter()
        .map(|(_, add)| {
            let tier_dir = match add.tier {
                layerdb::tier::StorageTier::Nvme => "sst",
                layerdb::tier::StorageTier::Hdd => "sst_hdd",
                layerdb::tier::StorageTier::S3 => "sst_s3",
            };
            let path = db
                .join(tier_dir)
                .join(format!("sst_{:016x}.sst", add.file_id));
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
    println!("db_check ok: {} files referenced", records.len());
    Ok(())
}

fn scrub(db: &Path) -> anyhow::Result<()> {
    let records = manifest_adds(db)?;
    let results: Vec<anyhow::Result<(u8, u64)>> = records
        .par_iter()
        .map(|(_, add)| {
            let tier_dir = match add.tier {
                layerdb::tier::StorageTier::Nvme => "sst",
                layerdb::tier::StorageTier::Hdd => "sst_hdd",
                layerdb::tier::StorageTier::S3 => "sst_s3",
            };
            let path = db
                .join(tier_dir)
                .join(format!("sst_{:016x}.sst", add.file_id));
            let reader = layerdb::sst::SstReader::open(&path)?;
            let mut iter = reader.iter(u64::MAX)?;
            iter.seek_to_first();
            let mut n = 0u64;
            while let Some(next) = iter.next() {
                let _ = next?;
                n += 1;
            }
            Ok((add.level, n))
        })
        .collect();

    let mut totals: BTreeMap<u8, u64> = BTreeMap::new();
    for result in results {
        let (level, n) = result?;
        *totals.entry(level).or_default() += n;
    }

    for (level, n) in totals {
        println!("scrub: level={level} entries={n}");
    }
    Ok(())
}

fn bench(db: &Path, keys: usize) -> anyhow::Result<()> {
    let options = layerdb::DbOptions {
        fsync_writes: false,
        memtable_bytes: 8 * 1024 * 1024,
        wal_segment_bytes: 8 * 1024 * 1024,
        ..Default::default()
    };
    let db = layerdb::Db::open(db, options)?;

    let start = std::time::Instant::now();
    for i in 0..keys {
        let key = format!("k{:08}", i);
        let val = format!("v{:08}", i);
        db.put(key, val, layerdb::WriteOptions { sync: false })?;
    }
    let write_elapsed = start.elapsed();

    let start = std::time::Instant::now();
    for i in 0..keys {
        let key = format!("k{:08}", i);
        let _ = db.get(key.as_bytes(), layerdb::ReadOptions::default())?;
    }
    let read_elapsed = start.elapsed();

    let write_qps = keys as f64 / write_elapsed.as_secs_f64();
    let read_qps = keys as f64 / read_elapsed.as_secs_f64();
    println!("bench keys={keys}");
    println!("write elapsed={write_elapsed:?} qps={write_qps:.0}");
    println!("read elapsed={read_elapsed:?} qps={read_qps:.0}");

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

fn manifest_adds(db: &Path) -> anyhow::Result<Vec<(usize, layerdb::version::manifest::AddFile)>> {
    let data = std::fs::read(db.join("MANIFEST"))?;
    let mut offset = 0usize;
    let mut adds: BTreeMap<u64, layerdb::version::manifest::AddFile> = BTreeMap::new();
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
                adds.insert(add.file_id, add);
            }
            layerdb::version::manifest::ManifestRecord::DeleteFile(del) => {
                adds.remove(&del.file_id);
            }
            layerdb::version::manifest::ManifestRecord::VersionEdit(edit) => {
                for add in edit.adds {
                    adds.insert(add.file_id, add);
                }
                for del in edit.deletes {
                    adds.remove(&del.file_id);
                }
            }
            layerdb::version::manifest::ManifestRecord::MoveFile(mv) => {
                if let Some(add) = adds.get_mut(&mv.file_id) {
                    add.tier = mv.tier;
                }
            }
            layerdb::version::manifest::ManifestRecord::FreezeFile(freeze) => {
                if let Some(add) = adds.get_mut(&freeze.file_id) {
                    add.tier = layerdb::tier::StorageTier::S3;
                }
            }
            layerdb::version::manifest::ManifestRecord::BranchHead(_) => {}
            layerdb::version::manifest::ManifestRecord::DropBranch(_) => {}
        }
        offset += len;
    }

    Ok(adds.into_values().enumerate().collect())
}
