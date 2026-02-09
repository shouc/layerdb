use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::ManifestDump { db } => manifest_dump(&db),
        Command::SstDump { sst } => sst_dump(&sst),
        Command::DbCheck { db } => db_check(&db),
        Command::Scrub { db } => scrub(&db),
        Command::Bench { db, keys } => bench(&db, keys),
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
    let missing: Vec<_> = records
        .par_iter()
        .filter_map(|(_, add)| {
            let path = db.join("sst").join(format!("sst_{:016x}.sst", add.file_id));
            (!path.exists()).then_some(path)
        })
        .collect();

    if !missing.is_empty() {
        for path in missing {
            eprintln!("missing referenced sst: {}", path.display());
        }
        anyhow::bail!("db_check failed");
    }

    println!("db_check ok: {} files referenced", records.len());
    Ok(())
}

fn scrub(db: &Path) -> anyhow::Result<()> {
    let records = manifest_adds(db)?;
    let results: Vec<anyhow::Result<(u8, u64)>> = records
        .par_iter()
        .map(|(_, add)| {
            let path = db.join("sst").join(format!("sst_{:016x}.sst", add.file_id));
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
        }
        offset += len;
    }

    Ok(adds.into_values().enumerate().collect())
}
