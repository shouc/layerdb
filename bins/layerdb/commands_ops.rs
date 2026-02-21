use std::ops::Bound;
use std::path::Path;

use anyhow::Context;
use layerdb::internal_key::{InternalKey, KeyKind};

use crate::BenchWorkload;

pub(crate) fn list_archives_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    for archive in db.list_archives() {
        println!(
            "archive id={} branch={} seqno={} path={}",
            archive.archive_id, archive.branch, archive.seqno, archive.archive_path
        );
    }
    Ok(())
}

pub(crate) fn drop_archive_cmd(db: &Path, archive_id: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.drop_archive(archive_id)?;
    println!("drop_archive id={archive_id}");
    Ok(())
}

pub(crate) fn gc_archives_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_archives()?;
    println!("gc_archives removed={removed}");
    Ok(())
}

pub(crate) fn bench(db: &Path, keys: usize, workload: BenchWorkload) -> anyhow::Result<()> {
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
            for next in iter {
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
                for next in iter {
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

pub(crate) fn compact_range_cmd(
    db: &Path,
    start: Option<&str>,
    end: Option<&str>,
) -> anyhow::Result<()> {
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

pub(crate) fn compact_auto_cmd(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let compacted = db.compact_if_needed()?;
    println!("compact_auto compacted={compacted}");
    Ok(())
}

pub(crate) fn ingest_sst_cmd(db: &Path, sst: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.ingest_sst(sst)?;
    println!("ingest_sst source={}", sst.display());
    Ok(())
}

pub(crate) fn rebalance_tiers(db: &Path) -> anyhow::Result<()> {
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

pub(crate) fn freeze_level(db: &Path, level: u8, max_files: Option<usize>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let moved = db.freeze_level_to_s3(level, max_files)?;
    println!("freeze_level level={level} moved={moved}");
    Ok(())
}

pub(crate) fn thaw_level(db: &Path, level: u8, max_files: Option<usize>) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let moved = db.thaw_level_from_s3(level, max_files)?;
    println!("thaw_level level={level} moved={moved}");
    Ok(())
}

pub(crate) fn gc_s3(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_orphaned_s3_files()?;
    println!("gc_s3 removed={removed}");
    Ok(())
}

pub(crate) fn gc_local(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let removed = db.gc_orphaned_local_files()?;
    println!("gc_local removed={removed}");
    Ok(())
}

pub(crate) fn drop_branch(db: &Path, name: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.drop_branch(name)?;
    println!("drop_branch name={name}");
    Ok(())
}

pub(crate) fn create_branch(
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

pub(crate) fn checkout_branch_cmd(db: &Path, name: &str) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    db.checkout(name)?;
    println!("checkout name={name}");
    Ok(())
}

pub(crate) fn branches(db: &Path) -> anyhow::Result<()> {
    let db = layerdb::Db::open(db, layerdb::DbOptions::default())?;
    let current = db.current_branch();
    for (name, seqno) in db.list_branches() {
        let marker = if name == current { "*" } else { " " };
        println!("{marker} {name} {seqno}");
    }
    Ok(())
}

pub(crate) fn frozen_objects(db: &Path) -> anyhow::Result<()> {
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

pub(crate) fn archive_branch_cmd(db: &Path, name: &str, out: &Path) -> anyhow::Result<()> {
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
    for next in iter {
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

pub(crate) fn get_cmd(db: &Path, key: &str, branch: Option<&str>) -> anyhow::Result<()> {
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

pub(crate) fn scan_cmd(
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
    for next in iter {
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

pub(crate) fn put_cmd(
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

pub(crate) fn delete_cmd(
    db: &Path,
    key: &str,
    branch: Option<&str>,
    sync: bool,
) -> anyhow::Result<()> {
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

pub(crate) fn write_batch_cmd(
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

pub(crate) fn delete_range_cmd(
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

pub(crate) fn retention_floor_cmd(db: &Path) -> anyhow::Result<()> {
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

pub(crate) fn metrics_cmd(db: &Path) -> anyhow::Result<()> {
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
