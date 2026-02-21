use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use rayon::prelude::*;
use serde::Deserialize;

pub(crate) fn manifest_dump(db: &Path) -> anyhow::Result<()> {
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

pub(crate) fn sst_dump(sst: &Path) -> anyhow::Result<()> {
    let reader = layerdb::sst::SstReader::open(sst)?;
    println!("file: {}", sst.display());
    println!("props: {:?}", reader.properties());

    let mut iter = reader.iter(u64::MAX)?;
    iter.seek_to_first();
    let mut count = 0u64;
    let mut first = None;
    let mut last = None;
    for next in iter {
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

pub(crate) fn db_check(db: &Path) -> anyhow::Result<()> {
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
            for next in iter {
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

pub(crate) fn verify(db: &Path) -> anyhow::Result<()> {
    db_check(db)
}

pub(crate) fn scrub(db: &Path) -> anyhow::Result<()> {
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

    let object_id = freeze.map(|f| f.object_id.as_str()).unwrap_or_else(|| "");
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
    let meta: S3ObjectMetaFile = bincode::deserialize(&meta_bytes).context("decode s3 meta")?;
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
