use super::*;

impl VersionSet {
    pub(super) fn cached_reader(&self, path: &Path) -> anyhow::Result<Arc<SstReader>> {
        let key = path.to_path_buf();
        if let Some(reader) = self.reader_cache.get(&key) {
            return Ok(reader);
        }

        let reader = if self.options.sst_use_io_executor_reads {
            let io_ctx = self
                .sst_io_ctx
                .as_ref()
                .context("sst io executor requested but not configured")?;
            Arc::new(SstReader::open_with_io(
                path,
                io_ctx.clone(),
                self.data_block_cache.clone(),
            )?)
        } else {
            Arc::new(SstReader::open_with_cache(
                path,
                self.data_block_cache.clone(),
            )?)
        };

        self.reader_cache.insert(key, reader.clone());
        Ok(reader)
    }

    pub(super) fn sst_root_dir(&self, level: u8) -> PathBuf {
        if self.options.enable_hdd_tier && level > self.options.hot_levels_max {
            self.dir.join("sst_hdd")
        } else {
            self.dir.join("sst")
        }
    }

    pub(super) fn tier_for_level(&self, level: u8) -> StorageTier {
        if self.options.enable_hdd_tier && level > self.options.hot_levels_max {
            StorageTier::Hdd
        } else {
            StorageTier::Nvme
        }
    }

    pub(crate) fn resolve_sst_path(&self, level: u8, file_id: u64) -> anyhow::Result<PathBuf> {
        let primary = self.sst_path(level, file_id);
        let secondary = if primary.parent().is_some_and(|p| p.ends_with("sst_hdd")) {
            self.dir.join("sst").join(format!("sst_{file_id:016x}.sst"))
        } else {
            self.dir
                .join("sst_hdd")
                .join(format!("sst_{file_id:016x}.sst"))
        };

        for candidate in [&primary, &secondary] {
            if candidate.exists() {
                return Ok(candidate.clone());
            }
        }

        let cache = self.sst_cache_path(file_id);
        if cache.exists() {
            let has_s3 = self
                .frozen_objects
                .read()
                .get(&file_id)
                .is_some_and(|frozen| {
                    self.s3_store
                        .exists(&self.s3_object_meta_key(frozen.level, &frozen.object_id))
                        .unwrap_or(false)
                });
            if has_s3 {
                self.tier_counters
                    .s3_get_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(cache);
        }

        if let Some(frozen) = self.frozen_objects.read().get(&file_id).cloned() {
            if self
                .s3_store
                .exists(&self.s3_object_meta_key(frozen.level, &frozen.object_id))
                .unwrap_or(false)
            {
                return self.hydrate_s3_object_to_cache(&frozen);
            }
        }

        anyhow::bail!(
            "manifest references missing sst file_id={file_id} level={level}: primary={primary:?} secondary={secondary:?} cache={cache:?}"
        )
    }

    fn sst_path(&self, level: u8, file_id: u64) -> PathBuf {
        self.sst_root_dir(level)
            .join(format!("sst_{file_id:016x}.sst"))
    }

    fn sst_path_for_tier(&self, tier: StorageTier, file_id: u64) -> PathBuf {
        let dir = match tier {
            StorageTier::Nvme => self.dir.join("sst"),
            StorageTier::Hdd => self.dir.join("sst_hdd"),
            StorageTier::S3 => self.dir.join("sst_s3"),
        };
        dir.join(format!("sst_{file_id:016x}.sst"))
    }

    fn sst_cache_path(&self, file_id: u64) -> PathBuf {
        self.dir
            .join("sst_cache")
            .join(format!("sst_{file_id:016x}.sst"))
    }

    fn hydrate_s3_object_to_cache(&self, frozen: &FrozenObjectMeta) -> anyhow::Result<PathBuf> {
        let cache = self.sst_cache_path(frozen.file_id);
        if cache.exists() {
            self.tier_counters
                .s3_get_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(cache);
        }

        if let Some(parent) = cache.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp = cache.with_extension("tmp");
        let _ = std::fs::remove_file(&tmp);
        let meta = self.load_s3_object_meta(frozen.level, &frozen.object_id)?;
        if meta.file_id != frozen.file_id {
            anyhow::bail!(
                "s3 meta file_id mismatch for {}: expected {} got {}",
                frozen.object_id,
                frozen.file_id,
                meta.file_id
            );
        }

        let mut out = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&tmp)
            .with_context(|| format!("open cache tmp {}", tmp.display()))?;

        for sb in &meta.superblocks {
            let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
            let data = self
                .s3_store
                .get(&key)
                .with_context(|| format!("read superblock key={key}"))?;
            if data.len() != sb.len as usize {
                anyhow::bail!(
                    "superblock length mismatch for {} sb={} expected {} got {}",
                    meta.object_id,
                    sb.id,
                    sb.len,
                    data.len()
                );
            }
            let hash = blake3::hash(&data);
            if hash.as_bytes() != sb.hash.as_slice() {
                anyhow::bail!(
                    "superblock hash mismatch for {} sb={}",
                    meta.object_id,
                    sb.id
                );
            }
            out.write_all(&data)
                .with_context(|| format!("write cache tmp {}", tmp.display()))?;
            self.tier_counters.s3_gets.fetch_add(1, Ordering::Relaxed);
        }

        out.sync_data()
            .with_context(|| format!("sync cache tmp {}", tmp.display()))?;
        drop(out);

        std::fs::rename(&tmp, &cache)
            .with_context(|| format!("rename cache {} -> {}", tmp.display(), cache.display()))?;

        let parent = cache
            .parent()
            .ok_or_else(|| anyhow::anyhow!("cache file has no parent"))?;
        let dir_fd = std::fs::File::open(parent)?;
        dir_fd.sync_all()?;

        Ok(cache)
    }

    fn s3_level_prefix(&self, level: u8) -> String {
        format!("L{level}/")
    }

    fn s3_object_prefix(&self, level: u8, object_id: &str) -> String {
        format!("{}{}", self.s3_level_prefix(level), object_id)
    }

    fn s3_object_meta_key(&self, level: u8, object_id: &str) -> String {
        format!("{}/meta.bin", self.s3_object_prefix(level, object_id))
    }

    fn s3_superblock_key(&self, level: u8, object_id: &str, superblock_id: u32) -> String {
        format!(
            "{}/sb_{superblock_id:08}.bin",
            self.s3_object_prefix(level, object_id)
        )
    }

    fn load_s3_object_meta(&self, level: u8, object_id: &str) -> anyhow::Result<S3ObjectMetaFile> {
        let key = self.s3_object_meta_key(level, object_id);
        let bytes = self
            .s3_store
            .get(&key)
            .with_context(|| format!("read s3 meta key={key}"))?;
        let meta: S3ObjectMetaFile =
            bincode::deserialize(&bytes).with_context(|| format!("decode s3 meta key={key}"))?;
        Ok(meta)
    }

    fn write_s3_object_from_file(
        &self,
        frozen: &FrozenObjectMeta,
        src: &Path,
    ) -> anyhow::Result<Option<String>> {
        let mut file = std::fs::File::open(src)
            .with_context(|| format!("open sst for freeze {}", src.display()))?;
        let mut superblocks = Vec::new();
        let mut total_bytes = 0u64;
        let mut uploaded: Vec<String> = Vec::new();
        let chunk_len: usize = frozen
            .superblock_bytes
            .try_into()
            .context("superblock_bytes overflow")?;
        if chunk_len == 0 {
            anyhow::bail!("superblock_bytes must be > 0");
        }

        let mut id = 0u32;
        loop {
            let mut buf = vec![0u8; chunk_len];
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            buf.truncate(n);

            let hash = blake3::hash(&buf);
            let sb_key = self.s3_superblock_key(frozen.level, &frozen.object_id, id);
            if let Err(err) = self.s3_store.put(&sb_key, &buf) {
                for key in uploaded {
                    let _ = self.s3_store.delete(&key);
                }
                return Err(err).with_context(|| format!("upload superblock key={sb_key}"));
            }
            uploaded.push(sb_key.clone());

            superblocks.push(S3SuperblockMeta {
                id,
                len: n as u32,
                hash: *hash.as_bytes(),
            });
            total_bytes = total_bytes.saturating_add(n as u64);
            id = id.saturating_add(1);

            self.tier_counters.s3_puts.fetch_add(1, Ordering::Relaxed);
        }

        let meta = S3ObjectMetaFile {
            file_id: frozen.file_id,
            level: frozen.level,
            object_id: frozen.object_id.clone(),
            superblock_bytes: frozen.superblock_bytes,
            total_bytes,
            superblocks,
        };
        let meta_bytes = bincode::serialize(&meta).context("encode s3 meta")?;
        let meta_key = self.s3_object_meta_key(frozen.level, &frozen.object_id);
        let version = match self.s3_store.put(&meta_key, &meta_bytes) {
            Ok(version) => version,
            Err(err) => {
                for key in uploaded {
                    let _ = self.s3_store.delete(&key);
                }
                return Err(err).with_context(|| format!("upload s3 meta key={meta_key}"));
            }
        };
        self.tier_counters.s3_puts.fetch_add(1, Ordering::Relaxed);
        Ok(version)
    }

    pub(super) fn delete_s3_object(&self, frozen: &FrozenObjectMeta) -> anyhow::Result<usize> {
        let meta = match self.load_s3_object_meta(frozen.level, &frozen.object_id) {
            Ok(meta) => meta,
            Err(_) => return Ok(0),
        };

        let mut removed = 0usize;
        for sb in meta.superblocks {
            let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
            self.s3_store
                .delete(&key)
                .with_context(|| format!("delete superblock key={key}"))?;
            removed += 1;
        }

        let meta_key = self.s3_object_meta_key(meta.level, &meta.object_id);
        self.s3_store
            .delete(&meta_key)
            .with_context(|| format!("delete meta key={meta_key}"))?;
        removed += 1;

        Ok(removed)
    }

    pub fn snapshots(&self) -> &SnapshotTracker {
        &self.snapshots
    }

    pub(crate) fn latest_seqno(&self) -> u64 {
        self.snapshots.latest_seqno()
    }

    pub(crate) fn min_retained_seqno(&self) -> u64 {
        let min_snapshot = self.snapshots.min_pinned_seqno();
        let min_branch = self
            .branches
            .read()
            .values()
            .copied()
            .min()
            .unwrap_or(min_snapshot);
        min_snapshot.min(min_branch)
    }

    pub(crate) fn snapshots_handle(&self) -> Arc<SnapshotTracker> {
        self.snapshots.clone()
    }

    pub fn metrics(&self) -> VersionMetrics {
        let mut levels = BTreeMap::new();
        let mut compaction_debt_bytes_by_level = BTreeMap::new();
        let mut compaction_level_metrics = BTreeMap::new();

        {
            let guard = self.levels.read();
            for (level, files) in [(0u8, &guard.l0), (1u8, &guard.l1)] {
                let bytes = files.iter().map(|f| f.size_bytes).sum();
                let file_count = files.len();
                let overlap_bytes = estimate_overlap_bytes(files);
                levels.insert(
                    level,
                    LevelMetricsSnapshot {
                        file_count,
                        bytes,
                        overlap_bytes,
                    },
                );
                compaction_level_metrics.insert(
                    level,
                    crate::compaction::LevelMetrics {
                        bytes,
                        file_count,
                        overlap_bytes,
                    },
                );
            }
        }

        let compaction_options = crate::compaction::CompactionOptions::default();
        let candidate = crate::compaction::CompactionPicker::pick_highest_score(
            &compaction_level_metrics,
            &compaction_options,
        );

        for (level, metrics) in &compaction_level_metrics {
            let target = compaction_options
                .target_level_bytes
                .get(level)
                .copied()
                .unwrap_or(0);
            let debt = metrics.bytes.saturating_sub(target);
            compaction_debt_bytes_by_level.insert(*level, debt);
        }

        let l0_file_debt = compaction_level_metrics
            .get(&0)
            .map(|m| {
                m.file_count
                    .saturating_sub(compaction_options.l0_file_trigger)
            })
            .unwrap_or(0);

        VersionMetrics {
            reader_cache: self.reader_cache.stats(),
            data_block_cache: self.data_block_cache.as_ref().map(|cache| cache.stats()),
            levels,
            compaction_debt_bytes_by_level,
            l0_file_debt,
            compaction_candidate_level: candidate.as_ref().map(|c| c.level),
            compaction_candidate_score: candidate.as_ref().map(|c| c.score),
            should_compact: crate::compaction::CompactionPicker::should_compact(
                &compaction_level_metrics,
                &compaction_options,
            ),
            frozen_s3_files: self.frozen_objects.read().len(),
            tier: self.tier_counters.snapshot(),
        }
    }

    pub(crate) fn allocate_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn get(
        &self,
        key: &[u8],
        snapshot_seqno: u64,
    ) -> anyhow::Result<Option<LookupResult>> {
        let (l0_files, l1_files) = {
            let levels = self.levels.read();
            (levels.l0.clone(), levels.l1.clone())
        };

        let mut candidate: Option<(u64, Option<Value>)> = None;

        // L0: searched newest-first; may overlap.
        for add in l0_files.iter().rev() {
            if !range_contains(
                &(
                    std::ops::Bound::Included(add.smallest_user_key.clone()),
                    std::ops::Bound::Included(add.largest_user_key.clone()),
                ),
                key,
            ) {
                continue;
            }
            let path = self.resolve_sst_path(add.level, add.file_id)?;
            let reader = self.cached_reader(&path)?;
            if let Some((seqno, v)) = reader.get(key, snapshot_seqno)? {
                match &candidate {
                    Some((best_seq, _)) if *best_seq >= seqno => {}
                    _ => candidate = Some((seqno, v)),
                }
            }
        }

        // L1: non-overlapping; binary search by key range.
        if let Some(add) = find_l1_file(&l1_files, key) {
            let path = self.resolve_sst_path(add.level, add.file_id)?;
            let reader = self.cached_reader(&path)?;
            if let Some((seqno, v)) = reader.get(key, snapshot_seqno)? {
                match &candidate {
                    Some((best_seq, _)) if *best_seq >= seqno => {}
                    _ => candidate = Some((seqno, v)),
                }
            }
        }

        let tombstone_seq = self
            .range_tombstones(snapshot_seqno)?
            .iter()
            .filter(|t| t.start_key.as_ref() <= key && key < t.end_key.as_ref())
            .map(|t| t.seqno)
            .max();

        let result = match (candidate, tombstone_seq) {
            (Some((seq, value)), Some(tseq)) => {
                if tseq >= seq {
                    LookupResult {
                        seqno: tseq,
                        value: None,
                    }
                } else {
                    LookupResult { seqno: seq, value }
                }
            }
            (Some((seq, value)), None) => LookupResult { seqno: seq, value },
            (None, Some(tseq)) => LookupResult {
                seqno: tseq,
                value: None,
            },
            (None, None) => return Ok(None),
        };

        Ok(Some(result))
    }

    pub fn rebalance_level_tiers(&self) -> anyhow::Result<usize> {
        let mut moves = Vec::new();
        {
            let guard = self.levels.read();
            for file in guard.l0.iter().chain(guard.l1.iter()) {
                if file.tier == StorageTier::S3 {
                    continue;
                }
                let target_tier = self.tier_for_level(file.level);
                if file.tier != target_tier {
                    moves.push((file.file_id, file.level, file.tier, target_tier));
                }
            }
        }

        let max_moves = self.options.tier_rebalance_max_moves;
        if max_moves == 0 {
            return Ok(0);
        }

        for (file_id, level, from_tier, to_tier) in moves.iter().take(max_moves) {
            self.move_file_between_tiers(*file_id, *level, *from_tier, *to_tier)?;
        }

        Ok(moves.len().min(max_moves))
    }

    pub fn freeze_level_to_s3(&self, level: u8, max_files: Option<usize>) -> anyhow::Result<usize> {
        let files = {
            let guard = self.levels.read();
            match level {
                0 => guard.l0.clone(),
                1 => guard.l1.clone(),
                _ => anyhow::bail!("unsupported level for freeze: {level}"),
            }
        };

        let limit = max_files.unwrap_or(usize::MAX);
        let mut moved = 0usize;
        for add in files {
            if moved >= limit {
                break;
            }
            if add.tier == StorageTier::S3 {
                continue;
            }
            self.freeze_file_to_s3(&add)?;
            moved += 1;
        }

        Ok(moved)
    }

    pub fn thaw_level_from_s3(&self, level: u8, max_files: Option<usize>) -> anyhow::Result<usize> {
        let files = {
            let guard = self.levels.read();
            match level {
                0 => guard.l0.clone(),
                1 => guard.l1.clone(),
                _ => anyhow::bail!("unsupported level for thaw: {level}"),
            }
        };

        let target_tier = self.tier_for_level(level);
        if target_tier == StorageTier::S3 {
            anyhow::bail!("cannot thaw level {level} into s3 tier");
        }

        let limit = max_files.unwrap_or(usize::MAX);
        let mut moved = 0usize;
        for add in files {
            if moved >= limit {
                break;
            }
            if add.tier != StorageTier::S3 {
                continue;
            }
            self.move_file_between_tiers(add.file_id, add.level, StorageTier::S3, target_tier)?;
            moved += 1;
        }

        Ok(moved)
    }

    pub fn gc_orphaned_local_files(&self) -> anyhow::Result<usize> {
        let referenced: std::collections::HashSet<u64> = {
            let guard = self.levels.read();
            guard
                .l0
                .iter()
                .chain(guard.l1.iter())
                .map(|file| file.file_id)
                .collect()
        };

        let mut removed = 0usize;
        for dir in [
            self.dir.join("sst"),
            self.dir.join("sst_hdd"),
            self.dir.join("sst_cache"),
        ] {
            if !dir.exists() {
                continue;
            }

            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(file_id) = parse_sst_file_id(name) else {
                    continue;
                };

                if referenced.contains(&file_id) {
                    continue;
                }

                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    pub fn gc_orphaned_s3_files(&self) -> anyhow::Result<usize> {
        let referenced: std::collections::HashSet<u64> = {
            let guard = self.levels.read();
            guard
                .l0
                .iter()
                .chain(guard.l1.iter())
                .filter(|file| file.tier == StorageTier::S3)
                .map(|file| file.file_id)
                .collect()
        };

        let mut deleted_keys = 0usize;
        let mut deleted_file_ids = std::collections::HashSet::new();
        let keys = self.s3_store.list("")?;
        for key in keys {
            let Some(file_id) = parse_file_id_from_s3_key(&key) else {
                continue;
            };
            if referenced.contains(&file_id) {
                continue;
            }

            self.s3_store
                .delete(&key)
                .with_context(|| format!("delete orphaned s3 key={key}"))?;
            deleted_keys += 1;
            deleted_file_ids.insert(file_id);
            let _ = std::fs::remove_file(self.sst_cache_path(file_id));
        }

        if deleted_keys > 0 {
            self.tier_counters
                .s3_deletes
                .fetch_add(deleted_keys as u64, Ordering::Relaxed);
        }

        Ok(deleted_file_ids.len())
    }

    fn freeze_file_to_s3(&self, add: &AddFile) -> anyhow::Result<()> {
        let src = self.resolve_sst_path(add.level, add.file_id)?;
        let mut frozen = FrozenObjectMeta {
            file_id: add.file_id,
            level: add.level,
            object_id: format!("L{}-{:016x}", add.level, add.file_id),
            object_version: None,
            superblock_bytes: 8 * 1024 * 1024,
        };

        frozen.object_version = self.write_s3_object_from_file(&frozen, &src)?;
        self.mark_file_frozen(
            add.file_id,
            frozen.object_id.clone(),
            frozen.object_version.clone(),
            frozen.superblock_bytes,
        )?;

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(self.sst_cache_path(add.file_id));

        Ok(())
    }

    pub fn mark_file_frozen(
        &self,
        file_id: u64,
        object_id: impl Into<String>,
        object_version: Option<String>,
        superblock_bytes: u32,
    ) -> anyhow::Result<()> {
        let mut target: Option<AddFile> = None;
        {
            let guard = self.levels.read();
            for add in guard.l0.iter().chain(guard.l1.iter()) {
                if add.file_id == file_id {
                    target = Some(add.clone());
                    break;
                }
            }
        }

        let Some(add) = target else {
            anyhow::bail!("unknown file_id for freeze: {file_id}");
        };

        let object_id = object_id.into();
        let freeze = FreezeFile {
            file_id,
            level: add.level,
            object_id: object_id.clone(),
            object_version: object_version.clone(),
            superblock_bytes,
        };

        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::FreezeFile(freeze), true)?;
            manifest.sync_dir()?;
        }

        {
            let mut levels = self.levels.write();
            for file in levels.l0.iter_mut() {
                if file.file_id == file_id {
                    file.tier = StorageTier::S3;
                }
            }
            for file in levels.l1.iter_mut() {
                if file.file_id == file_id {
                    file.tier = StorageTier::S3;
                }
            }
        }

        self.frozen_objects.write().insert(
            file_id,
            FrozenObjectMeta {
                file_id,
                level: add.level,
                object_id,
                object_version,
                superblock_bytes,
            },
        );
        Ok(())
    }

    fn move_file_between_tiers(
        &self,
        file_id: u64,
        level: u8,
        from_tier: StorageTier,
        to_tier: StorageTier,
    ) -> anyhow::Result<()> {
        if to_tier == StorageTier::S3 {
            let add = {
                let guard = self.levels.read();
                guard
                    .l0
                    .iter()
                    .chain(guard.l1.iter())
                    .find(|f| f.file_id == file_id && f.level == level)
                    .cloned()
            };
            let Some(add) = add else {
                anyhow::bail!("unknown sst for freeze file_id={file_id} level={level}");
            };
            if add.tier != StorageTier::S3 {
                self.freeze_file_to_s3(&add)?;
            }
            return Ok(());
        }

        let frozen = if from_tier == StorageTier::S3 {
            self.frozen_objects.read().get(&file_id).cloned()
        } else {
            None
        };
        let dst = self.sst_path_for_tier(to_tier, file_id);
        let dst_tmp = dst.with_extension("tmp");
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let src = self.sst_path_for_tier(from_tier, file_id);

        if from_tier == StorageTier::S3 {
            let frozen = frozen
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing frozen metadata for file_id={file_id}"))?;
            let meta = self.load_s3_object_meta(frozen.level, &frozen.object_id)?;
            let mut out = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&dst_tmp)
                .with_context(|| format!("open dst tmp {}", dst_tmp.display()))?;
            for sb in &meta.superblocks {
                let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
                let data = self
                    .s3_store
                    .get(&key)
                    .with_context(|| format!("read superblock key={key}"))?;
                if data.len() != sb.len as usize {
                    anyhow::bail!(
                        "superblock length mismatch for {} sb={} expected {} got {}",
                        meta.object_id,
                        sb.id,
                        sb.len,
                        data.len()
                    );
                }
                let hash = blake3::hash(&data);
                if hash.as_bytes() != sb.hash.as_slice() {
                    anyhow::bail!(
                        "superblock hash mismatch for {} sb={}",
                        meta.object_id,
                        sb.id
                    );
                }
                out.write_all(&data)
                    .with_context(|| format!("write dst tmp {}", dst_tmp.display()))?;
                self.tier_counters.s3_gets.fetch_add(1, Ordering::Relaxed);
            }
            out.sync_data()?;
        } else {
            if !src.exists() {
                // Recovery fallback: if file already moved, do not fail hard.
                if self.sst_path_for_tier(to_tier, file_id).exists() {
                    return Ok(());
                }
                anyhow::bail!(
                    "cannot move missing sst file_id={} from tier {:?}: {}",
                    file_id,
                    from_tier,
                    src.display()
                );
            }

            std::fs::copy(&src, &dst_tmp)?;
            {
                let fd = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&dst_tmp)?;
                fd.sync_data()?;
            }
        }

        std::fs::rename(&dst_tmp, &dst)?;
        {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("destination has no parent"))?;
            let dir_fd = std::fs::File::open(parent)?;
            dir_fd.sync_all()?;
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::MoveFile(MoveFile {
                    file_id,
                    level,
                    tier: to_tier,
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        {
            let mut levels = self.levels.write();
            for file in levels.l0.iter_mut() {
                if file.file_id == file_id && file.level == level {
                    file.tier = to_tier;
                }
            }
            for file in levels.l1.iter_mut() {
                if file.file_id == file_id && file.level == level {
                    file.tier = to_tier;
                }
            }
        }

        if to_tier != StorageTier::S3 {
            self.frozen_objects.write().remove(&file_id);
        }

        if from_tier == StorageTier::S3 {
            if let Some(frozen) = frozen.as_ref() {
                let deleted = self.delete_s3_object(frozen)?;
                if deleted > 0 {
                    self.tier_counters
                        .s3_deletes
                        .fetch_add(deleted as u64, Ordering::Relaxed);
                }
            }
        } else {
            let _ = std::fs::remove_file(src);
        }
        let _ = std::fs::remove_file(self.sst_cache_path(file_id));
        Ok(())
    }

    pub(crate) fn range_tombstones(
        &self,
        snapshot_seqno: u64,
    ) -> anyhow::Result<Vec<RangeTombstone>> {
        let guard = self.levels.read();
        let mut out = Vec::new();

        for file in guard.l0.iter().chain(guard.l1.iter()) {
            let path = self.resolve_sst_path(file.level, file.file_id)?;
            let reader = self.cached_reader(&path)?;
            out.extend(reader.range_tombstones(snapshot_seqno)?);
        }

        out.sort_by(|a, b| b.seqno.cmp(&a.seqno));
        Ok(out)
    }
}
