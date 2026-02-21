use super::*;

impl VersionSet {
    pub fn iter(&self, range: Range, snapshot_seqno: u64) -> anyhow::Result<SstIter> {
        let guard = self.levels.read();
        let mut paths = Vec::with_capacity(guard.l0.len() + guard.l1.len());
        for file in guard.l0.iter().chain(guard.l1.iter()) {
            paths.push(self.resolve_sst_path(file.level, file.file_id)?);
        }
        SstIter::new(paths, snapshot_seqno, range)
    }

    pub fn compact_l0_to_l1(
        &self,
        range: Option<&Range>,
        options: &DbOptions,
    ) -> anyhow::Result<()> {
        let (l0, l1) = {
            let guard = self.levels.read();
            if guard.l0.is_empty() {
                return Ok(());
            }
            (guard.l0.clone(), guard.l1.clone())
        };

        let range_bounds = range.map(crate::memtable::bounds_from_range);
        let l0_in_range: Vec<AddFile> = if let Some(bounds) = &range_bounds {
            l0.into_iter()
                .filter(|file| {
                    range_overlaps_file(
                        bounds,
                        file.smallest_user_key.as_ref(),
                        file.largest_user_key.as_ref(),
                    )
                })
                .collect()
        } else {
            l0
        };

        let target_tier = self.tier_for_level(1);
        if target_tier == StorageTier::Hdd {
            let budget = options.hdd_compaction_budget_bytes;
            if budget == 0 {
                return Ok(());
            }

            let mut total = 0u64;
            let mut limited = Vec::new();
            for add in l0_in_range {
                if !limited.is_empty() && total >= budget {
                    break;
                }

                limited.push(add.clone());
                total = total.saturating_add(add.size_bytes);
            }

            if limited.is_empty() {
                return Ok(());
            }

            return self.compact_l0_to_l1_inputs(limited, l1, range);
        }

        self.compact_l0_to_l1_inputs(l0_in_range, l1, range)
    }

    fn compact_l0_to_l1_inputs(
        &self,
        l0_in_range: Vec<AddFile>,
        l1: Vec<AddFile>,
        _range: Option<&Range>,
    ) -> anyhow::Result<()> {
        if l0_in_range.is_empty() {
            return Ok(());
        }

        let mut smallest: Option<Bytes> = None;
        let mut largest: Option<Bytes> = None;
        for file in &l0_in_range {
            smallest = Some(match smallest {
                None => file.smallest_user_key.clone(),
                Some(s) => std::cmp::min(s, file.smallest_user_key.clone()),
            });
            largest = Some(match largest {
                None => file.largest_user_key.clone(),
                Some(l) => std::cmp::max(l, file.largest_user_key.clone()),
            });
        }
        let smallest = smallest.unwrap_or_default();
        let largest = largest.unwrap_or_default();

        let mut compact_inputs = Vec::new();
        compact_inputs.extend(l0_in_range.clone());
        let mut l1_inputs = Vec::new();
        for file in &l1 {
            if overlaps(
                &smallest,
                &largest,
                &file.smallest_user_key,
                &file.largest_user_key,
            ) {
                l1_inputs.push(file.clone());
                compact_inputs.push(file.clone());
            }
        }

        let can_drop_obsolete_point_tombstones =
            self.compaction_inputs_fully_cover_range(&l0_in_range, &l1_inputs, &smallest, &largest);

        let mut entries = Vec::new();
        for file in &compact_inputs {
            let path = self.resolve_sst_path(file.level, file.file_id)?;
            let reader = self.cached_reader(&path)?;
            let mut iter = reader.iter(u64::MAX)?;
            iter.seek_to_first();
            for next in iter {
                let (user_key, seqno, kind, value) = next?;
                let key_kind = match kind {
                    OpKind::Put => KeyKind::Put,
                    OpKind::Del => KeyKind::Del,
                    OpKind::RangeDel => KeyKind::RangeDel,
                };
                entries.push((InternalKey::new(user_key, seqno, key_kind), value));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let min_snapshot_seqno = self.min_retained_seqno();
        let mut out_entries = Vec::with_capacity(entries.len());
        let mut idx = 0usize;
        while idx < entries.len() {
            let user_key = entries[idx].0.user_key.clone();
            let mut group = Vec::new();
            while idx < entries.len() && entries[idx].0.user_key == user_key {
                group.push(entries[idx].clone());
                idx += 1;
            }

            out_entries.extend(compact_user_key_entries(
                group,
                min_snapshot_seqno,
                can_drop_obsolete_point_tombstones,
            ));
        }

        out_entries = drop_obsolete_range_tombstones_bottommost(
            out_entries,
            min_snapshot_seqno,
            can_drop_obsolete_point_tombstones,
        );

        if out_entries.is_empty() {
            return Ok(());
        }

        let out_file_id = self.allocate_file_id();
        let sst_dir = self.sst_root_dir(1);
        let mut builder = if self.options.sst_use_io_executor_writes {
            let io = self
                .sst_io_ctx
                .as_ref()
                .context("sst io executor writes requested but not configured")?
                .io()
                .clone();
            crate::sst::SstBuilder::create_with_io(&sst_dir, out_file_id, 64 * 1024, io)?
        } else {
            crate::sst::SstBuilder::create(&sst_dir, out_file_id, 64 * 1024)?
        };
        for (key, value) in &out_entries {
            builder.add(key, value.as_ref())?;
        }
        let props = builder.finish()?;
        self.apply_compaction_edit(compact_inputs, vec![(out_file_id, props)])?;
        Ok(())
    }

    pub(crate) fn install_sst(&self, file_id: u64, props: &SstProperties) -> anyhow::Result<()> {
        let add = AddFile {
            file_id,
            level: 0,
            smallest_user_key: props.smallest_user_key.clone(),
            largest_user_key: props.largest_user_key.clone(),
            max_seqno: props.max_seqno,
            table_root: props.table_root,
            size_bytes: props.data_bytes + props.index_bytes,
            tier: self.tier_for_level(0),
            sst_format_version: props.format_version,
        };
        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::AddFile(add.clone()), true)?;
            manifest.sync_dir()?;
        }
        self.levels.write().l0.push(add);
        self.snapshots.set_latest_seqno(props.max_seqno);
        Ok(())
    }

    pub fn ingest_external_sst(&self, source_path: &Path) -> anyhow::Result<(u64, u64)> {
        let source_reader = SstReader::open(source_path)
            .with_context(|| format!("open source sst {}", source_path.display()))?;
        let props = source_reader.properties().clone();

        if props.entries == 0 {
            anyhow::bail!("cannot ingest empty sst: {}", source_path.display());
        }
        if props.smallest_user_key > props.largest_user_key {
            anyhow::bail!(
                "invalid sst key range: smallest > largest ({})",
                source_path.display()
            );
        }

        let file_id = self.allocate_file_id();
        let sst_dir = self.sst_root_dir(0);
        std::fs::create_dir_all(&sst_dir)
            .with_context(|| format!("create sst dir {}", sst_dir.display()))?;

        let tmp_path = sst_dir.join(format!("sst_{file_id:016x}.tmp"));
        let final_path = sst_dir.join(format!("sst_{file_id:016x}.sst"));

        std::fs::copy(source_path, &tmp_path).with_context(|| {
            format!(
                "copy sst {} -> {}",
                source_path.display(),
                tmp_path.display()
            )
        })?;

        let tmp_fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tmp_path)
            .with_context(|| format!("open tmp sst {}", tmp_path.display()))?;
        tmp_fd
            .sync_data()
            .with_context(|| format!("sync tmp sst {}", tmp_path.display()))?;
        drop(tmp_fd);

        std::fs::rename(&tmp_path, &final_path).with_context(|| {
            format!("rename {} -> {}", tmp_path.display(), final_path.display())
        })?;
        let dir_fd = std::fs::File::open(&sst_dir)
            .with_context(|| format!("open sst dir {}", sst_dir.display()))?;
        dir_fd
            .sync_all()
            .with_context(|| format!("sync sst dir {}", sst_dir.display()))?;

        self.install_sst(file_id, &props)?;
        self.advance_current_branch(props.max_seqno)?;
        Ok((file_id, props.max_seqno))
    }

    fn apply_compaction_edit(
        &self,
        inputs: Vec<AddFile>,
        outputs: Vec<(u64, SstProperties)>,
    ) -> anyhow::Result<()> {
        let frozen_to_remove: Vec<FrozenObjectMeta> = {
            let frozen = self.frozen_objects.read();
            inputs
                .iter()
                .filter_map(|input| frozen.get(&input.file_id).cloned())
                .collect()
        };

        let mut adds = Vec::new();
        for (file_id, props) in outputs {
            adds.push(AddFile {
                file_id,
                level: 1,
                smallest_user_key: props.smallest_user_key.clone(),
                largest_user_key: props.largest_user_key.clone(),
                max_seqno: props.max_seqno,
                table_root: props.table_root,
                size_bytes: props.data_bytes + props.index_bytes,
                tier: self.tier_for_level(1),
                sst_format_version: props.format_version,
            });
        }

        let mut deletes = Vec::new();
        for input in &inputs {
            deletes.push(DeleteFile {
                file_id: input.file_id,
                level: input.level,
            });
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::VersionEdit(VersionEdit {
                    adds: adds.clone(),
                    deletes: deletes.clone(),
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        {
            let mut guard = self.levels.write();
            // Remove deleted inputs.
            for del in &deletes {
                match del.level {
                    0 => guard.l0.retain(|f| f.file_id != del.file_id),
                    1 => guard.l1.retain(|f| f.file_id != del.file_id),
                    _ => {}
                }
            }

            // Add compaction outputs to L1.
            guard.l1.extend(adds);
            guard
                .l1
                .sort_by(|a, b| a.smallest_user_key.cmp(&b.smallest_user_key));
        }

        {
            let mut frozen = self.frozen_objects.write();
            for input in &inputs {
                frozen.remove(&input.file_id);
            }
        }

        for frozen in frozen_to_remove {
            if let Ok(deleted) = self.delete_s3_object(&frozen) {
                if deleted > 0 {
                    self.tier_counters
                        .s3_deletes
                        .fetch_add(deleted as u64, Ordering::Relaxed);
                }
            }
        }

        // Once manifest deletions are durable, remove old files from disk.
        for input in &inputs {
            let path_nvme = self
                .dir
                .join("sst")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let path_hdd = self
                .dir
                .join("sst_hdd")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let path_cache = self
                .dir
                .join("sst_cache")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let _ = std::fs::remove_file(path_nvme);
            let _ = std::fs::remove_file(path_hdd);
            let _ = std::fs::remove_file(path_cache);
        }
        Ok(())
    }

    fn compaction_inputs_fully_cover_range(
        &self,
        selected_l0: &[AddFile],
        selected_l1: &[AddFile],
        smallest: &[u8],
        largest: &[u8],
    ) -> bool {
        let guard = self.levels.read();
        compaction_inputs_cover_overlaps(
            &guard.l0,
            &guard.l1,
            selected_l0,
            selected_l1,
            smallest,
            largest,
        )
    }
}
