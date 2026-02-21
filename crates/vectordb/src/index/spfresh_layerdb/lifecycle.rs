use super::*;

impl SpFreshLayerDbIndex {
    pub fn close(mut self) -> anyhow::Result<()> {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                anyhow::bail!("spfresh-layerdb background worker panicked");
            }
        }
        self.flush_pending_commits()?;
        self.force_rebuild()?;
        self.persist_index_checkpoint()?;
        let _ = self.commit_tx.send(CommitRequest::Shutdown);
        if let Some(worker) = self.commit_worker.take() {
            if worker.join().is_err() {
                anyhow::bail!("spfresh-layerdb commit worker panicked");
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> SpFreshLayerDbStats {
        self.stats
            .snapshot(self.pending_ops.load(Ordering::Relaxed) as u64)
    }

    pub fn memory_mode(&self) -> SpFreshMemoryMode {
        self.cfg.memory_mode
    }

    pub(crate) fn vector_dim(&self) -> usize {
        self.cfg.spfresh.dim
    }

    pub fn health_check(&self) -> anyhow::Result<SpFreshLayerDbStats> {
        ensure_wal_exists(&self.db_path)?;
        ensure_metadata(&self.db, &self.cfg)?;
        let _ = ensure_active_generation(&self.db)?;
        Ok(self.stats())
    }

    /// Flush and compact vector data, then freeze level-1 SSTs into S3 tier.
    ///
    /// This is the primary durability/tiering operation for SPFresh-on-LayerDB.
    pub fn sync_to_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .compact_range(None)
            .context("compact before freeze-to-s3")?;
        self.db
            .freeze_level_to_s3(1, max_files)
            .context("freeze level-1 to s3")
    }

    /// Thaw frozen level-1 SSTs back to local tier.
    pub fn thaw_from_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .thaw_level_from_s3(1, max_files)
            .context("thaw level-1 from s3")
    }

    /// Garbage collect orphaned S3 objects that are no longer referenced.
    pub fn gc_orphaned_s3(&self) -> anyhow::Result<usize> {
        self.db
            .gc_orphaned_s3_files()
            .context("gc orphaned s3 files")
    }

    pub fn frozen_objects(&self) -> Vec<layerdb::version::FrozenObjectMeta> {
        self.db.frozen_objects()
    }

    pub fn snapshot_rows(&self) -> anyhow::Result<Vec<VectorRecord>> {
        let _update_guard = lock_write(&self.update_gate);
        self.flush_pending_commits()?;
        let generation = self.active_generation.load(Ordering::Relaxed);
        load_rows(&self.db, generation)
            .with_context(|| format!("load snapshot rows generation={generation}"))
    }
}

impl Drop for SpFreshLayerDbIndex {
    fn drop(&mut self) {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if let Err(err) = self.flush_pending_commits() {
            eprintln!("spfresh-layerdb flush pending commits on drop failed: {err:#}");
        }
        let _ = self.commit_tx.send(CommitRequest::Shutdown);
        if let Some(worker) = self.commit_worker.take() {
            let _ = worker.join();
        }
    }
}

impl VectorIndex for SpFreshLayerDbIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        self.try_upsert(id, vector)
            .unwrap_or_else(|err| panic!("spfresh-layerdb upsert failed for id={id}: {err:#}"));
    }

    fn delete(&mut self, id: u64) -> bool {
        self.try_delete(id)
            .unwrap_or_else(|err| panic!("spfresh-layerdb delete failed for id={id}: {err:#}"))
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let started = Instant::now();
        let generation = self.active_generation.load(Ordering::Relaxed);
        let diskmeta_snapshot = self.diskmeta_search_snapshot.load_full();
        let out = if let Some(index) = diskmeta_snapshot {
            let mut top = Vec::with_capacity(k);
            let postings = index.choose_probe_postings(query, k);
            for posting_id in postings {
                if index.posting_centroid(posting_id).is_none() {
                    continue;
                }
                let members = self
                    .load_posting_members_for(generation, posting_id)
                    .unwrap_or_else(|err| panic!("offheap-diskmeta load members failed: {err:#}"));
                let selected_ids = Self::select_diskmeta_candidates(members.as_ref());
                if selected_ids.is_empty() {
                    continue;
                }
                let (selected_exact, missing_selected_ids) = Self::load_distances_for_ids(
                    &self.db,
                    &self.vector_cache,
                    &self.vector_blocks,
                    generation,
                    &selected_ids,
                    query,
                )
                .unwrap_or_else(|err| {
                    panic!("offheap-diskmeta load selected distances failed: {err:#}")
                });
                for (id, distance) in selected_exact {
                    Self::push_neighbor_topk(&mut top, Neighbor { id, distance }, k);
                }

                // If selected candidates miss exact payloads, retry exact evaluation only for the
                // unresolved ids and fail closed if any referenced row remains missing.
                if !missing_selected_ids.is_empty() {
                    let (fallback_exact, fallback_missing) = Self::load_distances_for_ids(
                        &self.db,
                        &self.vector_cache,
                        &self.vector_blocks,
                        generation,
                        &missing_selected_ids,
                        query,
                    )
                    .unwrap_or_else(|err| {
                        panic!("offheap-diskmeta load fallback distances failed: {err:#}")
                    });
                    for (id, distance) in fallback_exact {
                        Self::push_neighbor_topk(&mut top, Neighbor { id, distance }, k);
                    }
                    if !fallback_missing.is_empty() {
                        panic!(
                            "offheap-diskmeta search missing exact vector payloads for {} ids in posting {}",
                            fallback_missing.len(),
                            posting_id
                        );
                    }
                }
            }
            top.sort_by(Self::neighbor_cmp);
            top
        } else {
            match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::Resident(index) => index.search(query, k),
                RuntimeSpFreshIndex::OffHeap(index) => {
                    let mut loader = Self::loader_for(
                        &self.db,
                        &self.vector_cache,
                        &self.vector_blocks,
                        generation,
                        None,
                    );
                    match index.search_with(query, k, &mut loader) {
                        Ok(out) => out,
                        Err(err) => panic!("spfresh-layerdb offheap search failed: {err:#}"),
                    }
                }
                RuntimeSpFreshIndex::OffHeapDiskMeta(_) => unreachable!("diskmeta handled above"),
            }
        };
        self.stats
            .record_search(started.elapsed().as_micros() as u64);
        out
    }

    fn len(&self) -> usize {
        match &*lock_read(&self.index) {
            RuntimeSpFreshIndex::Resident(index) => index.len(),
            RuntimeSpFreshIndex::OffHeap(index) => index.len(),
            RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.len(),
        }
    }
}
