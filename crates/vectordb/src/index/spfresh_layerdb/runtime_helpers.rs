use super::*;

impl SpFreshLayerDbIndex {
    pub(super) fn load_vector_for_id(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        id: u64,
    ) -> anyhow::Result<Option<Vec<f32>>> {
        if let Some(values) = lock_mutex(vector_cache).get(id) {
            return Ok(Some(values));
        }
        if let Some(values) = lock_mutex(vector_blocks).get(id) {
            lock_mutex(vector_cache).put(id, values.clone());
            return Ok(Some(values));
        }
        let Some(row) = load_row(db, generation, id)? else {
            return Ok(None);
        };
        let values = row.values;
        lock_mutex(vector_cache).put(id, values.clone());
        Ok(Some(values))
    }

    pub(super) fn load_distances_for_ids(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        ids: &[u64],
        query: &[f32],
    ) -> anyhow::Result<DistanceLoadResult> {
        let mut out = Vec::with_capacity(ids.len());
        let mut cache_misses = Vec::new();
        {
            let cache = lock_mutex(vector_cache);
            for id in ids.iter().copied() {
                if let Some(distance) = cache.distance_to(id, query) {
                    out.push((id, distance));
                } else {
                    cache_misses.push(id);
                }
            }
        }
        if cache_misses.is_empty() {
            return Ok((out, Vec::new()));
        }

        let unresolved = {
            let blocks = lock_mutex(vector_blocks);
            let (mut found, missing) = blocks.distances_for_ids(cache_misses.as_slice(), query);
            out.append(&mut found);
            missing
        };

        let mut missing = Vec::new();
        let mut fetched_for_cache = Vec::new();
        if !unresolved.is_empty() {
            let keys: Vec<Bytes> = unresolved
                .iter()
                .map(|id| Bytes::from(vector_key(generation, *id)))
                .collect();
            let rows = db
                .multi_get(&keys, ReadOptions::default())
                .context("diskmeta multi_get fallback vector rows for distance")?;
            for (id, row_raw) in unresolved.into_iter().zip(rows.into_iter()) {
                let Some(raw) = row_raw else {
                    missing.push(id);
                    continue;
                };
                let row = decode_vector_row_value(raw.as_ref()).with_context(|| {
                    format!("decode vector row for distance id={id} generation={generation}")
                })?;
                if row.deleted {
                    missing.push(id);
                    continue;
                }
                let distance = squared_l2(query, row.values.as_slice());
                out.push((id, distance));
                fetched_for_cache.push((id, row.values));
            }
        }

        if !fetched_for_cache.is_empty() {
            let mut cache = lock_mutex(vector_cache);
            for (id, values) in fetched_for_cache {
                cache.put(id, values);
            }
        }
        Ok((out, missing))
    }

    pub(super) fn loader_for<'a>(
        db: &'a Db,
        vector_cache: &'a Arc<Mutex<VectorCache>>,
        vector_blocks: &'a Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        override_row: Option<(u64, Vec<f32>)>,
    ) -> impl FnMut(u64) -> anyhow::Result<Option<Vec<f32>>> + 'a {
        move |id| {
            if let Some((override_id, values)) = &override_row {
                if *override_id == id {
                    return Ok(Some(values.clone()));
                }
            }
            Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)
        }
    }

    pub(super) fn load_posting_members_for(
        &self,
        generation: u64,
        posting_id: usize,
    ) -> anyhow::Result<Arc<Vec<u64>>> {
        if self.use_nondurable_fast_path() {
            if let Some(members) = self.load_ephemeral_posting_members_for(posting_id) {
                return Ok(members);
            }
        }
        if let Some(ids) = lock_mutex(&self.posting_members_cache).get(generation, posting_id) {
            return Ok(ids);
        }
        let loaded = load_posting_members(&self.db, generation, posting_id)?;
        if loaded.scanned_events >= POSTING_LOG_COMPACT_MIN_EVENTS
            && loaded.scanned_events
                > loaded
                    .members
                    .len()
                    .saturating_mul(POSTING_LOG_COMPACT_FACTOR)
        {
            if let Err(err) =
                self.compact_posting_members_log(generation, posting_id, &loaded.members)
            {
                eprintln!(
                    "spfresh-layerdb posting log compaction failed generation={} posting={}: {err:#}",
                    generation, posting_id
                );
            }
        }
        let mut ids = Vec::with_capacity(loaded.members.len());
        for member in loaded.members {
            ids.push(member.id);
        }
        ids.sort_unstable();
        let ids = Arc::new(ids);
        lock_mutex(&self.posting_members_cache).put_arc(generation, posting_id, ids.clone());
        Ok(ids)
    }

    fn compact_posting_members_log(
        &self,
        generation: u64,
        posting_id: usize,
        members: &[PostingMember],
    ) -> anyhow::Result<()> {
        let mut canonical = members.to_vec();
        canonical.sort_by_key(|m| m.id);

        let mut next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        let prefix = posting_members_prefix(generation, posting_id);
        let prefix_bytes = prefix.into_bytes();
        let end = prefix_exclusive_end(&prefix_bytes)?;
        let mut ops = vec![layerdb::Op::delete_range(prefix_bytes, end)];
        for member in canonical {
            let seq = next_seq;
            next_seq = next_seq.saturating_add(1);
            let scale = member.residual_scale.unwrap_or(1.0);
            let code = member.residual_code.unwrap_or_default();
            let value = posting_member_event_upsert_value_from_sketch(member.id, scale, &code)?;
            ops.push(layerdb::Op::put(
                posting_member_event_key(generation, posting_id, seq, member.id),
                value,
            ));
        }
        let posting_event_next =
            bincode::serialize(&next_seq).context("encode posting-event next seq")?;
        ops.push(layerdb::Op::put(
            config::META_POSTING_EVENT_NEXT_SEQ_KEY,
            posting_event_next,
        ));
        self.submit_commit(ops, false, true)
            .context("compact posting-member append log")?;
        self.posting_event_next_seq
            .store(next_seq, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn refresh_ephemeral_posting_members_from_storage(&self) -> anyhow::Result<()> {
        if !self.use_nondurable_fast_path()
            || self.cfg.memory_mode != SpFreshMemoryMode::OffHeapDiskMeta
        {
            *lock_mutex(&self.ephemeral_posting_members) = None;
            return Ok(());
        }
        let generation = self.active_generation.load(Ordering::Relaxed);
        let mut refreshed: EphemeralPostingMembers = HashMap::new();
        if let Some(snapshot) = self.diskmeta_search_snapshot.load_full() {
            for posting_id in snapshot.posting_ids() {
                let loaded = load_posting_members(&self.db, generation, posting_id)
                    .with_context(|| format!("load posting members for posting={posting_id}"))?;
                let mut posting_members =
                    FxHashSet::with_capacity_and_hasher(loaded.members.len(), Default::default());
                for member in loaded.members {
                    posting_members.insert(member.id);
                }
                refreshed.insert(posting_id, posting_members);
            }
        }
        *lock_mutex(&self.ephemeral_posting_members) = Some(refreshed);
        Ok(())
    }

    pub(super) fn refresh_ephemeral_row_states_from_storage(&self) -> anyhow::Result<()> {
        if !self.use_nondurable_fast_path()
            || self.cfg.memory_mode != SpFreshMemoryMode::OffHeapDiskMeta
        {
            *lock_mutex(&self.ephemeral_row_states) = None;
            return Ok(());
        }
        let generation = self.active_generation.load(Ordering::Relaxed);
        let (rows, assignments) = load_rows_with_posting_assignments(&self.db, generation)?;
        let mut refreshed: EphemeralRowStates = HashMap::with_capacity(rows.len());
        for row in rows {
            let Some(posting_id) = assignments.get(&row.id).copied() else {
                continue;
            };
            refreshed.insert(row.id, (posting_id, row.values));
        }
        *lock_mutex(&self.ephemeral_row_states) = Some(refreshed);
        Ok(())
    }

    pub(super) fn load_ephemeral_row_states_for_ids(&self, ids: &[u64]) -> DiskMetaStateMap {
        let guard = lock_mutex(&self.ephemeral_row_states);
        let mut out = HashMap::with_capacity(ids.len());
        if let Some(states) = guard.as_ref() {
            for id in ids {
                out.insert(*id, states.get(id).cloned());
            }
        } else {
            for id in ids {
                out.insert(*id, None);
            }
        }
        out
    }

    pub(super) fn apply_ephemeral_row_upserts(
        &self,
        mutations: &[(u64, Vec<f32>)],
        new_postings: &HashMap<u64, usize>,
    ) {
        let mut guard = lock_mutex(&self.ephemeral_row_states);
        let Some(states) = guard.as_mut() else {
            return;
        };
        for (id, vector) in mutations {
            let Some(new_posting) = new_postings.get(id).copied() else {
                continue;
            };
            states.insert(*id, (new_posting, vector.clone()));
        }
    }

    pub(super) fn apply_ephemeral_row_deletes(&self, ids: &[u64]) {
        let mut guard = lock_mutex(&self.ephemeral_row_states);
        let Some(states) = guard.as_mut() else {
            return;
        };
        for id in ids {
            states.remove(id);
        }
    }

    pub(super) fn apply_ephemeral_posting_upsert_deltas(
        &self,
        deltas: Vec<(u64, Option<usize>, usize)>,
    ) {
        if deltas.is_empty() {
            return;
        }
        let mut guard = lock_mutex(&self.ephemeral_posting_members);
        let Some(postings) = guard.as_mut() else {
            return;
        };
        for (id, old_posting, new_posting) in deltas {
            if let Some(old_posting) = old_posting {
                if old_posting != new_posting {
                    let mut remove_old = false;
                    if let Some(members) = postings.get_mut(&old_posting) {
                        members.remove(&id);
                        remove_old = members.is_empty();
                    }
                    if remove_old {
                        postings.remove(&old_posting);
                    }
                }
            }
            postings
                .entry(new_posting)
                .or_insert_with(FxHashSet::default)
                .insert(id);
        }
    }

    pub(super) fn apply_ephemeral_posting_delete_deltas(&self, deltas: Vec<(u64, usize)>) {
        if deltas.is_empty() {
            return;
        }
        let mut guard = lock_mutex(&self.ephemeral_posting_members);
        let Some(postings) = guard.as_mut() else {
            return;
        };
        for (id, posting_id) in deltas {
            let mut remove_posting = false;
            if let Some(members) = postings.get_mut(&posting_id) {
                members.remove(&id);
                remove_posting = members.is_empty();
            }
            if remove_posting {
                postings.remove(&posting_id);
            }
        }
    }

    fn load_ephemeral_posting_members_for(&self, posting_id: usize) -> Option<Arc<Vec<u64>>> {
        let guard = lock_mutex(&self.ephemeral_posting_members);
        let postings = guard.as_ref()?;
        let members = postings.get(&posting_id)?;
        let mut out: Vec<u64> = members.iter().copied().collect();
        out.sort_unstable();
        Some(Arc::new(out))
    }

    pub(super) fn neighbor_cmp(a: &Neighbor, b: &Neighbor) -> std::cmp::Ordering {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.id.cmp(&b.id))
    }

    pub(super) fn push_neighbor_topk(top: &mut Vec<Neighbor>, candidate: Neighbor, k: usize) {
        if k == 0 {
            return;
        }
        if top.len() < k {
            top.push(candidate);
            return;
        }
        let mut worst_idx = 0usize;
        for idx in 1..top.len() {
            if Self::neighbor_cmp(&top[idx], &top[worst_idx]).is_gt() {
                worst_idx = idx;
            }
        }
        if Self::neighbor_cmp(&candidate, &top[worst_idx]).is_lt() {
            top[worst_idx] = candidate;
        }
    }

    pub(super) fn mark_dirty(&self, id: u64) {
        let mut dirty = lock_mutex(&self.dirty_ids);
        dirty.insert(id);
        self.pending_ops.store(dirty.len(), Ordering::Relaxed);
    }

    pub(super) fn maybe_prewarm_vector_cache_from_blocks(&self) {
        let capacity = lock_mutex(&self.vector_cache).capacity;
        if capacity == 0 {
            return;
        }

        let warmed = {
            let blocks = lock_mutex(&self.vector_blocks);
            if blocks.live_len() > capacity {
                Vec::new()
            } else {
                let ids = blocks.live_ids();
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(values) = blocks.get(id) {
                        rows.push((id, values));
                    }
                }
                rows
            }
        };
        if warmed.is_empty() {
            return;
        }

        let mut cache = lock_mutex(&self.vector_cache);
        for (id, values) in warmed {
            cache.put(id, values);
        }
    }

    pub(super) fn maybe_prewarm_posting_members_cache(&self) {
        if self.cfg.memory_mode != SpFreshMemoryMode::OffHeapDiskMeta {
            return;
        }
        let generation = self.active_generation.load(Ordering::Relaxed);
        let Some(index) = self.diskmeta_search_snapshot.load_full() else {
            return;
        };
        let posting_ids = index.posting_ids();
        if posting_ids.is_empty() {
            return;
        }
        let capacity = lock_mutex(&self.posting_members_cache).capacity();
        if posting_ids.len() > capacity {
            return;
        }

        for posting_id in posting_ids {
            if let Err(err) = self.load_posting_members_for(generation, posting_id) {
                eprintln!(
                    "spfresh-layerdb posting-members cache prewarm failed generation={} posting={}: {err:#}",
                    generation, posting_id
                );
                break;
            }
        }
    }
}
