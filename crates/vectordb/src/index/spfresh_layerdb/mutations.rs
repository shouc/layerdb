use super::*;
use rustc_hash::{FxHashMap, FxHashSet};

impl SpFreshLayerDbIndex {
    pub fn bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        self.try_bulk_load(rows)
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let _update_guard = lock_write(&self.update_gate);
        self.flush_pending_commits()?;

        let old_generation = self.active_generation.load(Ordering::Relaxed);
        let new_generation = old_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("active generation overflow"))?;
        let mut disk_assignments = None;
        let mut disk_index = None;
        let mut posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let (index, assignments) =
                SpFreshDiskMetaIndex::build_with_assignments(self.cfg.spfresh.clone(), rows);
            disk_assignments = Some(assignments);
            disk_index = Some(index);
        }

        for batch in rows.chunks(1_024) {
            let mut ops = Vec::with_capacity(batch.len().saturating_mul(3));
            for row in batch {
                if let Some(assignments) = &disk_assignments {
                    let posting = assignments.get(&row.id).copied().ok_or_else(|| {
                        anyhow::anyhow!("missing diskmeta assignment for id={}", row.id)
                    })?;
                    let value = encode_vector_row_value_with_posting(row, Some(posting))
                        .with_context(|| format!("serialize vector row id={}", row.id))?;
                    ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
                    let centroid = disk_index
                        .as_ref()
                        .and_then(|idx| idx.posting_centroid(posting))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing centroid for posting {} during diskmeta bulk-load",
                                posting
                            )
                        })?;
                    let event_seq = posting_event_next_seq;
                    posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                    ops.push(layerdb::Op::put(
                        posting_member_event_key(new_generation, posting, event_seq, row.id),
                        posting_member_event_upsert_value_with_residual(
                            row.id,
                            &row.values,
                            centroid,
                        )?,
                    ));
                } else {
                    let value = encode_vector_row_value(row)
                        .with_context(|| format!("serialize vector row id={}", row.id))?;
                    ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
                }
            }
            self.submit_commit(ops, true, true)
                .context("persist spfresh bulk rows")?;
        }
        set_active_generation(&self.db, new_generation, true)?;
        self.active_generation
            .store(new_generation, Ordering::Relaxed);
        let new_epoch = self
            .startup_epoch
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.rotate_epoch(new_epoch) {
                eprintln!("spfresh-layerdb vector blocks rotate failed: {err:#}");
            } else {
                for row in rows {
                    let posting = disk_assignments
                        .as_ref()
                        .and_then(|assignments| assignments.get(&row.id).copied());
                    if let Err(err) =
                        blocks.append_upsert_with_posting(row.id, posting, &row.values)
                    {
                        eprintln!("spfresh-layerdb vector blocks bulk append failed: {err:#}");
                        break;
                    }
                }
            }
        }
        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            set_posting_event_next_seq(&self.db, posting_event_next_seq, true)?;
            self.posting_event_next_seq
                .store(posting_event_next_seq, Ordering::Relaxed);
        }

        // best-effort cleanup of the old generation after pointer switch.
        for prefix in [
            vector_prefix(old_generation),
            posting_map_prefix(old_generation),
            posting_members_generation_prefix(old_generation),
        ] {
            let prefix_bytes = prefix.as_bytes().to_vec();
            let end = prefix_exclusive_end(&prefix_bytes)?;
            if let Err(err) = self
                .db
                .delete_range(prefix_bytes, end, WriteOptions { sync: false })
            {
                eprintln!("spfresh-layerdb bulk-load cleanup failed for prefix={prefix}: {err:#}");
            }
        }

        *lock_write(&self.index) = match self.cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(self.cfg.spfresh.clone(), rows))
            }
            SpFreshMemoryMode::OffHeap => RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(
                self.cfg.spfresh.clone(),
                rows,
            )),
            SpFreshMemoryMode::OffHeapDiskMeta => {
                RuntimeSpFreshIndex::OffHeapDiskMeta(disk_index.unwrap_or_else(|| {
                    SpFreshDiskMetaIndex::build_with_assignments(self.cfg.spfresh.clone(), rows).0
                }))
            }
        };
        let snapshot = {
            let guard = lock_read(&self.index);
            Self::extract_diskmeta_snapshot(&guard)
        };
        self.diskmeta_search_snapshot.store(snapshot);
        if self.use_nondurable_fast_path()
            && self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta
        {
            let mut ephemeral_states =
                FxHashMap::with_capacity_and_hasher(rows.len(), Default::default());
            if let Some(assignments) = disk_assignments.as_ref() {
                for row in rows {
                    let Some(posting_id) = assignments.get(&row.id).copied() else {
                        continue;
                    };
                    ephemeral_states.insert(row.id, (posting_id, row.values.clone()));
                }
            }
            *lock_mutex(&self.ephemeral_row_states) = Some(ephemeral_states);
            self.refresh_ephemeral_posting_members_from_storage()?;
        } else {
            *lock_mutex(&self.ephemeral_row_states) = None;
            *lock_mutex(&self.ephemeral_posting_members) = None;
        }
        {
            let mut cache = lock_mutex(&self.vector_cache);
            cache.map.clear();
            cache.order.clear();
        }
        lock_mutex(&self.posting_members_cache).clear();
        self.maybe_prewarm_vector_cache_from_blocks();
        self.maybe_prewarm_posting_members_cache();
        lock_mutex(&self.dirty_ids).clear();
        self.pending_ops.store(0, Ordering::Relaxed);
        self.stats.set_last_rebuild_rows(rows.len());
        self.persist_index_checkpoint()?;
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        self.flush_pending_commits()?;
        let runtime = self.runtime();
        rebuild_once(&runtime)?;
        self.maybe_prewarm_vector_cache_from_blocks();
        self.maybe_prewarm_posting_members_cache();
        Ok(())
    }

    fn dedup_last_upserts(rows: &[VectorRecord]) -> Vec<(u64, Vec<f32>)> {
        let mut seen = FxHashSet::with_capacity_and_hasher(rows.len(), Default::default());
        let mut out_rev = Vec::with_capacity(rows.len());
        for row in rows.iter().rev() {
            if seen.insert(row.id) {
                out_rev.push((row.id, row.values.clone()));
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn dedup_last_upserts_owned(rows: Vec<VectorRecord>) -> Vec<(u64, Vec<f32>)> {
        let mut seen = FxHashSet::with_capacity_and_hasher(rows.len(), Default::default());
        let mut out_rev = Vec::with_capacity(rows.len());
        for row in rows.into_iter().rev() {
            if seen.insert(row.id) {
                out_rev.push((row.id, row.values));
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn dedup_ids(ids: &[u64]) -> Vec<u64> {
        let mut seen = FxHashSet::with_capacity_and_hasher(ids.len(), Default::default());
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if seen.insert(*id) {
                out.push(*id);
            }
        }
        out
    }

    fn dedup_last_mutations(mutations: &[VectorMutation]) -> Vec<VectorMutation> {
        let mut seen = FxHashSet::with_capacity_and_hasher(mutations.len(), Default::default());
        let mut out_rev = Vec::with_capacity(mutations.len());
        for mutation in mutations.iter().rev() {
            if seen.insert(mutation.id()) {
                out_rev.push(mutation.clone());
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn dedup_last_mutations_owned(mutations: Vec<VectorMutation>) -> Vec<VectorMutation> {
        let mut seen = FxHashSet::with_capacity_and_hasher(mutations.len(), Default::default());
        let mut out_rev = Vec::with_capacity(mutations.len());
        for mutation in mutations.into_iter().rev() {
            if seen.insert(mutation.id()) {
                out_rev.push(mutation);
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn validate_mutations_dims(&self, mutations: &[VectorMutation]) -> anyhow::Result<()> {
        let expected_dim = self.cfg.spfresh.dim;
        for mutation in mutations {
            let VectorMutation::Upsert(row) = mutation else {
                continue;
            };
            if row.values.len() != expected_dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    expected_dim
                );
            }
        }
        Ok(())
    }

    fn load_diskmeta_states_for_ids(
        &self,
        generation: u64,
        ids: &[u64],
    ) -> anyhow::Result<DiskMetaStateMap> {
        let mut out = FxHashMap::with_capacity_and_hasher(ids.len(), Default::default());
        if ids.is_empty() {
            return Ok(out);
        }

        let mut unresolved = Vec::new();
        {
            let blocks = lock_mutex(&self.vector_blocks);
            for id in ids.iter().copied() {
                if let Some(state) = blocks.get_state(id) {
                    if let Some(posting_id) = state.posting_id {
                        out.insert(id, Some((posting_id, state.values)));
                    } else {
                        unresolved.push(id);
                    }
                } else {
                    unresolved.push(id);
                }
            }
        }

        if unresolved.is_empty() {
            return Ok(out);
        }

        let row_keys: Vec<Bytes> = unresolved
            .iter()
            .map(|id| Bytes::from(vector_key(generation, *id)))
            .collect();
        let row_values = self
            .db
            .multi_get(&row_keys, ReadOptions::default())
            .context("diskmeta multi_get fallback vector rows")?;

        for (id, row_raw) in unresolved.into_iter().zip(row_values.into_iter()) {
            let Some(raw) = row_raw else {
                out.insert(id, None);
                continue;
            };
            let decoded = decode_vector_row_with_posting(raw.as_ref())
                .with_context(|| format!("decode vector row id={id} generation={generation}"))?;
            if decoded.row.deleted {
                out.insert(id, None);
                continue;
            }
            if let Some(posting_id) = decoded.posting_id {
                out.insert(id, Some((posting_id, decoded.row.values)));
            } else {
                out.insert(id, None);
            }
        }
        Ok(out)
    }

    pub fn try_upsert_batch(&mut self, rows: &[VectorRecord]) -> anyhow::Result<usize> {
        self.try_upsert_batch_with_commit_mode(rows, MutationCommitMode::Durable)
    }

    pub fn try_upsert_batch_with_commit_mode(
        &mut self,
        rows: &[VectorRecord],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for row in rows {
            if row.values.len() != self.cfg.spfresh.dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    self.cfg.spfresh.dim
                );
            }
        }

        // Last-write-wins dedup keeps persistence/apply work proportional to unique ids.
        let mutations = Self::dedup_last_upserts(rows);
        self.try_upsert_batch_mutations(mutations, commit_mode)
    }

    pub fn try_upsert_batch_owned(&mut self, rows: Vec<VectorRecord>) -> anyhow::Result<usize> {
        self.try_upsert_batch_owned_with_commit_mode(rows, MutationCommitMode::Durable)
    }

    pub fn try_upsert_batch_owned_with_commit_mode(
        &mut self,
        rows: Vec<VectorRecord>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for row in &rows {
            if row.values.len() != self.cfg.spfresh.dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    self.cfg.spfresh.dim
                );
            }
        }
        let mutations = Self::dedup_last_upserts_owned(rows);
        self.try_upsert_batch_mutations(mutations, commit_mode)
    }

    pub(crate) fn try_upsert_batch_deduped_owned_with_commit_mode(
        &mut self,
        rows: Vec<VectorRecord>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut mutations = Vec::with_capacity(rows.len());
        for row in rows {
            if row.values.len() != self.cfg.spfresh.dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    self.cfg.spfresh.dim
                );
            }
            mutations.push((row.id, row.values));
        }
        self.try_upsert_batch_mutations(mutations, commit_mode)
    }

    fn try_upsert_batch_mutations(
        &mut self,
        mutations: Vec<(u64, Vec<f32>)>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if mutations.is_empty() {
            return Ok(0);
        }
        self.poll_pending_commits()?;

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let use_fast_path = self.use_nondurable_fast_path();
            let ids: Vec<u64> = mutations.iter().map(|(id, _)| *id).collect();
            let states = if use_fast_path {
                self.load_ephemeral_row_states_for_ids(&ids)
            } else {
                self.load_diskmeta_states_for_ids(generation, &ids)?
            };
            let mut shadow = match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.clone(),
                _ => anyhow::bail!("diskmeta upsert batch called for non-diskmeta index"),
            };
            let mut posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
            let mut batch_ops = Vec::with_capacity(mutations.len().saturating_mul(3));
            let mut new_postings =
                FxHashMap::with_capacity_and_hasher(mutations.len(), Default::default());
            let mut dirty_postings = FxHashSet::with_capacity_and_hasher(
                mutations.len().saturating_mul(2),
                Default::default(),
            );
            let mut posting_deltas = Vec::with_capacity(mutations.len());
            for (id, vector) in &mutations {
                let old = states.get(id).and_then(|state| state.as_ref());
                let new_posting = shadow.choose_posting(vector).unwrap_or(0);
                let old_posting = old.map(|(posting, _)| *posting);
                if !use_fast_path {
                    let value = encode_vector_row_fields(
                        *id,
                        0,
                        false,
                        vector.as_slice(),
                        Some(new_posting),
                    )
                    .context("serialize vector row")?;
                    batch_ops.push(layerdb::Op::put(vector_key(generation, *id), value));
                    let centroid = shadow
                        .posting_centroid(new_posting)
                        .unwrap_or(vector.as_slice());
                    let upsert_event_seq = posting_event_next_seq;
                    posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                    batch_ops.push(layerdb::Op::put(
                        posting_member_event_key(generation, new_posting, upsert_event_seq, *id),
                        posting_member_event_upsert_value_with_residual(*id, vector, centroid)?,
                    ));
                    if let Some(old_posting) = old_posting {
                        if old_posting != new_posting {
                            let tombstone_event_seq = posting_event_next_seq;
                            posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                            batch_ops.push(layerdb::Op::put(
                                posting_member_event_key(
                                    generation,
                                    old_posting,
                                    tombstone_event_seq,
                                    *id,
                                ),
                                posting_member_event_tombstone_value(*id)?,
                            ));
                        }
                    }
                }
                posting_deltas.push((*id, old_posting, new_posting));
                if let Some(old_posting) = old_posting {
                    dirty_postings.insert(old_posting);
                }
                dirty_postings.insert(new_posting);
                shadow.apply_upsert_ref(
                    old.map(|(posting, values)| (*posting, values.as_slice())),
                    new_posting,
                    vector.as_slice(),
                );
                new_postings.insert(*id, new_posting);
            }
            if use_fast_path {
                self.stats
                    .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            } else {
                let posting_event_next = encode_u64_fixed(posting_event_next_seq);
                let trailer_ops = vec![layerdb::Op::put(
                    config::META_POSTING_EVENT_NEXT_SEQ_KEY,
                    posting_event_next,
                )];
                if let Err(err) = self.persist_with_wal_touch_batch_ids(
                    ids.as_slice(),
                    batch_ops,
                    trailer_ops,
                    commit_mode,
                ) {
                    self.stats
                        .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
                    self.stats.inc_persist_errors();
                    return Err(err);
                }
                self.posting_event_next_seq
                    .store(posting_event_next_seq, Ordering::Relaxed);
                self.stats
                    .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            }
            {
                let mut blocks = lock_mutex(&self.vector_blocks);
                if let Err(err) = blocks.append_upsert_batch_with_posting(&mutations, |id| {
                    new_postings.get(&id).copied()
                }) {
                    eprintln!("spfresh-layerdb vector blocks upsert append failed: {err:#}");
                }
            }

            let snapshot = {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta upsert batch called for non-diskmeta runtime");
                };
                *index = shadow;
                Some(Arc::new(index.clone()))
            };
            self.diskmeta_search_snapshot.store(snapshot);
            let mutation_count = mutations.len();
            {
                let mut members_cache = lock_mutex(&self.posting_members_cache);
                for posting_id in dirty_postings {
                    members_cache.invalidate(generation, posting_id);
                }
            }
            if use_fast_path {
                let mut cache = lock_mutex(&self.vector_cache);
                for (id, vector) in &mutations {
                    cache.put(*id, vector.clone());
                }
                self.apply_ephemeral_row_upserts(&mutations, &new_postings);
                self.apply_ephemeral_posting_upsert_deltas(posting_deltas);
            } else {
                let mut cache = lock_mutex(&self.vector_cache);
                for (id, vector) in mutations {
                    cache.put(id, vector);
                }
            }
            for _ in 0..mutation_count {
                self.stats.inc_upserts();
            }
            return Ok(mutation_count);
        }

        if !self.use_nondurable_fast_path() {
            let mut batch_ops = Vec::with_capacity(mutations.len());
            let mut touched_ids = Vec::with_capacity(mutations.len());
            for (id, vector) in &mutations {
                let value = encode_vector_row_fields(*id, 0, false, vector.as_slice(), None)
                    .context("serialize vector row")?;
                batch_ops.push(layerdb::Op::put(vector_key(generation, *id), value));
                touched_ids.push(*id);
            }
            if let Err(err) = self.persist_with_wal_touch_batch_ids(
                touched_ids.as_slice(),
                batch_ops,
                Vec::new(),
                commit_mode,
            ) {
                self.stats
                    .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
        }
        self.stats
            .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.append_upsert_batch_with_posting(&mutations, |_| None) {
                eprintln!("spfresh-layerdb vector blocks upsert append failed: {err:#}");
            }
        }

        let mutation_count = mutations.len();
        let mut cache_entries = Vec::with_capacity(mutation_count);
        let mut touched_ids = Vec::with_capacity(mutation_count);
        {
            let mut index = lock_write(&self.index);
            for (id, vector) in mutations {
                match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(id, vector.clone()),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            &self.db,
                            &self.vector_cache,
                            &self.vector_blocks,
                            generation,
                            Some((id, vector.clone())),
                        );
                        index
                            .upsert_with(id, vector.clone(), &mut loader)
                            .with_context(|| format!("offheap upsert id={id}"))?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(_) => {
                        anyhow::bail!("non-diskmeta upsert batch called for diskmeta runtime");
                    }
                }
                touched_ids.push(id);
                cache_entries.push((id, vector));
            }
        }
        {
            let mut cache = lock_mutex(&self.vector_cache);
            for (id, vector) in cache_entries {
                cache.put(id, vector);
            }
        }
        self.mark_dirty_batch(touched_ids.as_slice());
        for _ in 0..mutation_count {
            self.stats.inc_upserts();
        }
        let pending = self.pending_ops.load(Ordering::Relaxed);
        if pending >= self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
        Ok(mutation_count)
    }

    pub fn try_delete_batch(&mut self, ids: &[u64]) -> anyhow::Result<usize> {
        self.try_delete_batch_with_commit_mode(ids, MutationCommitMode::Durable)
    }

    pub fn try_delete_batch_with_commit_mode(
        &mut self,
        ids: &[u64],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mutations = Self::dedup_ids(ids);
        self.try_delete_batch_deduped_owned_with_commit_mode(mutations, commit_mode)
    }

    pub(crate) fn try_delete_batch_deduped_owned_with_commit_mode(
        &mut self,
        mutations: Vec<u64>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if mutations.is_empty() {
            return Ok(0);
        }
        self.poll_pending_commits()?;

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let use_fast_path = self.use_nondurable_fast_path();
            let states = if use_fast_path {
                self.load_ephemeral_row_states_for_ids(&mutations)
            } else {
                self.load_diskmeta_states_for_ids(generation, &mutations)?
            };
            let mut shadow = match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.clone(),
                _ => anyhow::bail!("diskmeta delete batch called for non-diskmeta index"),
            };
            let mut batch_ops = Vec::with_capacity(mutations.len().saturating_mul(2));
            let mut deleted = 0usize;
            let mut dirty_postings =
                FxHashSet::with_capacity_and_hasher(mutations.len(), Default::default());
            let mut posting_delete_deltas = Vec::with_capacity(mutations.len());
            let mut posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
            for id in &mutations {
                let old = states.get(id).cloned().unwrap_or(None);
                if !use_fast_path {
                    batch_ops.push(layerdb::Op::delete(vector_key(generation, *id)));
                }
                if let Some((old_posting, _)) = &old {
                    dirty_postings.insert(*old_posting);
                    posting_delete_deltas.push((*id, *old_posting));
                    if !use_fast_path {
                        let tombstone_event_seq = posting_event_next_seq;
                        posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                        batch_ops.push(layerdb::Op::put(
                            posting_member_event_key(
                                generation,
                                *old_posting,
                                tombstone_event_seq,
                                *id,
                            ),
                            posting_member_event_tombstone_value(*id)?,
                        ));
                    }
                }
                if shadow.apply_delete(old) {
                    deleted = deleted.saturating_add(1);
                }
            }
            if use_fast_path {
                self.stats
                    .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            } else {
                let posting_event_next = encode_u64_fixed(posting_event_next_seq);
                let trailer_ops = vec![layerdb::Op::put(
                    config::META_POSTING_EVENT_NEXT_SEQ_KEY,
                    posting_event_next,
                )];
                if let Err(err) = self.persist_with_wal_touch_batch_ids(
                    mutations.as_slice(),
                    batch_ops,
                    trailer_ops,
                    commit_mode,
                ) {
                    self.stats
                        .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
                    self.stats.inc_persist_errors();
                    return Err(err);
                }
                self.posting_event_next_seq
                    .store(posting_event_next_seq, Ordering::Relaxed);
                self.stats
                    .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            }
            {
                let mut blocks = lock_mutex(&self.vector_blocks);
                if let Err(err) = blocks.append_delete_batch(&mutations) {
                    eprintln!("spfresh-layerdb vector blocks tombstone append failed: {err:#}");
                }
            }

            let snapshot = {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta delete batch called for non-diskmeta runtime");
                };
                *index = shadow;
                Some(Arc::new(index.clone()))
            };
            self.diskmeta_search_snapshot.store(snapshot);
            {
                let mut cache = lock_mutex(&self.vector_cache);
                for id in &mutations {
                    cache.remove(*id);
                }
            }
            {
                let mut members_cache = lock_mutex(&self.posting_members_cache);
                for posting_id in dirty_postings {
                    members_cache.invalidate(generation, posting_id);
                }
            }
            if use_fast_path {
                self.apply_ephemeral_row_deletes(&mutations);
                self.apply_ephemeral_posting_delete_deltas(posting_delete_deltas);
            }
            for _ in 0..deleted {
                self.stats.inc_deletes();
            }
            return Ok(deleted);
        }

        if !self.use_nondurable_fast_path() {
            let mut batch_ops = Vec::with_capacity(mutations.len());
            let mut touched_ids = Vec::with_capacity(mutations.len());
            for id in &mutations {
                batch_ops.push(layerdb::Op::delete(vector_key(generation, *id)));
                touched_ids.push(*id);
            }
            if let Err(err) = self.persist_with_wal_touch_batch_ids(
                touched_ids.as_slice(),
                batch_ops,
                Vec::new(),
                commit_mode,
            ) {
                self.stats
                    .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
        }
        self.stats
            .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.append_delete_batch(&mutations) {
                eprintln!("spfresh-layerdb vector blocks tombstone append failed: {err:#}");
            }
        }

        let mut deleted = 0usize;
        let mut deleted_ids = Vec::new();
        {
            let mut index = lock_write(&self.index);
            for id in &mutations {
                let was_deleted = match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.delete(*id),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            &self.db,
                            &self.vector_cache,
                            &self.vector_blocks,
                            generation,
                            None,
                        );
                        index
                            .delete_with(*id, &mut loader)
                            .with_context(|| format!("offheap delete id={id}"))?
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(_) => {
                        anyhow::bail!("non-diskmeta delete batch called for diskmeta runtime");
                    }
                };
                if was_deleted {
                    deleted += 1;
                    deleted_ids.push(*id);
                }
            }
        }
        self.mark_dirty_batch(deleted_ids.as_slice());
        {
            let mut cache = lock_mutex(&self.vector_cache);
            for id in &mutations {
                cache.remove(*id);
            }
        }
        for _ in 0..deleted {
            self.stats.inc_deletes();
        }
        if deleted > 0 {
            let pending = self.pending_ops.load(Ordering::Relaxed);
            if pending >= self.cfg.rebuild_pending_ops.max(1) {
                let _ = self.rebuild_tx.send(());
            }
        }
        Ok(deleted)
    }

    pub fn try_apply_batch(
        &mut self,
        mutations: &[VectorMutation],
    ) -> anyhow::Result<VectorMutationBatchResult> {
        self.try_apply_batch_with_commit_mode(mutations, MutationCommitMode::Durable)
    }

    pub fn try_apply_batch_with_commit_mode(
        &mut self,
        mutations: &[VectorMutation],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if mutations.is_empty() {
            return Ok(VectorMutationBatchResult::default());
        }
        self.validate_mutations_dims(mutations)?;
        let deduped = Self::dedup_last_mutations(mutations);
        self.try_apply_batch_deduped_owned_with_commit_mode(deduped, commit_mode)
    }

    pub fn try_apply_batch_owned(
        &mut self,
        mutations: Vec<VectorMutation>,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        self.try_apply_batch_owned_with_commit_mode(mutations, MutationCommitMode::Durable)
    }

    pub fn try_apply_batch_owned_with_commit_mode(
        &mut self,
        mutations: Vec<VectorMutation>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if mutations.is_empty() {
            return Ok(VectorMutationBatchResult::default());
        }
        self.validate_mutations_dims(mutations.as_slice())?;
        let deduped = Self::dedup_last_mutations_owned(mutations);
        self.try_apply_batch_deduped_owned_with_commit_mode(deduped, commit_mode)
    }

    pub(crate) fn try_apply_batch_deduped_owned_with_commit_mode(
        &mut self,
        deduped: Vec<VectorMutation>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for mutation in deduped {
            match mutation {
                VectorMutation::Upsert(row) => upserts.push(row),
                VectorMutation::Delete { id } => deletes.push(id),
            }
        }
        let upserted =
            self.try_upsert_batch_deduped_owned_with_commit_mode(upserts, commit_mode)?;
        let deleted = self.try_delete_batch_deduped_owned_with_commit_mode(deletes, commit_mode)?;
        Ok(VectorMutationBatchResult {
            upserts: upserted,
            deletes: deleted,
        })
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        let row = VectorRecord::new(id, vector);
        let _ = self.try_upsert_batch_with_commit_mode(&[row], MutationCommitMode::Durable)?;
        Ok(())
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        Ok(self.try_delete_batch_with_commit_mode(&[id], MutationCommitMode::Durable)? > 0)
    }
}
