use super::*;

impl SpFreshLayerDbIndex {
    fn set_commit_error(&self, err: anyhow::Error) {
        let mut slot = lock_mutex(&self.commit_error);
        if slot.is_none() {
            *slot = Some(format!("{err:#}"));
        }
    }

    fn check_commit_error(&self) -> anyhow::Result<()> {
        if let Some(err) = lock_mutex(&self.commit_error).clone() {
            anyhow::bail!("async commit pipeline failed: {err}");
        }
        Ok(())
    }

    pub(super) fn use_nondurable_fast_path(&self) -> bool {
        false
    }

    fn handle_commit_result(&self, result: anyhow::Result<()>) -> anyhow::Result<()> {
        if let Err(err) = result {
            self.set_commit_error(anyhow::anyhow!("{err:#}"));
            return Err(err);
        }
        Ok(())
    }

    pub(super) fn poll_pending_commits(&self) -> anyhow::Result<()> {
        loop {
            let next = {
                let mut pending = lock_mutex(&self.pending_commit_acks);
                let Some(front) = pending.front() else {
                    break;
                };
                match front.try_recv() {
                    Ok(result) => {
                        pending.pop_front();
                        Some(result)
                    }
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        pending.pop_front();
                        Some(Err(anyhow::anyhow!(
                            "commit worker response channel disconnected"
                        )))
                    }
                }
            };
            let Some(result) = next else {
                break;
            };
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    fn throttle_pending_commits(&self) -> anyhow::Result<()> {
        while lock_mutex(&self.pending_commit_acks).len() > self.max_async_commit_inflight.max(1) {
            let Some(rx) = lock_mutex(&self.pending_commit_acks).pop_front() else {
                break;
            };
            let result = rx.recv().context("receive throttled async commit result")?;
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    pub(super) fn flush_pending_commits(&self) -> anyhow::Result<()> {
        loop {
            let next = lock_mutex(&self.pending_commit_acks).pop_front();
            let Some(rx) = next else {
                break;
            };
            let result = rx.recv().context("receive async commit result")?;
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    pub(super) fn submit_commit(
        &self,
        ops: Vec<layerdb::Op>,
        sync: bool,
        wait_for_ack: bool,
    ) -> anyhow::Result<()> {
        self.poll_pending_commits()?;
        let (resp_tx, resp_rx) = mpsc::channel::<anyhow::Result<()>>();
        self.commit_tx
            .send(CommitRequest::Write {
                ops,
                sync,
                resp: resp_tx,
            })
            .context("send commit request")?;
        if wait_for_ack {
            let result = resp_rx.recv().context("receive commit result")?;
            self.handle_commit_result(result)
        } else {
            lock_mutex(&self.pending_commit_acks).push_back(resp_rx);
            self.throttle_pending_commits()?;
            self.poll_pending_commits()
        }
    }
    pub(super) fn persist_with_wal_touch_batch_ids(
        &self,
        ids: &[u64],
        mut row_ops: Vec<layerdb::Op>,
        mut trailer_ops: Vec<layerdb::Op>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<()> {
        let start_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let next_seq = start_seq
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("spfresh wal sequence overflow"))?;
        row_ops.reserve(trailer_ops.len().saturating_add(2));
        let wal_value = encode_wal_touch_batch_ids(ids)?;
        row_ops.push(layerdb::Op::put(wal_key(start_seq), wal_value));
        let wal_next = encode_u64_fixed(next_seq);
        row_ops.push(layerdb::Op::put(
            config::META_INDEX_WAL_NEXT_SEQ_KEY,
            wal_next,
        ));
        row_ops.append(&mut trailer_ops);
        let sync = matches!(commit_mode, MutationCommitMode::Durable);
        self.submit_commit(row_ops, sync, true).with_context(|| {
            format!(
                "persist vector+wal touch-batch seq={} next={} ids={}",
                start_seq,
                next_seq,
                ids.len()
            )
        })?;
        self.wal_next_seq.store(next_seq, Ordering::Relaxed);
        Ok(())
    }
}
