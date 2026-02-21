use super::*;

pub struct SstBuilder {
    block_size: usize,
    file: Option<std::fs::File>,
    io_file: Option<std::fs::File>,
    path_tmp: PathBuf,
    path_final: PathBuf,
    buf: Vec<u8>,
    entries_in_block: u32,
    last_key: Option<InternalKey>,
    index: Vec<IndexEntry>,
    smallest_user_key: Option<Bytes>,
    largest_user_key: Option<Bytes>,
    max_seqno: u64,
    entries: u64,
    data_bytes: u64,
    table_hasher: blake3::Hasher,
    range_tombstones: Vec<RangeTombstone>,
    point_keys: Vec<Bytes>,
    entry_offsets: Vec<u32>,

    io: Option<crate::io::UringExecutor>,
    io_offset: u64,
    io_fixed_fd: Option<u32>,

    io_pending_writes: Vec<IoPendingWrite>,
    io_write_buf_pool: Vec<Vec<u8>>,
}

#[derive(Debug)]
struct IoPendingWrite {
    offset: u64,
    data: Vec<u8>,
}

fn max_bytes(a: Option<Bytes>, b: Bytes) -> Bytes {
    match a {
        Some(current) => std::cmp::max(current, b),
        None => b,
    }
}

impl SstBuilder {
    pub fn create(dir: &Path, file_id: u64, block_size: usize) -> Result<Self, SstError> {
        std::fs::create_dir_all(dir)?;
        let path_tmp = dir.join(format!("sst_{file_id:016x}.tmp"));
        let path_final = dir.join(format!("sst_{file_id:016x}.sst"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path_tmp)?;
        Ok(Self {
            block_size,
            file: Some(file),
            io_file: None,
            path_tmp,
            path_final,
            buf: Vec::with_capacity(block_size + 256),
            entries_in_block: 0,
            last_key: None,
            index: Vec::new(),
            smallest_user_key: None,
            largest_user_key: None,
            max_seqno: 0,
            entries: 0,
            data_bytes: 0,
            table_hasher: blake3::Hasher::new(),
            range_tombstones: Vec::new(),
            point_keys: Vec::new(),
            entry_offsets: Vec::new(),
            io: None,
            io_offset: 0,
            io_fixed_fd: None,
            io_pending_writes: Vec::new(),
            io_write_buf_pool: Vec::new(),
        })
    }

    pub fn create_with_io(
        dir: &Path,
        file_id: u64,
        block_size: usize,
        io: crate::io::UringExecutor,
    ) -> Result<Self, SstError> {
        std::fs::create_dir_all(dir)?;
        let path_tmp = dir.join(format!("sst_{file_id:016x}.tmp"));
        let path_final = dir.join(format!("sst_{file_id:016x}.sst"));
        {
            let _ = std::fs::remove_file(&path_tmp);
        }

        let io_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path_tmp)?;

        let io_fixed_fd = io.register_file_blocking(&io_file);

        Ok(Self {
            block_size,
            file: None,
            io_file: Some(io_file),
            path_tmp,
            path_final,
            buf: Vec::with_capacity(block_size + 256),
            entries_in_block: 0,
            last_key: None,
            index: Vec::new(),
            smallest_user_key: None,
            largest_user_key: None,
            max_seqno: 0,
            entries: 0,
            data_bytes: 0,
            table_hasher: blake3::Hasher::new(),
            range_tombstones: Vec::new(),
            point_keys: Vec::new(),
            entry_offsets: Vec::new(),
            io: Some(io),
            io_offset: 0,
            io_fixed_fd,
            io_pending_writes: Vec::new(),
            io_write_buf_pool: Vec::new(),
        })
    }

    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> Result<(), SstError> {
        if let Some(last) = &self.last_key {
            if key < last {
                return Err(SstError::Corrupt(
                    "internal keys must be added in sorted order",
                ));
            }
        }

        if self.smallest_user_key.is_none() {
            self.smallest_user_key = Some(key.user_key.clone());
        }

        match key.kind {
            KeyKind::RangeDel => {
                let end_key = Bytes::copy_from_slice(value);
                self.range_tombstones.push(RangeTombstone {
                    start_key: key.user_key.clone(),
                    end_key: end_key.clone(),
                    seqno: key.seqno,
                });
                self.smallest_user_key = Some(std::cmp::min(
                    self.smallest_user_key.clone().unwrap_or_default(),
                    key.user_key.clone(),
                ));
                self.largest_user_key = Some(max_bytes(self.largest_user_key.take(), end_key));
            }
            _ => {
                self.largest_user_key = Some(max_bytes(
                    self.largest_user_key.take(),
                    key.user_key.clone(),
                ));
                self.point_keys.push(key.user_key.clone());
            }
        }
        self.last_key = Some(key.clone());
        self.max_seqno = self.max_seqno.max(key.seqno);
        self.entries += 1;

        if self.entries_in_block == 0 {
            self.buf.extend_from_slice(&0u32.to_le_bytes());
        }

        let entry_offset: u32 = self
            .buf
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("sst block too large"))?;
        self.entry_offsets.push(entry_offset);
        key.encode_into(&mut self.buf);
        let val_len: u32 = value
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("value too large"))?;
        self.buf.extend_from_slice(&val_len.to_le_bytes());
        self.buf.extend_from_slice(value);
        self.entries_in_block += 1;
        self.buf[0..4].copy_from_slice(&self.entries_in_block.to_le_bytes());

        if self.buf.len() >= self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<SstProperties, SstError> {
        if self.entries_in_block > 0 {
            self.flush_block()?;
        }

        let index_offset = self.stream_position()?;

        let index_bytes =
            bincode::serialize(&self.index).map_err(|_| SstError::Corrupt("index serialize"))?;
        self.write_all(&index_bytes)?;
        let index_len: u32 = index_bytes
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("index too large"))?;

        let props_offset = self.stream_position()?;
        self.table_hasher.update(&index_bytes);
        let table_root = TableRoot(*self.table_hasher.finalize().as_bytes());
        let point_filter = build_point_filter(&self.point_keys)?;
        let props = SstProperties {
            smallest_user_key: self.smallest_user_key.clone().unwrap_or_default(),
            largest_user_key: self.largest_user_key.clone().unwrap_or_default(),
            max_seqno: self.max_seqno,
            entries: self.entries,
            data_bytes: self.data_bytes,
            index_bytes: index_bytes.len() as u64,
            table_root,
            format_version: 3,
            range_tombstones: self.range_tombstones.clone(),
            point_filter,
        };
        let props_bytes =
            bincode::serialize(&props).map_err(|_| SstError::Corrupt("props serialize"))?;
        let props_len: u32 = props_bytes
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("props too large"))?;
        self.write_all(&props_bytes)?;

        let footer = Footer {
            index_offset,
            index_len,
            props_offset,
            props_len,
            table_root,
        };
        let footer_bytes = encode_footer(&footer);
        self.write_all(&footer_bytes)?;
        self.write_all(MAGIC)?;
        self.sync_data()?;
        drop(self.file.take());
        drop(self.io_file.take());

        std::fs::rename(&self.path_tmp, &self.path_final)?;
        fsync_parent_dir(&self.path_final)?;
        Ok(props)
    }

    fn stream_position(&mut self) -> Result<u64, SstError> {
        if let Some(file) = &mut self.file {
            Ok(file.stream_position()?)
        } else {
            Ok(self.io_offset)
        }
    }

    fn take_io_write_buf(&mut self) -> Vec<u8> {
        self.io_write_buf_pool
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.block_size + 256))
    }

    fn flush_pending_writes(&mut self) -> Result<(), SstError> {
        if self.io_pending_writes.is_empty() {
            return Ok(());
        }

        let (Some(io), Some(file)) = (&self.io, self.io_file.as_ref()) else {
            return Err(SstError::Corrupt("sst builder missing output"));
        };

        let fixed_fd = self.io_fixed_fd;

        let refs: Vec<(u64, &[u8])> = self
            .io_pending_writes
            .iter()
            .map(|write| (write.offset, write.data.as_slice()))
            .collect();

        io.write_all_at_file_blocking_batch(file, fixed_fd, &refs)
            .map_err(|_| SstError::Corrupt("io write"))?;

        for mut pending in self.io_pending_writes.drain(..) {
            pending.data.clear();
            if self.io_write_buf_pool.len() < 256 {
                self.io_write_buf_pool.push(pending.data);
            }
        }

        Ok(())
    }

    fn write_vec(&mut self, data: Vec<u8>) -> Result<(), SstError> {
        if data.is_empty() {
            return Ok(());
        }

        let len = data.len();

        if let Some(file) = &mut self.file {
            file.write_all(&data)?;
        } else if self.io.is_some() {
            let offset = self.io_offset;
            self.io_pending_writes.push(IoPendingWrite { offset, data });
            if self.io_pending_writes.len() >= 64 {
                self.flush_pending_writes()?;
            }
        } else {
            return Err(SstError::Corrupt("sst builder missing output"));
        }

        self.io_offset = self
            .io_offset
            .checked_add(len as u64)
            .ok_or(SstError::Corrupt("sst size overflow"))?;
        Ok(())
    }

    fn write_all(&mut self, data: &[u8]) -> Result<(), SstError> {
        if let Some(file) = &mut self.file {
            file.write_all(data)?;
            self.io_offset = self
                .io_offset
                .checked_add(data.len() as u64)
                .ok_or(SstError::Corrupt("sst size overflow"))?;
            return Ok(());
        }

        if self.io.is_some() {
            return self.write_vec(data.to_vec());
        }

        Err(SstError::Corrupt("sst builder missing output"))
    }

    fn sync_data(&mut self) -> Result<(), SstError> {
        if let Some(file) = &self.file {
            file.sync_data()?;
            return Ok(());
        }

        if self.io.is_some() {
            self.flush_pending_writes()?;
        }

        if let (Some(io), Some(file)) = (&self.io, self.io_file.as_ref()) {
            if let Some(fixed) = self.io_fixed_fd {
                io.sync_file_file_blocking_fixed(file, fixed)
                    .map_err(|_| SstError::Corrupt("io sync"))?;
            } else {
                io.sync_file_file_blocking(file)
                    .map_err(|_| SstError::Corrupt("io sync"))?;
            }
            return Ok(());
        }

        Err(SstError::Corrupt("sst builder missing output"))
    }

    fn flush_block(&mut self) -> Result<(), SstError> {
        if self.entry_offsets.len() != self.entries_in_block as usize {
            return Err(SstError::Corrupt("sst block offset index mismatch"));
        }

        for offset in &self.entry_offsets {
            self.buf.extend_from_slice(&offset.to_le_bytes());
        }
        self.entry_offsets.clear();

        let payload_len = self.buf.len();
        let crc = RecordHasher::crc32c(&self.buf);
        let hash = RecordHasher::blake3(&self.buf);
        self.table_hasher.update(&hash.0);
        self.buf.extend_from_slice(&crc.0.to_le_bytes());
        self.buf.extend_from_slice(&hash.0);

        let offset = self.stream_position()?;
        let len: u32;
        if self.io.is_some() {
            let block = std::mem::take(&mut self.buf);
            len = block
                .len()
                .try_into()
                .map_err(|_| SstError::Corrupt("block too large"))?;
            self.buf = self.take_io_write_buf();
            self.buf.clear();
            self.write_vec(block)?;
        } else {
            let mut block = Vec::new();
            std::mem::swap(&mut block, &mut self.buf);
            len = block
                .len()
                .try_into()
                .map_err(|_| SstError::Corrupt("block too large"))?;
            self.write_all(&block)?;
            block.clear();
            std::mem::swap(&mut block, &mut self.buf);
        }

        let last_key = self
            .last_key
            .clone()
            .ok_or(SstError::Corrupt("missing last key"))?;
        self.index.push(IndexEntry {
            last_key,
            handle: BlockHandle { offset, len },
        });

        self.data_bytes += payload_len as u64;
        self.entries_in_block = 0;
        Ok(())
    }
}

impl Drop for SstBuilder {
    fn drop(&mut self) {
        let Some(io) = self.io.as_ref() else {
            return;
        };
        let Some(fixed) = self.io_fixed_fd.take() else {
            return;
        };
        io.unregister_file_blocking(fixed);
    }
}
