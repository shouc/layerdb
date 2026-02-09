# LayerDB “Laws” (v1)

This document is the correctness contract for LayerDB’s internal design.
Implementation details may evolve, but these invariants must remain true.

## Product Surface (API v1)

- `put(key, value, WriteOptions)`
- `delete(key, WriteOptions)`
- `write_batch(Vec<Op>, WriteOptions)` (atomic)
- `get(key, ReadOptions { snapshot })`
- `iter(range, ReadOptions { snapshot })`
- `create_snapshot() -> SnapshotId`
- `release_snapshot(snapshot)`
- `create_branch(name, from_snapshot)` (optional)
- `checkout(branch)` (optional)
- `compact_range(optional range)` (manual trigger)
- `ingest_sst(sst_path)` (optional; not implemented in v1)

### v2 extension in-progress

- `delete_range(start, end, WriteOptions)` stores range tombstones and applies
  them in point lookups + iterators + compaction merges.

## Semantics

- **Read-your-writes per handle**: A `Db` handle uses an acknowledged seqno as
  the default read snapshot.
- **Snapshots**: A snapshot is a consistent read at a seqno.
- **Snapshot release**: Releasing a snapshot unpins it for future compaction
  safety calculations.
- **Deletes**: Deletes are tombstones; visibility depends on snapshot seqno.
- **Range deletes**: Implemented conservatively. Tombstones are represented as
  `RangeDel` internal entries where key=`start`, value=`end` (`[start, end)`).
  Read visibility is snapshot-aware and compares point-entry seqno vs the
  covering tombstone seqno.

## Storage Model

### InternalKey

`InternalKey = (user_key ASC, seqno DESC, kind)`

Kinds:

- `Put`
- `Del` (point tombstone)
- `Merge` (optional)
- `RangeDel` (v2)
- `Meta`

### LSM invariants

1. **Memtables contain newest seqnos**.
2. **Leveling**:
   - L0 files may overlap key ranges.
   - L1+ are non-overlapping (leveled).
3. **Visibility**: For any `user_key` at snapshot `S`, the first visible entry
   in merged internal-key order determines the result (`Put`/`Del`).
4. **Compaction safety**: Compaction must preserve visibility semantics for all
   snapshots `>= min_snapshot_seq`.

### Crash-consistency invariants

- If the manifest says an SST exists, its file exists and is durable.
- If an SST exists but is not referenced by the manifest, it is ignored and may
  be garbage collected.

## Crash consistency protocol (local filesystem)

### Creating a new SST safely

1. Write SST to temp path: `sst_{id}.tmp`
2. `fdatasync(tmp_fd)`
3. `rename(tmp, sst_{id}.sst)` (atomic)
4. `fsync(dir_fd)` (directory)
5. Append `AddFile` to manifest
6. `fdatasync(manifest_fd)`

### Deleting old SSTs

1. Append `DeleteFile` to manifest + sync
2. Unlink old files (or move to trash)
3. (Optional) `fsync(dir_fd)`
