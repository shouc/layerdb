use proptest::prelude::*;

use layerdb::{Db, DbOptions, ReadOptions, SnapshotId, WriteOptions};
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum MiniOp {
    Put(u8),
    Del,
}

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

fn op_strategy() -> impl Strategy<Value = MiniOp> {
    prop_oneof![(0u8..=5u8).prop_map(MiniOp::Put), Just(MiniOp::Del)]
}

fn apply_model(state: &mut Option<u8>, op: &MiniOp) {
    match op {
        MiniOp::Put(v) => *state = Some(*v),
        MiniOp::Del => *state = None,
    }
}

proptest! {
    #[test]
    fn compaction_preserves_branch_snapshot_visibility(
        ops in prop::collection::vec(op_strategy(), 1..30),
    ) {
        let dir = TempDir::new().expect("tempdir");
        let db = Db::open(dir.path(), options()).expect("open db");

        let mut expected_main: Option<u8> = None;

        // Apply first op, create branch, then continue with random updates.
        match &ops[0] {
            MiniOp::Put(v) => db.put(&b"k"[..], vec![*v], WriteOptions { sync: true }).expect("put first"),
            MiniOp::Del => db.delete(&b"k"[..], WriteOptions { sync: true }).expect("delete first"),
        }
        apply_model(&mut expected_main, &ops[0]);

        let branch_snapshot: SnapshotId = db.create_snapshot().expect("snapshot");
        db.create_branch("feature", Some(branch_snapshot)).expect("create branch");
        db.release_snapshot(branch_snapshot);

        let expected_branch = expected_main;

        for op in ops.iter().skip(1) {
            match op {
                MiniOp::Put(v) => db.put(&b"k"[..], vec![*v], WriteOptions { sync: true }).expect("put"),
                MiniOp::Del => db.delete(&b"k"[..], WriteOptions { sync: true }).expect("delete"),
            }
            apply_model(&mut expected_main, op);
        }

        db.compact_range(None).expect("compact");

        db.checkout("main").expect("checkout main");
        let main = db.get(b"k", ReadOptions::default()).expect("main get");
        let expected_main_bytes = expected_main.map(|v| bytes::Bytes::copy_from_slice(&[v]));
        prop_assert_eq!(main, expected_main_bytes);

        db.checkout("feature").expect("checkout feature");
        let feature = db.get(b"k", ReadOptions::default()).expect("feature get");
        let expected_branch_bytes = expected_branch.map(|v| bytes::Bytes::copy_from_slice(&[v]));
        prop_assert_eq!(feature, expected_branch_bytes);
    }
}
