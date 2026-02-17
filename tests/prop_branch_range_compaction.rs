use proptest::prelude::*;

use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum MiniOp {
    Put { key: u8, value: u8 },
    Del { key: u8 },
    RangeDel { start: u8, end: u8 },
}

#[derive(Debug, Clone)]
struct Event {
    op: MiniOp,
    compact_after: bool,
}

const KEY_SPACE: u8 = 6;

type ModelLog = Vec<(u64, MiniOp)>;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

fn key_bytes(key: u8) -> Vec<u8> {
    debug_assert!(key < KEY_SPACE);
    vec![b'a' + key]
}

fn key_bound_bytes(bound: u8) -> Vec<u8> {
    debug_assert!(bound <= KEY_SPACE);
    vec![b'a' + bound]
}

fn value_bytes(value: u8) -> Vec<u8> {
    vec![b'0' + (value % 10)]
}

fn apply_db(db: &Db, op: &MiniOp) {
    match op {
        MiniOp::Put { key, value } => db
            .put(
                key_bytes(*key),
                value_bytes(*value),
                WriteOptions { sync: true },
            )
            .expect("put"),
        MiniOp::Del { key } => db
            .delete(key_bytes(*key), WriteOptions { sync: true })
            .expect("delete"),
        MiniOp::RangeDel { start, end } => db
            .delete_range(
                key_bound_bytes(*start),
                key_bound_bytes(*end),
                WriteOptions { sync: true },
            )
            .expect("range delete"),
    }
}

fn ref_latest_point(
    log: &ModelLog,
    key: u8,
    snapshot_seqno: u64,
) -> Option<(u64, Option<Vec<u8>>)> {
    for (seqno, op) in log.iter().rev() {
        if *seqno > snapshot_seqno {
            continue;
        }
        match op {
            MiniOp::Put { key: k, value } if *k == key => {
                return Some((*seqno, Some(value_bytes(*value))));
            }
            MiniOp::Del { key: k } if *k == key => {
                return Some((*seqno, None));
            }
            _ => {}
        }
    }
    None
}

fn ref_covering_tombstone_seq(log: &ModelLog, key: u8, snapshot_seqno: u64) -> Option<u64> {
    log.iter()
        .filter_map(|(seqno, op)| match op {
            MiniOp::RangeDel { start, end }
                if *seqno <= snapshot_seqno && *start <= key && key < *end =>
            {
                Some(*seqno)
            }
            _ => None,
        })
        .max()
}

fn ref_get(log: &ModelLog, key: u8, snapshot_seqno: u64) -> Option<Vec<u8>> {
    let point = ref_latest_point(log, key, snapshot_seqno);
    let tombstone_seq = ref_covering_tombstone_seq(log, key, snapshot_seqno);

    match (point, tombstone_seq) {
        (None, _) => None,
        (Some((_seq, value)), None) => value,
        (Some((point_seq, value)), Some(tseq)) => {
            if tseq >= point_seq {
                None
            } else {
                value
            }
        }
    }
}

fn event_strategy() -> impl Strategy<Value = Event> {
    let op = prop_oneof![
        (0u8..KEY_SPACE, any::<u8>()).prop_map(|(key, value)| MiniOp::Put { key, value }),
        (0u8..KEY_SPACE).prop_map(|key| MiniOp::Del { key }),
        (0u8..KEY_SPACE, 0u8..KEY_SPACE).prop_map(|(a, b)| {
            let start = a.min(b);
            let end = a.max(b).saturating_add(1).min(KEY_SPACE);
            let end = if end <= start {
                start.saturating_add(1)
            } else {
                end
            };
            MiniOp::RangeDel { start, end }
        }),
    ];

    (op, any::<bool>()).prop_map(|(op, compact_after)| Event { op, compact_after })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test]
    fn prop_branch_range_compaction_preserves_snapshot_visibility(
        events in prop::collection::vec(event_strategy(), 1..60),
    ) {
        let dir = TempDir::new().expect("tempdir");
        let db = Db::open(dir.path(), options()).expect("open db");

        let mut log: ModelLog = Vec::new();
        let mut seqno = 0u64;
        let mut branch_snapshot_seqno = 0u64;

        for (idx, event) in events.iter().enumerate() {
            seqno += 1;
            apply_db(&db, &event.op);
            log.push((seqno, event.op.clone()));

            if idx == 0 {
                let snap = db.create_snapshot().expect("snapshot");
                db.create_branch("feature", Some(snap))
                    .expect("create branch from first event snapshot");
                db.release_snapshot(snap);
                branch_snapshot_seqno = seqno;
            }

            if event.compact_after {
                db.compact_range(None).expect("compact");
            }
        }

        db.compact_range(None).expect("final compact");

        db.checkout("main").expect("checkout main");
        for key in 0..KEY_SPACE {
            let expected = ref_get(&log, key, seqno);
            let got = db
                .get(key_bytes(key), ReadOptions::default())
                .expect("main get")
                .map(|b| b.to_vec());
            prop_assert_eq!(got, expected);
        }

        db.checkout("feature").expect("checkout feature");
        for key in 0..KEY_SPACE {
            let expected = ref_get(&log, key, branch_snapshot_seqno);
            let got = db
                .get(key_bytes(key), ReadOptions::default())
                .expect("feature get")
                .map(|b| b.to_vec());
            prop_assert_eq!(got, expected);
        }
    }
}
