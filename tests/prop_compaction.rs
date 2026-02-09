use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::prelude::*;

use layerdb::{Db, DbOptions, Op, Range, ReadOptions, WriteOptions};

#[derive(Debug, Clone)]
enum MiniOp {
    Put { key: u8, value: u8 },
    Del { key: u8 },
    RangeDel { start: u8, end: u8 },
}

#[derive(Debug, Clone)]
enum Step {
    Batch(Vec<MiniOp>),
    Snapshot,
    Compact,
}

const KEY_SPACE: u8 = 8;

type PointModel = BTreeMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>>;
type RangeModel = Vec<(u64, Vec<u8>, Vec<u8>)>;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 8 * 1024,
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

fn ref_apply(points: &mut PointModel, ranges: &mut RangeModel, seqno: u64, op: &MiniOp) {
    match op {
        MiniOp::Put { key, value } => {
            points
                .entry(key_bytes(*key))
                .or_default()
                .push((seqno, Some(value_bytes(*value))));
        }
        MiniOp::Del { key } => {
            points
                .entry(key_bytes(*key))
                .or_default()
                .push((seqno, None));
        }
        MiniOp::RangeDel { start, end } => {
            ranges.push((seqno, key_bound_bytes(*start), key_bound_bytes(*end)));
        }
    }
}

fn ref_latest_point(
    points: &PointModel,
    key: &[u8],
    snapshot_seqno: u64,
) -> Option<(u64, Option<Vec<u8>>)> {
    let versions = points.get(key)?;
    for (seqno, value) in versions.iter().rev() {
        if *seqno <= snapshot_seqno {
            return Some((*seqno, value.clone()));
        }
    }
    None
}

fn ref_covering_tombstone_seq(ranges: &RangeModel, key: &[u8], snapshot_seqno: u64) -> Option<u64> {
    ranges
        .iter()
        .filter(|(seqno, start, end)| {
            *seqno <= snapshot_seqno && start.as_slice() <= key && key < end.as_slice()
        })
        .map(|(seqno, _, _)| *seqno)
        .max()
}

fn ref_get(
    points: &PointModel,
    ranges: &RangeModel,
    key: &[u8],
    snapshot_seqno: u64,
) -> Option<Vec<u8>> {
    let point = ref_latest_point(points, key, snapshot_seqno);
    let tombstone_seq = ref_covering_tombstone_seq(ranges, key, snapshot_seqno);

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

fn collect_iter(db: &Db, snap: Option<layerdb::SnapshotId>) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
    let mut iter = db.iter(Range::all(), ReadOptions { snapshot: snap })?;
    iter.seek_to_first();
    let mut out = Vec::new();
    while let Some(next) = iter.next() {
        let (k, v) = next?;
        if let Some(v) = v {
            out.push((k, v));
        }
    }
    Ok(out)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test]
    fn prop_compaction_matches_reference(steps in prop::collection::vec(step_strategy(), 1..80)) {
        let dir_a = tempfile::TempDir::new().unwrap();
        let dir_b = tempfile::TempDir::new().unwrap();
        let db_a = Db::open(dir_a.path(), options()).unwrap();
        let db_b = Db::open(dir_b.path(), options()).unwrap();

        let mut seqno = 0u64;
        let mut point_model: PointModel = BTreeMap::new();
        let mut range_model: RangeModel = Vec::new();
        let mut snaps_a: Vec<(layerdb::SnapshotId, u64)> = Vec::new();
        let mut snaps_b: Vec<(layerdb::SnapshotId, u64)> = Vec::new();

        for step in &steps {
            match step {
                Step::Batch(batch) => {
                    let mut ops = Vec::new();
                    for op in batch {
                        match op {
                            MiniOp::Put { key, value } => {
                                ops.push(Op::put(key_bytes(*key), value_bytes(*value)));
                            }
                            MiniOp::Del { key } => {
                                ops.push(Op::delete(key_bytes(*key)));
                            }
                            MiniOp::RangeDel { start, end } => {
                                ops.push(Op::delete_range(key_bound_bytes(*start), key_bound_bytes(*end)));
                            }
                        }
                    }

                    db_a.write_batch(ops.clone(), WriteOptions { sync: true }).unwrap();
                    db_b.write_batch(ops, WriteOptions { sync: true }).unwrap();

                    for op in batch {
                        seqno += 1;
                        ref_apply(&mut point_model, &mut range_model, seqno, op);
                    }
                }
                Step::Snapshot => {
                    let snap_seqno = seqno;
                    let a = db_a.create_snapshot().unwrap();
                    let b = db_b.create_snapshot().unwrap();
                    snaps_a.push((a, snap_seqno));
                    snaps_b.push((b, snap_seqno));
                }
                Step::Compact => {
                    db_b.compact_range(None).unwrap();
                }
            }

            // Spot-check a few point reads at the latest seqno.
            for k in 0..3u8 {
                let key = key_bytes(k);
                let expected = ref_get(&point_model, &range_model, &key, seqno);
                let got_a = db_a.get(&key, ReadOptions::default()).unwrap();
                let got_b = db_b.get(&key, ReadOptions::default()).unwrap();
                prop_assert_eq!(got_a.map(|b| b.to_vec()), expected.clone());
                prop_assert_eq!(got_b.map(|b| b.to_vec()), expected);
            }
        }

        // Verify all snapshots for all keys.
        for (idx, (snap_a, snap_seqno)) in snaps_a.iter().enumerate() {
            let (snap_b, snap_seqno_b) = snaps_b[idx];
            prop_assert_eq!(*snap_seqno, snap_seqno_b);

            for k in 0..KEY_SPACE {
                let key = key_bytes(k);
                let expected = ref_get(&point_model, &range_model, &key, *snap_seqno);
                let got_a = db_a.get(&key, ReadOptions { snapshot: Some(*snap_a) }).unwrap();
                let got_b = db_b.get(&key, ReadOptions { snapshot: Some(snap_b) }).unwrap();
                prop_assert_eq!(got_a.map(|b| b.to_vec()), expected.clone());
                prop_assert_eq!(got_b.map(|b| b.to_vec()), expected);
            }

            let expected_iter: Vec<(Bytes, Bytes)> = (0..KEY_SPACE)
                .filter_map(|k| {
                    let key = key_bytes(k);
                    ref_get(&point_model, &range_model, &key, *snap_seqno)
                        .map(|v| (Bytes::from(key), Bytes::from(v)))
                })
                .collect();

            let got_iter_a = collect_iter(&db_a, Some(*snap_a)).unwrap();
            let got_iter_b = collect_iter(&db_b, Some(snap_b)).unwrap();

            prop_assert_eq!(got_iter_a, expected_iter.clone());
            prop_assert_eq!(got_iter_b, expected_iter);
        }
    }
}

fn step_strategy() -> impl Strategy<Value = Step> {
    let mini_op = prop_oneof![
        (0u8..KEY_SPACE, any::<u8>()).prop_map(|(key, value)| MiniOp::Put { key, value }),
        (0u8..KEY_SPACE).prop_map(|key| MiniOp::Del { key }),
        (0u8..KEY_SPACE, 0u8..KEY_SPACE).prop_map(|(a, b)| {
            let start = a.min(b);
            let end = a.max(b) + 1;
            MiniOp::RangeDel { start, end }
        }),
    ];

    prop_oneof![
        prop::collection::vec(mini_op, 1..5).prop_map(Step::Batch),
        Just(Step::Snapshot),
        Just(Step::Compact),
    ]
}
