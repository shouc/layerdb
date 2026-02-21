use std::time::Duration;

use layerdb::{Db, DbOptions, WriteOptions};
use tempfile::TempDir;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

#[test]
fn background_scrubber_runs_and_reports() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;

    let scrubber = db.spawn_background_scrubber(Duration::from_millis(20))?;
    std::thread::sleep(Duration::from_millis(120));

    let state = scrubber.snapshot();
    assert!(state.runs >= 1, "expected at least one scrub run");
    assert!(
        state.last_error.is_none(),
        "unexpected scrub error: {state:?}"
    );
    assert!(
        state
            .last_report
            .as_ref()
            .is_some_and(|report| report.files_checked >= 1),
        "expected scrub report with checked files"
    );

    let final_state = scrubber.stop()?;
    assert!(final_state.runs >= state.runs);
    Ok(())
}
