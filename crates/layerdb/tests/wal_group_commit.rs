use std::sync::Arc;

use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 8,
        wal_segment_bytes: 64 * 1024,
        memtable_bytes: 32 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

#[test]
fn concurrent_writes_survive_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Arc::new(Db::open(dir.path(), options())?);

        let writers = 6usize;
        let per_writer = 80usize;
        let mut threads = Vec::new();

        for writer in 0..writers {
            let db = db.clone();
            threads.push(std::thread::spawn(move || -> anyhow::Result<()> {
                for i in 0..per_writer {
                    let key = format!("w{writer:02}_k{i:03}");
                    let value = format!("v{writer:02}_{i:03}");
                    db.put(
                        bytes::Bytes::from(key),
                        bytes::Bytes::from(value),
                        WriteOptions { sync: true },
                    )?;
                }
                Ok(())
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
        }
    }

    let db = Db::open(dir.path(), options())?;
    for writer in 0..6usize {
        for i in 0..80usize {
            let key = format!("w{writer:02}_k{i:03}");
            let expected = bytes::Bytes::from(format!("v{writer:02}_{i:03}"));
            assert_eq!(
                db.get(key.as_bytes(), ReadOptions::default())?,
                Some(expected)
            );
        }
    }

    Ok(())
}
