use std::sync::Arc;

use bytes::Bytes;
use layerdb::cache::{BlockCacheKey, ClockProCache};
use layerdb::internal_key::{InternalKey, KeyKind};
use layerdb::sst::{SstBuilder, SstReader};

#[test]
fn reader_populates_data_block_cache() {
    let dir = tempfile::TempDir::new().expect("tempdir");

    let mut builder = SstBuilder::create(dir.path(), 1, 128).expect("create");
    for i in 0..64u32 {
        let key = InternalKey::new(
            Bytes::from(format!("k{i:04}")),
            1000 - i as u64,
            KeyKind::Put,
        );
        let value = Bytes::from(format!("v{i:04}"));
        builder.add(&key, &value).expect("add");
    }
    let _props = builder.finish().expect("finish");

    let cache: Arc<ClockProCache<BlockCacheKey, Vec<(InternalKey, Bytes)>>> =
        Arc::new(ClockProCache::new(64));
    let reader = SstReader::open_with_cache(
        dir.path().join("sst_0000000000000001.sst"),
        Some(cache.clone()),
    )
    .expect("open");

    assert_eq!(cache.len(), 0);
    let got = reader.get(b"k0000", u64::MAX).expect("get");
    assert_eq!(got.and_then(|(_, v)| v), Some(Bytes::from_static(b"v0000")));
    assert!(cache.len() > 0);

    // A second read should still succeed while reusing cached blocks.
    let got2 = reader.get(b"k0001", u64::MAX).expect("get2");
    assert_eq!(
        got2.and_then(|(_, v)| v),
        Some(Bytes::from_static(b"v0001"))
    );
    assert!(cache.len() > 0);
}
