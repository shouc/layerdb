#![no_main]

use layerdb::internal_key::InternalKey;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = InternalKey::decode(data);
});
