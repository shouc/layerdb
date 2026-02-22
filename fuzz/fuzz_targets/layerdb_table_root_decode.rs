#![no_main]

use bytes::Bytes;
use layerdb::integrity::decode_table_root;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_table_root(Bytes::copy_from_slice(data));
});
