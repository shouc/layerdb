#![no_main]

use libfuzzer_sys::fuzz_target;
use vectdb::index::spfresh_layerdb::fuzzing;

fuzz_target!(|data: &[u8]| {
    fuzzing::decode_posting_member_event(data);
});
