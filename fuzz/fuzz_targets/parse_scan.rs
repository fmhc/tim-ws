#![no_main]

use libfuzzer_sys::fuzz_target;
use tim_ws::frame::Pipeline;
use tim_ws::scan::parse_scan;

fuzz_target!(|data: &[u8]| {
    if let Ok(scan) = parse_scan(data) {
        let mut p = Pipeline::new(false);
        let frame = p.ingest(scan, 0);
        let _ = frame.to_json("fuzz", 1);
        let _ = frame.to_binary(1);
    }
});
