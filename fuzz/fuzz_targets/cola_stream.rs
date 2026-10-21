#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tim_ws::cola::ColaA;
use tokio_util::codec::Decoder;

// First byte picks the read size, so frames get split at arbitrary points.
fuzz_target!(|data: &[u8]| {
    let Some((&chunk, rest)) = data.split_first() else { return };
    let mut codec = ColaA::new(512);
    let mut buf = BytesMut::new();
    for piece in rest.chunks(usize::from(chunk).max(1)) {
        buf.extend_from_slice(piece);
        while let Ok(Some(frame)) = codec.decode(&mut buf) {
            assert!(!frame.contains(&tim_ws::cola::STX) && !frame.contains(&tim_ws::cola::ETX));
        }
    }
});
