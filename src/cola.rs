//! CoLa A framing: every telegram is `STX payload ETX` on a TCP byte stream.
//!
//! TCP gives no message boundaries, so a telegram may arrive split over many
//! reads or several telegrams may arrive in one. The decoder resynchronises on
//! the next STX after garbage, a truncated frame or an oversized frame.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub const STX: u8 = 0x02;
pub const ETX: u8 = 0x03;

/// Largest payload accepted. A TiM551 scan telegram is about 1.5 KiB.
pub const DEFAULT_MAX_FRAME: usize = 64 * 1024;

#[derive(Debug)]
pub struct ColaA {
    max_frame: usize,
    /// Bytes after the current STX that were already searched for ETX.
    scanned: usize,
    pub discarded_frames: u64,
}

impl Default for ColaA {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME)
    }
}

impl ColaA {
    pub fn new(max_frame: usize) -> Self {
        Self { max_frame, scanned: 0, discarded_frames: 0 }
    }
}

impl Decoder for ColaA {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Bytes>> {
        loop {
            if self.scanned == 0 {
                match src.iter().position(|&b| b == STX) {
                    Some(start) => src.advance(start),
                    None => {
                        src.clear();
                        return Ok(None);
                    }
                }
            }
            // src[0] is STX here.
            let from = 1 + self.scanned;
            match src[from..].iter().position(|&b| b == ETX || b == STX) {
                Some(rel) if src[from + rel] == ETX => {
                    let end = from + rel;
                    if end - 1 > self.max_frame {
                        // Complete but too long: drop it whole, keep what follows.
                        src.advance(end + 1);
                        self.scanned = 0;
                        self.discarded_frames += 1;
                        continue;
                    }
                    let mut frame = src.split_to(end + 1);
                    frame.advance(1);
                    frame.truncate(end - 1);
                    self.scanned = 0;
                    return Ok(Some(frame.freeze()));
                }
                Some(rel) => {
                    // A new STX before ETX: the previous frame was cut off.
                    src.advance(from + rel);
                    self.scanned = 0;
                    self.discarded_frames += 1;
                }
                None => {
                    self.scanned = src.len() - 1;
                    if self.scanned > self.max_frame {
                        src.clear();
                        self.scanned = 0;
                        self.discarded_frames += 1;
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Encoder<&str> for ColaA {
    type Error = io::Error;

    fn encode(&mut self, command: &str, dst: &mut BytesMut) -> io::Result<()> {
        if command.bytes().any(|b| b == STX || b == ETX) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "command contains STX/ETX"));
        }
        dst.reserve(command.len() + 2);
        dst.put_u8(STX);
        dst.put_slice(command.as_bytes());
        dst.put_u8(ETX);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(codec: &mut ColaA, chunks: &[&[u8]]) -> Vec<Bytes> {
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for chunk in chunks {
            buf.extend_from_slice(chunk);
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                out.push(frame);
            }
        }
        out
    }

    #[test]
    fn frames_split_across_every_byte() {
        let stream = b"\x02sEA LMDscandata 1\x03\x02sRA DIornr 1060445\x03";
        let chunks: Vec<&[u8]> = stream.chunks(1).collect();
        let frames = decode_all(&mut ColaA::default(), &chunks);
        assert_eq!(frames, vec![&b"sEA LMDscandata 1"[..], &b"sRA DIornr 1060445"[..]]);
    }

    #[test]
    fn garbage_and_truncated_frames_are_skipped() {
        let mut codec = ColaA::default();
        let frames = decode_all(&mut codec, &[b"junk\x03\x02cut off", b"\x02ok\x03"]);
        assert_eq!(frames, vec![&b"ok"[..]]);
        assert_eq!(codec.discarded_frames, 1);
    }

    #[test]
    fn oversized_frame_is_dropped_and_stream_recovers() {
        let mut codec = ColaA::new(8);
        let frames = decode_all(&mut codec, &[b"\x02123456789012", b"345\x03\x02ok\x03"]);
        assert_eq!(frames, vec![&b"ok"[..]]);
        assert_eq!(codec.discarded_frames, 1);
    }

    #[test]
    fn oversized_complete_frame_is_dropped() {
        let mut codec = ColaA::new(8);
        assert_eq!(decode_all(&mut codec, &[b"\x02123456789\x03\x0212345678\x03"]), vec![&b"12345678"[..]]);
        assert_eq!(codec.discarded_frames, 1);
        let mut codec = ColaA::new(8);
        assert_eq!(decode_all(&mut codec, &[b"\x0212345678", b"9\x03\x02ok\x03"]), vec![&b"ok"[..]]);
        assert_eq!(codec.discarded_frames, 1);
    }

    #[test]
    fn encodes_command() {
        let mut buf = BytesMut::new();
        ColaA::default().encode("sEN LMDscandata 1", &mut buf).unwrap();
        assert_eq!(&buf[..], b"\x02sEN LMDscandata 1\x03");
    }
}
