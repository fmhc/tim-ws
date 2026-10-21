//! Turns parsed scans into timestamped frames and serialises them once for
//! all WebSocket clients (JSON text and a compact binary layout).

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

use crate::clock::DeviceClock;
use crate::scan::Scan;

/// Window for the offset minimum: 256 scans ≈ 17 s at 15 Hz.
const CLOCK_WINDOW: usize = 256;

#[derive(Debug, Clone)]
pub struct ScanFrame {
    pub seq: u64,
    pub host_recv_utc_us: i64,
    pub scan_utc_us_est: i64,
    /// Scans missing since the previous one, from the device scan counter.
    pub counter_gap: u32,
    pub device_clock_restarted: bool,
    pub replay: bool,
    pub scan: Scan,
}

#[derive(Debug)]
pub struct Pipeline {
    clock: DeviceClock,
    last_counter: Option<u16>,
    seq: u64,
    replay: bool,
}

impl Pipeline {
    pub fn new(replay: bool) -> Self {
        Self { clock: DeviceClock::new(CLOCK_WINDOW), last_counter: None, seq: 0, replay }
    }

    /// Call after a reconnect: the scanner may have rebooted meanwhile.
    pub fn reset(&mut self) {
        self.clock.reset();
        self.last_counter = None;
    }

    pub fn ingest(&mut self, scan: Scan, host_recv_utc_us: i64) -> ScanFrame {
        let times = self.clock.observe(scan.time_since_startup_us, scan.time_of_transmission_us, host_recv_utc_us);
        let counter_gap = match self.last_counter {
            Some(prev) if !times.restarted => {
                let d = scan.scan_counter.wrapping_sub(prev);
                if d < 0x8000 {
                    u32::from(d.saturating_sub(1))
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.last_counter = Some(scan.scan_counter);
        self.seq += 1;
        ScanFrame {
            seq: self.seq,
            host_recv_utc_us,
            scan_utc_us_est: times.scan_utc_us,
            counter_gap,
            device_clock_restarted: times.restarted,
            replay: self.replay,
            scan,
        }
    }
}

#[derive(Serialize)]
struct Angle {
    unit: &'static str,
    /// SICK device convention: TiM55x scans -45..225 deg, 90 deg = straight ahead.
    convention: &'static str,
    start: f64,
    step: f64,
    count: usize,
}

#[derive(Serialize)]
struct Device<'a> {
    serial: String,
    status: [u16; 2],
    telegram_counter: u16,
    scan_counter: u16,
    time_since_startup_us: u32,
    time_of_transmission_us: u32,
    outputs: [u16; 2],
    name: Option<&'a str>,
}

#[derive(Serialize)]
struct ScanJson<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    sensor: &'a str,
    seq: u64,
    replay: bool,
    host_recv_utc_us: i64,
    scan_utc_us_est: i64,
    counter_gap: u32,
    device_clock_restarted: bool,
    scan_hz: f64,
    angle: Angle,
    /// Metres; `null` where the raw value is below `invalid_below` (no echo).
    range_m: Vec<Option<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rssi: Option<&'a [u16]>,
    device: Device<'a>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NoDistanceChannel;

impl ScanFrame {
    pub fn to_json(&self, sensor: &str, invalid_below: u16) -> Result<String, NoDistanceChannel> {
        let s = &self.scan;
        let dist = s.channel("DIST").ok_or(NoDistanceChannel)?;
        let (scale, offset) = (dist.scale, dist.offset);
        let range_m = dist
            .values
            .iter()
            .map(|&v| (v >= invalid_below).then(|| (f32::from(v) * scale + offset) / 1000.0))
            .collect();
        let json = ScanJson {
            kind: "scan",
            sensor,
            seq: self.seq,
            replay: self.replay,
            host_recv_utc_us: self.host_recv_utc_us,
            scan_utc_us_est: self.scan_utc_us_est,
            counter_gap: self.counter_gap,
            device_clock_restarted: self.device_clock_restarted,
            scan_hz: s.scan_frequency_hz,
            angle: Angle {
                unit: "deg",
                convention: "sick_device",
                start: dist.start_deg(),
                step: dist.step_deg(),
                count: dist.values.len(),
            },
            range_m,
            rssi: s.channel("RSSI").map(|c| c.values.as_slice()),
            device: Device {
                serial: format!("{:X}", s.serial),
                status: s.device_status,
                telegram_counter: s.telegram_counter,
                scan_counter: s.scan_counter,
                time_since_startup_us: s.time_since_startup_us,
                time_of_transmission_us: s.time_of_transmission_us,
                outputs: s.outputs,
                name: s.device_name.as_deref(),
            },
        };
        Ok(serde_json::to_string(&json).expect("scan JSON is always serialisable"))
    }

    /// Binary layout v1, little endian (see README):
    /// magic `TIM1`, i64 host_recv_utc_us, i64 scan_utc_us_est, u64 seq,
    /// u16 scan_counter, u16 counter_gap (saturated), u16 invalid_below, u16 count,
    /// f32 start_deg, f32 step_deg, f32 scale, f32 offset_mm, count × u16 raw.
    pub fn to_binary(&self, invalid_below: u16) -> Result<Bytes, NoDistanceChannel> {
        let dist = self.scan.channel("DIST").ok_or(NoDistanceChannel)?;
        let mut b = BytesMut::with_capacity(52 + 2 * dist.values.len());
        b.put_slice(b"TIM1");
        b.put_i64_le(self.host_recv_utc_us);
        b.put_i64_le(self.scan_utc_us_est);
        b.put_u64_le(self.seq);
        b.put_u16_le(self.scan.scan_counter);
        b.put_u16_le(self.counter_gap.min(u32::from(u16::MAX)) as u16);
        b.put_u16_le(invalid_below);
        b.put_u16_le(dist.values.len() as u16);
        b.put_f32_le(dist.start_deg() as f32);
        b.put_f32_le(dist.step_deg() as f32);
        b.put_f32_le(dist.scale);
        b.put_f32_le(dist.offset);
        for &v in &dist.values {
            b.put_u16_le(v);
        }
        Ok(b.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::{parse_scan, tests::synthetic};

    fn frame(p: &mut Pipeline, counter: u16, startup: u32, recv: i64) -> ScanFrame {
        p.ingest(parse_scan(synthetic(counter, startup, &[0, 176, 0x717]).as_bytes()).unwrap(), recv)
    }

    #[test]
    fn json_frame() {
        let mut p = Pipeline::new(false);
        let f = frame(&mut p, 7, 1_000_000, 1_700_000_000_000_000);
        let v: serde_json::Value = serde_json::from_str(&f.to_json("tim", 1).unwrap()).unwrap();
        assert_eq!(v["type"], "scan");
        assert_eq!(v["range_m"], serde_json::json!([null, 0.176, 1.815]));
        assert_eq!(v["angle"]["start"], -45.0);
        assert_eq!(v["scan_utc_us_est"], 1_700_000_000_000_000i64 - 2814);
        assert_eq!(v["device"]["name"], "not defined");
        assert!(v.get("rssi").is_none());
    }

    #[test]
    fn counter_gaps() {
        let mut p = Pipeline::new(false);
        assert_eq!(frame(&mut p, 65534, 0, 0).counter_gap, 0);
        assert_eq!(frame(&mut p, 65535, 66_667, 66_667).counter_gap, 0);
        assert_eq!(frame(&mut p, 2, 4 * 66_667, 4 * 66_667).counter_gap, 2);
    }

    #[test]
    fn binary_frame() {
        let mut p = Pipeline::new(false);
        let b = frame(&mut p, 7, 1_000_000, 42).to_binary(1).unwrap();
        assert_eq!(&b[..4], b"TIM1");
        assert_eq!(b.len(), 52 + 6);
        assert_eq!(i64::from_le_bytes(b[4..12].try_into().unwrap()), 42);
        assert_eq!(u16::from_le_bytes(b[34..36].try_into().unwrap()), 3);
        assert_eq!(f32::from_le_bytes(b[36..40].try_into().unwrap()), -45.0);
        assert_eq!(u16::from_le_bytes(b[54..56].try_into().unwrap()), 176);
    }
}
