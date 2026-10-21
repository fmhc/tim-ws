//! Host clock status and mapping of the scanner's free-running clock to UTC.
//!
//! The scanner only has a microsecond counter since power on. Each telegram
//! carries that counter at transmission; the host notes its own UTC receive
//! time. `receive - transmit` is the clock offset plus network and stack
//! latency, so the minimum over a sliding window is the best offset estimate.
//! The estimate is therefore late by the minimum latency (sub-millisecond on
//! a direct link), carries the host clock's own NTP error, and drifts with
//! the rate difference of the two clocks across the window (about 1.7 ms at
//! 100 ppm over 256 scans, either sign). Rates are not estimated.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Microseconds since the Unix epoch (UTC) from the system clock.
pub fn now_utc_us() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_micros() as i64,
        Err(e) => -(e.duration().as_micros() as i64),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClockStatus {
    /// Kernel reports the clock as synchronised (by chrony, timesyncd, ntpd …).
    pub synced: bool,
    /// Kernel estimate of the clock error, microseconds.
    pub est_error_us: i64,
    /// Kernel maximum error bound, microseconds.
    pub max_error_us: i64,
}

/// Reads the kernel NTP state via `adjtimex(2)` without changing anything.
#[cfg(target_os = "linux")]
pub fn host_clock() -> Option<ClockStatus> {
    // SAFETY: modes = 0 makes adjtimex a pure read into the zeroed struct.
    let mut tx: libc::timex = unsafe { std::mem::zeroed() };
    let state = unsafe { libc::adjtimex(&mut tx) };
    if state < 0 {
        return None;
    }
    Some(ClockStatus {
        synced: state != libc::TIME_ERROR && tx.status & libc::STA_UNSYNC == 0,
        est_error_us: tx.esterror as i64,
        max_error_us: tx.maxerror as i64,
    })
}

#[cfg(not(target_os = "linux"))]
pub fn host_clock() -> Option<ClockStatus> {
    None
}

/// Sliding-window minimum of host-minus-device offsets.
#[derive(Debug)]
pub struct DeviceClock {
    last_raw: Option<u32>,
    unwrapped: u64,
    /// Monotonic deque of (sample index, offset); front is the window minimum.
    window: VecDeque<(u64, i64)>,
    samples: u64,
    capacity: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceTimes {
    /// Device "time since startup" field mapped to UTC microseconds.
    pub scan_utc_us: i64,
    /// Device transmission time mapped to UTC microseconds.
    pub transmit_utc_us: i64,
    /// Device clock restarted (or went backwards) at this sample.
    pub restarted: bool,
}

impl DeviceClock {
    pub fn new(capacity: usize) -> Self {
        Self {
            last_raw: None,
            unwrapped: 0,
            window: VecDeque::with_capacity(capacity),
            samples: 0,
            capacity: capacity.max(1) as u64,
        }
    }

    pub fn reset(&mut self) {
        self.last_raw = None;
        self.window.clear();
    }

    pub fn observe(&mut self, startup_raw: u32, transmit_raw: u32, host_recv_utc_us: i64) -> DeviceTimes {
        let mut restarted = false;
        match self.last_raw {
            None => self.unwrapped = u64::from(transmit_raw),
            Some(last) => {
                let delta = transmit_raw.wrapping_sub(last);
                if delta >= 1 << 31 {
                    // Backwards: the scanner rebooted or the stream was replayed again.
                    restarted = true;
                    self.window.clear();
                    self.unwrapped = u64::from(transmit_raw);
                } else {
                    self.unwrapped += u64::from(delta);
                }
            }
        }
        self.last_raw = Some(transmit_raw);

        let offset = host_recv_utc_us - self.unwrapped as i64;
        while self.window.back().is_some_and(|&(_, o)| o >= offset) {
            self.window.pop_back();
        }
        self.window.push_back((self.samples, offset));
        while self.window.front().is_some_and(|&(i, _)| i + self.capacity <= self.samples) {
            self.window.pop_front();
        }
        self.samples += 1;
        let best = self.window.front().map_or(offset, |&(_, o)| o);

        let scan_device = self.unwrapped as i64 - i64::from(transmit_raw.wrapping_sub(startup_raw));
        DeviceTimes { scan_utc_us: scan_device + best, transmit_utc_us: self.unwrapped as i64 + best, restarted }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_minimum_latency_offset() {
        let mut c = DeviceClock::new(4);
        let base = 1_000_000_000_000;
        // Device ticks 66_667 us per scan; host sees varying latency.
        let latencies = [900, 300, 1500, 700];
        let mut last = None;
        for (k, lat) in latencies.iter().enumerate() {
            let dev = 10_000 + k as u32 * 66_667;
            last = Some(c.observe(dev - 2814, dev, base + i64::from(dev) + lat));
        }
        let t = last.unwrap();
        assert_eq!(t.transmit_utc_us, base + i64::from(10_000 + 3 * 66_667) + 300);
        assert_eq!(t.transmit_utc_us - t.scan_utc_us, 2814);
    }

    #[test]
    fn old_minimum_leaves_the_window() {
        let mut c = DeviceClock::new(2);
        c.observe(0, 0, 100);
        c.observe(10, 10, 210);
        let t = c.observe(20, 20, 320);
        assert_eq!(t.transmit_utc_us, 20 + 200);
    }

    #[test]
    fn unwraps_u32_overflow() {
        let mut c = DeviceClock::new(8);
        let before = u32::MAX - 1000;
        c.observe(before, before, 5_000_000_000);
        let after = 2000u32; // 3001 us later, across the wrap
        let t = c.observe(after, after, 5_000_003_001);
        assert!(!t.restarted);
        assert_eq!(t.transmit_utc_us, 5_000_003_001);
    }

    #[test]
    fn detects_restart() {
        let mut c = DeviceClock::new(8);
        c.observe(900_000_000, 900_000_000, 1_000_000_000);
        let t = c.observe(5_000, 5_000, 1_000_066_000);
        assert!(t.restarted);
        assert_eq!(t.transmit_utc_us, 1_000_066_000);
    }

    #[test]
    fn host_clock_is_readable() {
        if cfg!(target_os = "linux") {
            let s = host_clock().expect("adjtimex");
            assert!(s.max_error_us >= 0);
        }
    }
}
