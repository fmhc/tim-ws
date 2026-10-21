//! Parser for the CoLa A `LMDscandata` telegram (ASCII, space separated).
//!
//! Numbers are hexadecimal unless they carry a `+`/`-` sign, in which case
//! they are decimal. Strings are length prefixed and may contain spaces
//! (a TiM551 sends its device name as `1 B not defined`), so the tokenizer
//! reads them by length instead of splitting on whitespace.
//!
//! Field order as observed from a TiM551-2050001, firmware V2.54:
//!
//! ```text
//! sSN LMDscandata <version> <device no> <serial> <status 2x>
//! <telegram counter> <scan counter> <time since startup us> <time of transmission us>
//! <inputs 2x> <outputs 2x> <reserved> <scan freq 1/100 Hz> <measurement freq 100 Hz>
//! <encoder count> {<position> <speed>}*
//! <16-bit channel count> {<name> <scale f32> <offset f32> <start 1/10000 deg> <step 1/10000 deg> <n> <n values>}*
//! <8-bit channel count> {same}*
//! <position flag> <name flag> [<len> <name>] <comment flag> [<len> <text>] <time flag> [...] <event flag>
//! ```

use std::fmt;

pub const MAX_CHANNELS: u32 = 8;
pub const MAX_VALUES: u32 = 8192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    NotScanData,
    UnexpectedEnd(&'static str),
    BadNumber(&'static str),
    OutOfRange(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotScanData => f.write_str("not an LMDscandata telegram"),
            Self::UnexpectedEnd(what) => write!(f, "telegram ends before {what}"),
            Self::BadNumber(what) => write!(f, "malformed number for {what}"),
            Self::OutOfRange(what) => write!(f, "value out of range for {what}"),
        }
    }
}

impl std::error::Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, Clone, PartialEq)]
pub struct Channel {
    /// Content identifier, e.g. `DIST1` or `RSSI1`.
    pub content: String,
    pub scale: f32,
    pub offset: f32,
    /// Start angle in 1/10000 degree, device convention.
    pub start_angle: i32,
    /// Angular step in 1/10000 degree.
    pub step: u16,
    pub values: Vec<u16>,
}

impl Channel {
    pub fn start_deg(&self) -> f64 {
        f64::from(self.start_angle) / 10_000.0
    }

    pub fn step_deg(&self) -> f64 {
        f64::from(self.step) / 10_000.0
    }
}

/// Optional real-time clock block of the telegram (device local time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceTime {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub version: u16,
    pub device_number: u16,
    pub serial: u32,
    pub device_status: [u16; 2],
    pub telegram_counter: u16,
    pub scan_counter: u16,
    /// Device clock in microseconds since power on; wraps at 2^32.
    pub time_since_startup_us: u32,
    pub time_of_transmission_us: u32,
    pub inputs: [u16; 2],
    pub outputs: [u16; 2],
    pub scan_frequency_hz: f64,
    pub measurement_frequency_hz: f64,
    pub channels16: Vec<Channel>,
    pub channels8: Vec<Channel>,
    pub device_name: Option<String>,
    pub device_time: Option<DeviceTime>,
    /// False if an optional trailing block was present but not decoded.
    pub tail_complete: bool,
}

impl Scan {
    pub fn channel(&self, prefix: &str) -> Option<&Channel> {
        self.channels16.iter().chain(&self.channels8).find(|c| c.content.starts_with(prefix))
    }
}

pub fn is_scan_telegram(payload: &[u8]) -> bool {
    payload.starts_with(b"sSN LMDscandata ") || payload.starts_with(b"sRA LMDscandata ")
}

pub fn parse_scan(payload: &[u8]) -> Result<Scan> {
    if !is_scan_telegram(payload) {
        return Err(ParseError::NotScanData);
    }
    let mut t = Tokens { b: payload, i: "sSN LMDscandata".len() };

    let version = t.u16("version")?;
    let device_number = t.u16("device number")?;
    let serial = t.uint("serial number", u32::MAX)?;
    let device_status = [t.u16("device status")?, t.u16("device status")?];
    let telegram_counter = t.u16("telegram counter")?;
    let scan_counter = t.u16("scan counter")?;
    let time_since_startup_us = t.uint("time since startup", u32::MAX)?;
    let time_of_transmission_us = t.uint("time of transmission", u32::MAX)?;
    let inputs = [t.u16("digital inputs")?, t.u16("digital inputs")?];
    let outputs = [t.u16("digital outputs")?, t.u16("digital outputs")?];
    t.u16("reserved")?;
    let scan_frequency_hz = f64::from(t.uint("scan frequency", u32::MAX)?) / 100.0;
    let measurement_frequency_hz = f64::from(t.uint("measurement frequency", u32::MAX)?) * 100.0;

    for _ in 0..t.uint("encoder count", 16)? {
        t.uint("encoder position", u32::MAX)?;
        t.u16("encoder speed")?;
    }

    let n16 = t.uint("16-bit channel count", MAX_CHANNELS)?;
    let channels16 = (0..n16).map(|_| t.channel(0xFFFF)).collect::<Result<Vec<_>>>()?;
    let n8 = t.uint("8-bit channel count", MAX_CHANNELS)?;
    let channels8 = (0..n8).map(|_| t.channel(0xFF)).collect::<Result<Vec<_>>>()?;

    let mut scan = Scan {
        version,
        device_number,
        serial,
        device_status,
        telegram_counter,
        scan_counter,
        time_since_startup_us,
        time_of_transmission_us,
        inputs,
        outputs,
        scan_frequency_hz,
        measurement_frequency_hz,
        channels16,
        channels8,
        device_name: None,
        device_time: None,
        tail_complete: false,
    };
    // The trailing blocks are informational; a scan stays valid without them.
    scan.tail_complete = t.tail(&mut scan).unwrap_or(false);
    Ok(scan)
}

struct Tokens<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Tokens<'a> {
    fn word(&mut self, what: &'static str) -> Result<&'a [u8]> {
        while self.b.get(self.i) == Some(&b' ') {
            self.i += 1;
        }
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != b' ' {
            self.i += 1;
        }
        if start == self.i {
            return Err(ParseError::UnexpectedEnd(what));
        }
        Ok(&self.b[start..self.i])
    }

    /// Hex, or signed decimal when prefixed with `+`/`-`.
    fn number(&mut self, what: &'static str) -> Result<i64> {
        let w = self.word(what)?;
        let (radix, negative, digits) = match w[0] {
            b'+' => (10, false, &w[1..]),
            b'-' => (10, true, &w[1..]),
            _ => (16, false, w),
        };
        if digits.is_empty() || digits.len() > if radix == 16 { 8 } else { 10 } {
            return Err(ParseError::BadNumber(what));
        }
        let mut v: i64 = 0;
        for &d in digits {
            let digit = (d as char).to_digit(radix).ok_or(ParseError::BadNumber(what))?;
            v = v * i64::from(radix) + i64::from(digit);
        }
        Ok(if negative { -v } else { v })
    }

    fn uint(&mut self, what: &'static str, max: u32) -> Result<u32> {
        let v = self.number(what)?;
        if !(0..=i64::from(max)).contains(&v) {
            return Err(ParseError::OutOfRange(what));
        }
        Ok(v as u32)
    }

    fn u16(&mut self, what: &'static str) -> Result<u16> {
        Ok(self.uint(what, 0xFFFF)? as u16)
    }

    fn u8(&mut self, what: &'static str) -> Result<u8> {
        Ok(self.uint(what, 0xFF)? as u8)
    }

    /// Hex values are 32-bit two's complement; signed decimal is taken as is.
    fn i32(&mut self, what: &'static str) -> Result<i32> {
        let v = self.number(what)?;
        if v > i64::from(u32::MAX) || v < i64::from(i32::MIN) {
            return Err(ParseError::OutOfRange(what));
        }
        Ok(if v > i64::from(i32::MAX) { v as u32 as i32 } else { v as i32 })
    }

    fn f32(&mut self, what: &'static str) -> Result<f32> {
        let bits = self.uint(what, u32::MAX)?;
        let v = f32::from_bits(bits);
        if !v.is_finite() {
            return Err(ParseError::OutOfRange(what));
        }
        Ok(v)
    }

    /// `n` raw bytes after exactly one separating space.
    fn bytes(&mut self, n: usize, what: &'static str) -> Result<&'a [u8]> {
        if self.b.get(self.i) != Some(&b' ') || self.i + 1 + n > self.b.len() {
            return Err(ParseError::UnexpectedEnd(what));
        }
        let s = &self.b[self.i + 1..self.i + 1 + n];
        self.i += 1 + n;
        Ok(s)
    }

    fn string(&mut self, what: &'static str) -> Result<String> {
        let len = self.uint(what, 0xFFFF)? as usize;
        Ok(String::from_utf8_lossy(self.bytes(len, what)?).into_owned())
    }

    fn channel(&mut self, max_value: u32) -> Result<Channel> {
        let content = String::from_utf8_lossy(self.word("channel content")?).into_owned();
        let scale = self.f32("channel scale")?;
        let offset = self.f32("channel offset")?;
        let start_angle = self.i32("channel start angle")?;
        let step = self.u16("channel angular step")?;
        let n = self.uint("channel value count", MAX_VALUES)?;
        let mut values = Vec::with_capacity(n as usize);
        for _ in 0..n {
            values.push(self.uint("channel value", max_value)? as u16);
        }
        Ok(Channel { content, scale, offset, start_angle, step, values })
    }

    /// Returns Ok(true) if all trailing blocks were understood.
    fn tail(&mut self, scan: &mut Scan) -> Result<bool> {
        if self.uint("position flag", 1)? == 1 {
            return Ok(false);
        }
        if self.uint("name flag", 1)? == 1 {
            scan.device_name = Some(self.string("device name")?);
        }
        if self.uint("comment flag", 1)? == 1 {
            self.string("comment")?;
        }
        if self.uint("time flag", 1)? == 1 {
            scan.device_time = Some(DeviceTime {
                year: self.u16("year")?,
                month: self.u8("month")?,
                day: self.u8("day")?,
                hour: self.u8("hour")?,
                minute: self.u8("minute")?,
                second: self.u8("second")?,
                microsecond: self.uint("microsecond", u32::MAX)?,
            });
        }
        Ok(self.uint("event flag", 1)? == 0)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Synthetic telegram with the TiM551 field layout; values are made up.
    pub fn synthetic(scan_counter: u16, startup_us: u32, values: &[u16]) -> String {
        let data: Vec<String> = values.iter().map(|v| format!("{v:X}")).collect();
        format!(
            "sSN LMDscandata 1 1 ABCDEF 0 0 {sc:X} {sc:X} {st:X} {tx:X} 0 0 1 0 0 5DC 36 0 \
             1 DIST1 3F800000 00000000 FFF92230 2710 {n:X} {data} 0 0 1 B not defined 0 0 0",
            sc = scan_counter,
            st = startup_us,
            tx = startup_us.wrapping_add(2814),
            n = values.len(),
            data = data.join(" "),
        )
    }

    #[test]
    fn parses_tim551_layout() {
        let s = parse_scan(synthetic(0x89E, 0x238E_F4C2, &[0xB0, 0, 0x717]).as_bytes()).unwrap();
        assert_eq!(s.version, 1);
        assert_eq!(s.serial, 0xABCDEF);
        assert_eq!(s.scan_counter, 0x89E);
        assert_eq!(s.time_since_startup_us, 0x238E_F4C2);
        assert_eq!(s.time_of_transmission_us, 0x238E_F4C2 + 2814);
        assert_eq!(s.outputs, [1, 0]);
        assert_eq!(s.scan_frequency_hz, 15.0);
        assert_eq!(s.measurement_frequency_hz, 5400.0);
        let d = s.channel("DIST").unwrap();
        assert_eq!(d.content, "DIST1");
        assert_eq!((d.scale, d.offset), (1.0, 0.0));
        assert_eq!(d.start_deg(), -45.0);
        assert_eq!(d.step_deg(), 1.0);
        assert_eq!(d.values, vec![0xB0, 0, 0x717]);
        assert_eq!(s.device_name.as_deref(), Some("not defined"));
        assert!(s.tail_complete);
    }

    #[test]
    fn signed_decimal_numbers() {
        let t = synthetic(1, 1, &[1]).replace("FFF92230", "-450000");
        assert_eq!(parse_scan(t.as_bytes()).unwrap().channels16[0].start_angle, -450_000);
    }

    #[test]
    fn rssi_channel_and_time_block() {
        let t = synthetic(1, 1, &[5, 6]).replace(
            " 0 0 1 B not defined 0 0 0",
            " 1 RSSI1 3F800000 00000000 FFF92230 2710 2 FE FF 0 0 0 1 7EA A 15 1 19 14 3E8 0",
        );
        let s = parse_scan(t.as_bytes()).unwrap();
        assert_eq!(s.channel("RSSI").unwrap().values, vec![0xFE, 0xFF]);
        assert_eq!(s.device_name, None);
        let time = s.device_time.unwrap();
        assert_eq!((time.year, time.month, time.day, time.second, time.microsecond), (2026, 10, 21, 20, 1000));
        assert!(s.tail_complete);
    }

    #[test]
    fn truncated_telegram_is_an_error() {
        let full = synthetic(1, 1, &[1, 2, 3]);
        let cut = &full[..full.find(" 2 3").unwrap()];
        assert_eq!(parse_scan(cut.as_bytes()), Err(ParseError::UnexpectedEnd("channel value")));
    }

    #[test]
    fn missing_tail_keeps_scan() {
        let full = synthetic(1, 1, &[1]);
        let s = parse_scan(&full.as_bytes()[..full.find(" 0 1 B").unwrap()]).unwrap();
        assert!(!s.tail_complete);
    }

    #[test]
    fn rejects_bad_values() {
        let too_big = synthetic(1, 1, &[0xFFFF]).replace(" FFFF ", " 10000 ");
        assert!(parse_scan(too_big.as_bytes()).is_err());
        assert_eq!(parse_scan(b"sRA DIornr 1060445"), Err(ParseError::NotScanData));
        let nan_scale = synthetic(1, 1, &[1]).replace("3F800000", "7FC00000");
        assert_eq!(parse_scan(nan_scale.as_bytes()), Err(ParseError::OutOfRange("channel scale")));
        let junk = synthetic(1, 1, &[1]).replace("5DC", "5XC");
        assert_eq!(parse_scan(junk.as_bytes()), Err(ParseError::BadNumber("scan frequency")));
    }
}
