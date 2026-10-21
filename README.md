# tim-ws

A small bridge from a **SICK TiM** 2D LiDAR to **WebSocket** clients.
It connects to the scanner over TCP (CoLa A, port 2111), subscribes to the scan
stream, and fans every scan out as JSON or a compact binary frame, with UTC
timestamps and the host's NTP clock status.

- Non-blocking: one async task per connection (tokio, single thread). Every
  message is serialised once and shared by all clients. A slow client
  skips frames and gets a `lagged` notice; it never stalls the sensor.
- Robust framing: handles telegrams split across TCP reads, garbage, truncated
  and oversized frames, and length-prefixed strings that contain spaces.
- Honest liveness: a watchdog reconnects after silence, with backoff, and a
  status message every second carries `last_scan_age_ms`. A dead link shows
  up as a state change and is never mistaken for an empty scene.
- Time: host receive time in UTC µs, the scanner's free-running µs clock
  mapped to UTC (sliding-minimum offset estimate), and the kernel NTP state
  (`adjtimex`, read-only).
- Raw recording to JSONL with a size limit, and replay paced by the device
  clock, so the whole pipeline runs offline.

Verified live with a **TiM551-2050001, firmware V2.54** (15 Hz, 271 points
from -45° to 225° in 1° steps). Over 30 s it delivered 15.03 Hz with no
counter gaps and no bad telegrams to a JSON and a binary client at once.
Receive jitter was 0.22 ms (standard deviation); the UTC-mapped device clock
reduced that to 0.04 ms. It reconnected 250 ms after the TCP connection was
killed and confirmed the unsubscribe on shutdown. Other TiM5xx models with
CoLa A should work, but that is untested.

Throughput is not the bottleneck: an unthrottled replay pushed about 25,000
scans/s to two clients at once, in 3.4 MB of resident memory.

## Usage

```sh
cargo build --release

# Live: scanner address as configured on the device
./target/release/tim-ws --sensor 192.168.0.1:2111

# Also record every raw telegram (a new file; never overwrites)
./target/release/tim-ws --sensor 192.168.0.1:2111 --record scans-$(date -u +%Y%m%dT%H%M%SZ).jsonl

# Offline: replay a recording in real time, 4x, or as fast as possible (0)
./target/release/tim-ws --replay scans.jsonl --speed 1 --loop
```

By default it listens on `127.0.0.1:9002`. Use `--listen 0.0.0.0:9002` to serve
the network. Connect to `ws://host:9002/` for JSON, or `ws://host:9002/?format=binary`
for binary scans. Status messages are always JSON text.

Other options:

- `--sensor-id`: name for the scanner in all messages.
- `--invalid-below`: raw distances below this become `null`. The default and
  minimum is `16`, because smaller values are status codes (see below).
  Raise it to `50` to also drop readings under the TiM551's 0.05 m working range.
- `--max-clients`: simultaneous clients, pending handshakes included (default 32).
- `--allow-origin`: an extra browser Origin that may connect (repeatable, see below).
- `--watchdog-ms` (default 1000), `--record-max-mib` (default 256), `-v`.

On shutdown (Ctrl-C/SIGTERM) the scanner is unsubscribed, in every connection
phase. If the data source fails (for example an unreadable replay file), the
process exits with an error instead of serving stale status.

### Browser access

Browsers do not apply CORS to WebSockets, so without a check any web page
could read the scans of your room. tim-ws always accepts clients without an
`Origin` header (scripts, services) and browser pages served from `localhost`,
`127.0.0.1` or `[::1]`. Any other page needs `--allow-origin`, for example
`--allow-origin http://192.168.178.41:8000`. A file opened directly from disk
sends `Origin: null`, which is rejected unless you pass `--allow-origin null`.
None of this is authentication: when you listen on the network, any non-browser
client on that network can connect.

## Viewer

`examples/viewer.html` is a dependency-free browser view: a polar plot with
range rings, rate, data age, gaps, status codes and NTP state. Serve it on
localhost and point it at the bridge:

```sh
cd examples && python3 -m http.server 8765 --bind 127.0.0.1
# open http://127.0.0.1:8765/viewer.html?ws=ws://127.0.0.1:9002/
```

The mouse wheel zooms. Data age is measured with the browser's monotonic
clock from when the scan arrived, so clock skew between machines cannot make
old data look fresh.

## Messages

### `scan` (JSON)

```json
{
  "type": "scan", "sensor": "192.168.0.1:2111", "seq": 42, "replay": false,
  "host_recv_utc_us": 1792546520017899,
  "scan_utc_us_est": 1792546520015085,
  "counter_gap": 0, "device_clock_restarted": false, "scan_hz": 15.0,
  "angle": { "unit": "deg", "convention": "sick_device", "start": -45.0, "step": 1.0, "count": 271 },
  "range_m": [0.176, 0.169, null, 1.014, "…"],
  "device": { "serial": "…", "status": [0, 0], "telegram_counter": 8708, "scan_counter": 8710,
              "time_since_startup_us": 596571330, "time_of_transmission_us": 596574144,
              "outputs": [1, 0], "name": "not defined" }
}
```

- `range_m[i]` belongs to angle `start + i * step`.
- `invalid` counts the `null` points by reason, and `codes` lists
  `[index, raw]` for every point with a status code other than 0, for example
  `[[102, 2]]`. A dazzled scanner (`dazzled > 0`) is blind at those angles,
  which is not the same as seeing nothing. The angle is in the
  device convention: for a TiM55x, -45…225° with 90° straight ahead
  (inferred from the symmetric range, check against your mounting).
  `null` means there is no valid distance. That is not the same as free space.
- `counter_gap` counts scans the device numbered but we never received.
- `rssi` (raw) is included when the scanner sends an `RSSI` channel.
- `scan_utc_us_est` maps the telegram's "time since startup" field to UTC.
  The offset is the minimum of `receive − transmit` over the last 256 scans
  (about 17 s). This removes network jitter: live over Wi-Fi, receive times
  jittered by 6 ms while the estimate stayed within 0.04 ms. The error has
  three parts:
  - the minimum network latency (the estimate is late by it),
  - the host clock's own NTP error,
  - drift between the scanner and host clocks across the window: about
    1.7 ms at 100 ppm, with either sign.

  A step of the host clock shifts the estimate once it enters the window.
  `host_recv_utc_us` is the plain receive time.

### Raw status codes

From the TiM55x/56x/57x/58x operating instructions (annex, "LMDscandata –
reserved values"), valid distances start at raw value 16:

| Raw | Meaning | JSON reason |
| --- | --- | --- |
| 0 | invalid: very low remission, out of range (too close or too far), or removed by a filter | `no_echo` |
| 1 | invalid: the device was dazzled, e.g. by sunlight | `dazzled` |
| 2 | implausible measurement | `implausible` |
| 3–15 | reserved | `reserved` |
| ≥ 16 | valid distance (max. 10,000 mm; beyond that the device sends 0) | |

On a real TiM551, raw `2` appeared at one angle that looked across a depth
edge: the neighbouring readings were 2.5 m, then 3.0 m, then 2.6 m. Two
objects at different distances shared the light spot. Recordings and the
binary format always keep the unmodified raw values.

### `status` (JSON, on every change and once per second)

`state` is one of `connecting`, `streaming`, `disconnected`, `replay_ended` or
`stopped`. `last_scan_age_ms` is measured with a monotonic clock. The message also has `detail`, `state_since_utc_us`, `now_utc_us`,
`last_scan_utc_us`, `last_scan_age_ms`, the counters `scans`, `counter_gaps`
and `bad_telegrams`, the device `identity` (`DeviceIdent`, `DItype`, `DIornr`,
`SCdevicestate` replies), and `host_clock`.

`host_clock` holds `{synced, est_error_us, max_error_us}` as reported by
the kernel. How meaningful the error fields are depends on the NTP daemon.
`systemd-timesyncd`, for example, leaves `est_error_us` at 0, and
`max_error_us` is only a loose upper bound.

### `lagged` (JSON)

`{"type":"lagged","skipped":n}`: this client was too slow and missed `n` messages.

### Binary scan (`?format=binary`), little endian

| Offset | Type | Field |
| --- | --- | --- |
| 0 | `[u8; 4]` | magic `TIM1` |
| 4 | `i64` | `host_recv_utc_us` |
| 12 | `i64` | `scan_utc_us_est` |
| 20 | `u64` | `seq` |
| 28 | `u16` | device scan counter |
| 30 | `u16` | `counter_gap` (saturated) |
| 32 | `u16` | `invalid_below` (≥ 16; raw values below it are not distances) |
| 34 | `u16` | point count `n` |
| 36 | `f32` | start angle, degrees |
| 40 | `f32` | step, degrees |
| 44 | `f32` | scale |
| 48 | `f32` | offset, mm |
| 52 | `[u16; n]` | raw distances; mm = raw × scale + offset |

## Recording format

JSON lines. The first line is `{"type":"meta",…}`, then one telegram per line:
`{"t_utc_us":…,"dir":"rx"|"tx","frame":"sSN LMDscandata …"}`. STX/ETX are
stripped, and payloads that are not printable ASCII go into `frame_hex`.
Replay also accepts lines with `direction` and `frame_hex` that still include
STX/ETX.

Recordings of a real room show its geometry. Keep them out of public
repositories (`*.jsonl` is in `.gitignore`).

## Network notes

Factory setting is `192.168.0.1/24`, but devices may be set to DHCP. This one
was, and it sent DHCP requests (`udhcp 1.16.1`) until it got a lease. On a
direct cable you then need a DHCP server on that port, or a static address
configured with SOPAS. Also check that no local network (Docker bridges!)
already uses the scanner's subnet.

## Tests

```sh
cargo test
```

Unit tests cover framing, the parser, clock mapping and frame encoding.
`tests/end_to_end.rs` runs a mock scanner over real sockets:

- identification and subscription, with scans fragmented into 37-byte writes
- a missing scan counter
- a dropped connection with reconnect, and a silent link that trips the watchdog
- a binary client
- unsubscribe when cancelled before the subscription is acknowledged, and a
  prompt stop when cancelled during identification
- a foreign Origin (rejected), the client limit, and an idle handshake that
  is dropped at its deadline The fixtures are synthetic, built in the TiM551 telegram
layout.

Against a real 5-second TiM551 recording (75 scans), replay decoded every
scan with no errors and matched an independent Python decoder value for value.

Fuzzing (nightly, `cargo install cargo-fuzz`):

```sh
cargo fuzz run parse_scan fuzz/corpus/parse_scan -- -max_len=4096
cargo fuzz run cola_stream -- -max_len=4096
```

`contrib/tim-ws.service` is an example systemd unit.

## License

MIT or Apache-2.0, at your option.

SICK and TiM are trademarks of SICK AG. This project is not affiliated with
or endorsed by SICK AG. It implements the publicly documented CoLa A
telegrams for reading scan data and only sends read and subscribe commands;
it never changes scanner parameters.
