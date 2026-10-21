# Changelog

## 0.1.1 – 2026-10-21

- License is now MIT only (0.1.0 was MIT or Apache-2.0).
- README: install and Docker instructions, corrected sections.

## 0.1.0 – 2026-10-21

First version.

- CoLa A client for SICK TiM scanners: identification, scan subscription,
  watchdog, reconnect with backoff, confirmed unsubscribe on shutdown.
- WebSocket fan-out as JSON or a compact binary layout, plus status
  heartbeats with data age and the host's NTP state.
- Device clock mapped to UTC.
- Raw JSONL recording with a size limit; replay paced by the device clock.
- Container image (non-root) with a streaming health check.
- Browser viewer (`examples/viewer.html`), fuzz targets, end-to-end tests.
- Raw distance values below 16 are reported as SICK status codes (no echo,
  dazzled, implausible, reserved) instead of distances.
- Server limits: client cap, handshake deadline, small message limits,
  Origin check for browser clients.
- Verified live against a TiM551-2050001 (firmware V2.54), direct and via a
  router over Wi-Fi.
