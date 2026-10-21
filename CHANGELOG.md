# Changelog

## 0.1.0 – unreleased

First version.

- CoLa A client for SICK TiM scanners: identification, scan subscription,
  watchdog, reconnect with backoff, confirmed unsubscribe on shutdown.
- WebSocket fan-out as JSON or a compact binary layout, plus status
  heartbeats with data age and the host's NTP state.
- Device clock mapped to UTC.
- Raw JSONL recording with a size limit; replay paced by the device clock.
- Browser viewer (`examples/viewer.html`), fuzz targets, end-to-end tests.
- Verified live against a TiM551-2050001 (firmware V2.54).
