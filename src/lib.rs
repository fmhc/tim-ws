//! Streams SICK TiM LiDAR scans (CoLa A over TCP) to WebSocket clients.
//!
//! Data flow: [`sensor`] or [`replay`] → [`hub`] (serialise once) → [`server`].

pub mod clock;
pub mod cola;
pub mod frame;
pub mod hub;
pub mod record;
pub mod replay;
pub mod scan;
pub mod sensor;
pub mod server;
