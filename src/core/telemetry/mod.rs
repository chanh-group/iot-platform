//! Telemetry payload types and validation (`02` §2.6).

mod ingest;
mod payload;
mod validate;

pub use ingest::{IngestHandle, IngestResult, IngestSubmitError, spawn_worker};
pub use payload::{TelemetryPayload, ValidatedTelemetry};
pub use validate::{TelemetryError, event_id_from_raw, validate};
