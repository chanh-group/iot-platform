//! Telemetry payload types and validation (`02` §2.6).

mod ingest;
mod payload;
mod repository;
mod validate;

pub use ingest::{
    IngestHandle, IngestResult, IngestSubmitError, IngestWorkerControl, spawn_worker,
};
pub use payload::{TelemetryPayload, ValidatedTelemetry};
pub use repository::{TelemetryInsertResult, TelemetryRepository};
pub(crate) use validate::valid_values;
pub use validate::{TelemetryError, event_id_from_raw, validate};
