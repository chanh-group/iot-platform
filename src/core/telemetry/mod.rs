//! Telemetry payload types and validation (`02` §2.6).

mod ingest;
mod payload;
mod repository;
mod validate;

pub use ingest::{
    IngestHandle, IngestResult, IngestSubmitError, IngestWorkerControl, spawn_worker,
};
pub use payload::{TelemetryPayload, ValidatedTelemetry};
pub use repository::{
    LatestTelemetryValue, TelemetryHistoryRange, TelemetryInsertResult, TelemetryReadRepository,
    TelemetryRepository, TelemetryRetentionRepository, TelemetryRow, filtered_values,
};
pub(crate) use validate::valid_values;
pub use validate::{TelemetryError, event_id_from_raw, validate};
