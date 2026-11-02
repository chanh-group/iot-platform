CREATE INDEX telemetry_device_internal_idx
    ON telemetry_events(device_id, internal_id);
