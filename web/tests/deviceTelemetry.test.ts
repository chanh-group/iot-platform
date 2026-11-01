import { describe, expect, it } from "bun:test";
import { createRoot } from "solid-js";
import { ApiError, type ApiClient } from "../src/lib/api";
import type { Page, TelemetryRow } from "../src/lib/models";
import { useDeviceTelemetry } from "../src/lib/deviceTelemetry";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

describe("device telemetry backfill lifecycle", () => {
  it("aborts and invalidates an old request before invalid or future-start errors", async () => {
    for (const start of ["not-a-datetime", "9999-12-31T23:59"]) {
      const oldResponse = deferred<Page<TelemetryRow>>();
      let holdNextRequest = false;
      let oldSignal: AbortSignal | undefined;
      let staleContinuationRequests = 0;
      const api = {
        get<T>(path: string, signal?: AbortSignal): Promise<T> {
          if (path.endsWith("/telemetry/latest")) {
            return Promise.resolve({ items: [] } as T);
          }
          if (path.includes("cursor=stale-cursor")) {
            staleContinuationRequests += 1;
            return Promise.resolve({ items: [], next_cursor: null } as T);
          }
          if (holdNextRequest) {
            holdNextRequest = false;
            oldSignal = signal;
            // Ignore cancellation here to exercise the generation guard independently of fetch.
            return oldResponse.promise as Promise<T>;
          }
          return Promise.resolve({ items: [], next_cursor: null } as T);
        },
      } as unknown as ApiClient;

      let telemetry!: ReturnType<typeof useDeviceTelemetry>;
      const dispose = createRoot((disposeRoot) => {
        telemetry = useDeviceTelemetry(api, "device-id", () => 0, () => []);
        return disposeRoot;
      });

      holdNextRequest = true;
      telemetry.retryBackfill();
      expect(telemetry.backfill().status).toBe("loading");

      telemetry.changeFrom(start);
      telemetry.retryBackfill();
      expect(telemetry.backfill().status).toBe("error");
      expect(telemetry.backfill().error).toBeInstanceOf(ApiError);
      expect(oldSignal?.aborted).toBe(true);

      oldResponse.resolve({
        items: [{
          internal_id: 1,
          event_id: "stale-backfill",
          ts: 1,
          received_at: 1,
          values: { temperature: 20 },
        }],
        next_cursor: "stale-cursor",
      });
      await Bun.sleep(0);

      expect(telemetry.backfill().status).toBe("error");
      expect(telemetry.backfill().error).toBeInstanceOf(ApiError);
      expect(staleContinuationRequests).toBe(0);
      dispose();
    }
  });
});
