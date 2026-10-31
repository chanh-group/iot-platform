import { expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import type { ServerWebSocket } from "bun";
import { createServer } from "vite";
import { resolve } from "node:path";
import type { LatestValue, TelemetryRow } from "../src/lib/models";

interface DevtoolsTarget {
  type: string;
  webSocketDebuggerUrl: string;
}

interface DevtoolsReply {
  id?: number;
  result?: { result?: { value?: unknown }; exceptionDetails?: unknown };
  error?: { message: string };
}

const deviceId = "018f3e20-8b00-7000-8000-000000000001";
const otherDeviceId = "018f3e20-8b00-7000-8000-000000000002";

it("tests live telemetry selection, paged history, scoped refreshes, config conflicts, and logout cleanup in Solid", async () => {
  let authenticated = false;
  let logoutRequests = 0;
  let websocketOpened = 0;
  let websocketClosed = 0;
  let eventId = 0;
  let historyEnabled = false;
  let backfillEnabled = false;
  let historyOverlap: TelemetryRow | undefined;
  let lateEventSample: TelemetryRow | undefined;
  let lateEventReturned = false;
  let latestItems: LatestValue[] = [];
  let configRevision = 7;
  let configLevel = "info";
  let retentionDays = 30;
  let batchSize = 500;
  let flushIntervalMs = 1_000;
  let deviceReads = 0;
  let attributeReads = 0;
  let latestReads = 0;
  let commandReads = 0;
  const historyRequests: Array<{ from: string | null; to: string | null; limit: string | null; cursor: string | null; key: string | null }> = [];
  const backfillRequests: Array<{ from: string | null; to: string | null; limit: string | null; cursor: string | null; key: string | null }> = [];
  const configPatches: Array<{ expected_revision: number; settings: Record<string, unknown> }> = [];
  const websocketActions: Array<{ action?: string; channels?: string[] }> = [];
  const sockets = new Set<ServerWebSocket<{ fixture: true }>>();
  const broadcast = (type: string, data: Record<string, unknown>, eventDeviceId?: string) => {
    eventId += 1;
    const message = JSON.stringify({
      type,
      id: `smoke-event-${eventId}`,
      ts: Date.now(),
      ...(eventDeviceId ? { device_id: eventDeviceId } : {}),
      data,
    });
    for (const socket of sockets) socket.send(message);
  };
  const device = () => ({
    id: deviceId,
    name: "smoke-sensor",
    enabled: true,
    online: true,
    metadata: {},
    created_at: Date.now() - 10_000,
    updated_at: Date.now() - 10_000,
    last_seen_at: Date.now() - 1_000,
    credential: { id: "smoke-credential", created_at: Date.now() - 10_000 },
  });
  const configResponse = () => ({
    revision: configRevision,
    config: {
      logging: { level: configLevel },
      telemetry: { retention_days: retentionDays, batch_size: batchSize, flush_interval_ms: flushIntervalMs },
    },
    restart_required: [],
  });
  const waitForFixture = async (ready: () => boolean, label: string) => {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      if (ready()) return;
      await Bun.sleep(20);
    }
    throw new Error(`Timed out waiting for fixture ${label}.`);
  };
  const fixture = Bun.serve<{ fixture: true }>({
    port: 0,
    fetch: async (request, server) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/v1/events") {
        if (!request.headers.get("cookie")?.includes("iot_session=smoke")) {
          return Response.json({ code: "unauthenticated", message: "unauthenticated", request_id: "smoke-ws-401" }, { status: 401 });
        }
        server.upgrade(request, { data: { fixture: true } });
        return undefined;
      }
      if (url.pathname === "/api/v1/auth/me") {
        return authenticated
          ? Response.json({ id: "smoke-admin-id", username: "smoke-admin", expires_at: Date.now() + 60_000 })
          : Response.json({ code: "unauthenticated", message: "unauthenticated", request_id: "smoke-me-401" }, { status: 401 });
      }
      if (url.pathname === "/api/v1/auth/login" && request.method === "POST") {
        const body = await request.json() as { username?: string; password?: string };
        if (body.username !== "smoke-admin" || body.password !== "smoke-password") {
          return Response.json({ code: "invalid_credentials", message: "invalid credentials", request_id: "smoke-login-401" }, { status: 401 });
        }
        authenticated = true;
        return Response.json({
          user: { id: "smoke-admin-id", username: "smoke-admin" },
          expires_at: Date.now() + 60_000,
        }, { headers: { "Set-Cookie": "iot_session=smoke; Path=/; HttpOnly; SameSite=Strict" } });
      }
      if (url.pathname === "/api/v1/auth/logout" && request.method === "POST") {
        logoutRequests += 1;
        authenticated = false;
        return new Response(null, {
          status: 204,
          headers: { "Set-Cookie": "iot_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0" },
        });
      }
      if (url.pathname === "/api/v1/overview") {
        return Response.json({
          devices: { total: 0, online: 0, disabled: 0 },
          telemetry: { events_24h: 0, last_received_at: null },
          commands: { pending: 0, dispatched: 0, failed_24h: 0, expired_24h: 0 },
          ingest: { queue_depth: 0, queue_capacity: 4_096, rejected_24h: 0 },
          storage_ok: true,
        });
      }
      if (url.pathname === "/api/v1/devices") {
        return Response.json({ items: [device()], next_cursor: null });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}`) {
        deviceReads += 1;
        return Response.json(device());
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/attributes`) {
        attributeReads += 1;
        return Response.json({ items: [], next_cursor: null });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/telemetry/latest`) {
        latestReads += 1;
        return Response.json({ items: latestItems });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/telemetry`) {
        const query = {
          from: url.searchParams.get("from"),
          to: url.searchParams.get("to"),
          limit: url.searchParams.get("limit"),
          cursor: url.searchParams.get("cursor"),
          key: url.searchParams.get("keys"),
        };
        historyRequests.push(query);
        if (backfillEnabled && query.key === null) {
          backfillRequests.push(query);
          const pageNumber = query.cursor ? Number(query.cursor.replace("backfill-", "")) : 1;
          const timestamp = Number(query.from) + pageNumber;
          lateEventSample = {
            internal_id: 99,
            event_id: "late-event-before-range",
            ts: Number(query.from) - 1,
            received_at: Number(query.from) + 1,
            values: { temperature: 19 },
          };
          const inEventTimeRange = lateEventSample.ts >= Number(query.from) && lateEventSample.ts < Number(query.to);
          lateEventReturned ||= inEventTimeRange;
          return Response.json({
            items: [...(inEventTimeRange ? [lateEventSample] : []), {
              internal_id: pageNumber,
              event_id: `backfill-${pageNumber}`,
              ts: timestamp,
              received_at: timestamp,
              values: { temperature: 20 + pageNumber },
            }],
            next_cursor: pageNumber < 6 ? `backfill-${pageNumber + 1}` : null,
          });
        }
        if (!historyEnabled) return Response.json({ items: [], next_cursor: null });
        const firstRow = historyOverlap ?? {
          internal_id: 1,
          event_id: "history-1",
          ts: Date.now() - 2_000,
          received_at: Date.now() - 1_900,
          values: { humidity: 61, temperature: 22 },
        };
        const row: TelemetryRow = query.cursor === "history-cursor-2"
          ? {
            internal_id: 2,
            event_id: "history-2",
            ts: firstRow.ts + 100,
            received_at: firstRow.received_at + 100,
            values: { humidity: 62, temperature: 23 },
          }
          : firstRow;
        const selectedValues = query.key ? { [query.key]: row.values[query.key] } : row.values;
        return Response.json({
          items: [{ ...row, values: selectedValues }],
          next_cursor: query.cursor ? null : "history-cursor-2",
        });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/commands`) {
        commandReads += 1;
        return Response.json({ items: [], next_cursor: null });
      }
      if (url.pathname === "/api/v1/config/runtime" && request.method === "PATCH") {
        const patch = await request.json() as { expected_revision: number; settings: Record<string, unknown> };
        configPatches.push(patch);
        if (patch.expected_revision !== configRevision) {
          return Response.json({
            code: "revision_conflict",
            message: "configuration revision changed",
            request_id: "smoke-config-conflict",
            details: { current_revision: configRevision },
          }, { status: 409 });
        }
        const logging = patch.settings.logging as { level?: string } | undefined;
        const telemetry = patch.settings.telemetry as {
          retention_days?: number;
          batch_size?: number;
          flush_interval_ms?: number;
        } | undefined;
        if (logging?.level !== undefined) configLevel = logging.level;
        if (telemetry?.retention_days !== undefined) retentionDays = telemetry.retention_days;
        if (telemetry?.batch_size !== undefined) batchSize = telemetry.batch_size;
        if (telemetry?.flush_interval_ms !== undefined) flushIntervalMs = telemetry.flush_interval_ms;
        configRevision += 1;
        return Response.json(configResponse());
      }
      if (url.pathname === "/api/v1/config") {
        return Response.json(configResponse());
      }
      return Response.json({ code: "not_found", message: "not found", request_id: "smoke-404" }, { status: 404 });
    },
    websocket: {
      open(socket) {
        websocketOpened += 1;
        sockets.add(socket);
        socket.send(JSON.stringify({ type: "hello", server_time: Date.now(), max_subscriptions: 8 }));
      },
      message(socket, message) {
        const payload = JSON.parse(String(message)) as { action?: string; channels?: string[] };
        websocketActions.push(payload);
        if (payload.action === "subscribe") {
          socket.send(JSON.stringify({ type: "subscribed", channels: payload.channels ?? [] }));
        }
      },
      close(socket) {
        sockets.delete(socket);
        websocketClosed += 1;
      },
    },
  });

  const oldProxy = process.env.API_PROXY_TARGET;
  const chromePath = process.env.CHROME_BIN ?? "google-chrome";
  const profile = await mkdtemp("/tmp/opencode/iot-dashboard-smoke-");
  let vite: Awaited<ReturnType<typeof createServer>> | undefined;
  let chrome: ReturnType<typeof Bun.spawn> | undefined;
  let devtools: WebSocket | undefined;
  try {
    process.env.API_PROXY_TARGET = `http://127.0.0.1:${fixture.port}`;
    const root = resolve(import.meta.dir, "..");
    vite = await createServer({
      configFile: resolve(root, "vite.config.ts"),
      server: { host: "127.0.0.1", port: 0, strictPort: true, hmr: false },
    });
    await vite.listen();
    const address = vite.httpServer?.address();
    if (!address || typeof address === "string") throw new Error("Vite did not open a TCP listener.");
    const appUrl = `http://127.0.0.1:${address.port}`;

    chrome = Bun.spawn([
      chromePath,
      "--headless=new",
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--remote-allow-origins=*",
      "--remote-debugging-port=0",
      `--user-data-dir=${profile}`,
      "about:blank",
    ], { stdout: "ignore", stderr: "ignore" });
    const activePortFile = `${profile}/DevToolsActivePort`;
    let devtoolsPort = "";
    for (let attempt = 0; attempt < 100 && !devtoolsPort; attempt += 1) {
      if (await Bun.file(activePortFile).exists()) {
        devtoolsPort = (await Bun.file(activePortFile).text()).split("\n")[0] ?? "";
      } else {
        await Bun.sleep(50);
      }
    }
    if (!devtoolsPort) throw new Error("Chrome did not publish its remote debugging port.");

    let target: DevtoolsTarget | undefined;
    for (let attempt = 0; attempt < 50 && !target; attempt += 1) {
      const response = await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`);
      const targets = await response.json() as DevtoolsTarget[];
      target = targets.find((item) => item.type === "page");
      if (!target) await Bun.sleep(50);
    }
    if (!target) throw new Error("Chrome did not expose a page target.");

    devtools = new WebSocket(target.webSocketDebuggerUrl);
    await new Promise<void>((resolveOpen, reject) => {
      devtools!.addEventListener("open", () => resolveOpen(), { once: true });
      devtools!.addEventListener("error", () => reject(new Error("Could not connect to Chrome DevTools.")), { once: true });
    });
    let nextId = 0;
    const pending = new Map<number, { resolve: (value: DevtoolsReply) => void; reject: (error: Error) => void }>();
    devtools.addEventListener("message", (event) => {
      const reply = JSON.parse(String(event.data)) as DevtoolsReply;
      if (reply.id === undefined) return;
      const waiting = pending.get(reply.id);
      if (!waiting) return;
      pending.delete(reply.id);
      if (reply.error) waiting.reject(new Error(reply.error.message));
      else waiting.resolve(reply);
    });
    const call = (method: string, params: Record<string, unknown> = {}) => new Promise<DevtoolsReply>((resolveReply, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve: resolveReply, reject });
      devtools!.send(JSON.stringify({ id, method, params }));
    });
    const evaluate = async (expression: string): Promise<unknown> => {
      const reply = await call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
      if (reply.result?.exceptionDetails) throw new Error("Browser evaluation failed.");
      return reply.result?.result?.value;
    };
    const waitFor = async (expression: string, expected: (value: unknown) => boolean, label: string) => {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const value = await evaluate(expression);
        if (expected(value)) return;
        await Bun.sleep(100);
      }
      throw new Error(`Timed out waiting for ${label}.`);
    };

    await call("Page.enable");
    await call("Runtime.enable");
    await call("Page.navigate", { url: appUrl });
    await waitFor("document.querySelector('form[aria-label=\\\"Admin login\\\"]') !== null", Boolean, "login form");
    await evaluate(`(() => {
      const username = document.querySelector('input[name="username"]');
      const password = document.querySelector('input[name="password"]');
      if (!username || !password) return false;
      username.value = "smoke-admin";
      username.dispatchEvent(new Event("input", { bubbles: true }));
      password.value = "smoke-password";
      password.dispatchEvent(new Event("input", { bubbles: true }));
      document.querySelector('form button[type="submit"]')?.focus();
      return true;
    })()`);
    await call("Input.dispatchKeyEvent", { type: "keyDown", key: "Enter", code: "Enter", text: "\r", unmodifiedText: "\r", windowsVirtualKeyCode: 13 });
    await call("Input.dispatchKeyEvent", { type: "keyUp", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13 });
    await waitFor("document.querySelector('#overview-title')?.textContent?.trim()", (value) => value === "System overview", "dashboard overview");
    await waitFor("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates connected", "event socket");
    expect(await evaluate("localStorage.length")).toBe(0);

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "devices view");
    await waitFor("document.body.innerText.includes('smoke-sensor')", Boolean, "device list result");
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'smoke-sensor')?.click()`);
    await waitFor("document.querySelector('#device-title')?.textContent?.trim()", (value) => value === "Device detail", "device detail");
    await waitForFixture(() => historyRequests.length > 0, "initial empty history request");
    const initialRangeTo = historyRequests.at(-1)?.to;
    expect(initialRangeTo).toBeString();
    expect(await evaluate("document.querySelector('.chart-host') === null")).toBe(true);

    const firstLiveTimestamp = Number(initialRangeTo) - 1_000;
    const firstReceivedAt = firstLiveTimestamp + 100;
    broadcast("telemetry", {
      event_id: "live-1",
      ts: firstLiveTimestamp,
      received_at: firstReceivedAt,
      values: { temperature: 22, humidity: 61 },
    }, deviceId);
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for humidity, 1 numeric sample",
      "chart from live data when history is empty",
    );
    expect(websocketActions.some((message) => message.action === "subscribe" && message.channels?.includes(`device:${deviceId}`))).toBe(true);
    const latestReadsBeforeOutOfOrder = latestReads;
    broadcast("telemetry", {
      event_id: "live-2",
      ts: firstLiveTimestamp,
      received_at: firstReceivedAt - 1,
      values: { temperature: 99, humidity: 99 },
    }, deviceId);
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for humidity, 2 numeric samples",
      "out-of-order live chart point",
    );
    expect(latestReads).toBe(latestReadsBeforeOutOfOrder);
    expect(await evaluate(`([...document.querySelectorAll('section[aria-labelledby="latest-title"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'temperature')?.children[1]?.textContent)`)).toBe("22");

    latestItems = [
      { key: "firmware", value: "1.4.2", ts: firstLiveTimestamp, received_at: firstReceivedAt, event_id: "live-3" },
      { key: "relay", value: true, ts: firstLiveTimestamp, received_at: firstReceivedAt, event_id: "live-3" },
      { key: "humidity", value: 61, ts: firstLiveTimestamp, received_at: firstReceivedAt, event_id: "live-1" },
      { key: "temperature", value: 22, ts: firstLiveTimestamp, received_at: firstReceivedAt, event_id: "live-1" },
    ];
    const latestReadsBeforeTie = latestReads;
    broadcast("telemetry", {
      event_id: "live-3",
      ts: firstLiveTimestamp,
      received_at: firstReceivedAt,
      values: { temperature: 23, humidity: 63 },
    }, deviceId);
    await waitForFixture(() => latestReads > latestReadsBeforeTie, "REST latest tie-break refetch");
    await waitFor(
      "[...document.querySelectorAll('section[aria-labelledby=\\\"latest-title\\\"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'temperature')?.children[1]?.textContent",
      (value) => value === "22",
      "REST-resolved latest value",
    );
    expect(latestReads).toBe(latestReadsBeforeTie + 1);
    await Bun.sleep(300);
    expect(latestReads).toBe(latestReadsBeforeTie + 1);
    expect(await evaluate("[...document.querySelectorAll('section[aria-labelledby=\\\"latest-title\\\"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'firmware')?.children[1]?.textContent")).toBe("1.4.2");
    expect(await evaluate("[...document.querySelectorAll('section[aria-labelledby=\\\"latest-title\\\"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'relay')?.children[1]?.textContent")).toBe("true");
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for humidity, 3 numeric samples",
      "same-ts live chart point",
    );

    const displayedTo = await evaluate("document.querySelector('#history-to')?.value");
    const liveQueryCount = historyRequests.length;
    broadcast("telemetry", {
      event_id: "live-4",
      ts: Number(initialRangeTo) + 1_000,
      received_at: Number(initialRangeTo) + 1_100,
      values: { temperature: 24, humidity: 64 },
    }, deviceId);
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for humidity, 4 numeric samples",
      "live chart update",
    );
    expect(historyRequests).toHaveLength(liveQueryCount);
    expect(await evaluate("document.querySelector('#history-to')?.value")).toBe(displayedTo);
    await waitFor(
      "[...document.querySelectorAll('section[aria-labelledby=\\\"latest-title\\\"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'temperature')?.children[1]?.textContent",
      (value) => value === "24",
      "newer telemetry latest value",
    );

    await evaluate(`(() => {
      const key = document.querySelector('#telemetry-key');
      if (!key) return false;
      key.value = 'temperature';
      key.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()`);
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => typeof value === "string" && value.startsWith("Telemetry chart for temperature,"),
      "chart selection follows selected key",
    );

    historyOverlap = {
      internal_id: 1,
      event_id: "live-1",
      ts: firstLiveTimestamp,
      received_at: firstReceivedAt,
      values: { temperature: 22, humidity: 61 },
    };
    historyEnabled = true;
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Reload page')?.click()`);
    await waitFor(
      "document.querySelector('[aria-label=\\\"Next history page\\\"]')?.disabled",
      (value) => value === false,
      "bounded history page with a next cursor",
    );
    expect(historyRequests.at(-1)?.limit).toBe("200");
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for temperature, 4 numeric samples",
      "REST/live duplicate telemetry deduplication",
    );
    expect(historyRequests.filter((query) => query.cursor === "history-cursor-2")).toHaveLength(0);
    await evaluate("document.querySelector('[aria-label=\\\"Next history page\\\"]')?.click()");
    await waitForFixture(() => historyRequests.some((query) => query.cursor === "history-cursor-2"), "explicit next history page");
    await waitFor("document.querySelector('[aria-label=\\\"Telemetry history pages\\\"]')?.textContent", (value) => typeof value === "string" && value.includes("Page 2"), "history page two");

    const unrelatedCounts = { deviceReads, attributeReads, latestReads, commandReads, historyLength: historyRequests.length };
    broadcast("system", { kind: "queue_full", severity: "warning", message: "routing-scope-check" });
    await waitFor("document.body.innerText.includes('routing-scope-check')", Boolean, "unrelated event notice");
    broadcast("presence", { online: false, last_seen_at: Date.now() }, otherDeviceId);
    broadcast("command", {
      command_id: "other-command",
      device_id: otherDeviceId,
      status: "pending",
      state_version: 1,
      completed_at: null,
      error: null,
    }, otherDeviceId);
    await Bun.sleep(200);
    expect({ deviceReads, attributeReads, latestReads, commandReads, historyLength: historyRequests.length }).toEqual(unrelatedCounts);

    const beforePresence = { deviceReads, attributeReads, latestReads, commandReads, historyLength: historyRequests.length };
    broadcast("presence", { online: false, last_seen_at: Date.now() }, deviceId);
    await waitForFixture(() => deviceReads > beforePresence.deviceReads, "affected device presence refresh");
    expect({ attributeReads, latestReads, commandReads, historyLength: historyRequests.length }).toEqual({
      attributeReads: beforePresence.attributeReads,
      latestReads: beforePresence.latestReads,
      commandReads: beforePresence.commandReads,
      historyLength: beforePresence.historyLength,
    });

    const beforeCommand = { deviceReads, attributeReads, latestReads, commandReads, historyLength: historyRequests.length };
    broadcast("command", {
      command_id: "device-command",
      device_id: deviceId,
      status: "pending",
      state_version: 1,
      completed_at: null,
      error: null,
    }, deviceId);
    await waitForFixture(() => commandReads > beforeCommand.commandReads, "affected device command refresh");
    expect({ deviceReads, attributeReads, latestReads, historyLength: historyRequests.length }).toEqual({
      deviceReads: beforeCommand.deviceReads,
      attributeReads: beforeCommand.attributeReads,
      latestReads: beforeCommand.latestReads,
      historyLength: beforeCommand.historyLength,
    });

    const backfillToControl = await evaluate("document.querySelector('#history-to')?.value");
    const previousSocketCount = websocketOpened;
    backfillEnabled = true;
    for (const socket of [...sockets]) socket.close(1012, "smoke reconnect");
    await waitForFixture(() => websocketOpened > previousSocketCount, "WebSocket reconnect");
    await waitForFixture(() => backfillRequests.length === 5, "bounded multi-page reconnect backfill");
    await waitFor("document.body.innerText.includes('Reconnect backfill query is bounded')", Boolean, "visible bounded backfill");
    expect(backfillRequests).toHaveLength(5);
    expect(backfillRequests.every((query) => query.limit === "200" && query.key === null)).toBe(true);
    expect(backfillRequests.map((query) => query.cursor)).toEqual([null, "backfill-2", "backfill-3", "backfill-4", "backfill-5"]);
    expect(new Set(backfillRequests.map((query) => query.from)).size).toBe(1);
    expect(new Set(backfillRequests.map((query) => query.to)).size).toBe(1);
    expect(Number(backfillRequests[0]?.to) - Number(backfillRequests[0]?.from)).toBeLessThanOrEqual(31 * 24 * 60 * 60 * 1_000);
    expect(lateEventSample).toBeDefined();
    expect(lateEventSample!.ts).toBeLessThan(Number(backfillRequests[0]?.from));
    expect(lateEventSample!.received_at).toBeGreaterThanOrEqual(Number(backfillRequests[0]?.from));
    expect(lateEventSample!.received_at).toBeLessThan(Number(backfillRequests[0]?.to));
    expect(lateEventReturned).toBe(false);
    expect(await evaluate("document.body.innerText.includes('complete recovery cannot be established')")).toBe(true);
    expect(await evaluate("document.querySelector('#history-to')?.value")).toBe(backfillToControl);

    expect(await evaluate("document.querySelector('.chart-host') !== null")).toBe(true);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "return to devices");
    await waitFor("document.querySelector('.chart-host') === null", Boolean, "chart unmount");
    expect(websocketActions.some((message) => message.action === "unsubscribe" && message.channels?.includes(`device:${deviceId}`))).toBe(true);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Config')?.click()`);
    await waitFor("document.querySelector('#config-title')?.textContent?.trim()", (value) => value === "Runtime configuration", "config view");

    await waitFor("document.body.innerText.includes('Current revision: 7')", Boolean, "initial config revision");
    await evaluate(`(() => {
      const level = document.querySelector('#logging-level');
      if (!level) return false;
      level.value = 'debug';
      level.dispatchEvent(new Event('change', { bubbles: true }));
      document.querySelector('#logging-level')?.closest('form')?.requestSubmit();
      return true;
    })()`);
    await waitForFixture(() => configPatches.length === 1, "single-field config patch");
    await waitFor("document.body.innerText.includes('Current revision: 8')", Boolean, "saved config revision");
    expect(configPatches[0]?.expected_revision).toBe(7);
    expect(configPatches[0]?.settings).toEqual({ logging: { level: "debug" } });

    await evaluate(`(() => {
      const batch = document.querySelector('#batch-size');
      if (!batch) return false;
      batch.value = '600';
      batch.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    })()`);
    configRevision = 9;
    retentionDays = 14;
    broadcast("config", { revision: configRevision });
    await waitFor("document.body.innerText.includes('Configuration changed on the server')", Boolean, "concurrent config conflict");
    expect(await evaluate("document.querySelector('#batch-size')?.value")).toBe("600");
    expect(await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.includes('Save runtime settings'))?.disabled`)).toBe(true);
    expect(configPatches).toHaveLength(1);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.includes('Discard edits'))?.click()`);
    await waitFor("document.querySelector('#batch-size')?.value", (value) => value === "500", "discarded config draft");
    expect(await evaluate("document.querySelector('#retention-days')?.value")).toBe("14");

    await call("Page.reload");
    await waitFor("document.querySelector('#overview-title')?.textContent?.trim()", (value) => value === "System overview", "overview after reload");
    await waitFor("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates connected", "event socket after reload");

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Log out')?.click()`);
    await waitFor("document.querySelector('form[aria-label=\\\"Admin login\\\"]') !== null", Boolean, "logout and login screen");
    for (let attempt = 0; attempt < 50 && websocketClosed < websocketOpened; attempt += 1) await Bun.sleep(50);

    expect(authenticated).toBe(false);
    expect(logoutRequests).toBe(1);
    expect(websocketOpened).toBeGreaterThan(1);
    expect(websocketClosed).toBeGreaterThanOrEqual(websocketOpened);
  } finally {
    devtools?.close();
    if (chrome) {
      chrome.kill();
      await chrome.exited;
    }
    if (vite) await vite.close();
    fixture.stop(true);
    if (oldProxy === undefined) delete process.env.API_PROXY_TARGET;
    else process.env.API_PROXY_TARGET = oldProxy;
    await rm(profile, { recursive: true, force: true });
  }
}, 45_000);
