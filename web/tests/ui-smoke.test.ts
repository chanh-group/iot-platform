import { expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import type { ServerWebSocket } from "bun";
import { createServer } from "vite";
import solid from "vite-plugin-solid";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { Command, Device, LatestValue, TelemetryRow } from "../src/lib/models";

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
  let sessionExpired = false;
  let logoutRequests = 0;
  let websocketOpened = 0;
  let websocketClosed = 0;
  let eventId = 0;
  let historyEnabled = false;
  let backfillEnabled = false;
  let checkpointRequests = 0;
  let initialCatchupRequests = 0;
  let checkpointHighWater = 5_000;
  let initialCatchupPending = false;
  let delayPrimaryDeviceAck = false;
  let missedNearOpen = false;
  let initialOverviewRequestId: number | undefined;
  let historyOverlap: TelemetryRow | undefined;
  let latestItems: LatestValue[] = [];
  let holdNextLatestResponse = false;
  let releaseHeldLatestResponse: (() => void) | undefined;
  let configRevision = 7;
  let configLevel = "info";
  let retentionDays = 30;
  let batchSize = 500;
  let flushIntervalMs = 1_000;
  let deviceReads = 0;
  let attributeReads = 0;
  let latestReads = 0;
  let commandReads = 0;
  let failOverviewOnce = true;
  let overviewStorageOk = true;
  let createdDevice: Device | undefined;
  let commandSequence = 0;
  let commandRecords: Command[] = [];
  const devicePatches: Array<{ id: string; patch: Record<string, unknown> }> = [];
  let deviceCreateRequests = 0;
  let credentialRotations = 0;
  const historyRequests: Array<{ from: string | null; to: string | null; afterId: string | null; limit: string | null; cursor: string | null; key: string | null }> = [];
  const backfillRequests: Array<{ from: string | null; to: string | null; afterId: string | null; limit: string | null; cursor: string | null; key: string | null }> = [];
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
  const baseDevice: Device = {
    id: deviceId,
    name: "smoke-sensor",
    enabled: true,
    online: true,
    metadata: {},
    created_at: Date.now() - 10_000,
    updated_at: Date.now() - 10_000,
    last_seen_at: Date.now() - 1_000,
    credential: { id: "smoke-credential", created_at: Date.now() - 10_000 },
  };
  const device = (id = deviceId): Device => {
    const value = id === otherDeviceId
      ? createdDevice ?? { ...baseDevice, id, name: "created-sensor", credential: { ...baseDevice.credential } }
      : baseDevice;
    return { ...value, metadata: { ...value.metadata }, credential: { ...value.credential } };
  };
  const configResponse = () => ({
    revision: configRevision,
    config: {
      logging: { level: configLevel },
      telemetry: { retention_days: retentionDays, batch_size: batchSize, flush_interval_ms: flushIntervalMs },
    },
    restart_required: [],
  });
  const waitForFixture = async (ready: () => boolean, label: string) => {
    for (let attempt = 0; attempt < 500; attempt += 1) {
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
        if (sessionExpired || !authenticated || !request.headers.get("cookie")?.includes("iot_session=smoke")) {
          return Response.json({ code: "unauthenticated", message: "unauthenticated", request_id: "smoke-ws-401" }, { status: 401 });
        }
        server.upgrade(request, { data: { fixture: true } });
        return undefined;
      }
      if (url.pathname === "/api/v1/auth/me") {
        return authenticated && !sessionExpired
          ? Response.json({ id: "smoke-admin-id", username: "smoke-admin", expires_at: Date.now() + 60_000 })
          : Response.json({ code: "unauthenticated", message: "unauthenticated", request_id: "smoke-me-401" }, { status: 401 });
      }
      if (url.pathname === "/api/v1/auth/login" && request.method === "POST") {
        const body = await request.json() as { username?: string; password?: string };
        if (body.username !== "smoke-admin" || body.password !== "smoke-password") {
          return Response.json({ code: "invalid_credentials", message: "invalid credentials", request_id: "smoke-login-401" }, { status: 401 });
        }
        authenticated = true;
        sessionExpired = false;
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
      if (sessionExpired && url.pathname.startsWith("/api/v1/")) {
        return Response.json({ code: "unauthenticated", message: "session expired", request_id: "smoke-session-401" }, { status: 401 });
      }
      if (url.pathname === "/api/v1/overview") {
        if (failOverviewOnce) {
          failOverviewOnce = false;
          return Response.json({
            code: "storage_unavailable",
            message: "storage unavailable",
            request_id: "smoke-storage-503",
          }, { status: 503 });
        }
        return Response.json({
          devices: { total: 0, online: 0, disabled: 0 },
          telemetry: { events_24h: 0, last_received_at: null },
          commands: { pending: 0, dispatched: 0, failed_24h: 0, expired_24h: 0 },
          ingest: { queue_depth: 0, queue_capacity: 4_096, rejected_24h: 0 },
          storage_ok: overviewStorageOk,
        });
      }
      if (url.pathname === "/api/v1/devices") {
        if (request.method === "POST") {
          deviceCreateRequests += 1;
          const body = await request.json() as { name: string; metadata: Record<string, unknown>; enabled: boolean };
          createdDevice = {
            ...device(otherDeviceId),
            name: body.name,
            metadata: body.metadata,
            enabled: body.enabled,
            updated_at: Date.now(),
          };
          return Response.json({
            device: device(otherDeviceId),
            credential: { id: "created-credential", token: "iot_created_one_time_token", created_at: Date.now() },
          }, { status: 201 });
        }
        const query = url.searchParams.get("q")?.toLowerCase() ?? "";
        const enabledFilter = url.searchParams.get("enabled");
        const all = [device(), ...(createdDevice ? [device(otherDeviceId)] : [])]
          .filter((item) => !query || item.name.toLowerCase().includes(query) || item.id.includes(query))
          .filter((item) => enabledFilter === null || String(item.enabled) === enabledFilter);
        const cursor = url.searchParams.get("cursor");
        if (cursor === "devices-cursor-2") return Response.json({ items: all.slice(1), next_cursor: null });
        const hasNext = !query && enabledFilter === null && all.length > 1;
        return Response.json({ items: hasNext ? all.slice(0, 1) : all, next_cursor: hasNext ? "devices-cursor-2" : null });
      }
      const devicePath = url.pathname.match(/^\/api\/v1\/devices\/([^/]+)$/);
      if (devicePath && request.method === "PATCH") {
        const id = devicePath[1] ?? "";
        const patch = await request.json() as Record<string, unknown>;
        devicePatches.push({ id, patch });
        const current = id === otherDeviceId ? createdDevice : baseDevice;
        if (!current) return Response.json({ code: "device_not_found", message: "device not found" }, { status: 404 });
        if (typeof patch.name === "string") current.name = patch.name;
        if (typeof patch.enabled === "boolean") current.enabled = patch.enabled;
        if (typeof patch.metadata === "object" && patch.metadata !== null && !Array.isArray(patch.metadata)) {
          current.metadata = patch.metadata as Record<string, unknown>;
        }
        current.updated_at = Date.now();
        return Response.json(device(id));
      }
      if (devicePath && request.method === "GET") {
        const id = devicePath[1] ?? "";
        deviceReads += 1;
        return Response.json(device(id));
      }
      const credentialPath = url.pathname.match(/^\/api\/v1\/devices\/([^/]+)\/credentials\/rotate$/);
      if (credentialPath && request.method === "POST") {
        credentialRotations += 1;
        const id = credentialPath[1] ?? "";
        const current = id === otherDeviceId ? createdDevice : baseDevice;
        if (!current) return Response.json({ code: "device_not_found", message: "device not found" }, { status: 404 });
        current.credential = { id: `rotated-credential-${credentialRotations}`, created_at: Date.now() };
        return Response.json({
          device_id: id,
          credential: { ...current.credential, token: `iot_rotated_token_${credentialRotations}` },
        });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/attributes`) {
        attributeReads += 1;
        return Response.json({ items: [], next_cursor: null });
      }
      if (url.pathname === `/api/v1/devices/${otherDeviceId}/attributes`) {
        attributeReads += 1;
        return Response.json({ items: [{ key: "firmware", value: "2.0", device_ts: null, updated_at: Date.now() }], next_cursor: null });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/telemetry/latest`) {
        latestReads += 1;
        if (holdNextLatestResponse) {
          holdNextLatestResponse = false;
          await new Promise<void>((resolve) => { releaseHeldLatestResponse = resolve; });
        }
        return Response.json({ items: latestItems });
      }
      if (url.pathname === `/api/v1/devices/${deviceId}/telemetry`) {
        const query = {
          from: url.searchParams.get("from"),
          to: url.searchParams.get("to"),
          afterId: url.searchParams.get("after_id"),
          limit: url.searchParams.get("limit"),
          cursor: url.searchParams.get("cursor"),
          key: url.searchParams.get("keys"),
        };
        historyRequests.push(query);
        if (query.afterId !== null && query.key === null) {
          const afterId = Number(query.afterId);
          if (afterId === 0 && query.limit === "1") {
            checkpointRequests += 1;
            initialCatchupPending = true;
            return Response.json({
              items: [{
                internal_id: 1,
                event_id: "old-history-bootstrap",
                ts: Date.now() - 24 * 60 * 60_000,
                received_at: Date.now() - 24 * 60 * 60_000,
                values: { temperature: 1 },
              }],
              next_cursor: "old-history-after-first-page",
              through_id: checkpointHighWater,
            });
          }
          if (initialCatchupPending && afterId === checkpointHighWater) {
            initialCatchupPending = false;
            initialCatchupRequests += 1;
            if (missedNearOpen) {
              return Response.json({
                items: [{
                  internal_id: afterId + 1,
                  event_id: `missed-between-acks-${afterId}`,
                  ts: Date.now() - 500,
                  received_at: Date.now(),
                  values: { humidity: "late-open-event" },
                }],
                next_cursor: null,
                through_id: afterId + 1,
              });
            }
            return Response.json({ items: [], next_cursor: null, through_id: afterId });
          }
          if (!backfillEnabled) {
            return Response.json({ items: [], next_cursor: null, through_id: afterId });
          }
          backfillRequests.push(query);
          const pageNumber = query.cursor ? Number(query.cursor.replace("backfill-", "")) : 1;
          if (afterId >= 10_000 && !query.cursor) {
            const selectedTo = Number([...historyRequests].reverse().find((history) => history.to !== null)?.to ?? Date.now());
            return Response.json({
              items: [{
                internal_id: afterId + 1,
                event_id: `second-reconnect-${afterId}`,
                ts: selectedTo - 1,
                received_at: Date.now(),
                values: { temperature: 27 },
              }],
              next_cursor: null,
              through_id: afterId + 10,
            });
          }
          const throughId = 10_000;
          const selectedFrom = Number([...historyRequests].reverse().find((history) => history.from !== null)?.from ?? Date.now() - 60_000);
          const selectedTo = Number([...historyRequests].reverse().find((history) => history.to !== null)?.to ?? Date.now());
          const span = Math.max(1, selectedTo - selectedFrom);
          const firstInternalId = afterId + (pageNumber - 1) * 200 + 1;
          return Response.json({
            items: Array.from({ length: 200 }, (_, index) => {
              const internalId = firstInternalId + index;
              return {
                internal_id: internalId,
                event_id: `backfill-${afterId}-${internalId}`,
                ts: selectedFrom + internalId % span,
                received_at: Date.now(),
                values: { temperature: 20 + index % 10 },
              };
            }),
            next_cursor: pageNumber < 6 ? `backfill-${pageNumber + 1}` : null,
            through_id: throughId,
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
      if (url.pathname === `/api/v1/devices/${otherDeviceId}/telemetry/latest`) {
        return Response.json({ items: [] });
      }
      if (url.pathname === `/api/v1/devices/${otherDeviceId}/telemetry`) {
        const afterId = url.searchParams.get("after_id");
        return afterId === null
          ? Response.json({ items: [], next_cursor: null })
          : Response.json({ items: [], next_cursor: null, through_id: Number(afterId) });
      }
      const commandsPath = url.pathname.match(/^\/api\/v1\/devices\/([^/]+)\/commands$/);
      if (commandsPath && request.method === "POST") {
        const id = commandsPath[1] ?? "";
        const body = await request.json() as { name: string; params: Record<string, unknown>; ttl_ms?: number };
        const now = Date.now();
        const commandId = `smoke-command-${++commandSequence}`;
        const command: Command = {
          id: commandId,
          device_id: id,
          name: body.name,
          params: body.params,
          status: "pending",
          created_at: now,
          expires_at: now + (body.ttl_ms ?? 60_000),
          dispatched_at: null,
          completed_at: null,
          result: null,
          error: null,
          attempts: 0,
        };
        commandRecords = [command, ...commandRecords];
        return Response.json({ command_id: commandId, device_id: id, status: command.status, expires_at: command.expires_at }, { status: 202 });
      }
      if (commandsPath && request.method === "GET") {
        commandReads += 1;
        const id = commandsPath[1] ?? "";
        return Response.json({ items: commandRecords.filter((command) => command.device_id === id), next_cursor: null });
      }
      const commandPath = url.pathname.match(/^\/api\/v1\/commands\/([^/]+)$/);
      if (commandPath && request.method === "GET") {
        const command = commandRecords.find((item) => item.id === commandPath[1]);
        return command
          ? Response.json(command)
          : Response.json({ code: "command_not_found", message: "command not found" }, { status: 404 });
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
        const payload = JSON.parse(String(message)) as { action?: string; channels?: string[]; request_id?: number };
        websocketActions.push(payload);
        if (payload.action === "subscribe") {
          if (payload.channels?.includes("overview") && !payload.channels.some((channel) => channel.startsWith("device:"))) {
            initialOverviewRequestId ??= payload.request_id;
          }
          if (delayPrimaryDeviceAck && payload.channels?.includes(`device:${deviceId}`)) {
            delayPrimaryDeviceAck = false;
            socket.send(JSON.stringify({
              type: "subscribed",
              channels: ["overview"],
              request_id: initialOverviewRequestId,
            }));
            setTimeout(() => {
              missedNearOpen = true;
              socket.send(JSON.stringify({
                type: "subscribed",
                channels: ["overview", `device:${deviceId}`],
                request_id: payload.request_id,
              }));
            }, 150);
          } else {
            socket.send(JSON.stringify({
              type: "subscribed",
              channels: [...new Set(["overview", ...(payload.channels ?? [])])],
              request_id: payload.request_id,
            }));
          }
        }
      },
      close(socket) {
        sockets.delete(socket);
        websocketClosed += 1;
      },
    },
  });

  const chromePath = process.env.CHROME_BIN ?? "google-chrome";
  const profile = await mkdtemp(join(tmpdir(), "iot-dashboard-smoke-"));
  let vite: Awaited<ReturnType<typeof createServer>> | undefined;
  let chrome: ReturnType<typeof Bun.spawn> | undefined;
  let devtools: WebSocket | undefined;
  try {
    const root = resolve(import.meta.dir, "..");
    vite = await createServer({
      configFile: false,
      root,
      plugins: [solid()],
      server: {
        host: "127.0.0.1",
        port: 0,
        strictPort: true,
        hmr: false,
        proxy: {
          "/api": {
            target: `http://127.0.0.1:${fixture.port}`,
            changeOrigin: false,
            ws: true,
          },
        },
      },
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
    ], { stdout: "ignore", stderr: "inherit" });
    const activePortFile = `${profile}/DevToolsActivePort`;
    let devtoolsPort = "";
    for (let attempt = 0; attempt < 600 && !devtoolsPort; attempt += 1) {
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
      password.value = "wrong-password";
      password.dispatchEvent(new Event("input", { bubbles: true }));
      document.querySelector('form[aria-label="Admin login"]')?.requestSubmit();
      return true;
    })()`);
    await waitFor("document.body.innerText.includes('invalid credentials')", Boolean, "invalid login error");
    expect(await evaluate("document.body.innerText.includes('smoke-login-401')")).toBe(true);
    await evaluate(`(() => {
      const password = document.querySelector('input[name="password"]');
      if (!password) return false;
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
    await waitFor("document.body.innerText.includes('storage unavailable')", Boolean, "overview storage error");
    expect(await evaluate("document.body.innerText.includes('smoke-storage-503')")).toBe(true);
    overviewStorageOk = false;
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Refresh')?.click()`);
    await waitFor("document.body.innerText.includes('Storage is unavailable. New telemetry may not be saved.')", Boolean, "storage warning");
    overviewStorageOk = true;
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Refresh')?.click()`);
    await waitFor("document.body.innerText.includes('Devices online')", Boolean, "recovered overview data");

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "devices view");
    await waitFor("document.body.innerText.includes('smoke-sensor')", Boolean, "device list result");

    expect(await evaluate(`Array.from(document.querySelectorAll('input, textarea, select')).every((control) => control.labels?.length || control.getAttribute('aria-label'))`)).toBe(true);
    await evaluate(`(() => {
      const search = document.querySelector('#device-search');
      if (!search) return false;
      search.value = 'smoke';
      search.dispatchEvent(new Event('input', { bubbles: true }));
      search.closest('form')?.requestSubmit();
      return true;
    })()`);
    await waitFor("document.querySelector('#device-search')?.value", (value) => value === "smoke", "device search filter");
    await evaluate(`(() => {
      const search = document.querySelector('#device-search');
      if (!search) return false;
      search.value = '';
      search.dispatchEvent(new Event('input', { bubbles: true }));
      search.closest('form')?.requestSubmit();
      return true;
    })()`);

    await evaluate(`(() => {
      const name = document.querySelector('#new-device-name');
      const metadata = document.querySelector('#new-device-metadata');
      if (!name || !metadata) return false;
      name.value = 'created-sensor';
      name.dispatchEvent(new Event('input', { bubbles: true }));
      metadata.value = '{"zone":"west"}';
      metadata.dispatchEvent(new Event('input', { bubbles: true }));
      name.closest('form')?.requestSubmit();
      return true;
    })()`);
    await waitFor("document.querySelector('#issued-token')?.value", (value) => value === "iot_created_one_time_token", "one-time create credential");
    expect(deviceCreateRequests).toBe(1);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.includes('Close and clear token'))?.click()`);
    await waitFor("document.querySelector('#issued-token') === null", Boolean, "cleared create credential");
    expect(await evaluate("localStorage.length")).toBe(0);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Next')?.click()`);
    await waitFor("document.body.innerText.includes('created-sensor')", Boolean, "second device page");
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'created-sensor')?.click()`);
    await waitFor("document.querySelector('#device-title')?.textContent?.trim()", (value) => value === "Device detail", "created device detail");
    await waitFor("document.body.innerText.includes('2.0')", Boolean, "latest attributes");
    await waitFor(`(() => {
      const name = document.querySelector('#edit-device-name');
      const metadata = document.querySelector('#edit-device-metadata');
      const form = name?.closest('form');
      return Boolean(form && metadata?.closest('form') === form && form.querySelector('button[type="submit"]')?.textContent?.trim() === 'Save name and metadata');
    })()`, Boolean, "device edit form with name, metadata, and save controls");
    const editFormSubmitted = await evaluate(`(() => {
      const name = document.querySelector('#edit-device-name');
      const metadata = document.querySelector('#edit-device-metadata');
      const form = name?.closest('form');
      if (!name || !metadata || !form || metadata.closest('form') !== form) return false;
      name.value = 'edited-sensor';
      name.dispatchEvent(new Event('input', { bubbles: true }));
      metadata.value = '{"zone":"east"}';
      metadata.dispatchEvent(new Event('input', { bubbles: true }));
      form.requestSubmit();
      return true;
    })()`);
    if (editFormSubmitted !== true) throw new Error("Device edit form controls disappeared before submission.");
    await waitForFixture(() => devicePatches.some(({ id, patch }) => id === otherDeviceId && patch.name === "edited-sensor"), "device name and metadata PATCH");
    await waitFor("document.querySelector('#device-summary-title')?.textContent?.trim()", (value) => value === "edited-sensor", "device name and metadata edit");
    expect(devicePatches.some(({ id, patch }) => id === otherDeviceId && patch.name === "edited-sensor" && JSON.stringify(patch.metadata) === '{"zone":"east"}')).toBe(true);

    await evaluate(`(() => {
      const name = document.querySelector('#command-name');
      const form = name?.closest('form');
      if (!form) return false;
      name.value = 'restart';
      name.dispatchEvent(new Event('input', { bubbles: true }));
      form.requestSubmit();
      return true;
    })()`);
    await waitFor("document.body.innerText.includes('smoke-command-1')", Boolean, "command create and detail");
    commandRecords[0] = { ...commandRecords[0]!, status: "succeeded", completed_at: Date.now(), result: { accepted: true } };
    broadcast("command", {
      command_id: "smoke-command-1",
      device_id: otherDeviceId,
      status: "succeeded",
      state_version: 3,
      completed_at: commandRecords[0]?.completed_at ?? null,
      error: null,
    }, otherDeviceId);
    await waitFor("document.querySelector('section[aria-labelledby=\\\"commands-title\\\"]')?.innerText.toLowerCase().includes('succeeded')", Boolean, "command ACK state refresh");
    await waitFor("document.querySelector('section[aria-labelledby=\\\"commands-title\\\"]')?.innerText.includes('accepted')", Boolean, "command result detail refresh");
    const beforeStaleCommandEvent = commandReads;
    broadcast("command", {
      command_id: "smoke-command-1",
      device_id: otherDeviceId,
      status: "dispatched",
      state_version: 2,
      completed_at: null,
      error: null,
    }, otherDeviceId);
    await waitForFixture(() => commandReads > beforeStaleCommandEvent, "stale command event REST invalidation");
    await waitFor("document.querySelector('section[aria-labelledby=\\\"commands-title\\\"]')?.innerText.toLowerCase().includes('succeeded')", Boolean, "stale command event keeps REST terminal state");
    const beforeDuplicateCommandEvent = commandReads;
    const duplicateCommandEvent = JSON.stringify({
      type: "command",
      id: "duplicate-command-event",
      ts: Date.now(),
      device_id: otherDeviceId,
      data: { command_id: "smoke-command-1", device_id: otherDeviceId, status: "pending", state_version: 1 },
    });
    for (const socket of sockets) socket.send(duplicateCommandEvent);
    for (const socket of sockets) socket.send(duplicateCommandEvent);
    await waitForFixture(() => commandReads > beforeDuplicateCommandEvent, "duplicate command event REST invalidation");
    await Bun.sleep(200);
    expect(commandReads).toBe(beforeDuplicateCommandEvent + 1);

    await evaluate(`document.querySelector('#command-name')?.closest('form')?.requestSubmit()`);
    await waitFor("document.body.innerText.includes('smoke-command-2')", Boolean, "second command create");
    commandRecords[0] = { ...commandRecords[0]!, status: "expired", completed_at: Date.now() };
    broadcast("command", {
      command_id: "smoke-command-2",
      device_id: otherDeviceId,
      status: "expired",
      state_version: 3,
      completed_at: commandRecords[0]?.completed_at ?? null,
      error: null,
    }, otherDeviceId);
    await waitFor("document.querySelector('section[aria-labelledby=\\\"commands-title\\\"]')?.innerText.toLowerCase().includes('expired')", Boolean, "expired command status");

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Disable device')?.click()`);
    await waitFor("[...document.querySelectorAll('button')].some((button) => button.textContent?.trim() === 'Enable device')", Boolean, "disable device action");
    expect(device(otherDeviceId).enabled).toBe(false);
    await evaluate("window.confirm = () => true");
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Rotate token')?.click()`);
    await waitFor("document.querySelector('#issued-token')?.value", (value) => value === "iot_rotated_token_1", "one-time rotated credential");
    expect(credentialRotations).toBe(1);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.includes('Close and clear token'))?.click()`);
    await waitFor("document.querySelector('#issued-token') === null", Boolean, "cleared rotated credential");

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "return to device list");
    await evaluate(`(() => {
      const filter = document.querySelector('#device-enabled');
      if (!filter) return false;
      filter.value = 'false';
      filter.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()`);
    await waitFor("document.body.innerText.includes('edited-sensor')", Boolean, "disabled-device filter");
    await evaluate(`(() => {
      const filter = document.querySelector('#device-enabled');
      if (!filter) return false;
      filter.value = 'all';
      filter.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()`);
    await waitFor("document.body.innerText.includes('smoke-sensor')", Boolean, "all-device filter");
    delayPrimaryDeviceAck = true;
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'smoke-sensor')?.click()`);
    await waitFor("document.querySelector('#device-title')?.textContent?.trim()", (value) => value === "Device detail", "device detail");
    await waitForFixture(() => checkpointRequests === 1 && initialCatchupRequests === 1, "initial commit checkpoint and post-subscribe catch-up");
    expect(missedNearOpen).toBe(true);
    await waitFor("document.body.innerText.includes('retains 1 recent events')", Boolean, "event committed between stale and device ACKs recovered");
    expect(historyRequests.find((query) => query.afterId === "0" && query.limit === "1")?.cursor).toBe(null);
    expect(historyRequests.find((query) => query.afterId === "5000")?.limit).toBe("200");
    expect(historyRequests.some((query) => query.from !== null && query.to !== null)).toBe(true);
    const historyToInput = await evaluate(`(() => {
      const date = new Date(Date.now());
      const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
      return local.toISOString().slice(0, 23);
    })()`) as string;
    expect(await evaluate(`(() => {
      const input = document.querySelector('#history-to');
      if (!input) return false;
      input.value = ${JSON.stringify(historyToInput)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    })()`)).toBe(true);
    const selectedToMs = await evaluate("new Date(document.querySelector('#history-to')?.value ?? '').getTime()") as number;
    await waitForFixture(() => historyRequests.some((query) => query.to === String(selectedToMs)), "selected history upper bound");
    const initialRangeTo = historyRequests.at(-1)?.to;
    expect(initialRangeTo).toBeString();
    const initialRangeToMs = Number(initialRangeTo);
    expect(Number.isFinite(initialRangeToMs)).toBe(true);
    expect(await evaluate("document.querySelector('.chart-host') === null")).toBe(true);

    const firstLiveTimestamp = initialRangeToMs - 1_000;
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
    holdNextLatestResponse = true;
    broadcast("telemetry", {
      event_id: "live-3",
      ts: firstLiveTimestamp,
      received_at: firstReceivedAt,
      values: { temperature: 23, humidity: 63 },
    }, deviceId);
    await waitForFixture(() => latestReads === latestReadsBeforeTie + 1 && releaseHeldLatestResponse !== undefined, "REST latest tie-break refetch");
    await waitFor("document.querySelector('section[aria-labelledby=\"latest-title\"] .loading')?.textContent", Boolean, "latest tie refetch loading state");
    const manyTieEvents: Array<Record<string, unknown>> = [];
    for (let group = 0; group < 80; group += 1) {
      for (let index = 0; index < 2; index += 1) {
        const event = {
          event_id: `tie-burst-${group}-${index}`,
          ts: firstLiveTimestamp + 5_000,
          received_at: firstReceivedAt + 5_000,
          values: { [`tie_key_${group}`]: group * 10 + index },
        };
        manyTieEvents.push(event);
        broadcast("telemetry", event, deviceId);
      }
    }
    await waitFor(`[...document.querySelectorAll('#telemetry-key option')].some((option) => option.value === "tie_key_79")`, Boolean, "all latest tie groups arrive during loading");
    expect(latestReads).toBe(latestReadsBeforeTie + 1);
    releaseHeldLatestResponse?.();
    releaseHeldLatestResponse = undefined;
    await waitForFixture(() => latestReads === latestReadsBeforeTie + 2, "one coalesced refresh for newly observed tie groups");
    await waitFor(
      "[...document.querySelectorAll('section[aria-labelledby=\\\"latest-title\\\"] tbody tr')].find((row) => row.firstElementChild?.textContent === 'temperature')?.children[1]?.textContent",
      (value) => value === "22",
      "REST-resolved latest value",
    );
    expect(latestReads).toBe(latestReadsBeforeTie + 2);
    await Bun.sleep(300);
    expect(latestReads).toBe(latestReadsBeforeTie + 2);
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
      ts: initialRangeToMs + 1_000,
      received_at: initialRangeToMs + 1_100,
      values: { temperature: 24, humidity: 64 },
    }, deviceId);
    await waitFor(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for humidity, 3 numeric samples",
      "out-of-range live event excluded from selected history range",
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
      (value) => value === "Telemetry chart for temperature, 3 numeric samples",
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
    const retainedLiveTs = Date.now() - 500;
    broadcast("telemetry", {
      event_id: "live-retained-across-reconnects",
      ts: retainedLiveTs,
      received_at: retainedLiveTs,
      values: { temperature: 31 },
    }, deviceId);
    await waitFor("document.querySelector('.chart-host')?.getAttribute('aria-label')", (value) => typeof value === "string" && value.includes("numeric sample"), "live point before reconnect");
    const chartPointsBeforeReconnect = Number(((await evaluate("document.querySelector('.chart-host')?.getAttribute('aria-label')")) as string).match(/, (\d+) numeric/)?.[1]);
    const previousSocketCount = websocketOpened;
    const latestReadsBeforeReconnect = latestReads;
    backfillEnabled = true;
    for (const socket of [...sockets]) socket.close(1012, "smoke reconnect");
    await waitForFixture(() => websocketOpened > previousSocketCount, "WebSocket reconnect");
    await waitForFixture(() => backfillRequests.length === 5, "bounded multi-page reconnect backfill");
    await waitFor("document.body.innerText.includes('Reconnect backfill is bounded')", Boolean, "visible bounded backfill");
    expect(backfillRequests).toHaveLength(5);
    expect(backfillRequests.every((query) => query.limit === "200" && query.key === null)).toBe(true);
    expect(backfillRequests.map((query) => query.cursor)).toEqual([null, "backfill-2", "backfill-3", "backfill-4", "backfill-5"]);
    expect(new Set(backfillRequests.map((query) => query.afterId)).size).toBe(1);
    expect(backfillRequests.every((query) => query.from === null && query.to === null)).toBe(true);
    expect(await evaluate("document.body.innerText.includes('More pages remain; continue from the saved cursor.')")).toBe(true);
    expect(await evaluate("document.querySelector('#history-to')?.value")).toBe(backfillToControl);

    const secondSocketCount = websocketOpened;
    for (const socket of [...sockets]) socket.close(1013, "second smoke reconnect");
    await waitForFixture(() => websocketOpened > secondSocketCount, "second WebSocket reconnect");
    await waitForFixture(() => backfillRequests.length === 7, "catch-up after saved cursor");
    await waitFor("document.body.innerText.includes('committed checkpoint')", Boolean, "complete commit-order backfill");
    expect(backfillRequests[5]?.cursor).toBe("backfill-6");
    expect(backfillRequests[6]?.afterId).toBe("10000");
    const recoveredStatus = await evaluate("document.querySelector('section[aria-labelledby=\"telemetry-title\"] .notice[role=\"status\"]')?.textContent") as string | undefined;
    if (!recoveredStatus?.includes("retains 100 recent events")) throw new Error(`Unexpected backfill status: ${recoveredStatus}`);
    const chartPointsAfterReconnects = Number(((await evaluate("document.querySelector('.chart-host')?.getAttribute('aria-label')")) as string).match(/, (\d+) numeric/)?.[1]);
    expect(chartPointsAfterReconnects).toBeGreaterThan(chartPointsBeforeReconnect);
    expect(recoveredStatus).toContain("evicted from chart memory");
    const historyReadsBeforeExpandedRange = historyRequests.length;
    const expandedTo = await evaluate(`(() => {
      const date = new Date(Date.now() + 60_000);
      const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
      return local.toISOString().slice(0, 23);
    })()`) as string;
    await evaluate(`(() => {
      const input = document.querySelector('#history-to');
      if (!input) return false;
      input.value = ${JSON.stringify(expandedTo)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    })()`);
    const expandedToMs = await evaluate("new Date(document.querySelector('#history-to')?.value ?? '').getTime()") as number;
    await waitForFixture(() => historyRequests.some((query) => query.from !== null && query.to === String(expandedToMs)), "history reload after expanding range with evicted backfill rows");
    expect(historyRequests.length).toBeGreaterThan(historyReadsBeforeExpandedRange);

    await waitForFixture(() => latestReads > latestReadsBeforeReconnect, "latest REST refetch after reconnect");
    await waitFor("document.querySelector('section[aria-labelledby=\"latest-title\"] .loading') === null", Boolean, "latest response after reconnect");
    const latestReadsBeforeDuplicateTie = latestReads;
    for (const event of manyTieEvents.slice(0, 2)) broadcast("telemetry", event, deviceId);
    await Bun.sleep(300);
    expect(latestReads).toBe(latestReadsBeforeDuplicateTie);

    expect(await evaluate("document.querySelector('.chart-host') !== null")).toBe(true);
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "return to devices");
    await waitFor("document.querySelector('.chart-host') === null", Boolean, "chart unmount");
    expect(websocketActions.some((message) => message.action === "unsubscribe" && message.channels?.includes(`device:${deviceId}`))).toBe(true);

    checkpointHighWater = Math.max(checkpointHighWater, 10_010) + 1_001;
    missedNearOpen = false;
    await waitFor("[...document.querySelectorAll('button')].some((button) => button.textContent?.trim() === 'smoke-sensor')", Boolean, "device available for re-open");
    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'smoke-sensor')?.click()`);
    await waitFor("document.querySelector('#device-title')?.textContent?.trim()", (value) => value === "Device detail", "reopened device after old events accumulated");
    await waitForFixture(() => checkpointRequests === 2 && initialCatchupRequests === 2, "fresh checkpoint on device re-open");
    expect(historyRequests.some((query) => query.afterId === String(checkpointHighWater))).toBe(true);
    expect(backfillRequests).toHaveLength(7);

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Devices')?.click()`);
    await waitFor("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "leave reopened device");
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

    sessionExpired = true;
    for (const socket of [...sockets]) socket.close(4401, "session expired");
    await waitFor("document.querySelector('form[aria-label=\\\"Admin login\\\"]') !== null", Boolean, "session-expired login screen");
    await waitFor("document.body.innerText.includes('Your session has expired. Sign in again.')", Boolean, "session expiry notice");
    sessionExpired = false;
    await evaluate(`(() => {
      const username = document.querySelector('input[name="username"]');
      const password = document.querySelector('input[name="password"]');
      if (!username || !password) return false;
      username.value = 'smoke-admin';
      username.dispatchEvent(new Event('input', { bubbles: true }));
      password.value = 'smoke-password';
      password.dispatchEvent(new Event('input', { bubbles: true }));
      document.querySelector('form[aria-label="Admin login"]')?.requestSubmit();
      return true;
    })()`);
    await waitFor("document.querySelector('#overview-title')?.textContent?.trim()", (value) => value === "System overview", "sign in after session expiry");

    await evaluate(`[...document.querySelectorAll('button')].find((button) => button.textContent?.trim() === 'Log out')?.click()`);
    await waitFor("document.querySelector('form[aria-label=\\\"Admin login\\\"]') !== null", Boolean, "logout and login screen");
    for (let attempt = 0; attempt < 50 && websocketClosed < websocketOpened; attempt += 1) await Bun.sleep(50);

    expect(authenticated).toBe(false);
    expect(logoutRequests).toBe(1);
    expect(websocketOpened).toBeGreaterThan(1);
    expect(websocketClosed).toBeGreaterThanOrEqual(websocketOpened);
  } finally {
    releaseHeldLatestResponse?.();
    devtools?.close();
    if (chrome) {
      chrome.kill();
      await chrome.exited;
    }
    if (vite) await vite.close();
    fixture.stop(true);
    await rm(profile, { recursive: true, force: true });
  }
}, 90_000);
