# IoT Server dashboard

Client-only SolidJS SPA using same-origin HTTP and WebSocket APIs. Navigation is
tab state only; reloading starts at Overview. No router, SSR, state library, or
UI framework is used.

## Toolchain

- Bun 1.4.2 for install, test, and build.
- SolidJS 1.9.18, `vite-plugin-solid` 2.11.15, Vite 8.3.5,
  TypeScript 7.0.2, and uPlot 1.6.32 are exact-pinned.

```sh
bun install --frozen-lockfile
bun run --bun typecheck
bun run --bun test
bun run --bun build
```

Start the backend on `127.0.0.1:8080`, then run `bun run --bun dev`. Vite proxies
`/api` without rewriting `Host` or `Origin`; cookies remain same-origin and the
backend CSRF checks still apply. The frontend never persists session or device
tokens in browser storage.

`bun test` runs API/request/chart/WebSocket regressions, a headless Chrome test
with an in-memory API fixture, and `backend-ui.test.ts`, which builds and runs a
temporary Rust server and drives the Solid app in Chrome against its HTTP API and
MQTT broker. MQTT traffic uses the focused `examples/backend_ui_mqtt.rs` helper
with `rumqttc`, not a browser-test packet implementation. The backend test requires
Cargo and Chrome; set `CHROME_BIN` if Chrome is not on PATH. Both browser tests
clean up their temporary server/profile.
