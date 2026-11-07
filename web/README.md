# Dashboard IoT Server

SPA SolidJS chỉ chạy phía client, dùng HTTP và WebSocket API same-origin. Điều hướng chỉ là trạng thái tab; reload bắt đầu ở Overview. Không dùng router, SSR, state library hay UI framework.

## Bộ công cụ

- Bun 1.4.2 cho install, test và build.
- SolidJS 1.9.18, `vite-plugin-solid` 2.11.15, Vite 8.3.5, TypeScript 7.0.2 và uPlot 1.6.32 được pin chính xác.

```sh
bun install --frozen-lockfile
bun run --bun typecheck
bun run --bun test
bun run --bun build
```

Khởi động backend tại `127.0.0.1:8080`, rồi chạy `bun run --bun dev`. Vite proxy `/api` mà không rewrite `Host` hoặc `Origin`; cookie vẫn same-origin và CSRF check của backend vẫn áp dụng. Frontend không bao giờ lưu session hoặc device token trong browser storage.

`bun test` chạy regression API/request/chart/WebSocket, một test Chrome headless với fixture API trong bộ nhớ, và `backend-ui.test.ts` — test này build và chạy server Rust tạm, rồi điều khiển ứng dụng Solid trong Chrome qua HTTP API và MQTT broker của server. Traffic MQTT dùng helper `examples/backend_ui_mqtt.rs` với `rumqttc`, không dùng implementation packet trong browser test. Test backend cần Cargo và Chrome; đặt `CHROME_BIN` nếu Chrome không có trong PATH. Cả hai browser test đều dọn server/profile tạm của chúng.
