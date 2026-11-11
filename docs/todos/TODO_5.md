# TODO — Phase 5: PostgreSQL và MySQL sau MVP

## Mục tiêu và nguồn sự thật

Thêm PostgreSQL, sau đó MySQL, theo từng đợt độc lập; giữ SQLite là mặc định. Chỉ công bố hỗ trợ một backend khi backend đó đạt A18: cùng bộ integration test, migration, backup/restore và crash recovery như SQLite. Không coi việc kết nối hoặc migration thành công là A18 PASS.

- `docs/SPEC.md` §8, §11, §13 A18 và §14: semantics storage, vận hành và thứ tự backend.
- `docs/plans/01-stack-and-architecture.md` ADR-003, stack/version và hướng phụ thuộc.
- `docs/plans/02-contracts.md`: contract MQTT/HTTP, SQL, readiness, transaction, retention và test vector hiện hành. Khi cần đổi hành vi, sửa contract và test **trước** code.
- `docs/plans/03-delivery-plan.md` §8 P5-01–P5-06, §9 và §12: thứ tự, gate và Definition of Done.
- `docs/todos/TODO_4.md`, `docs/release-checklist.md`: evidence Phase 4 và điều kiện release.

## 0. Điều kiện vào và nguyên tắc

- [x] **Release gate Phase 4 đã đạt:** `v0.1.1`, commit `59b44fdb22ff94b719bbc56d40aa5f8ca7e78932`; GHCR digest, OIDC, SBOM/scan/checksum và GitHub Release được xác minh tại [TODO_4 §16](TODO_4.md#16-commit-và-hồ-sơ-bàn-giao) và [release report](../reports/release-v0.1.1-2026-11-09.md). GHCR private theo chủ đích; group pull cần `read:packages`.
- [x] Ghi baseline trên commit bắt đầu: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`, frontend và Docker smoke theo release checklist; ghi thời gian, số test pass/ignored và artifact. Test ignored chạy riêng khi thuộc gate.

  Historical baseline 2026-11-07T19:15:15+07:00 trên commit `86ca100301ca9d5aa3cb97155f47816ae76354ec`: Rust 1.98.0 — fmt, Clippy `-D warnings`, `cargo test --all` (248 passed, 0 failed, 2 ignored) PASS. Bun 1.4.4 — frozen install, typecheck, 29 tests/195 assertions và build PASS. Docker Engine 29.9.2 — `sh tests/docker_smoke.sh`, gồm Compose recreate A17 (1 passed) PASS.

  Baseline tại điểm vào Phase 5, 2026-11-09 trên release commit `59b44fdb22ff94b719bbc56d40aa5f8ca7e78932`: `cargo fmt --all --check`, Clippy, `cargo test --all`, `cargo test --release --locked`, `cargo build --release --locked`, frontend frozen/typecheck/29 tests/194 assertions/build, `cargo deny check`, `cargo audit` và Docker A17 smoke PASS. CI run 42626360028 xanh cả bốn job; `cargo audit` không còn vulnerability (hai cảnh báo unmaintained có exception hiện hành). Test manual load vẫn ignored; artifact `web/dist` build tại chỗ không phải release artifact.

  **Gate release Phase 5 đã mở; P5-01 hoàn tất** (inventory, port boundary, A18 matrix, committed-prefix contract và SQLite interleaving test trong [audit report](../reports/p5-01-storage-audit-2026-11-09.md)). P5-02 đang triển khai trên `feat/phas5/storage-audit-ci`: ADR-010 chốt PostgreSQL 18.6 / SQLx 0.9.0, cập nhật contract; domain ports, cleanup scheduler, PostgreSQL code và A18 vẫn chưa được đánh dấu PASS.
- [ ] Đọc lại contract trước mỗi task; nếu cần bật SQLx feature cho backend, chọn version engine/image hoặc đổi kiến trúc/config/TLS, ghi quyết định và lý do trong `01` trước khi triển khai. Không tự nâng version/dependency.
- [ ] PostgreSQL hoàn tất và qua gate riêng trước đợt MySQL. SQLite vẫn chạy độc lập, không cần database server ngoài, không regression.
- [ ] Giữ queue hữu hạn, timeout/pool hữu hạn, transaction rõ ràng, redaction và nguyên tắc ACK sau commit. Không tự chuyển dữ liệu giữa engine khi đổi URL; export/import là hạng mục riêng.
- [ ] Mỗi task có test hoặc evidence chạy lại được; test dùng DB/data dir riêng, port tự do và cleanup fixture của chính test. Không dùng production DB hoặc secret thật làm fixture.

## 1. Audit storage và contract — P5-01

- [x] Inventory các đường SQLite, transaction/failure boundaries và callers cho bootstrap/session, device/credential/audit, MQTT auth/presence, telemetry/attributes/history/retention, command/ACK, readiness, migration và shutdown trong [P5-01 audit](../reports/p5-01-storage-audit-2026-11-09.md).
- [x] Ghi SQLite leaks vào HTTP/MQTT/runtime và engine-specific SQL/ID/cursor; `CommandRepository` và telemetry insert port là một phần, không phải toàn bộ abstraction.
- [x] Chốt port/domain boundaries tối thiểu cho audit này; giữ core không import SQLx/adapter, không generic ORM, và đặt audit nguyên tử bên trong operation/repository sở hữu transaction.
- [x] Sửa `02` để định nghĩa committed-prefix checkpoint độc lập engine và thêm test SQLite cho duplicate/conflict allocator gaps cùng writer interleaving vào `tests/telemetry_api.rs`. Mỗi backend tương lai vẫn phải có interleaving test riêng với T1/T2, rollback và full backfill trước A18; không dùng sequence/identity/auto-increment đơn lẻ làm bằng chứng.
- [x] Đối chiếu semantics hiện tại về số/UTC ms, JSON, collation, uniqueness/FK, rollback, pagination, isolation và retention. Audit report ghi rõ contract cố định lẫn lựa chọn engine-specific còn mở; các lựa chọn mở là gate trước code P5-02. Không sửa migration SQLite đã áp dụng.
- [x] Tạo A18 suite matrix chung trong audit report: scenario, fixture/suite, lệnh, SQLite evidence và ô version/result cần điền độc lập cho PostgreSQL/MySQL. Khác biệt setup/fault injection phải được ghi lại, không bỏ scenario.

## 2. PostgreSQL — P5-02

- [x] Chốt ADR-010 trong `01`: PostgreSQL 18.6, image `postgres:18.6-bookworm`, SQLx 0.9.0 PostgreSQL/TLS feature, URL selection, pool/retry, migration, type/collation/transaction/checkpoint mapping; cập nhật `02` trước code. Đây chỉ là quyết định, không phải A18 PASS.
- [x] Wire session cleanup vào scheduler hourly backend-neutral; cả hai adapter xóa expired/old-revoked theo batch 5 000. Giữ cancellation/metrics và kiểm tra cleanup adapter PostgreSQL trong test DB riêng.
- [x] Hoàn tất/wire domain ports để HTTP/MQTT/runtime/CLI chọn adapter theo URL; chuyển SQLite qua cùng ports trước PostgreSQL và giữ transaction/audit semantics. SQLite regression chạy trong gate bên dưới.
- [x] Pool hữu hạn, timeout/retry tối đa 5 lần; runtime migrate trước listener và fail closed; CLI migrate chọn đúng backend. PostgreSQL migration chạy trên DB mới và chạy lại idempotent.
- [x] Implement repository/transaction PostgreSQL theo ma trận §1 với bind parameters; giữ SQLite hoạt động. Chưa coi adapter smoke test là evidence đủ cho mất kết nối/readiness.
- [x] Kiểm thử PostgreSQL mất kết nối rồi phục hồi, readiness và không ACK thành công giả.
- [ ] Chạy chung A18: bootstrap/auth/ACL, telemetry commit→application ACK/dedup/conflict, REST/WS cursor và reconnect, command/idempotency/ACK/expiry, audit/credential, retention, shutdown, migration và recovery. Assert transaction/audit rollback đúng contract; không trả ACK lưu thành công hay HTTP success trước commit bền vững. Bao phủ race/rollback, DB failure/readiness và A08/A09 tương đương, gồm kill trước commit, sau commit trước ACK và sau ACK.
- [ ] Hoàn tất Compose/backup/restore §4 và baseline §5 cho PostgreSQL. Ghi lệnh, phiên bản, timestamp, test pass/fail/ignored, artifact và giới hạn; chỉ đánh dấu PostgreSQL hỗ trợ sau khi tất cả gate backend pass.

**Evidence P5-02 hiện tại (2026-11-10):** `tests/postgres_storage.rs` chạy riêng với `postgres:18.6-bookworm` qua `IOT_TEST_POSTGRES_URL` tới DB test riêng tên `iot_test`. Trước `DROP SCHEMA`, test xác minh `current_database()` và `current_schema()` trên connection thực tế; URL có `dbname` override khác fixture bị từ chối. Test chạy CLI migrate/admin-create, fresh + rerun migration, admin/session (lookup ASCII case-insensitive), device/credential/audit, attributes, command/idempotency/dispatch/ACK/expiry, telemetry dedup/conflict/NUL rejection/rollback, allocator writer interleaving, overview, retention và cleanup. `cli_check` xác minh URL/options và TLS policy mà không kết nối DB; telemetry/attributes/metadata/command/ACK validation chung từ chối U+0000. Đây là adapter smoke/interleaving evidence, **không phải A18 PASS**; test PostgreSQL cố ý `ignored` trong `cargo test --all` và được chạy thủ công riêng.

**Evidence outage/recovery (2026-11-10T15:28+07:00):** trên Rust/Cargo 1.98.0, Docker Engine 29.9.2, `postgres:18.6-bookworm` image ID `sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5`, chạy `sh tests/postgres_recovery.sh`. Harness tạo container có label riêng, port host tự chọn cố định qua restart và DB `iot_test`; test xác minh image/label cùng `current_database()`/`current_schema()` trước ghi, không reset/xóa schema. `cargo test --test postgres_outage`: 1 passed, 1 ignored; startup-failure probe nhận đúng 5 kết nối thử, process exit code 4 trong <4 s, HTTP/MQTT port vẫn bind được. Lượt `--ignored`: 1 passed, 1 filtered; container PostgreSQL stop/start khi app còn chạy, readiness 503→200, lỗi ingest chỉ trả `retryable/storage_unavailable`, không có success ACK lúc DB down, write đang chờ được commit sau hồi phục, retry nhận `duplicate`, tổng telemetry row = 1; readiness chỉ lên lại sau khi command dispatcher ghi attempt vào DB. Không có fail trong lần chạy cuối; script xóa đúng container fixture sau test. Giới hạn: fault là dừng toàn container trên một host loopback, không mô phỏng packet loss/partition dài, disk-full, HA/failover hoặc nhiều app instance.

**A08 crash boundaries PostgreSQL (cùng lệnh/script và image):** harness tạo thêm DB riêng `iot_a18_test`, rồi chạy cùng ba test `tests/crash_recovery.rs` trên URL PostgreSQL theo thứ tự tuần tự (`--test-threads=1`), không drop schema. 3 passed, 0 failed, 0 ignored: kill trước commit, sau commit/trước ACK, và sau ACK; cả ba khôi phục/retry đúng, một telemetry row duy nhất, pending command sống qua restart. Hook trước commit được đặt trong PostgreSQL transaction, hook sau commit dùng chung ingest worker. Đây là cùng test/assertions đã chạy cho SQLite trong `cargo test --all`; chỉ khác DB fixture/placeholder SQL trong helper. Chưa chứng minh power-loss/hardware failure, disk-full hoặc A08 cùng với network partition.

**A18 common black-box partial (2026-11-10T15:47+07:00):** `tests/backend_a18.rs` dùng chung các assertions cho SQLite và PostgreSQL. SQLite: `cargo test --test backend_a18` — 1 passed, 1 ignored; PostgreSQL: `sh tests/postgres_recovery.sh` — outage 1 passed/1 ignored in default run, outage ignored 1 passed/1 filtered, `tests/crash_recovery.rs` 3 passed/0 failed/0 ignored, `tests/backend_a18.rs` 1 passed/1 filtered. Trên cùng image `postgres:18.6-bookworm` ID `sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5`, fixture có label riêng; tests kiểm tra DB `iot_test` hoặc `iot_a18_test` và schema `public`, không reset/drop schema. Shared suite covers idempotent migrations, no default admin, uppercase login, MQTT bad-token/cross-device ACL, telemetry `stored`/`duplicate`/`event_id_conflict`, REST cursor paging + WS live/reconnect backfill, command idempotency/ACK, command-audit rollback and redaction, competing queue-limit creates, credential rotation, retention delete, SIGTERM/restart persistence. Cùng ba A08 crash boundaries được chạy cho từng backend. Chưa phải A18 PASS: còn PostgreSQL Compose recreate, backup/restore, load baseline; chưa chạy đủ matrix (đặc biệt full auth/ACL, command expiry/late ACK, session-cleanup scheduler, shutdown failure/deadline, rollback/race matrix và toàn bộ API suites) trên PostgreSQL. Disk/power-loss/HA/failover không được mô phỏng.

**Rust/security rerun sau nhóm A18 (2026-11-10T15:50+07:00):** `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`, `cargo deny check`, `cargo audit` pass. `cargo test --all`: 292 passed, 0 failed, 5 ignored (Docker recreate, manual load, và 3 PostgreSQL tests được chạy riêng); `cargo deny check` có duplicate-version warnings hiện hành; `cargo audit` có 2 allowed unmaintained warnings (`atomic-polyfill`, `instant`), không có vulnerability.

**Gate PostgreSQL trước MySQL:** chỉ bắt đầu §3 khi mọi mục PostgreSQL ở §2, §4 và §5 đã pass; ma trận A18 đầy đủ và SQLite regression pass; test ignored thuộc gate đã chạy riêng; evidence/version/artifact và giới hạn đã được ghi. PostgreSQL chỉ được công bố hỗ trợ độc lập sau gate này; không lấy kết quả compile, connect hoặc migrate thay A18.

## 3. MySQL — P5-03

- [ ] Chỉ bắt đầu sau **Gate PostgreSQL trước MySQL** ở §2; chốt version/feature/config/migration trong `01`/`02` trước code, không giả định SQL hoặc isolation giống PostgreSQL/SQLite.
- [ ] Implement pool, migration và repository theo cùng contract; kiểm tra JSON, collation, unique/FK, transaction/isolation, cursor và ACK sau commit bằng test thực tế. Không dùng thay đổi backend này để làm hỏng SQLite/PostgreSQL.
- [ ] Chạy cùng ma trận A18 §1 và các trường hợp migration, crash/recovery, fault, security, command và telemetry như PostgreSQL; ghi evidence và mọi khác biệt fixture/fault injection.
- [ ] Hoàn tất Compose/backup/restore §4 và baseline §5 cho MySQL. Chỉ công bố hỗ trợ khi toàn bộ gate riêng pass; kết quả PostgreSQL không thay thế evidence MySQL.

## 4. Compose và backup/restore — P5-04, lặp cho từng backend

- [ ] Thêm biến thể Compose riêng theo backend: database server ngoài có volume, healthcheck, cấu hình/secret phù hợp; app retry kết nối có giới hạn, không chỉ dựa vào thứ tự startup. Không nhúng password cố định trong image/repo hoặc log URL đầy đủ.
- [ ] Smoke trên môi trường cô lập: migrate, bootstrap admin, MQTT→commit→ACK→REST, recreate app/database với volume và xác nhận device, credential, telemetry, command và runtime revision còn đúng.
- [ ] Viết quy trình backup/restore theo engine và kiểm tra quyền, tính nhất quán, version, checksum và thất bại; restore vào DB/data dir mới, khởi động app và kiểm tra dữ liệu qua API/MQTT. Không ghi đè dữ liệu đang dùng; ghi artifact, lệnh và timestamp.
- [ ] Tài liệu hóa cách triển khai, config, secret, health/readiness, backup, restore, rollback, giới hạn và troubleshooting; SQLite/WAL procedure không bị thay thế bằng hướng dẫn engine ngoài.

**Implementation draft, gate chưa chạy (2026-11-11):** thêm `deploy/compose.postgres.yml`, local secret wrapper, config, `backup-postgres.sh`, `restore-postgres.sh`, operations docs và `tests/postgres_compose.sh`/`tests/postgres_compose.rs`. Kiểm tra tĩnh: `sh -n deploy/compose-postgres.sh deploy/backup-postgres.sh deploy/restore-postgres.sh tests/postgres_compose.sh tests/postgres_recovery.sh` PASS; `IOT_POSTGRES_PASSWORD_FILE=<temporary hex fixture> IOT_TEST_POSTGRES_PORT=54329 deploy/compose-postgres.sh --file tests/compose.postgres.test.yml config --quiet` PASS; `cargo test --test postgres_compose` 0 pass, 0 fail, 1 ignored (fixture test compiled, not executed). Compose image-build/recreate/pg_dump/restore smoke **chưa chạy tới kết quả**; thao tác thử dài bị hủy theo yêu cầu người dùng, không có container fixture còn lại. Vì chưa có dynamic evidence, các gate §4 vẫn unchecked.

Sau khi thêm harness Compose: `cargo fmt --all --check`, Clippy `-D warnings`, `cargo test --all` PASS (292 passed, 0 failed, 6 ignored; PostgreSQL/Compose ignored tests chỉ compile). Đây không thay thế `sh tests/postgres_compose.sh` và không đóng gate.

## 5. Đo tải và giới hạn — P5-05, lặp cho từng backend

- [ ] Chạy baseline riêng cho mỗi backend, ghi hardware/cgroup, version engine, cấu hình pool/DB, message size, QoS, số client, thời gian, publish request, PUBACK, application ACK/drop, commit, p95, RSS và disk growth; ghi rõ cửa sổ đo và drain.
- [ ] Ghi cấu hình retry và kết quả/giới hạn đo; so sánh SQLite chỉ khi điều kiện tương đương. Không suy tốc độ commit từ tốc độ tạo request; không công bố throughput/SLO khi chưa có evidence phù hợp. Phase 4 baseline SQLite có application ACK drop cao, không dùng làm sizing production.
- [ ] Ghi báo cáo và giới hạn vận hành/backend; nếu cần tối ưu, đo trước, giữ correctness và bounded resources, thêm test tương ứng.

PostgreSQL load harness/report chưa có; không suy throughput từ SQLite baseline. Không đánh dấu P5-05 PASS.

## 6. Cân nhắc hạng mục sau MVP — P5-06

- [ ] Ghi quyết định riêng về TLS MQTT tích hợp, MQTT 5 và cross-device ACL: nhu cầu, threat model, contract, ADR, test/gate và owner; **không mặc định triển khai** trong việc hỗ trợ SQL backend. Những mục sau MVP khác trong SPEC §2.2 cũng không tự vào scope.
- [ ] Cho tới khi có listener TLS được chốt và kiểm thử, giữ cảnh báo MQTT plaintext và dùng TLS proxy khi truy cập qua mạng công khai.

## 7. Gate cuối và hồ sơ

- [ ] A18 PostgreSQL PASS và A18 MySQL PASS độc lập theo ma trận §1; nếu một backend chưa pass thì không công bố hỗ trợ backend đó. SQLite regression, protocol/security, shutdown và các gate liên quan vẫn pass.
- [ ] Fmt, Clippy, `cargo test --all`, integration/crash/backup/restore và Docker Compose cho từng backend pass; ghi lệnh, timestamp, phiên bản và số test pass/ignored. Test ignored phải có evidence chạy riêng; không tick từ test chỉ compile.
- [ ] Mỗi backend có báo cáo load, quy trình vận hành/restore được kiểm thử, migration/rollback note và tài liệu support rõ version/giới hạn. Không đưa credential, DB URL, payload nhạy cảm hoặc artifact tạm vào Git.
- [ ] Cập nhật checklist release và CI cho backend đã hỗ trợ; lưu SBOM/scan/signature/provenance theo release checklist. Tạo tag/push/publish chỉ khi được yêu cầu và mọi release gate trên đúng commit đã pass.
- [ ] Ghi hồ sơ: commit, engine/image version, timestamp, kết quả A18 từng backend, danh sách test/artifact, backup/restore, load và các giới hạn/rủi ro còn lại. Commit theo nhóm task, mỗi commit build/test được.

**Phase 5 PASS** khi cả PostgreSQL và MySQL đạt A18 riêng, regression SQLite pass và các gate vận hành/tài liệu/evidence trên hoàn tất. Trạng thái hỗ trợ từng backend được công bố độc lập, không suy từ kết quả backend còn lại.
