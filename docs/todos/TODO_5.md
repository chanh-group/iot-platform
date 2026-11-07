# TODO — Phase 5: PostgreSQL và MySQL sau MVP

## Mục tiêu và nguồn sự thật

Thêm PostgreSQL, sau đó MySQL, theo từng đợt độc lập; giữ SQLite là mặc định. Chỉ công bố hỗ trợ một backend khi backend đó đạt A18: cùng bộ integration test, migration, backup/restore và crash recovery như SQLite. Không coi việc kết nối hoặc migration thành công là A18 PASS.

- `docs/SPEC.md` §8, §11, §13 A18 và §14: semantics storage, vận hành và thứ tự backend.
- `docs/plans/01-stack-and-architecture.md` ADR-003, stack/version và hướng phụ thuộc.
- `docs/plans/02-contracts.md`: contract MQTT/HTTP, SQL, readiness, transaction, retention và test vector hiện hành. Khi cần đổi hành vi, sửa contract và test **trước** code.
- `docs/plans/03-delivery-plan.md` §8 P5-01–P5-06, §9 và §12: thứ tự, gate và Definition of Done.
- `docs/todos/TODO_4.md`, `docs/release-checklist.md`: evidence Phase 4 và điều kiện release.

## 0. Điều kiện vào và nguyên tắc

- [ ] **Chặn bắt đầu triển khai Phase 5 cho tới khi Phase 4 đã release thật**, theo delivery plan §8. Ghi tag, commit, thời gian, digest image và đường dẫn release/evidence đã verify. Phase 4 gate/dry-run PASS không đồng nghĩa đã release; hiện chưa có evidence GHCR/OIDC thật.
- [x] Ghi baseline trên commit bắt đầu: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`, frontend và Docker smoke theo release checklist; ghi thời gian, số test pass/ignored và artifact. Test ignored chạy riêng khi thuộc gate.

  Evidence 2026-11-07T19:15:15+07:00 trên commit `86ca100301ca9d5aa3cb97155f47816ae76354ec`: Rust 1.98.0 — fmt, Clippy `-D warnings`, `cargo test --all` (248 passed, 0 failed, 2 ignored) PASS. Bun 1.4.4 — frozen install, typecheck, 29 tests/195 assertions và build PASS. Docker Engine 29.9.2 — `sh tests/docker_smoke.sh`, gồm Compose recreate A17 (1 passed) PASS. Artifact frontend `web/dist` được build tại chỗ, không phải artifact release. Test manual load vẫn ignored; không thuộc baseline release checklist và report Phase 4 hiện hữu là evidence riêng.

  **Gate Phase 5 vẫn BLOCKED:** `TODO_4 §16` xác nhận chưa có tag/release GHCR, digest image đã ký OIDC, hoặc đường dẫn artifact release đã verify. Các kết quả baseline/dry-run không thay thế release thật. Không bắt đầu P5-01 hay các task triển khai backend cho tới khi có đủ evidence release ở trên.
- [ ] Đọc lại contract trước mỗi task; nếu cần bật SQLx feature cho backend, chọn version engine/image hoặc đổi kiến trúc/config/TLS, ghi quyết định và lý do trong `01` trước khi triển khai. Không tự nâng version/dependency.
- [ ] PostgreSQL hoàn tất và qua gate riêng trước đợt MySQL. SQLite vẫn chạy độc lập, không cần database server ngoài, không regression.
- [ ] Giữ queue hữu hạn, timeout/pool hữu hạn, transaction rõ ràng, redaction và nguyên tắc ACK sau commit. Không tự chuyển dữ liệu giữa engine khi đổi URL; export/import là hạng mục riêng.
- [ ] Mỗi task có test hoặc evidence chạy lại được; test dùng DB/data dir riêng, port tự do và cleanup fixture của chính test. Không dùng production DB hoặc secret thật làm fixture.

## 1. Audit storage và contract — P5-01

- [ ] Liệt kê từng đường truy cập SQLite và transaction hiện tại: bootstrap/session, device/credential/audit, MQTT auth/presence, telemetry ingest/history/latest/retention, command/ACK, readiness, migration và shutdown. Ghi nơi dùng pool, lỗi, ranh giới transaction và caller.
- [ ] Xác định SQLite leak vào HTTP/MQTT/runtime và type/query đặc thù (`SqlitePool`, `SqliteConnection`, PRAGMA, placeholder, JSON, `INDEXED BY`, ID/cursor). Hiện đã có `CommandRepository`, nhưng không giả định mọi đường storage đã qua port.
- [ ] Chốt port/domain type tối thiểu cần cho backend thứ hai; giữ `core` không import SQLx/adapter, không tạo generic ORM/framework. Giữ nguyên transaction atomically gắn audit/credential/command và semantics lỗi khi chuyển qua port.
- [ ] Đối chiếu cursor telemetry theo **thứ tự commit**, đặc biệt WebSocket catch-up: ID được cấp trước commit hoặc transaction commit đảo thứ tự không tự chứng minh high-water mark an toàn. Trước khi viết repository/backend, cập nhật contract `02` và test để chốt thuật toán high-water/checkpoint riêng nếu cần. Test interleaving có kiểm soát cho từng engine (T1 cấp ID rồi chờ; T2 commit và chụp checkpoint; T1 commit; tiếp tục catch-up) phải chứng minh không thể bỏ sót event; không dùng sequence/auto-increment làm bằng chứng thứ tự commit nếu chưa chứng minh được.
- [ ] Đối chiếu kiểu số, timestamp UTC milliseconds, JSON, collation/tên device, giới hạn, uniqueness, foreign key, rollback, pagination, isolation và retention. Chốt khác biệt/contract trong `02` trước code; không sửa migration SQLite đã áp dụng.
- [ ] Tạo ma trận A18 chung: tên test, dữ liệu fixture, backend, phiên bản engine, lệnh chạy, kết quả và timestamp. Các test chạy cùng scenario/assertion; khác biệt kỹ thuật setup hoặc fault injection phải ghi rõ, không bỏ test âm thầm.

## 2. PostgreSQL — P5-02

- [ ] Sau gate §0 và audit §1, chốt cấu hình engine/URL, SQLx feature, migration và hướng dẫn credential theo `01`/`02`; `url` và `url_env` loại trừ nhau, không fallback hoặc lộ URL/secret trong log/API.
- [ ] Pool hữu hạn, timeout và retry có giới hạn; migrate trước listener, migration fail thì startup fail. Migration riêng, idempotent theo cơ chế migration; kiểm tra DB mới, chạy lại và nâng cấp từ schema cũ; không downgrade tự động.
- [ ] Implement các repository/transaction cần thiết theo ma trận §1; query dùng bound parameters; giữ SQLite hoạt động. Kiểm thử DB lỗi/mất kết nối rồi phục hồi, readiness và không ACK thành công giả.
- [ ] Chạy chung A18: bootstrap/auth/ACL, telemetry commit→application ACK/dedup/conflict, REST/WS cursor và reconnect, command/idempotency/ACK/expiry, audit/credential, retention, shutdown, migration và recovery. Assert transaction/audit rollback đúng contract; không trả ACK lưu thành công hay HTTP success trước commit bền vững. Bao phủ race/rollback, DB failure/readiness và A08/A09 tương đương, gồm kill trước commit, sau commit trước ACK và sau ACK.
- [ ] Hoàn tất Compose/backup/restore §4 và baseline §5 cho PostgreSQL. Ghi lệnh, phiên bản, timestamp, test pass/fail/ignored, artifact và giới hạn; chỉ đánh dấu PostgreSQL hỗ trợ sau khi tất cả gate backend pass.

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

## 5. Đo tải và giới hạn — P5-05, lặp cho từng backend

- [ ] Chạy baseline riêng cho mỗi backend, ghi hardware/cgroup, version engine, cấu hình pool/DB, message size, QoS, số client, thời gian, publish request, PUBACK, application ACK/drop, commit, p95, RSS và disk growth; ghi rõ cửa sổ đo và drain.
- [ ] Ghi cấu hình retry và kết quả/giới hạn đo; so sánh SQLite chỉ khi điều kiện tương đương. Không suy tốc độ commit từ tốc độ tạo request; không công bố throughput/SLO khi chưa có evidence phù hợp. Phase 4 baseline SQLite có application ACK drop cao, không dùng làm sizing production.
- [ ] Ghi báo cáo và giới hạn vận hành/backend; nếu cần tối ưu, đo trước, giữ correctness và bounded resources, thêm test tương ứng.

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
