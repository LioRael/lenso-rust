# 交付记录

- 基线：`126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2`；独立 clone / 分支 `feat/signed-webhook-receiver`。
- 来源：task-33 `FIRST-BATCH.zh-CN.md` / `SOURCES.zh-CN.md`，TS `packages/webhooks/src/signing.ts` / `receiver.ts`；遵守源仓 AGENTS 与 lenso-rust skill。
- 范围：仅 `crates/webhook-receiver/**` 和本候选最小 workspace 注册 / lockfile crate 条目。Runtime SDK revision 与其他依赖版本未变化；未改源仓或 Tasks。
- 实现：Lenso v1 raw-byte HMAC、固定 key/namespace、时间窗口、source/type allowlist、流式大小/读超时、JSON 深度/节点上限、持久 SQLite inbox；receipt 与业务 SQL 同事务；并发/重启去重、异体冲突；受限业务 SQL capability；Core lifecycle 与取消等待后的事务 drain。

验证（本地离线缓存；Rust 1.94.0；2026-10-22）：

- `cargo test --offline --locked -p lenso-webhook-receiver`：1 unit + 6 integration，全通过。
- `cargo run --offline --locked -p lenso-webhook-receiver --example partner_order`：通过；`accepted, duplicate, reopened duplicate; durable orders=1`。
- `node crates/webhook-receiver/tests/verify-fixture.mjs`：通过 Node crypto 独立核验固定协议字节；fixture key 明确公开测试用途。
- `cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings`：通过。
- `cargo fmt --all --check`：通过。
- `cargo test --offline --locked --workspace`：失败于已有 `plugin-runtime/tests/lifecycle.rs` 的 6 个本地 TcpListener bind，`Operation not permitted`；未绕过、未提升权限。该测试命令后续未执行的测试不能视为通过。

证据日志保存在候选 checkout 的父目录：`receiver-test.log`、`receiver-example.log`、`workspace-test.log`、`workspace-clippy.log`。

未验证/边界：无真实 HTTP listener/provider/回调测试，无真实密钥或用户数据库，无发布/部署/推送；不宣称跨库 exactly-once。只保证相同 SQLite 事务内 receipt 与业务 SQL 原子性。业务回调是受信 Rust，不能 sandbox 任意原生代码；不得另开连接或执行外部副作用。超时限制覆盖 body 读取与 SQLite 争锁，不强制中断任意业务回调。独立普通 service 需显式 close；完整 admission drain 由 App shutdown 提供。参考 README 的宿主请求头、content type、授权与去重保留责任。
