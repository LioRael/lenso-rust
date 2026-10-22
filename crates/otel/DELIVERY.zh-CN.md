# Observability 本地候选交付

基线：`c31a47b52522302c3365855eaff47d970ac985e7`，分支 `plugin/observability`。
独立 clone：`/Users/leosouthey/Documents/Codex/2026-10-22/task-41/lenso-observability`。
原来源本地 HEAD 为 `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2` 且 clean；指定较新 SHA 经正常审批 fetch 到独立 clone。

## 交付

- `src/lib.rs`：官方 OTel SDK service/plugin、显式 HTTP/operation/DB/Task boundary、W3C carrier、Logger scope 关联、有限标签 span/error/metrics、owned/borrowed provider 和取消安全关闭。
- `collector`：官方 OTLP HTTP/protobuf exporter，1024 queue / 128 batch；无手写协议。
- `examples/traced_service.rs`：公开 API 消费，HTTP boundary fixture → operation → 真正 SQLite 内存业务表 + 现有持久 Tasks 临时队列 → handler trace 恢复。示例实际输出 4 个同 trace spans 和脱敏结构化日志。
- `tests/telemetry.rs`：8 项定向测试。
- 根 Cargo.toml 仅新增 workspace member；Cargo.lock 新增官方依赖及消歧，无移除/升级基线版本；Runtime git pin 未改。
- 无 Core、Logger、HTTP、Tasks owner 代码修改，无来源目录修改，无推送、发布、部署或真实遥测外传。

## 实际执行通过

- `cargo check -p lenso-otel`
- `cargo test -p lenso-otel --locked --all-targets`：8 passed。
- `cargo run -p lenso-otel --locked --example traced_service`：4 correlated spans，token `[REDACTED]`。
- `cargo clippy -p lenso-otel --locked --all-targets --all-features -- -D warnings`
- `cargo fmt --all --check`

测试证明：并发树隔离和父子关系；安全错误；无 baggage/无效 carrier；borrowed 不关闭宿主；结构化日志 ID 一致与脱敏；官方 OTLP exporter 经内存 HTTP client 输出可由官方 proto 解码的请求，关闭触发 flush；关闭 waiter 取消后资源 cleanup 继续且失败安全可见；metrics counter=3；Core WorkGuard drain 后 provider 关闭。

构建依赖经正常许可安装。普通 sandbox 启动首先遇到 mise Operation not permitted；随后正常 require_escalated 审批通过执行。没有审批拒绝后的绕过操作。

## 未验证 / 明确边界

- HTTP 为 boundary fixture；未启动真实 listener，未接入并行 HTTP owner 路由。
- OTLP 使用内存 HTTP transport；未连真实 collector，网络重试/断线/TLS/真实 exporter 超时、生产吞吐和队列溢出仍未验证。默认 collector convenience 仅 HTTP traces，TLS 由宿主官方 exporter 适配；metrics 使用宿主 SDK provider。
- Span 固定四种 boundary 名；未提供任意 attribute/业务名称或 link 的高层 API，防止敏感数据/高基数输入。上下文不用于身份。
- 宿主必须持有外层 WorkGuard 并在 standalone close 前 drain；adapter 不重复 admission。取消业务 future 会结束 span，但不计入成功/失败 metric。
- SDK timeout 取决于 processor 契约；不承诺强制终止任意阻塞 exporter。trace/metric 各自使用 timeout。
- workspace 全量 tests/Clippy、跨插件集成、合并与发布由唯一集成线完成，本候选未执行。
