# Media 交付证据

- 独立 checkout：`/Users/leosouthey/Documents/Codex/2026-10-22/task-38/lenso-rust`
- 分支：`feat/media-images`
- 来源旧 checkout 干净，HEAD `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2`。独立 clone 后经正常审批 fetch 官方 main，核实 `c31a47b52522302c3365855eaff47d970ac985e7`，fast-forward 后开始修改。
- 已读 AGENTS.md、Native Rust Lenso 技能和 native 生命周期参考，以及 task-33 PLAN/SOURCES；现有 workspace 没有 Media。Storage 已具备所需接口。
- 改动限于 `crates/media/**`、根 Cargo member 和必要锁文件新增依赖；Runtime pin、Core、Storage、Search、Notifications、Webhook 均未修改。

## 可消费能力

`MediaPlugin<S: ObjectStorage>` 精确实例注入；`Media::{inspect,derive,read}`；宿主 Authority；命名版本化 presets；PNG/JPEG 探测、等比缩略图、矩形裁剪、格式转换和重新编码。SQLite manifest 缓存、源 ETag/scope 隔离、发布前/读取时再授权；pending 上传恢复清理；单任务非排队限流，字节/尺寸/像素/decoder allocation 限制。取消 waiter 不取消已接纳任务，Core shutdown 等待任务及存储操作结束。

## 已实际运行（2026-10-22，macOS 本地缓存）

- `cargo test --locked --offline -p lenso-media`：4 个集成测试通过（最终代码），doc tests 0。
- `cargo clippy --locked --offline -p lenso-media --all-targets --all-features -- -D warnings`：通过（最终代码）。
- `cargo fmt --package lenso-media --check`：通过。
- `cargo run --locked --offline -p lenso-media --example thumbnail`：实际 LocalStorage PNG 48×24 → JPEG 16×8，再次解码验证尺寸/颜色，重复调用缓存命中。该证据在最后新增边界测试和 create-only 冲突保护之前取得；例子及 codec 行为未再改变，最终 all-targets 检查再次编译例子。
- `git diff --check`：通过。

测试包含 PNG/JPEG 输入输出、裁剪越界、像素与输入/输出大小限制、无效配置/图片、scope 隔离/撤权、ETag 变化、重启缓存、SQLite trigger 注入发布失败后的对象清理、重开 pending 清理且 ready 保留、取消 waiter 后并发 Busy/shutdown drain/重启读取已发布结果。

## 未验与边界

未跑全 workspace CI/组合集成（由唯一集成线负责），未做真实远程 Storage、真实用户数据、进程 SIGKILL/断电耐久性、OS RSS 压测。恢复测试是在真实 SQLite/LocalStorage 上构造中断后的 pending 状态；并非断电测试。没有推送、合并 main、发布或部署。

内存配置约束工作缓冲预算与 decoder allocation；并非 OS 强制 RSS 上限。仅 PNG/JPEG；inspect 只验证头部/尺寸。无视频、网络拉取、自动 ready GC、EXIF orientation normalization。宿主必须保护 Storage 直读入口并保留 `media-derived/` 命名空间；Authority 是宿主可信身份/ACL 的接线点，scope 字符串不是权限证明。每个 manifest 同时只允许一个 owner；每实例只运行一个 job。

构建最初被 mise 沙箱限制阻止；按正常审批执行后成功。无剩余审批拒绝或执行阻塞。
