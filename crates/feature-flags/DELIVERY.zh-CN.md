# Feature Flags 本地交付

基线：c31a47b52522302c3365855eaff47d970ac985e7。
独立 checkout：/Users/leosouthey/Documents/Codex/2026-10-22/task-42/lenso-rust。
分支：feat/feature-flags。来源 checkout 实际 clean HEAD 为 126b778；在独立
checkout 经正常网络审批取得指定基线后开始。未改来源或其他 owner 目录。

实现：Configuration / FeatureFlags / FeatureFlagsPlugin / Snapshot / Context /
Identity / Rule / Rollout / Decision。完整配置 CAS 原子发布；保留最近 16 个
历史 revision，回滚生成新 revision；请求固定身份及配置；用户、组织规则；
稳定用户/组织百分比分流；错误/缺失显式默认值。无 Cache 依赖。

共享改动仅 Cargo.toml 新增 member 和 Cargo.lock 新包 stanza，无现有依赖升级，
Runtime rev 保持 dd0eb084e490e5e4bda05ce0dbb503f3aae50590。

本轮验证（正常工具链审批后执行，未改变工具链/缓存路径）：

- cargo test --locked --offline -p lenso-feature-flags：7/7 integration tests 通过。
- cargo run --locked --offline -p lenso-feature-flags --example checkout_rollout：通过。
  25% fixture rollout: 235/1000 checkouts bundled；bundled -> standard -> rollback
  bundled；在途请求快照保持原行为。
- cargo clippy --locked --offline -p lenso-feature-flags --all-targets --all-features -- -D warnings：通过。
- cargo fmt --all -- --check：通过。
- git diff --check：通过。

定向覆盖：规则优先级、缺失默认、无效身份、CAS 竞态只有一次成功、历史淘汰、
回滚、新旧请求隔离、关闭、重复规则、大小限制、分流 0/100%、嵌套 cohort、
固定 bucket 8782 的边界及组织 cohort 的跨用户一致性。

未验证/范围边界：全 workspace tests 与全 workspace Clippy、多插件组合由唯一
集成 owner 验证；未推送、合并 main、发布或部署。未使用真实数据库/邮件/付费
服务。仅本地 fixture，配置和历史为进程内存，不提供持久化、跨进程同步或重启
保留；没有需要登记的文件/数据库/worker 持久资源。百分比是确定性抽样，不保证
任意小样本恰好占比；非加密 hash 不抵抗身份操控；不是权限系统。
