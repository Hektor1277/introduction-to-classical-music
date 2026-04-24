# 代码库关注点

**分析日期：** 2026-04-24

## 技术债

**Owner 单体过大（前后端都偏重）：**
- 问题：`apps/owner/server/owner-app.ts` 与 `apps/owner/web/app.js` 体量大、职责混合（路由编排、业务规则、UI 状态耦合）。
- 影响：改动回归面大、调试成本高、上手慢。
- 建议：按域拆分路由/服务层（library、automation、batch-import、articles），前端按功能模块拆分。

**关键路径存在类型安全绕过：**
- 问题：Owner 服务端核心文件存在 `@ts-nocheck`。
- 影响：高频变更路径缺少编译期保护。
- 建议：先抽离 typed helper，再逐步移除 `@ts-nocheck` 并收紧 `noImplicitAny`。

**跨层规则重复实现：**
- 问题：前后端存在相似归一化/推断逻辑，易漂移。
- 影响：UI 预处理与后端落盘行为不一致。
- 建议：沉淀共享规则到 `packages/shared/src/`，前后端共同复用。

**规则型自动化集中在超长文件：**
- 问题：大量 regex/token 规则集中，边界难推理。
- 影响：易出现隐性误判，回归难定位。
- 建议：按规则域拆分并建立规则级契约测试。

**JSON 持久化缺少事务性保护：**
- 问题：关键状态直接写文件，缺少锁与版本检查。
- 影响：并发操作可能覆盖或写入不完整。
- 建议：采用“临时文件 + rename”原子写，配版本号与可选文件锁。

## 已知缺陷

**空 `localPath` 可能打开非预期目录：**
- 触发：`POST /api/open-resource` 传 `{"linkType":"local","localPath":""}`。
- 涉及：`apps/owner/server/owner-app.ts`、`packages/data-core/src/open-target.ts`。
- 建议：`path.resolve` 前先拒绝空路径。

**Cello 可能被误判为 Bassoon：**
- 触发：依赖 Owner UI 辅助规则自动推断作品分组。
- 涉及：`apps/owner/web/ui-helpers.js`、`packages/data-core/src/owner-entity-helpers.ts`。
- 建议：统一前后端 regex 语义并补一致性测试。

**带副作用的 GET 接口：**
- 触发：访问 `GET /__local-resource?path=...` 可触发本地打开行为。
- 涉及：`packages/data-core/src/local-site-server.ts`、`resource-links.ts`。
- 建议：改为 POST + CSRF/签名 nonce + 显式用户确认。

## 安全关注

**高权限 Owner API 缺少鉴权：**
- 现状：仅依赖 loopback（`127.0.0.1`）限制。
- 风险：本机其他进程可直接调用修改类接口。
- 建议：增加会话令牌、Origin 校验、破坏性接口 capability token。

**本地资源打开接口路径范围过宽：**
- 风险：可被诱导打开任意本地路径。
- 建议：仅允许白名单根目录（活动库目录 + 配置媒体目录），拒绝系统路径。

**远程图片代理存在 SSRF 风险面：**
- 风险：可作为本地网络探测/抓取通道。
- 建议：加 host/IP 策略（阻断内网段/loopback）、大小限制、超时预算。

**密钥以明文形式落地：**
- 风险：本地磁盘或浏览器上下文被读取时可能泄露。
- 建议：优先使用系统密钥库，停止在 `localStorage` 持久化 API key。

**缺少防御中间件：**
- 风险：缺少 `helmet`、细粒度 body size 限制、速率限制。
- 建议：在 Owner 服务端补齐最小安全基线。

## 性能瓶颈

**搜索接口每次全量构建：**
- 问题：`/api/search` 请求内执行全量映射与字符串归一化。
- 建议：在写入后增量维护内存索引，查询走预计算 token map。

**高频写路径触发全量产物刷新：**
- 问题：保存/应用路径频繁调用 `writeGeneratedArtifacts`。
- 建议：引入 dirty-set 增量生成与批量防抖。

**Owner 前端脚本过大：**
- 问题：`apps/owner/web/app.js`（6000+ 行）首屏加载与维护压力高。
- 建议：按功能分包并延迟加载低频模块。

**运行记录读取无分页：**
- 问题：run/session 列表读取需解析大量 JSON。
- 建议：增加摘要索引与分页接口。

**图片代理全量缓冲：**
- 问题：`arrayBuffer()` 后再转 `Buffer`，内存峰值高。
- 建议：改为流式转发并加最大字节数限制。

## 脆弱区域

**跨运行时路径与打包模式耦合：**
- 涉及：`apps/desktop/main.ts`、`packages/data-core/src/app-paths.ts`、`scripts/dev-server.mjs`。
- 风险：dev/packaged/asar 路径差异导致启动问题。
- 建议：补齐“开发态 + 打包态”启动矩阵测试。

**提案状态机跨层耦合：**
- 涉及：`owner-app.ts`、`automation.ts`、`automation-checks.ts`、`recording-retrieval.ts`。
- 风险：状态流转假设分散，容易出现隐式不一致。
- 建议：集中状态迁移函数并限制跨层直写。

**规则驱动的清洗逻辑易回归：**
- 涉及：`person-cleanup.ts`、`automation-checks.ts`、`recording-rules.ts`。
- 风险：新语料/多语言边界容易触发误判。
- 建议：建立语料回放回归集，规则变更先跑回归。

## 扩展性上限

**JSON 文件存储上限明显：**
- 现状：读写常为全量数组加载。
- 风险：数据规模增长后延迟与内存上升。
- 方向：考虑 SQLite 或事件日志 + 物化视图。

**Owner 单进程承担重任务：**
- 风险：长耗时任务影响接口响应。
- 方向：将构建/自动化下沉后台 worker，前台只做状态轮询。

**历史运行数据无限增长：**
- 风险：列表与启动性能持续退化。
- 方向：引入保留策略、归档压缩、按数量/时间裁剪。

## 风险依赖

**Astro CLI 路径依赖打包布局：**
- 涉及：`packages/data-core/src/site-build-runner.ts`。
- 风险：打包结构变化可能导致构建链路失效。
- 建议：封装稳定内部入口并加入发布前验证。

**Windows PowerShell 网络兜底复杂：**
- 涉及：`packages/automation/src/external-fetch.ts`。
- 风险：行为与标准 `fetch` 差异，问题定位困难。
- 建议：抽象适配层并补合同测试与故障遥测。

**依赖 overrides 数量较多：**
- 风险：升级时可能掩盖不兼容或引入隐患。
- 建议：记录每条 override 理由并定期清理。

## 缺失的关键能力

**Owner 控制面认证：**
- 现状：高风险接口无明确认证授权流程。

**破坏性操作二次确认：**
- 现状：导入/删除/应用类操作调用即执行。

**持久化数据版本迁移机制：**
- 现状：JSON 状态缺少清晰 schema 版本升级路径。

## 测试缺口

**缺少 E2E 场景：**
- 未覆盖：桌面启动器 -> Owner -> 构建 -> 本地站点全链路。
- 优先级：高。

**缺少 HTTP 集成测试：**
- 未覆盖：路由校验、状态码约定、接口副作用。
- 优先级：高。

**安全行为测试不足：**
- 未覆盖：`/api/open-resource`、`/api/remote-image`、`/__local-resource` 滥用场景。
- 优先级：高。

**性能回归缺少基线：**
- 未覆盖：搜索性能、产物生成耗时、运行记录扩张影响。
- 优先级：中。

---

*关注点审计：2026-04-24*
