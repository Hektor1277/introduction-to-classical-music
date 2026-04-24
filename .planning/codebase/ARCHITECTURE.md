# 架构

**分析日期：** 2026-04-24

## 架构概览

**整体模式：** 分层 monorepo + 共享领域包 + 多运行时壳层。

**核心特征：**
- 领域逻辑集中在 `packages/shared/src`、`packages/data-core/src`、`packages/automation/src`，应用层尽量复用而非重复实现。
- 按运行面拆分：桌面编排（`apps/desktop/main.ts`）、Owner HTTP 服务（`apps/owner/server/owner-app.ts`）、Astro 静态站点（`apps/site/src/pages/*.astro`）。
- 持久化以文件 JSON 为主，通过 `packages/data-core/src/library-store.ts` 与 `library-manager.ts` 统一管理。

## 分层说明

**展示层（Desktop + Web）：**
- 位置：`apps/desktop/*`、`apps/owner/web/*`、`apps/site/src/{pages,components,layouts}/*`
- 责任：UI 展示、用户交互、触发 API/IPC。

**应用服务层（Owner API + Desktop 编排）：**
- 位置：`apps/owner/server/*`、`apps/desktop/{main.ts,preload.ts}`
- 责任：流程编排、输入校验、调用 domain/data-core/automation，输出 HTTP/IPC 响应。

**领域与数据核心层：**
- 位置：`packages/shared/src/*`、`packages/data-core/src/*`
- 责任：Schema 与规则、库文件读写、索引生成、bundle 生命周期、站点构建编排。

**自动化层：**
- 位置：`packages/automation/src/*`
- 责任：自动检查、提案生命周期、批量导入、检索与 LLM 集成。

**脚本与构建编排层：**
- 位置：`scripts/*`
- 责任：构建、审计、开发服务管理、发布打包。

## 数据流

**流程 1：Owner 编辑 -> 持久化 -> 生成产物 -> 站点展示**
1. Owner 前端（`apps/owner/web/app.js`）调用 `apps/owner/server/owner-app.ts` 的 `/api/*`。
2. 服务端完成标准化与校验（依赖 `packages/shared/src/schema.ts` 与 `packages/data-core/src/owner-entity-helpers.ts`）。
3. 通过 `packages/data-core/src/library-store.ts` 写入活动库 JSON。
4. 调用 `writeGeneratedArtifacts()` 刷新 `library.json`、`indexes.json`、`site.json`、`articles.json`。
5. 站点构建链路（`site-build-runner.ts`）触发 Astro 输出到库站点目录。

**流程 2：Desktop 启动编排**
1. `apps/desktop/main.ts` 注册 IPC（`launcher:*`、`desktop:*`）并注入 `ICM_*` 运行时变量。
2. 调用 `packages/data-core/src/library-manager.ts` 激活/引导库。
3. 按操作启动 Owner 服务、检索服务、本地站点服务，并打开对应窗口。
4. `apps/desktop/preload.ts` 暴露受控桥接 API 给 `launcher.js`。

**流程 3：静态站点读取生成数据渲染**
1. `apps/site/src/pages/*.astro` 通过 `apps/site/src/lib/*` 薄封装读取 `packages/data-core/src/library-store.ts`。
2. 页面结合 `packages/shared/src/display.ts` 进行展示模型转换。
3. Astro 输出静态 HTML 到配置目录。

## 关键抽象

**运行时路径抽象：**
- 文件：`packages/data-core/src/app-paths.ts`、`library-manager.ts`、`library-bundle.ts`
- 价值：统一 legacy/bundle 路径解析，减少硬编码。

**生成产物边界：**
- 文件：`packages/data-core/src/library-store.ts`、`indexes.ts`
- 价值：把“可编辑源数据”和“可读优化产物”分离。

**自动化提案模型：**
- 文件：`packages/automation/src/automation.ts`、`automation-jobs.ts`、`apps/owner/server/proposal-patch-utils.ts`
- 价值：统一风险、状态、快照与回滚语义。

**站点 Lib 薄封装：**
- 文件：`apps/site/src/lib/*.ts`
- 价值：Astro 端保持稳定导入路径，底层实现可继续沉到 `packages/*`。

## 入口点

**桌面入口：** `apps/desktop/main.ts`
- 负责窗口管理、IPC、子进程服务编排、退出清理。

**Owner 服务入口：** `apps/owner/server/owner-app.ts`
- 负责本地 API、静态资源、自动化/导入/构建等操作入口。

**站点入口：** `apps/site/src/pages/*.astro`
- 负责从生成 JSON 读取并渲染终端页面。

**脚本入口：** `scripts/build-library-site.ts`、`scripts/build-indexes.ts`、`scripts/dev-server.mjs`
- 负责构建、索引与本地开发链路。

## 错误处理策略

**策略：** 边界校验优先、异常结构化返回、尽量保证本地运行连续性。

**典型模式：**
- Express 路由 `try/catch` 并返回 JSON 状态码（`400/404/500`）。
- 领域校验失败直接抛错并向调用方显式暴露。
- Desktop 启动链路包含超时、重试、端口探测和模式降级。

## 横切关注点

**日志：** 桌面与构建链路落本地日志；Owner UI 在页面内反馈操作状态。  
**校验：** 统一经 `packages/shared/src/schema.ts` 做实体规范化与验证。  
**鉴权：** 本地服务默认绑定 `127.0.0.1`，未发现完整认证系统。

---

*架构分析：2026-04-24*
