# 代码库结构

**分析日期：** 2026-04-24

## 目录布局

```text
introduction-to-classical-music/
├── apps/                       # 运行时应用（site、owner、desktop）
├── packages/                   # 共享领域/数据/自动化包
├── scripts/                    # 构建、审计、发布与开发编排脚本
├── data/                       # 旧模式/默认结构化数据与自动化状态
├── materials/                  # 模板、参考资料、默认资产
├── docs/                       # 架构、规范、发布与运维文档
├── tests/                      # 自动化测试（unit/integration/e2e）
├── tools/                      # 外部检索服务工作区
├── output/                     # 构建与运行时产物
├── package.json                # 脚本、依赖、electron-builder 配置
├── tsconfig.json               # 基础 TS 配置与别名
├── tsconfig.runtime.json       # 运行时编译目标
└── vitest.config.ts            # Vitest 配置
```

## 目录职责

**`apps/site`：**
- 静态站点前端（Astro 路由/组件/布局/样式）。
- 关键文件：`apps/site/astro.config.mjs`、`apps/site/src/pages/index.astro`、`apps/site/src/pages/search.astro`。

**`apps/owner`：**
- 本地维护工具（服务端 + 前端）。
- 关键文件：`apps/owner/server/owner-app.ts`、`apps/owner/web/app.js`。

**`apps/desktop`：**
- Electron 启动器与本地流程编排。
- 关键文件：`apps/desktop/main.ts`、`apps/desktop/preload.ts`、`apps/desktop/launcher.js`。

**`packages/shared`：**
- 核心 schema/types/rules/display 工具。
- 关键文件：`packages/shared/src/schema.ts`、`display.ts`、`recording-rules.ts`。

**`packages/data-core`：**
- 库存储、路径模型、索引与站点构建、bundle 生命周期。
- 关键文件：`packages/data-core/src/library-store.ts`、`library-manager.ts`、`app-paths.ts`、`site-build-runner.ts`。

**`packages/automation`：**
- 自动化检查、提案生命周期、批量导入、检索/LLM 集成。
- 关键文件：`packages/automation/src/automation.ts`、`automation-jobs.ts`、`automation-store.ts`、`recording-retrieval.ts`。

**`scripts`：**
- 运维与开发脚本总入口。
- 关键文件：`scripts/build-library-site.ts`、`scripts/build-indexes.ts`、`scripts/dev-server.mjs`。

**`tests`：**
- 自动化验证目录，当前以 `tests/unit` 为主。

**`tools/recording-retrieval-service`：**
- 外部检索服务源码、协议文档、打包脚本。

## 关键文件定位

**入口文件：**
- `apps/desktop/main.ts`：桌面主进程。
- `apps/owner/server/owner-app.ts`：Owner API 服务启动。
- `apps/site/src/pages/index.astro`：站点首页路由。
- `scripts/build-library-site.ts`：站点构建脚本入口。

**配置文件：**
- `package.json`：脚本、依赖、打包配置。
- `tsconfig.json`：共享 TS 配置与别名（`@`、`@data`、`@generated`）。
- `tsconfig.runtime.json`：运行时编译范围与输出路径。
- `apps/site/astro.config.mjs`：Astro 输出与 Vite 别名。
- `vitest.config.ts`：测试范围与覆盖率报告配置。
- `.editorconfig`：统一换行、缩进、尾随空白规则。

**核心逻辑：**
- `packages/shared/src/*`：领域定义与规则。
- `packages/data-core/src/*`：存储、路径、构建链路。
- `packages/automation/src/*`：自动化与提案工作流。
- `apps/owner/server/*`：HTTP 编排层。

**测试：**
- `tests/unit/*.test.ts`：当前主力测试。
- `tests/integration`、`tests/e2e`：目录存在但用例很少。

## 命名约定

**文件命名：**
- TS/JS 模块多使用 kebab-case：如 `library-store.ts`、`automation-jobs.ts`。
- Astro 动态路由使用方括号参数：如 `recordings/[id].astro`。
- 测试文件统一 `*.test.ts`。

**目录命名：**
- 应用按运行面分组：`apps/{site,owner,desktop}`。
- 共享逻辑按职责分组：`packages/{shared,data-core,automation}`。

## 新代码放置建议

**新增功能：**
- 站点 UI：`apps/site/src/{pages,components,lib}`。
- Owner 前后端：`apps/owner/server` + `apps/owner/web`。
- 可复用业务逻辑：`packages/data-core/src` 或 `packages/automation/src`。
- 公共类型/规则：`packages/shared/src`。

**新增测试：**
- 首选 `tests/unit` 按模块域新增 `*.test.ts`。
- 涉及路由行为时，优先补 `tests/integration`。

## 特殊目录

**`output/`：**
- 构建/运行时/发布产物目录。
- 生成目录，不应提交产物。

**`apps/site/.astro`：**
- Astro 本地缓存。
- 生成目录，不提交。

**`node_modules`：**
- 依赖目录，生成目录，不提交。

**`data/automation`：**
- 自动化运行状态目录。
- 部分结构可追踪，运行产物需按流程管理。

---

*结构分析：2026-04-24*
