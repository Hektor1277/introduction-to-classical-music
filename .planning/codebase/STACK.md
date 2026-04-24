# 技术栈

**分析日期：** 2026-04-24

## 语言

**主要语言：**
- TypeScript（5.9.3）：运行时脚本、桌面壳、Owner 服务端、共享/领域包，分布在 `scripts/`、`apps/desktop/`、`apps/owner/server/`、`packages/`（见 `package.json`、`tsconfig.runtime.json`）。
- JavaScript（ESM）：Astro/Node 脚本与 Owner 前端，分布在 `apps/owner/web/`、`scripts/*.mjs`、`apps/site/astro.config.mjs`。

**次要语言：**
- Python（>=3.13）：录音检索服务，位于 `tools/recording-retrieval-service/app/app/`（见 `tools/recording-retrieval-service/app/pyproject.toml`）。
- Astro 组件语法：静态站点页面，位于 `apps/site/src/**/*.astro`。
- PowerShell：Windows 引导与打包脚本，位于 `scripts/*.ps1` 与 `tools/recording-retrieval-service/app/packaging/*.ps1`。

## 运行时

**环境：**
- Node.js 22.x（`package.json` `engines.node`，CI 见 `.github/workflows/ci.yml`）。
- Python 3.13（`README.md`、CI 和检索服务配置均要求）。

**包管理器：**
- npm（项目脚本与 CI 均使用 npm）。
- 锁文件：`package-lock.json`（lockfileVersion 3）。

## 框架与核心组件

**核心框架：**
- Astro `^5.18.1`：`apps/site` 静态站点构建与渲染。
- Electron `^41.2.1`：桌面启动器运行时（`apps/desktop/main.ts`）。
- Express `^5.2.1`：Owner 本地 API 与静态资源服务（`apps/owner/server/owner-app.ts`）。
- FastAPI `>=0.115,<1.0`：录音检索本地 HTTP 服务（`tools/recording-retrieval-service/app/app/main.py`）。

**测试：**
- Vitest `^4.1.4`：Node/TS 单测（`tests/**/*.test.ts`，见 `vitest.config.ts`）。
- Pytest `>=8.3,<9.0`：Python 服务测试（`tools/recording-retrieval-service/app/tests`）。

**构建与发布：**
- `tsc`：运行时代码转译到 `output/runtime`。
- `electron-builder` `^26.0.12`：Windows 安装包构建。
- `PyInstaller` `>=6.12,<7.0`：检索服务便携打包。

## 关键依赖

**业务关键：**
- `zod`：模式校验与数据契约（`packages/shared/src/schema.ts`）。
- `astro`：站点生成。
- `electron`：桌面运行时。
- `express`：Owner 本地服务路由。
- `httpx`（Python）：检索服务外部 API 客户端（`platform_clients.py`）。
- `playwright`（Python）：浏览器辅助检索/兜底流程（`browser_fetcher.py`）。

**基础设施：**
- `typescript`：类型检查与构建。
- `vitest`：测试执行。
- `sanitize-html`、`markdown-it`、`cheerio`：内容清洗与解析（自动化与站点内容链路）。

## 配置

**环境变量：**
- 站点与运行时路径：`ICM_SITE_BASE`、`ICM_SITE_OUT_DIR`、`ICM_REPO_ROOT`、`ICM_RUNTIME_MODE`、`ICM_ACTIVE_LIBRARY_DIR`、`ICM_APP_DATA_DIR`、`ICM_DEFAULT_LIBRARY_DIR`。
- 本地服务端口与地址：`OWNER_PORT`、`LIBRARY_SITE_PORT`、`RECORDING_RETRIEVAL_SERVICE_URL`。
- 检索/LLM 配置由 Python 服务读取（见 `llm_client.py`、`platform_search_config.py`）。

**构建配置文件：**
- TypeScript：`tsconfig.json`、`tsconfig.runtime.json`
- 测试：`vitest.config.ts`
- 站点：`apps/site/astro.config.mjs`
- 桌面打包：`package.json` 中 `build` 字段 + `scripts/electron-after-pack.mjs`
- Python 服务：`tools/recording-retrieval-service/app/pyproject.toml`
- CI：`.github/workflows/ci.yml`

## 平台要求

**开发环境：**
- 以 Windows 工作流为主，要求 Node.js 22 + npm + Python 3.13。
- CI 在 Linux 上执行 verify/build，具备跨平台构建验证。

**生产目标：**
- 主要发行目标为 Windows 10/11 x64 桌面端。
- 交付形态包括安装包与便携版产物。

---

*技术栈分析：2026-04-24*
