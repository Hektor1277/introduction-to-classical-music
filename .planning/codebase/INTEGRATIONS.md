# 外部集成

**分析日期：** 2026-04-24

## API 与外部服务

**古典音乐元数据/检索来源：**
- Wikipedia API：实体搜索与摘要补全（`packages/automation/src/automation-checks.ts`）。
- Wikimedia Commons API：候选图片检索。
- 百度百科/百度搜索：中文实体检索与兜底。
- 上述调用均通过 Node 原生 `fetch`，未发现鉴权要求。

**录音检索平台 API（Python 服务）：**
- YouTube Data API v3：结构化视频搜索；鉴权变量 `RECORDING_RETRIEVAL_YOUTUBE_API_KEY`。
- Apple Music API：曲目/专辑检索；鉴权变量 `RECORDING_RETRIEVAL_APPLE_DEVELOPER_TOKEN`。
- iTunes Search API：Apple 兜底检索（通常无需鉴权）。
- Bilibili WBI/Web API：视频搜索与元数据补充；依赖 `RECORDING_RETRIEVAL_BILIBILI_COOKIE` 等配置。
- 客户端实现位于 `tools/recording-retrieval-service/app/app/services/platform_clients.py`（`httpx`）。

**本地服务间集成：**
- Owner 通过本地 HTTP 协议调用检索服务（`packages/automation/src/recording-retrieval.ts`）。
- 地址来自 `RECORDING_RETRIEVAL_SERVICE_URL`，默认 loopback（`127.0.0.1`）。

**LLM 端点（OpenAI 兼容协议形态）：**
- Node 侧：`packages/automation/src/llm.ts`。
- Python 侧：`tools/recording-retrieval-service/app/app/services/llm_client.py`。
- 鉴权来源：本地配置或环境变量 `RECORDING_RETRIEVAL_LLM_API_KEY`。

## 数据存储

**数据库：**
- 未检测到关系型或文档数据库（无 ORM、无迁移脚本）。

**文件存储：**
- 以本地文件系统为主：
- 业务数据：`data/`（legacy）与运行时库目录（bundle 模式）。
- 运行时产物：library/app-data 路径（由 `packages/data-core/src/app-paths.ts` 管理）。
- 检索服务配置、缓存、日志：`retrieval-service` 运行目录。

**缓存：**
- 文件级本地缓存，无独立缓存中间件。

## 身份与鉴权

**身份系统：**
- 未接入外部身份提供商（无 OAuth/OIDC/SaaS IdP）。
- 当前模式为本地单机服务，默认无登录流程。

## 监控与可观测性

**错误追踪：**
- 未发现 Sentry/Datadog/New Relic 等 SaaS SDK。

**日志：**
- 桌面端记录本地日志（见 `apps/desktop/main.ts`）。
- 检索服务写本地日志目录。
- CI 日志由 GitHub Actions 输出。

## CI/CD 与部署

**部署形态：**
- 主要是桌面分发（Windows 安装包 + 便携版），非在线托管后端。
- 静态站点构建到本地输出目录（默认 `output/site`）。

**CI 流水线：**
- `.github/workflows/ci.yml`：
- Ubuntu 进行 verify/build。
- Windows 进行打包。
- `v*` tag 触发 release 产物构建与发布。

## 环境配置

**核心环境变量：**
- 运行时/站点路径：`ICM_REPO_ROOT`、`ICM_SITE_BASE`、`ICM_SITE_OUT_DIR`、`ICM_RUNTIME_MODE` 等。
- 本地服务地址：`OWNER_PORT`、`OWNER_BASE_URL`、`LIBRARY_SITE_PORT`、`RECORDING_RETRIEVAL_SERVICE_URL`。
- 平台检索凭证：YouTube/Apple/Bilibili 对应 `RECORDING_RETRIEVAL_*`。
- LLM 配置：`RECORDING_RETRIEVAL_LLM_BASE_URL`、`RECORDING_RETRIEVAL_LLM_API_KEY`、`RECORDING_RETRIEVAL_LLM_MODEL`、`RECORDING_RETRIEVAL_LLM_TIMEOUT_MS`。

**密钥存放位置：**
- Node 侧：app-data 下 JSON 配置（`getRuntimePaths().appData.secretsPath`）。
- Python 侧：`retrieval-service/config/*.local.json`（可被 `ICM_APP_DATA_DIR` 重定向）。
- CI：`secrets.GITHUB_TOKEN`。

## Webhook 与回调

**入站：**
- 未检测到外部 webhook 接收接口（服务主要绑定 loopback）。

**出站：**
- 未检测到 webhook 回调机制。
- 主要是请求-响应式 HTTP 调用（Node/Python 到外部 API）。

---

*外部集成审计：2026-04-24*
