# Recording Retrieval Service

本目录承载独立工具 `Recording Retrieval Service`，中文名统一为“版本自动检索工具”。

它同时服务两种入口：

- `Contract Surface`（合同面）：供父项目 owner 通过本地 HTTP 协议调用。
- `Standalone Surface`（独立面）：工具自身提供 `Web UI`（网页界面），用户可直接启动并手工提交检索任务。

## 目录用途

- 存放工具自己的源码、依赖、缓存、日志、下载产物和打包脚本。
- 不直接读写主项目的 `apps/`、`packages/`、`data/` 或 `apps/site/public/`。
- 与父项目的唯一正式集成面仍是 [`PROTOCOL.md`](../PROTOCOL.md) 定义的本地 HTTP 协议。

## 阅读顺序

1. [`PROJECT_CONTEXT.md`](../PROJECT_CONTEXT.md)
2. [`PROTOCOL.md`](../PROTOCOL.md)
3. 本文件

## 当前实现

- 后端：`Python + FastAPI`（快速接口框架）
- 启动模式：
  - `python -m app.main --mode service`
  - `python -m app.main --mode ui`
- 默认地址：`http://127.0.0.1:4780`
- 协议版本：`v1`
- 当前检索器：规则检索管线 `RetrievalPipeline`
- 默认来源：`materials/source-profiles/`
- 可选 LLM 配置：`config/llm.local.json`

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe -m app.main --mode service
.\.venv\Scripts\python.exe -m app.main --mode ui
```

也可以使用包装脚本：

```powershell
.\start-service.cmd
.\start-ui.cmd
.\install-windows.cmd
.\build-portable.cmd
.\scripts\start-service.cmd
.\scripts\start-ui.cmd
```

推荐优先双击根目录的 `start-ui.cmd` 或 `start-service.cmd`。

## 目录结构

```text
tools/recording-retrieval-service/
  README.md
  PROJECT_CONTEXT.md
  PROTOCOL.md
  pyproject.toml
  app/
    main.py
    models/
    services/
    ui/
  tests/
  scripts/
  packaging/
  materials/
  config/
  cache/
  logs/
  downloads/
```

## Standalone UI

独立 UI 首版只提供：

- 原始文本 + 关键字段提示的混合输入
- 粗条目解析预览
- 原始请求 JSON 预览
- 任务状态轮询
- 字段级 before / after（变更前后）对比
- 证据、候选链接、warnings、logs 展示
- 终态结果展示

不提供：

- owner 候选审查
- 项目数据写回
- batch 模板解析

## Windows 便携分发

便携包脚本位于 `packaging/build-portable.ps1`，当前策略是：

- 使用 `PyInstaller onedir`（单目录可执行包）生成 `recording-retrieval-service.exe`
- 附带 `start-service.cmd` 与 `start-ui.cmd`
- 每次输出新的发布目录到 `dist/releases/`
- 同时刷新 `dist/portable/`，它始终指向当前最新的一份便携目录
- 同时生成一个可直接交付的 `dist/recording-retrieval-service-portable-<timestamp>.zip`

启动建议：

- 在源码目录中，优先双击根目录的 `start-ui.cmd` 或 `start-service.cmd`
- 在便携目录中，优先双击同目录下的 `start-ui.cmd` 或 `start-service.cmd`
- 如果端口 `4780` 已被占用，脚本会保留错误信息并暂停，避免窗口瞬间关闭

## 规则与配置

- 高质量来源与流媒体来源规则位于 `materials/source-profiles/`
- 规则采用纯文本格式：一行一个 URL 或 hostname，上到下为优先级
- LLM 配置文件使用 `config/llm.local.json`，格式与父项目现有 OpenAI-compatible 配置一致：
- 平台搜索 API 配置使用 `config/platform-search.local.json`，示例文件为 `config/platform-search.example.json`
- 平台配置与人工介入步骤见 `docs/platform-api-setup-checklist.md`

```json
{
  "enabled": true,
  "baseUrl": "https://your-llm-endpoint/v1",
  "apiKey": "your-secret",
  "model": "your-model",
  "timeoutMs": 30000
}
```

## owner 调用前提

- 服务已启动
- `GET /health` 可达
- `protocolVersion === "v1"`

## 非目标

- 不直接修改项目数据文件
- 不直接写入 owner 的 `AutomationRun`
- 不直接维护候选审查状态
- 不直接解析批量导入模板文本
