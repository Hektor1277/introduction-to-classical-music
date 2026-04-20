# 仓库结构说明

## 目标
当前仓库采用“公开站点 + owner 维护工具 + 共享领域包 + 本地结构化数据 + 外部工具”的分层结构，目的是把源码、运行产物、历史资料与维护日志拆开，降低多人协作时的误操作概率。

## 顶层目录
- `apps/site`：Astro 公开站点。
- `apps/owner`：owner 维护工具，分为 `server` 与 `web` 两层。
- `packages/shared`：通用类型、schema（模式）、slug（路径标识）与显示逻辑。
- `packages/data-core`：数据读写、索引、目录树、站点内容与实体存储。
- `packages/automation`：自动检查、批量导入、任务编排、外部版本检索客户端。
- `data`：项目运行需要的结构化数据。
- `materials`：历史资料、模板、样例、截图、参考文本。
- `tools/recording-retrieval-service`：外部版本自动检索工具的安装位与接口文档。
- `docs`：架构、计划、操作手册和线程交接文档。
- `scripts`：仓库级脚本。
- `tests`：测试集合，当前分为 `unit / integration / e2e` 三层。
- `output`：构建产物，本地生成，不入库。

## 说明
这里描述的是“应被长期维护的业务结构”，不是仓库根目录在任意时刻的完整物理列表。

因此根目录中仍然可能看到以下内容：
- 仓库级配置文件：`package.json`、`package-lock.json`、`tsconfig*.json`、`vitest.config.ts`
- 仓库治理文件：`.gitignore`、`.gitattributes`、`.editorconfig`
- GitHub 仓库配置：`.github/`
- 本地工具辅助目录：`.codex/`、`.codex-handoff/`

而像 `.astro/`、`node_modules/`、`output/`、`.playwright-cli/` 这样的目录，则属于本地缓存、依赖或产物，不属于正式目标树形。

## 结构原则
- 根目录只保留高层目录和仓库级配置。
- 公开站点源码与 owner 后台源码必须隔离。
- 共享逻辑应放入 `packages/*`，不要在 `apps/site` 与 `apps/owner` 中复制实现。
- 核心数据留在 `data/`，历史资料与非运行时文件留在 `materials/`。
- 构建产物、日志、缓存和临时目录不进入 Git。

## 协作原则
- 新增模块时优先考虑应该放在 `apps/`、`packages/`、`data/` 还是 `materials/`，避免再次堆回根目录。
- 任何对外服务集成都应以协议为边界，避免直接依赖服务内部实现。
- 运行态目录若必须落盘，应放在 `data/automation/` 或工具自有目录，并补充 `.gitignore`。
