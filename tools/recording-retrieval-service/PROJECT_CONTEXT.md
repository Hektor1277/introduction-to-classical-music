# Recording Retrieval Service 项目背景与功能框架

## 1. 这个项目是什么
“Introduction to Classical Music” 是一个由三部分组成的系统：

1. 公开静态站点（public static site，公开静态网站）
2. owner 维护工具（maintenance tool，维护后台）
3. 本地结构化数据（structured local data，结构化本地数据）

公开站点只面向读者发布静态内容，不暴露维护入口。owner 只在维护者本地运行，用于录入、编辑、自动检查、候选审查、批量导入和专栏管理。所有正式数据最终都由 owner 写回项目的数据文件。

## 2. 当前数据模型
当前核心实体共有五类：
- `composer`：作曲家
- `person`：人物，例如指挥、乐团、艺术家
- `work`：作品
- `recording`：版本
- `article`：专栏

其中只有 `recording`（版本）自动检查在本轮被外置。其余实体的自动检查仍由 owner 内部完成。

### 为什么 `recording` 单独外置
`recording` 的信息来源最分散、检索链路最复杂、字段质量波动最大，典型包括：
- 资源链接
- 专辑名 `albumTitle`
- 发行商 `label`
- 发行时间 `releaseDate`
- 地点 `venueText`
- 版本说明、封面、候选图片

它比 `composer / person / work` 更依赖多源检索、证据筛选和后处理，因此适合独立成一个可单独演进的外部工具。

## 3. owner 在整个系统中的角色
owner 仍然是唯一数据真源（single source of truth，唯一真源）。它负责：
- 读取和保存本项目的 JSON 数据
- 维护候选审查状态
- 批量导入文本分析
- 创建粗版本条目
- 调用外部版本检索服务
- 把外部返回翻译成内部 `AutomationProposal`
- 让用户确认、微调、应用或放弃变更

外部工具永远不直接写仓库数据，也不直接参与最终应用。

## 4. owner 当前工作流
owner 目前主要包含以下工作流：
- 搜索与详情（Search & Details，搜索与详情）
- 自动检查（Automation Check，自动检查）
- 候选审查（Proposal Review，候选审查）
- 批量导入（Batch Import，批量导入）
- 专栏（Articles，专栏）

本次外置化只改变其中与 `recording` 自动检查相关的部分。

## 5. 外部工具在新流程中的位置
新流程下，`recording` 的工作链路是：

1. 用户在 owner 中确认版本条目已经创建。
2. owner 根据版本条目的“种子字段（seed，初始字段）”构造请求。
3. owner 通过本地 HTTP 调用 `Recording Retrieval Service`。
4. 外部工具并行检索多个版本。
5. 外部工具返回结构化补全结果、证据、候选链接、候选图片和日志。
6. owner 将结果翻译成内部候选。
7. 用户在 owner 中审查、微调并最终应用。

这意味着外部工具的输出必须是“对 owner 友好的结构化补全结果”，而不是直接写库或直接返回 owner 内部状态。

## 6. 外部工具的明确目标
外部工具只做一件事：

根据 owner 提供的 `recording seed`（版本种子信息），为每个版本检索并补全高价值字段。

它的直接输入包括：
- 版本标题
- 作曲家中文名 / 拉丁名
- 作品中文名 / 原文名
- 作品号 / catalogue
- 演出时间文本
- 地点文本
- 已有专辑名、发行商、发行时间
- 参与者 `credits`
- 已有资源链接
- 批量导入原始行
- 作品类型提示 `workTypeHint`

它的直接输出包括：
- 结构化字段补全结果
- 字段级证据
- 链接候选
- 图片候选
- 条目级与任务级日志

## 7. 外部工具不做什么
为了让边界稳定，外部工具必须严格避免以下行为：
- 不解析批量导入模板文本
- 不创建 `composer / person / work` 条目
- 不直接更新 owner 的候选状态
- 不直接修改本项目任何数据文件
- 不直接决定最终采用哪一个候选
- 不直接应用图片、链接或字段改动到项目库

owner 需要的不是“自动写库器”，而是“版本结构化检索与补全服务”。

## 8. 与 owner 的集成方式
owner 与外部工具只通过本地 HTTP 通信，协议在 [`PROTOCOL.md`](./PROTOCOL.md) 中定义。

联调时必须满足这些前提：
- 外部工具作为本地常驻 HTTP 服务启动
- owner 先调用 `GET /health`
- `protocolVersion` 必须匹配 `v1`
- owner 使用 `POST /v1/jobs` 创建任务
- owner 使用 `GET /v1/jobs/:jobId` 拉取状态
- owner 使用 `GET /v1/jobs/:jobId/results` 获取终态结果

## 9. 为什么协议采用 `requestId + jobId + itemId`
因为 owner 会同时下发多个版本检索请求，且批量导入与单条版本检查都会进入同一个服务。为避免多版本并发时的结果混淆，系统把追踪键分成三层：
- `requestId`：owner 发起的本次请求
- `jobId`：外部工具接受后的任务标识
- `itemId`：单个版本条目的唯一映射键

这套设计的目标是：永远不要用数组顺序推断结果属于哪个版本。

## 10. 为什么 owner 不直接把外部结果当最终结论
owner 的内部模型是“候选驱动（proposal-driven，候选驱动）”的。任何自动检查结果都要经过候选审查，这样才能保证：
- 维护者能看到 before / after（变更前后）
- 能看到证据和警告
- 能手动微调字段
- 能决定应用、放弃或标记已读

因此外部工具不应输出 owner review state（审查状态），只需要返回可以被翻译成候选的结构化结果。

## 11. 建议的外部工具内部框架
这里只给“盒子级（box-level，盒子级）”框架，不限定实现语言和内部技术。

### `API Layer`（接口层）
- 提供 `/health`、`/v1/jobs`、`/v1/jobs/:jobId`、`/v1/jobs/:jobId/results`、`/v1/jobs/:jobId/cancel`
- 负责请求校验、协议版本检查、错误格式统一

### `Job Orchestrator`（任务编排层）
- 接收多条 `items`
- 控制并发、取消、超时、重试
- 维护 `queued / running / partial / succeeded / failed / timed_out / canceled`

### `Retrieval Pipeline`（检索管线）
- 根据 `seed` 构造查询
- 访问高质量来源、搜索来源、流媒体来源、已有链接元数据等
- 在必要时调用 LLM 做候选整合

### `Normalization & Validation`（规范化与校验）
- 统一时间、平台、发行商、专辑名等字段格式
- 拒绝明显错链、误链、无关页面
- 保证输出字段与协议一致

### `Result Assembler`（结果组装层）
- 按 `itemId` 输出最终结果
- 附带 `warnings / evidence / linkCandidates / imageCandidates / logs`
- 支持部分成功与未找到结果

### `Local Cache / Artifacts`（本地缓存与产物）
- 把下载缓存、日志、临时文件留在工具自己的目录中
- 不把这些文件散落到项目根目录

## 12. 需要在新线程继续决定的事项
本文档故意不替你锁死实现细节。新线程里还需要继续确定：
- 使用什么语言和框架实现
- 检索源优先级如何排序
- LLM 在哪些阶段介入
- 是否启用本地缓存、缓存多久
- 超时和重试的具体策略
- 置信度（confidence）如何计算
- 是否需要对链接、图片做二次评分

## 13. 开发外部工具时的硬性要求
- 只接受 owner 协议定义的输入结构
- 只返回协议允许的结构化结果
- 必须支持多 `item` 并行
- 必须按 `itemId` 隔离结果
- 不得把内部实现细节泄漏成 owner 侧必须依赖的行为
- 不得假设 owner 会接受不完整或不合规的结果

## 14. 建议阅读顺序
如果你要在新线程里开发外部工具，建议按下面顺序吸收信息：

1. 本文件：理解项目背景、角色分工、外置动机、边界
2. [`PROTOCOL.md`](./PROTOCOL.md)：理解 HTTP 协议与字段约束
3. 根目录 [`README.md`](../../README.md)：理解当前仓库工程结构

做到这三步后，你就可以把 `Recording Retrieval Service` 当成一个独立“盒子”来实现，而不需要先理解 owner 的全部内部代码。
