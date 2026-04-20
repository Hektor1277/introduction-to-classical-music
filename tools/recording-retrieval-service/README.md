# Recording Retrieval Service

本目录用于承载 `Recording Retrieval Service`，中文名统一为“版本自动检索工具”。

## 目录用途
- 父仓库在这里保留协议、背景和集成说明。
- 独立工具的真实实现位于 `tools/recording-retrieval-service/app/`，源码随公共仓库一同发布。
- owner 与该工具的唯一正式集成面是本地 HTTP 协议，见 [`PROTOCOL.md`](./PROTOCOL.md)。

## 阅读顺序
1. [`PROJECT_CONTEXT.md`](./PROJECT_CONTEXT.md)
2. [`PROTOCOL.md`](./PROTOCOL.md)
3. 本文件

## owner 侧默认约定
- 句柄：`recording-retrieval-service`
- 运行形态：本地常驻 HTTP 服务
- 默认地址：`http://127.0.0.1:4780`
- 协议版本：`v1`

## 当前工作区结构
```text
tools/recording-retrieval-service/
  README.md
  PROJECT_CONTEXT.md
  PROTOCOL.md
  app/                 # 工具源码根目录
    README.md
    start-ui.cmd
    start-service.cmd
    install-windows.cmd
    dist/portable/
```

## 如何启动独立工具
- 源码运行：双击或执行 `tools/recording-retrieval-service/app/start-ui.cmd`
- 服务模式：双击或执行 `tools/recording-retrieval-service/app/start-service.cmd`
- 便携包入口：`tools/recording-retrieval-service/app/dist/releases/<timestamp>/start-ui.cmd`

## owner 调用前提
- 目录存在。
- 服务已启动。
- `GET /health` 可达。
- `protocolVersion === "v1"`。

## 非目标
- 不直接修改项目数据文件。
- 不直接写入 owner 的 `AutomationRun`。
- 不直接维护候选审查状态。
- 不直接解析批量导入模板文本。
