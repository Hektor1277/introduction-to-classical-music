# Changelog

本项目遵循 `SemVer`（语义化版本）。

## [0.1.0] - 2026-04-19

### Added

- 建立公共发布仓库结构
- 引入 `Apache-2.0` 代码许可证与独立内容许可说明
- 新增 `bootstrap:windows`、`doctor:windows`、`package:windows` 命令
- 将 `tools/recording-retrieval-service/app/` 纳入公共仓库版本控制
- 增加 Windows 打包与环境检查 CI

### Changed

- 公共默认数据调整为空资料库
- 默认库首次启动仅注入《不全书使用手册》专栏
- Windows 公共 Release 固定为安装版，不公开个人资料库便携版
- 安装升级策略固定为同 `AppId` 覆盖安装并保留 `%APPDATA%` 用户数据

### Removed

- 从公共发布目标中移除便携版应用程序产物

### Notes

- 首发版本不提供应用内自动更新
