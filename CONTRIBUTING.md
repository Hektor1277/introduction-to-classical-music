# 贡献指南

感谢你为 `不全书` 贡献代码或文档。

## 基本要求

- 使用 `UTF-8`
- 保持提交粒度清晰
- 不要把构建产物、缓存、下载物、日志或本地密钥提交进仓库
- 不要向公共仓库提交开发者个人资料库或未审计版权内容

## 开发流程

1. Fork（派生）或创建分支。
2. 运行 `npm run bootstrap:windows`。
3. 运行 `npm run doctor:windows`。
4. 完成修改后运行 `npm run check`。
5. 如涉及安装版，运行 `npm run package:windows` 做回归。
6. 提交 Pull Request，并说明变更范围、测试结果和潜在风险。

## 变更边界

- 面向公共仓库的默认数据必须保持为空资料库
- 仅允许通过 `materials/default-library` 注入公开使用手册内容
- 任何版权来源不明确的图片、文本或媒体链接不得直接并入默认公开内容

## 文档要求

涉及发布、安装、升级、卸载、兼容性或许可的改动，必须同步更新：

- `README.md`
- `docs/release/*`
- `CHANGELOG.md`
- `RELEASING.md`（如流程发生变化）
