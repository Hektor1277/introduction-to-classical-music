# 发布流程

## 发布目标

公共 Release 只发布两类内容：

- 源代码仓库
- `不全书 Setup <version>.exe`

开发者个人资料库便携版不进入公共 Release。

## 发布前检查

1. 运行 `npm run bootstrap:windows`
2. 运行 `npm run doctor:windows`
3. 运行 `npm run check`
4. 运行 `npm run build`
5. 运行 `npm run package:windows`
6. 检查安装、启动、覆盖升级、卸载和用户数据保留
7. 更新 `CHANGELOG.md`、`docs/release/*` 和 Release Notes

## 打包结果

打包完成后，Windows 安装版默认位于：

- `output/releases/`

请确认只上传安装版 `Setup` 文件到公共 Release。

## 升级与卸载

- 使用稳定 `appId` 进行覆盖升级
- 卸载程序必须可用
- 卸载默认保留 `%APPDATA%\Introduction to Classical Music`

## 发布后动作

- 打 Tag
- 上传安装版
- 发布 Release Notes
- 在下一轮开发开始前确认公共仓库仍然不含私有资料、缓存和构建物
