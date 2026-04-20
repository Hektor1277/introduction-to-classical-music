# 第三方依赖声明

本项目依赖若干第三方开源组件。它们分别通过各自的许可证发布。

## Node.js 生态

- 直接依赖与传递依赖的版本锁定见 `package-lock.json`
- 主要依赖包括 `Astro`、`Electron`、`electron-builder`、`Express`、`Vitest`、`TypeScript`

## Python 生态

- 版本检索服务依赖定义见 `tools/recording-retrieval-service/app/pyproject.toml`
- 主要依赖包括 `FastAPI`、`Playwright`、`httpx`、`pydantic`、`uvicorn`、`PyInstaller`

## 使用原则

- 再分发本项目时，应同时保留本仓库的 `LICENSE`、`NOTICE` 和必要的第三方许可证声明
- 若你打包或再发布修改版本，应自行核对新增依赖的许可证兼容性
- 对于浏览器运行时、安装器生成物和平台运行库，应以对应上游项目的官方许可证为准
