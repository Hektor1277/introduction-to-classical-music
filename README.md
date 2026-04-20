# 不全书

`不全书`（`Introduction to Classical Music`，古典音乐导览）是一个面向 `Windows 10/11 x64` 的开源项目，用于维护本地古典音乐资料库、构建静态站点，并通过桌面启动器统一打开浏览、维护和版本检索工具。

本公共仓库只发布两类公开交付物：

- GitHub 源代码仓库
- Windows 安装版：`不全书 Setup <version>.exe`

基于开发者个人资料库构建的便携版属于私有分发资产，不包含在公共仓库和公共 `Release` 中。

## 功能范围

- 维护本地资料库：作曲家、人物、作品、版本、专栏
- 构建静态站点：将当前活动资料库编译为可浏览的本地或公开站点
- 桌面启动器：统一打开本地站点、维护工具和版本检索工具
- 版本检索服务：随 Windows 安装版一同打包，用于辅助版本条目检查

## 支持矩阵

- 操作系统：`Windows 10/11 x64`
- Node.js：`22.x`
- npm：随 `Node.js 22` 安装
- Python：`3.13.x`

当前公共支持目标仅覆盖 Windows。其他平台暂不承诺安装、打包或运行兼容性。

## 快速开始

### 1. 获取源码

```powershell
git clone <your-github-url>
cd introduction-to-classical-music-public
```

### 2. 引导开发环境

```powershell
npm run bootstrap:windows
```

该命令会：

- 安装 Node 依赖
- 创建 `tools/recording-retrieval-service/app/.venv`
- 安装版本检索服务的 Python 依赖
- 安装并校验 `Playwright`（浏览器自动化框架）所需的 `Chromium`（浏览器内核）

### 3. 运行检查

```powershell
npm run doctor:windows
npm run check
```

`doctor:windows` 会检查：

- `Node.js 22.x`
- `Python 3.13.x`
- 依赖是否完整
- `Playwright` 浏览器是否可用
- 关键目录是否可写
- 默认端口被占用时是否能自动回避到其他空闲端口

### 4. 构建与打包

```powershell
npm run build
npm run package:windows
```

`package:windows` 会先执行 `doctor:windows`，再生成 Windows 安装版。

## 发布产物

### 公共产物

- GitHub 仓库源码
- `不全书 Setup <version>.exe`

### 非公共产物

- 基于开发者个人资料库构建的便携版站点包

## 默认公开内容

公共仓库中的默认种子数据遵循以下边界：

- `data/library/*.json` 为空结构
- `data/site/articles.json` 为空
- 默认资料库首次启动时只注入《不全书使用手册》专栏和配套截图
- 不包含开发者个人资料库、历史归档、下载缓存、日志、构建产物或本地密钥

## 仓库结构

```text
.
|-- apps/
|   |-- desktop/                    # Electron 桌面启动器
|   |-- owner/                      # 本地维护工具
|   `-- site/                       # 公共静态站点
|-- data/
|   |-- library/                    # 公共默认种子数据（空）
|   `-- site/                       # 公共站点配置与专栏数据
|-- docs/                           # 发布、架构、运维与协作文档
|-- materials/
|   `-- default-library/            # 默认库使用手册截图等公开素材
|-- packages/                       # 共享领域逻辑
|-- scripts/                        # 仓库级脚本
|-- tests/                          # 单元/集成测试
`-- tools/recording-retrieval-service/app/
    `-- ...                         # 版本检索服务源码
```

## 开发命令

```powershell
npm run bootstrap:windows
npm run doctor:windows
npm run check
npm run build
npm run package:windows
npm run owner
npm run desktop:dev
```

## 安装、升级与卸载

- 安装：运行 `不全书 Setup <version>.exe`，执行当前用户级安装，默认路径为 `%LOCALAPPDATA%\Programs\buquanshu`
- 默认资料库：安装程序会在程序目录内提供一个自带的 `library` 空库模板，只包含库结构与《不全书使用手册》专栏
- 升级：新版本安装器可直接覆盖旧版本，不要求先卸载
- 卸载：安装器会提供卸载程序；默认删除程序目录及其内置 `library`，但保留 `%APPDATA%\Introduction to Classical Music` 下的设置、日志等运行时数据
- 备份建议：如果你已经在安装目录默认库中录入内容，请先通过维护工具导出到独立目录，再执行升级或卸载
- 自动更新：首发版本不提供应用内自动更新

详细说明见：

- [docs/release/README.md](docs/release/README.md)
- [docs/release/0.1.0-installation-guide.md](docs/release/0.1.0-installation-guide.md)
- [RELEASING.md](RELEASING.md)

## 版权与许可

- 代码：`Apache-2.0`
- 默认手册截图、示例专栏、默认公开内容：见 [LICENSE-CONTENT.md](LICENSE-CONTENT.md)
- 第三方依赖声明：见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

## 贡献

欢迎提交 `Issue`、`Pull Request`（拉取请求）和文档修正。开始贡献前请先阅读：

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)

## 已知限制

- 仅承诺 `Windows 10/11 x64`
- 不提供公共便携版
- 不包含开发者个人资料库
- 不承诺应用内自动更新
