# 不全书

**不全书（BuQuanShu / Introduction to Classical Music）** 是一套面向 Windows 用户的古典音乐资料整理与建站工具。

它把“资料维护”“本地静态站点构建”和“桌面启动入口”放在同一套工作流中，适合长期整理自己古典音乐目录的普通乐迷、研究者与社群组织者。

你可以把它理解成一个专门为古典音乐资料设计的个人资料库：

- 在维护工具中录入作曲家、人物、团体、作品、版本与专栏
- 一键构建出可浏览的本地静态站点
- 通过桌面启动器打开站点、维护工具与版本检索工具

默认安装后附带的是**空资料库模板**，只包含《不全书使用手册》，不包含测试条目或开发者个人资料。

## 当前发布内容

当前公开发布包含三类内容：

### 1. Windows 安装版

适合绝大多数普通用户。

- 在 [Releases](https://github.com/Hektor1277/introduction-to-classical-music/releases) 下载 `Setup.0.1.0.exe`
- 双击安装后即可使用
- 自带默认空资料库与《不全书使用手册》
- 首次打开进入桌面启动器

### 2. GitHub 源码仓库

适合希望自己构建、调试或参与后续开发的用户。

- 可以直接 `git clone` 本仓库
- 按下文的“源码安装”完成环境准备和构建
- 源码默认构建结果仍是空资料库模板，不包含个人资料库内容

### 3. Windows 便携版

古典导聆不全书离线版，与网页端内容一致。

- 在 [Releases](https://github.com/Hektor1277/introduction-to-classical-music/releases) 下载：
  - `BuQuanShu-Portable-0.1.0.exe`
  - `BuQuanShu-Portable-0.1.0.zip`
- 便携版打开后直接进入不全书站点界面
- 单文件 `EXE` 首次启动会略慢，因为它需要先自解压
- `ZIP` 解压后运行通常更快

## 安装与使用

### 方式一：直接安装

1. 打开 [Releases](https://github.com/Hektor1277/introduction-to-classical-music/releases)
2. 下载 `Setup.0.1.0.exe`
3. 双击运行安装程序
4. 选择安装目录并完成安装
5. 安装完成后启动 `不全书`

安装版特点：

- 支持自定义安装路径
- 安装目录中自带默认资料库
- 支持覆盖升级
- 卸载时删除程序目录，但不会主动删除 `%APPDATA%\Introduction to Classical Music` 中的运行数据

### 方式二：源码安装

适用环境：

- `Windows 10/11 x64`
- `Node.js 22`
- `npm`
- `Python 3.13`

执行步骤：

```powershell
git clone https://github.com/Hektor1277/introduction-to-classical-music.git
cd introduction-to-classical-music
npm run bootstrap:windows
npm run doctor:windows
npm run check
npm run build
```

如果还需要自己打包安装版：

```powershell
npm run package:windows
```

### 方式三：便携版使用

1. 打开 [Releases](https://github.com/Hektor1277/introduction-to-classical-music/releases)
2. 下载 `BuQuanShu-Portable-0.1.0.exe` 或 `BuQuanShu-Portable-0.1.0.zip`
3. 直接运行，或解压 ZIP 后运行内部程序

说明：

- 单文件 `EXE` 首次打开通常较慢
- `ZIP` 解压后启动更快
- 便携版打开后直接进入站点首页

## 第一次打开后如何使用

安装版启动后，你会看到三个入口：

- **打开本地构建库**
- **打开维护工具**
- **打开版本检索工具**

推荐的第一次使用顺序：

1. 打开维护工具
2. 先创建基础条目：作曲家、人物、团体、作品
3. 再创建版本条目，补充导聆文字、封面和资源链接
4. 运行自动检查与候选审查
5. 构建站点
6. 回到启动器并打开本地构建库查看效果

## 默认资料库说明

公开发布的默认资料库遵守以下边界：

- `composer / person / work / recording` 默认全部为空
- 只保留《不全书使用手册》
- 手册图片来自 `materials/default-library/usage-guide`
- 不包含测试条目
- 不包含开发者个人资料库

这意味着安装后拿到的是一个干净起点，而不是演示库。

## 关于资源链接

维护工具允许给版本条目添加两类资源链接：

- 外部资源链接，例如网站、视频页、流媒体页
- 本地资源链接（local link，本地链接），例如本机上的音频、视频或文档

本地资源链接会显示在前台站点中，但通常只在当前设备有效。
如果把资料库导出到另一台设备，这类链接通常需要重新整理。

## 项目边界

当前公开版本遵守以下边界：

- 当前正式支持平台为 `Windows 10/11 x64`
- 首发版本不提供应用内自动更新
- 默认资料库是空模板，不是演示资料库
- 本地资源链接只在当前设备有效
- 桌面源码仓库、便携版发布资产和互联网部署站点采用分离工作区

详细边界说明见 [docs/release/PROJECT_BOUNDARIES.md](docs/release/PROJECT_BOUNDARIES.md)。

## 更多文档

- [发布文档索引](docs/release/README.md)
- [安装说明](docs/release/0.1.0-installation-guide.md)
- [发布说明](docs/release/0.1.0-release-notes.md)
- [发布流程](RELEASING.md)
- [贡献说明](CONTRIBUTING.md)
- [安全说明](SECURITY.md)

## 许可

- 代码：`Apache-2.0`
- 默认手册、截图与公开内容说明：见 [LICENSE-CONTENT.md](LICENSE-CONTENT.md)
- 第三方依赖声明：见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

本项目不会在公共仓库中附带来源不明或版权不清晰的音视频文件、封面图或开发者个人内容包。
