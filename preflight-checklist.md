# 项目前置配置清单（Preflight Checklist）

## 1. 项目概况
- 项目名称：`Introduction to Classical Music`
- 项目类型：`web`
- 技术栈：`nodejs, typescript, astro, vitest, python`
- 推荐运行环境：`windows`
- 推荐终端：`powershell`
- 决策依据：项目更贴近 Windows 生态或当前团队工具链以 Windows native（Windows 原生）为主。

## 2. Codex 启动前设置（用户手动）
- [ ] `Agent environment`（Agent 运行环境）设置为：`windows`
- [ ] `Integrated terminal shell`（集成终端）设置为：`powershell`
- [ ] `Custom instructions`（自定义指令）写入团队约束（语言、代码规范、测试策略）
- [ ] 默认打开目录指向项目根目录，避免跨盘符权限问题

## 3. 系统与权限门禁
- [ ] Git（版本控制）已安装并在 PATH 中
- [ ] 项目语言运行时已安装（如 Node.js / Python / Java / .NET）
- [ ] 包管理器可用（npm/pnpm/yarn/pip/poetry/maven/gradle 等）
- [ ] Chrome（谷歌浏览器）已安装，且可被自动化工具发现
- [ ] 额外工具已就绪：git
- [ ] 网络/代理/证书策略已验证可访问依赖源
- [ ] 项目目录具备读写权限（避免 ACL/受控文件夹阻止）
- [ ] PowerShell ExecutionPolicy（执行策略）或 Bash 执行权限满足脚本运行
- [ ] 代理（Proxy）与证书（Certificate）已配置（如公司网络要求）

## 4. 建议执行命令（可复制）
```bash
git --version
python --version  # 或 py --version
node --version
npm --version
chrome --version  # 或在系统中手动打开 Chrome
```

## 5. Plan B（失败替代路径）
- 若 `WSL` 无法访问企业网络：先改 `Windows native` + `PowerShell` 完成依赖安装，再回切 `WSL`
- 若 `PowerShell` 执行策略阻止脚本：使用签名脚本或临时进程级策略，不建议全局放开
- 若浏览器自动化失败：先手动确认 Chrome 可启动，再重装 Playwright 浏览器
- 若 RP 不可用：让用户在宿主环境先完成 RP 安装并重启终端

## 6. 完成确认（用户回填）
请用户回复以下块：

```text
[Preflight 完成确认]
- 环境选择：windows
- 终端：powershell
- 关键依赖安装：完成/未完成（列出未完成项）
- 权限与网络验证：通过/未通过（附报错）
- 可以进入开发：是/否
```

## 7. 门禁规则
在用户未明确回复“Preflight 完成确认”前，不进入具体开发与实现阶段。
