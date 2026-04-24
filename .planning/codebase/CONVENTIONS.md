# 编码约定

**分析日期：** 2026-04-24

## 命名模式

**文件命名：**
- TS/JS 模块普遍使用 kebab-case，例如 `packages/data-core/src/library-store.ts`、`packages/automation/src/recording-retrieval.ts`。
- Astro 动态路由使用方括号参数，如 `apps/site/src/pages/recordings/[id].astro`。
- Vitest 测试文件使用 `.test.ts` 后缀，集中在 `tests/unit/`。

**函数命名：**
- 使用 camelCase（如 `createSiteBuildEnvironment`、`mergeLlmConfigPatch`）。
- 布尔判断函数常用 `is*`、`has*`、`can*` 前缀。
- 状态变更函数常以动词开头（如 `applyAutomationProposal`、`saveLibraryToDisk`）。

**变量命名：**
- 不可变引用优先 `const`。
- 模块级常量常见 UPPER_SNAKE_CASE。
- UI/运行时状态常用稳定 key 的 plain object。

**类型命名：**
- 类型别名使用 PascalCase，并常以 `*Config`、`*Result`、`*Summary` 结尾。
- 领域状态倾向使用字符串字面量联合类型。

## 代码风格

**格式化：**
- 以 `.editorconfig` 为准：UTF-8、LF、去尾空白、文件末尾换行、2 空格缩进。
- JSON 持久化常见模式：`JSON.stringify(data, null, 2) + "\n"`。

**Lint 现状：**
- 仓库根目录未检测到强约束 ESLint/Prettier/Biome 配置。
- 实际执行上以“跟随现有文件风格 + `.editorconfig`”为主。

## 导入组织

**常见顺序：**
1. Node 内建模块（`node:*`）。
2. 第三方依赖。
3. 项目内部模块（必要时使用 `import type`）。

**路径别名：**
- `@/*` 指向 `apps/site/src/*`。
- `@data/*`、`@generated/*` 由 `tsconfig.json` 提供。
- `apps/site/src/lib/*` 常作为对 `packages/*` 的薄封装重导出层。

## 错误处理

**惯用模式：**
- 边界先校验，非法状态显式抛错。
- 文件/网络边界用 `try/catch`，在可降级场景返回安全默认值。
- HTTP 路由将异常转换为结构化 JSON 响应并设置状态码。

## 日志策略

**框架：** 以 `console` 为主。  
**模式：**
- 脚本场景使用 `console.log`/`console.error` 输出可操作信息。
- 桌面生命周期日志通过 `writeDesktopLog` 追加到文件。

## 注释策略

**何时写注释：**
- TS 模块整体偏少注释，更强调可读命名与小函数拆分。
- JS 文件（尤其 Owner 前端工具）会用 JSDoc 补参数契约。

**JSDoc/TSDoc：**
- JSDoc 在 `.js` 工具模块中较常见。
- `.ts` 中较少大段 TSDoc，更多依赖类型签名表达语义。

## 函数设计

**函数粒度：**
- 倾向小函数、单一职责、组合式实现。

**参数风格：**
- 倾向对象参数 + 默认值，便于扩展。

**返回风格：**
- 偏好命名字段对象，少用位置依赖强的 tuple 返回。

## 模块设计

**导出策略：**
- 运行时代码通常使用 named exports。
- default export 多用于配置入口（如 Astro 配置、打包脚本 hook）。

**Barrel/封装层：**
- `apps/site/src/lib/` 以薄重导出为主，保持 Astro 导入稳定。

---

*约定分析：2026-04-24*
