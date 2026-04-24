# scripts 整理迁移说明（硬收敛后）

更新时间：2026-04-24

## 目标

在不影响发布主链路、CI、文档和 PowerShell 诊断流程的前提下，逐步收敛 `package.json` 中的脚本暴露面。

当前阶段已完成“硬收敛”：

- 已删除非主链路维护脚本
- 保留稳定入口不改名
- 提供删除脚本的等价替代调用

## 稳定入口（保留，不改名）

以下脚本在 CI、发布文档、检查清单或测试中存在直接耦合，属于稳定入口：

- `bootstrap:windows`
- `doctor:windows`
- `check`
- `build`
- `package:windows`
- `runtime:build`
- `desktop:dist`
- `desktop:dist:release`
- `retrieval:portable:build`
- `desktop:portable:build`

## 已删除脚本（2026-04-24）

以下脚本已从 `package.json` 删除：

- `entity-vitals:sync`
- `import:legacy`
- `recording:legacy-audit`
- `recording:live-audit`
- `recording:live-verify`
- `reference:audit`
- `reference:query`
- `reference:sync`
- `owner`

## 替代调用建议

对于已删除脚本，建议直接调用底层脚本文件（必要时先执行 `npm run runtime:build`）：

- `node scripts/sync-entity-vitals-review.mjs && node scripts/apply-entity-vitals-review.mjs`
- `node output/runtime/scripts/import-legacy.js`
- `node output/runtime/scripts/audit-recording-legacy-alignment.js`
- `node scripts/audit-recording-retrieval-live.mjs`
- `node scripts/verify-recording-retrieval-live-integration.mjs`
- `node output/runtime/scripts/audit-reference-registry.js`
- `node output/runtime/scripts/query-reference-registry.js`
- `node output/runtime/scripts/sync-reference-registry.js`
- `node output/runtime/apps/owner/server/owner-app.js`

## 执行记录

1. 第一轮：删除严格重复脚本（已完成：`build:library`）。
2. 第二轮：补迁移文档与入口链接（已完成）。
3. 第三轮：删除候选脚本并复检 `docs/`、CI、PowerShell 引用（已完成）。
