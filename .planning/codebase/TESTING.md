# 测试模式

**分析日期：** 2026-04-24

## 测试框架

**运行器：**
- Vitest `^4.1.4`（见 `package.json`、`vitest.config.ts`）。

**断言：**
- 使用 Vitest 内置 `expect`（大量示例位于 `tests/unit/*.test.ts`）。

**常用命令：**
```bash
npm run test
npm run test:watch
npx vitest run --coverage
```

## 测试文件组织

**目录：**
- 主体测试在 `tests/unit/`。
- `tests/integration/` 与 `tests/e2e/` 目前仅 `.gitkeep`。

**命名：**
- 使用 `*.test.ts`，通常和目标模块/能力对应。

**结构：**
```text
tests/
  unit/
    *.test.ts
  integration/
    .gitkeep
  e2e/
    .gitkeep
```

## 用例结构

**通用骨架：**
```typescript
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  // 重置 mock、环境变量、模块缓存
});

describe("某能力域", () => {
  it("在某场景下应满足某行为", async () => {
    // arrange
    // act
    // assert
  });
});
```

**常见模式：**
- 使用 `mkdtemp + os.tmpdir()` 构造隔离文件系统沙箱。
- `afterEach` 中清理临时目录、恢复 env、`vi.resetModules()`。
- 断言覆盖同步异常与异步拒绝（`toThrow` / `rejects.toThrow`）。

## Mock 策略

**工具：** `vi`（Vitest）。

**常见做法：**
- 网络边界（`fetch`/外部 API）使用函数 mock。
- Node API 使用 `vi.spyOn` 做局部替换。
- 依赖环境变量初始化的模块，通过 `vi.resetModules()` + 动态导入重载。

**应 mock：**
- 外部网络/平台依赖。
- 文件系统异常分支与平台差异行为。

**不建议 mock：**
- 核心领域转换与 schema 校验逻辑（优先真数据断言）。
- 在临时目录内可真实执行的文件持久化链路。

## 夹具与工厂

**现状：**
- 夹具多以内联对象形式写在单测文件中。
- 未发现统一共享 fixture 目录。

**风格：**
- 复杂场景会在测试文件本地定义 helper/factory，保持上下文就近可读。

## 覆盖率

**阈值：**
- `vitest.config.ts` 未配置强制最低覆盖率阈值。

**查看：**
- 使用 `npx vitest run --coverage`，报告包含 text + html。

## 测试类型分布

**单元测试：**
- 当前主力，覆盖 schema、转换、自动化规则、运行时路径与脚本工具。

**集成测试：**
- 基本空缺（目录存在，用例不足）。

**E2E 测试：**
- 基本空缺（目录存在，用例不足）。

## 常见断言模式

**异步：**
- `await expect(promise).resolves...`
- `await expect(promise).rejects.toThrow(...)`

**错误分支：**
- 同步校验使用 `expect(() => fn()).toThrow(...)`。
- 执行期失败使用 `rejects.toThrow(...)`。

---

*测试分析：2026-04-24*
