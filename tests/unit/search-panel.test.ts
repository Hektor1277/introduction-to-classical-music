import { describe, expect, it } from "vitest";

import { buildSearchGroups } from "../../apps/site/src/lib/search-panel.ts";

describe("search panel article groups", () => {
  it("surfaces article results alongside other searchable kinds", () => {
    const groups = buildSearchGroups(
      [
        {
          id: "usage-guide",
          kind: "article",
          primaryText: "不全书使用文档",
          secondaryText: "介绍导入库、构建站点与日常维护流程。",
          href: "/columns/usage-guide/",
          matchTokens: ["不全书使用文档", "导入库", "构建站点"],
          aliasTokens: ["使用文档"],
        },
      ],
      "导入库",
      {},
    );

    expect(groups).toHaveLength(1);
    expect(groups[0]?.kind).toBe("article");
    expect(groups[0]?.items[0]?.href).toBe("/columns/usage-guide/");
  });
});
