import { describe, expect, it } from "vitest";

import {
  createDefaultLibraryArticles,
  getDefaultLibraryDocumentationAssetFileNames,
} from "../../packages/data-core/src/default-library-content.js";

describe("default library usage guide quality", () => {
  it("ships a non-placeholder usage guide article for the public default library", () => {
    const articles = createDefaultLibraryArticles("2026-04-19T00:00:00.000Z");

    expect(articles).toHaveLength(1);
    expect(articles[0]?.title.trim()).not.toBe("");
    expect(articles[0]?.summary.trim()).not.toBe("");
    expect(articles[0]?.summary.toLowerCase()).not.toContain("test");
    expect(articles[0]?.showOnHome).toBe(true);
    expect(articles[0]?.markdown).toContain("# ");
    expect(articles[0]?.markdown).toContain("launcher-home.png");
    expect(articles[0]?.markdown).toContain("retrieval-home.png");
  });

  it("references every bundled usage-guide screenshot from managed assets", () => {
    const article = createDefaultLibraryArticles("2026-04-19T00:00:00.000Z")[0];

    for (const fileName of getDefaultLibraryDocumentationAssetFileNames()) {
      expect(article?.markdown).toContain(`/library-assets/managed/articles/usage-guide/${fileName}`);
    }
  });

  it("documents the bundled install-directory default library instead of app-data storage", () => {
    const article = createDefaultLibraryArticles("2026-04-19T00:00:00.000Z")[0];

    expect(article?.markdown).toContain("安装目录中的 `library` 文件夹");
    expect(article?.markdown).not.toContain("%APPDATA%");
  });
});
