import { describe, expect, it } from "vitest";

import { buildArticlePreviewModel, renderArticleMarkdown, validateArticles } from "@/lib/articles";

describe("articles", () => {
  it("validates article collections", () => {
    const articles = validateArticles([
      {
        id: "article-beethoven-9",
        slug: "beethoven-9",
        title: "\u8d1d\u591a\u82ac\u7b2c\u4e5d\u4ea4\u54cd\u66f2\u5bfc\u8bfb",
        summary: "\u7b80\u4ecb",
        markdown: "# \u6807\u9898",
        showOnHome: true,
        createdAt: "2026-03-13T00:00:00.000Z",
        updatedAt: "2026-03-13T00:00:00.000Z",
      },
    ]);

    expect(articles).toHaveLength(1);
    expect(articles[0]?.slug).toBe("beethoven-9");
    expect(articles[0]?.showOnHome).toBe(true);
  });

  it("renders markdown and strips unsafe content", () => {
    const html = renderArticleMarkdown(
      "# \u6807\u9898\n\n[\u5b89\u5168\u94fe\u63a5](https://example.com)\n\n[\u5371\u9669\u94fe\u63a5](javascript:alert(1))\n\n<script>alert(1)</script>",
    );

    expect(html).toContain("<h1>\u6807\u9898</h1>");
    expect(html).toContain('href="https://example.com"');
    expect(html).not.toContain('href="javascript:alert(1)"');
    expect(html).not.toContain("<script>");
  });

  it("builds a full preview model for the owner preview pane", () => {
    const preview = buildArticlePreviewModel({
      title: "\u5e03\u9c81\u514b\u7eb3\u5bfc\u8bfb",
      summary: "\u4e00\u6bb5\u63d0\u8981",
      markdown: "## \u7b2c\u4e00\u8282\n\n\u5185\u5bb9",
    });

    expect(preview.title).toBe("\u5e03\u9c81\u514b\u7eb3\u5bfc\u8bfb");
    expect(preview.summary).toBe("\u4e00\u6bb5\u63d0\u8981");
    expect(preview.bodyHtml).toContain("<h2>\u7b2c\u4e00\u8282</h2>");
    expect(preview.isEmpty).toBe(false);
  });

  it("supports article image size annotations", () => {
    const html = renderArticleMarkdown("![图](https://example.com/a.jpg){size=small}");

    expect(html).toContain('class="article-image article-image--small"');
    expect(html).toContain('src="https://example.com/a.jpg"');
  });
});
