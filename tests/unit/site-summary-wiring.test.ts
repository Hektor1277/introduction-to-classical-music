import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

const detailPages = [
  "apps/site/src/pages/composers/[slug].astro",
  "apps/site/src/pages/conductors/[slug].astro",
  "apps/site/src/pages/people/[slug].astro",
  "apps/site/src/pages/orchestras/[slug].astro",
  "apps/site/src/pages/works/[id].astro",
];

describe("expandable summary wiring", () => {
  it("uses a shared expandable summary component on detail pages", async () => {
    const pageContents = await Promise.all(detailPages.map((file) => fs.readFile(path.resolve(file), "utf8")));

    for (const page of pageContents) {
      expect(page).toContain('import ExpandableSummary from "@/components/ExpandableSummary.astro"');
      expect(page).toContain("<ExpandableSummary");
    }
  });

  it("renders expand or collapse as a footer-like tail below the centered summary block", async () => {
    const component = await fs.readFile(path.resolve("apps/site/src/components/ExpandableSummary.astro"), "utf8");
    const stylesheet = await fs.readFile(path.resolve("apps/site/src/styles/global.css"), "utf8");
    const workPage = await fs.readFile(path.resolve("apps/site/src/pages/works/[id].astro"), "utf8");
    const composerPage = await fs.readFile(path.resolve("apps/site/src/pages/composers/[slug].astro"), "utf8");
    const summaryBlock = stylesheet.match(/\.expandable-summary\s*\{[^}]+\}/)?.[0] ?? "";
    const tailBlock = stylesheet.match(/\.expandable-summary__tail\s*\{[^}]+\}/)?.[0] ?? "";

    expect(component).toContain("containerClassName");
    expect(component).toContain("data-expandable-summary");
    expect(component).toContain('class="expandable-summary__tail"');
    expect(component).toContain('class="expandable-summary__toggle expandable-summary__toggle--inline"');
    expect(component).toContain("展开");
    expect(component).toContain("折叠");
    expect(summaryBlock).toContain("display: grid;");
    expect(summaryBlock).toContain("width: 100%;");
    expect(tailBlock).toContain("display: flex;");
    expect(tailBlock).toContain("justify-content: flex-end;");
    expect(tailBlock).toContain("width: 100%;");
    expect(tailBlock).not.toContain("justify-self");
    expect(stylesheet).not.toContain(".expandable-summary__content-wrap");
    expect(stylesheet).not.toContain(".expandable-summary__inline-tail");
    expect(workPage).toContain('containerClassName="composer-summary"');
    expect(composerPage).toContain('containerClassName="composer-summary"');
  });

  it("renders guide panels as a lower-level Chinese-only section with plain text links", async () => {
    const component = await fs.readFile(path.resolve("apps/site/src/components/EntityInfoPanel.astro"), "utf8");
    const stylesheet = await fs.readFile(path.resolve("apps/site/src/styles/global.css"), "utf8");
    const composerPage = await fs.readFile(path.resolve("apps/site/src/pages/composers/[slug].astro"), "utf8");
    const workPage = await fs.readFile(path.resolve("apps/site/src/pages/works/[id].astro"), "utf8");
    const recordingPage = await fs.readFile(path.resolve("apps/site/src/pages/recordings/[id].astro"), "utf8");

    expect(component).toContain("导览");
    expect(component).not.toContain("Related Guide");
    expect(component).toContain("entity-info-panel__title");
    expect(component).toContain("entity-info-panel__link");
    expect(component).not.toContain("resource-button");
    expect(stylesheet).toMatch(/\.entity-info-panel__title\s*\{/);
    expect(stylesheet).toMatch(/\.entity-info-panel__link\s*\{/);
    expect(stylesheet).toMatch(/\.section-heading--subsection[\s\S]*h2/);
    expect(composerPage).toContain('section-heading section-heading--centered section-heading--subsection');
    expect(workPage).toContain('section-heading section-heading--centered section-heading--subsection');
    expect(recordingPage).toContain('section-heading section-heading--subsection');
  });

  it("uses a shared page-intro heading shell on top-level landing pages so switching tabs keeps titles aligned", async () => {
    const directoryBrowser = await fs.readFile(path.resolve("apps/site/src/components/DirectoryBrowser.astro"), "utf8");
    const searchPanel = await fs.readFile(path.resolve("apps/site/src/components/SearchPanel.astro"), "utf8");
    const aboutPage = await fs.readFile(path.resolve("apps/site/src/pages/about.astro"), "utf8");
    const stylesheet = await fs.readFile(path.resolve("apps/site/src/styles/global.css"), "utf8");

    expect(directoryBrowser).toContain('directory-browser__header section-heading section-heading--page-intro');
    expect(searchPanel).toContain('<div class="section-heading section-heading--page-intro">');
    expect(aboutPage).toContain('<div class="section-heading section-heading--page-intro">');
    expect(aboutPage).toContain('<div class="container page-section page-section--plain">');
    expect(aboutPage).toContain('<p class="page-lead">{site.description}</p>');
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s*\{/);
    expect(stylesheet).toMatch(/\.page-section--plain\s*\{[\s\S]*--page-intro-top-gap:\s*clamp\(/i);
    expect(stylesheet).toMatch(/\.page-section--plain\s*\{[\s\S]*padding-top:\s*var\(--page-intro-top-gap\)/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s*\{[\s\S]*padding-top:\s*0/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s*\{[\s\S]*width:\s*min\(/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s*\{[\s\S]*min-height:\s*0/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s+\.section-heading__eyebrow\s*\{[\s\S]*margin:\s*0/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s+h1[\s\S]*margin-top:\s*var\(--page-intro-stack-gap\)/i);
    expect(stylesheet).toMatch(/\.section-heading--page-intro\s+\.page-lead[\s\S]*margin-top:\s*var\(--page-intro-stack-gap\)/i);
    expect(stylesheet).toMatch(/\.search-panel\s*\{[\s\S]*padding-top:\s*0/i);
  });
});
