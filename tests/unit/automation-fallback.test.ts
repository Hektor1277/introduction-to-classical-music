import { describe, expect, it } from "vitest";

import { runAutomationChecks } from "@/lib/automation-checks";
import { defaultLlmConfig } from "@/lib/llm";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "mahler",
      slug: "mahler",
      name: "马勒",
      fullName: "古斯塔夫·马勒",
      nameLatin: "Gustav Mahler",
      displayName: "马勒",
      displayFullName: "古斯塔夫·马勒",
      displayLatinName: "Gustav Mahler",
      country: "",
      avatarSrc: "",
      aliases: [],
      abbreviations: [],
      sortKey: "mahler",
      summary: "",
    },
  ],
  people: [],
  workGroups: [],
  works: [],
  recordings: [],
});

describe("automation fallback", () => {
  it("falls back to Baidu Baike when Wikipedia is unavailable", async () => {
    const run = await runAutomationChecks(
      library,
      { categories: ["composer"], composerIds: ["mahler"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") || value.includes("api/rest_v1/page/summary")) {
          throw new Error(`blocked: ${value}`);
        }
        return {
          ok: true,
          url: "https://baike.baidu.com/item/%E9%A9%AC%E5%8B%92",
          text: async () =>
            '<html><head><meta property="og:title" content="古斯塔夫·马勒"><meta name="description" content="Gustav Mahler Austrian composer, 1860-1911."><meta property="og:image" content="https://img.example.com/mahler.jpg"></head></html>',
        } as Response;
      },
    );

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.fields.some((field) => field.path === "country" && field.after === "Austria")).toBe(true);
    expect(run.proposals[0]?.imageCandidates?.length ?? 0).toBeGreaterThanOrEqual(0);
    expect(run.proposals[0]?.warnings?.some((warning) => warning.includes("Wikipedia"))).toBe(true);
    expect(
      run.proposals[0]?.warnings?.some(
        (warning) => warning.includes("未找到可用图片候选") || warning.includes("被过滤"),
      ),
    ).toBe(true);
  });

  it("falls back to LLM knowledge when wikipedia and baidu are unavailable", async () => {
    const llmConfig = {
      ...defaultLlmConfig,
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-reasoner",
    };

    const run = await runAutomationChecks(
      library,
      { categories: ["composer"], composerIds: ["mahler"] },
      async (url) => {
        const value = String(url);
        if (value.includes("/chat/completions")) {
          return {
            ok: true,
            json: async () => ({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      summary: "奥地利作曲家、指挥家，晚期浪漫主义交响乐的重要代表。",
                      country: "Austria",
                      birthYear: 1860,
                      deathYear: 1911,
                      displayName: "马勒",
                      displayFullName: "古斯塔夫·马勒",
                      displayLatinName: "Gustav Mahler",
                      aliases: ["Gustav Mahler"],
                      abbreviations: [],
                      confidence: 0.92,
                      rationale: "依据常见传记事实生成，仍需人工复核。",
                    }),
                  },
                },
              ],
            }),
          } as Response;
        }
        throw new Error(`blocked: ${value}`);
      },
      llmConfig,
    );

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.fields.some((field) => field.path === "summary")).toBe(true);
    expect(run.proposals[0]?.fields.some((field) => field.path === "country" && field.after === "Austria")).toBe(true);
    expect(run.proposals[0]?.warnings?.some((warning) => warning.includes("Wikipedia"))).toBe(true);
    expect(run.notes.some((note) => note.includes("LLM"))).toBe(true);
  });

  it("falls back to Baidu search snippets when Wikipedia and Baidu Baike are unavailable", async () => {
    const run = await runAutomationChecks(
      library,
      { categories: ["composer"], composerIds: ["mahler"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") || value.includes("api/rest_v1/page/summary") || value.includes("baike.baidu.com")) {
          throw new Error(`blocked: ${value}`);
        }
        if (value.includes("www.baidu.com/s")) {
          return {
            ok: true,
            url: "https://www.baidu.com/s?wd=%E9%A9%AC%E5%8B%92",
            text: async () =>
              '<html><body><div class="result"><h3><a href="https://www.example.com/mahler">Gustav Mahler</a></h3><div class="c-abstract">Gustav Mahler was an Austrian composer and conductor, 1860-1911.</div></div></body></html>',
          } as Response;
        }
        throw new Error(`unexpected: ${value}`);
      },
    );

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.fields.some((field) => field.path === "country" && field.after === "Austria")).toBe(true);
    expect(run.proposals[0]?.sources.some((source) => source.includes("www.baidu.com/s"))).toBe(true);
  });
});
