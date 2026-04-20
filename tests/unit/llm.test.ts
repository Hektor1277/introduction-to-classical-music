import { describe, expect, it, vi } from "vitest";

import {
  defaultLlmConfig,
  generateEntityKnowledgeCandidate,
  mergeLlmConfigPatch,
  reviewAutomationProposalWithLlm,
  sanitizeLlmConfig,
  testOpenAiCompatibleConfig,
} from "@/lib/llm";

describe("llm config", () => {
  it("preserves an existing API key when the patch omits it", () => {
    const current = {
      ...defaultLlmConfig,
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "test-model",
    };

    const merged = mergeLlmConfigPatch(current, {
      enabled: true,
      baseUrl: "https://api.example.com/v1/",
      model: "test-model-2",
    });

    expect(merged.apiKey).toBe("secret-key");
    expect(merged.baseUrl).toBe("https://api.example.com/v1");
    expect(merged.model).toBe("test-model-2");
  });

  it("returns apiKey in the sanitized owner payload so the local form can stay filled", () => {
    const sanitized = sanitizeLlmConfig({
      ...defaultLlmConfig,
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "test-model",
    });

    expect(sanitized.apiKey).toBe("secret-key");
    expect(sanitized.hasApiKey).toBe(true);
  });

  it("tests an OpenAI-compatible endpoint", async () => {
    const result = await testOpenAiCompatibleConfig(
      {
        ...defaultLlmConfig,
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "test-model",
      },
      async () =>
        ({
          ok: true,
          json: async () => ({
            model: "test-model",
            choices: [{ message: { content: "OK" } }],
          }),
        }) as Response,
    );

    expect(result.ok).toBe(true);
    expect(result.output).toBe("OK");
  });

  it("returns a readable Chinese validation message when config is incomplete", async () => {
    const result = await testOpenAiCompatibleConfig({
      ...defaultLlmConfig,
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "",
      model: "test-model",
    });

    expect(result.ok).toBe(false);
    expect(result.message).toBe("请先填写 base URL、API key 和 model。");
  });

  it("repairs malformed first-pass entity knowledge output and still returns useful fields", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content: JSON.stringify({
                  normalizedTitle: "Anton Bruckner",
                  wikipediaUrl: "https://en.wikipedia.org/wiki/Anton_Bruckner",
                }),
              },
            },
          ],
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content: JSON.stringify({
                  displayName: "布鲁克纳",
                  displayFullName: "安东·布鲁克纳",
                  displayLatinName: "Anton Bruckner",
                  aliases: ["布鲁克纳"],
                  abbreviations: [],
                  country: "Austria",
                  birthYear: 1824,
                  deathYear: 1896,
                  confidence: 0.82,
                  rationale: "依据常见中文译名与英文全名补全。",
                }),
              },
            },
          ],
        }),
      });

    const result = await generateEntityKnowledgeCandidate({
      config: {
        ...defaultLlmConfig,
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "deepseek-reasoner",
      },
      title: "Anton Bruckner",
      entityType: "composer",
      knownDisplayName: "布鲁克纳",
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(result).toMatchObject({
      displayName: "布鲁克纳",
      displayFullName: "安东·布鲁克纳",
      displayLatinName: "Anton Bruckner",
      country: "Austria",
      birthYear: 1824,
      deathYear: 1896,
    });
    expect(result?.aliases).toContain("布鲁克纳");
  });

  it("routes deepseek reasoner extraction through deepseek-chat and retries when the first answer lacks chinese naming", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content: JSON.stringify({
                  displayName: "Anton Bruckner",
                  displayFullName: "Anton Bruckner",
                  displayLatinName: "Anton Bruckner",
                  country: "Austria",
                  birthYear: 1824,
                  deathYear: 1896,
                  confidence: 0.4,
                  rationale: "fallback",
                }),
              },
            },
          ],
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content: JSON.stringify({
                  displayName: "布鲁克纳",
                  displayFullName: "安东·布鲁克纳",
                  displayLatinName: "Anton Bruckner",
                  aliases: ["布鲁克纳", "安东·布鲁克纳"],
                  abbreviations: [],
                  country: "Austria",
                  birthYear: 1824,
                  deathYear: 1896,
                  confidence: 0.9,
                  rationale: "使用中文常译名补全。",
                }),
              },
            },
          ],
        }),
      });

    const result = await generateEntityKnowledgeCandidate({
      config: {
        ...defaultLlmConfig,
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "deepseek-reasoner",
      },
      title: "Anton Bruckner",
      entityType: "composer",
      knownDisplayName: "布鲁克纳",
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    const firstPayload = JSON.parse(String(fetchImpl.mock.calls[0]?.[1]?.body ?? "{}"));
    const secondPayload = JSON.parse(String(fetchImpl.mock.calls[1]?.[1]?.body ?? "{}"));
    expect(firstPayload.response_format).toEqual({ type: "json_object" });
    expect(firstPayload.model).toBe("deepseek-chat");
    expect(secondPayload.model).toBe("deepseek-chat");
    expect(result).toMatchObject({
      displayName: "布鲁克纳",
      displayFullName: "安东·布鲁克纳",
      displayLatinName: "Anton Bruckner",
    });
  });

  it("falls back to reasoning_content when final content is empty", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        choices: [
          {
            message: {
              content: "",
              reasoning_content: JSON.stringify({
                displayName: "布鲁克纳",
                displayFullName: "安东·布鲁克纳",
                displayLatinName: "Anton Bruckner",
                aliases: ["布鲁克纳"],
                abbreviations: [],
                country: "Austria",
                birthYear: 1824,
                deathYear: 1896,
                confidence: 0.88,
                rationale: "reasoning fallback",
              }),
            },
          },
        ],
      }),
    });

    const result = await generateEntityKnowledgeCandidate({
      config: {
        ...defaultLlmConfig,
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "custom-chat",
      },
      title: "Anton Bruckner",
      entityType: "composer",
      knownDisplayName: "布鲁克纳",
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });

    expect(result).toMatchObject({
      displayName: "布鲁克纳",
      displayFullName: "安东·布鲁克纳",
      displayLatinName: "Anton Bruckner",
      country: "Austria",
    });
  });
  it("parses structured proposal review verdicts with rejection reasons and normalized suggestions", async () => {
    const result = await reviewAutomationProposalWithLlm({
      config: {
        ...defaultLlmConfig,
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "test-model",
      },
      entityType: "person",
      title: "Anton Bruckner",
      roles: ["composer"],
      current: { name: "布鲁克纳", country: "" },
      preview: { name: "布鲁克纳", country: "Austria" },
      fields: [{ path: "country", before: "", after: "Austria" }],
      sources: ["en.wikipedia.org"],
      evidence: [
        {
          field: "country",
          sourceLabel: "Wikipedia",
          sourceUrl: "https://en.wikipedia.org/wiki/Anton_Bruckner",
          confidence: 0.93,
        },
      ],
      fetchImpl: async () =>
        ({
          ok: true,
          json: async () => ({
            choices: [
              {
                message: {
                  content: JSON.stringify({
                    verdict: "reject",
                    reasons: ["候选值与现有规范字段冲突"],
                    rejectBecause: "现有中文全名已经是高质量规范值",
                    normalizedValue: {
                      country: "Austria",
                    },
                    confidence: 0.91,
                    rationale: "应阻止低价值覆盖。",
                  }),
                },
              },
            ],
          }),
        }) as Response,
    });

    expect(result).toMatchObject({
      verdict: "reject",
      status: "needs-attention",
      rejectBecause: "现有中文全名已经是高质量规范值",
      normalizedValue: { country: "Austria" },
      confidence: 0.91,
    });
    expect(result?.reasons).toContain("候选值与现有规范字段冲突");
    expect(result?.issues).toContain("现有中文全名已经是高质量规范值");
  });
});
