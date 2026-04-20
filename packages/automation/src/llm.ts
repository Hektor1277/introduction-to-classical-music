export type LlmConfig = {
  enabled: boolean;
  baseUrl: string;
  apiKey: string;
  model: string;
  timeoutMs: number;
};

export type LlmTestResult = {
  ok: boolean;
  message: string;
  model?: string;
  output?: string;
};

export type LlmEntityKnowledgeCandidate = {
  summary?: string;
  country?: string;
  birthYear?: number;
  deathYear?: number;
  displayName?: string;
  displayFullName?: string;
  displayLatinName?: string;
  aliases?: string[];
  abbreviations?: string[];
  confidence?: number;
  rationale?: string;
};

export type LlmWorkKnowledgeCandidate = {
  titleLatin?: string;
  catalogue?: string;
  summary?: string;
  aliases?: string[];
  confidence?: number;
  rationale?: string;
};

export type LlmProposalReview = {
  verdict: "accept" | "needs-attention" | "reject";
  status: "ok" | "needs-attention";
  issues: string[];
  reasons: string[];
  confidence?: number;
  rationale?: string;
  rejectBecause?: string;
  normalizedValue?: Record<string, unknown>;
};

type EntityKnowledgePromptInput = {
  title: string;
  entityType: "composer" | "person";
  roles: string[];
  knownDisplayName?: string;
  knownDisplayFullName?: string;
  knownDisplayLatinName?: string;
  knownAliases?: string[];
  knownAbbreviations?: string[];
};

type WorkKnowledgePromptInput = {
  title: string;
  composerName: string;
  composerLatinName?: string;
  groupPath?: string[];
  knownTitleLatin?: string;
  knownCatalogue?: string;
  knownSummary?: string;
  knownAliases?: string[];
};

type ProposalReviewPromptInput = {
  entityType: "composer" | "person" | "work";
  title: string;
  roles?: string[];
  current: Record<string, unknown>;
  preview: Record<string, unknown>;
  fields: Array<{ path: string; before: unknown; after: unknown }>;
  sources: string[];
  evidence?: Array<{ field?: string; sourceLabel?: string; sourceUrl?: string; confidence?: number }>;
};

type LlmRequestPurpose = "entity-knowledge" | "summary" | "connectivity" | "proposal-review";

export const defaultLlmConfig: LlmConfig = {
  enabled: false,
  baseUrl: "",
  apiKey: "",
  model: "",
  timeoutMs: 30000,
};

function trimTrailingSlash(value: string) {
  return value.replace(/\/+$/, "");
}

function parseJsonFromText<T>(value: string): T | null {
  const source = String(value ?? "").trim();
  if (!source) {
    return null;
  }

  const fenced = source.match(/```(?:json)?\s*([\s\S]+?)```/i);
  const candidate = fenced?.[1]?.trim() || source;

  try {
    return JSON.parse(candidate) as T;
  } catch {
    return null;
  }
}

function containsCjk(value: string) {
  return /[\u3400-\u9fff]/.test(String(value ?? ""));
}

function looksReasoningModel(model: string) {
  return /(reasoner|o1|o3|o4|thinking|r1)/i.test(String(model ?? ""));
}

function getEffectiveModel(config: LlmConfig, purpose: LlmRequestPurpose) {
  const model = String(config.model ?? "").trim();
  if (!model) {
    return model;
  }
  if ((purpose === "entity-knowledge" || purpose === "summary") && /deepseek-reasoner/i.test(model)) {
    return "deepseek-chat";
  }
  return model;
}

function createAbortController(timeoutMs: number) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs || 30000);
  return {
    controller,
    clear() {
      clearTimeout(timeout);
    },
  };
}

function getEffectiveTimeoutMs(config: LlmConfig, purpose: LlmRequestPurpose) {
  const configured = Number(config.timeoutMs) || 30000;
  const effectiveModel = getEffectiveModel(config, purpose);
  if (purpose === "entity-knowledge" && looksReasoningModel(effectiveModel)) {
    return Math.max(configured, 90000);
  }
  if (purpose === "summary" && looksReasoningModel(effectiveModel)) {
    return Math.max(configured, 60000);
  }
  if (purpose === "proposal-review" && looksReasoningModel(effectiveModel)) {
    return Math.max(configured, 60000);
  }
  return configured;
}

export function isLlmConfigured(config?: Partial<LlmConfig> | null): config is LlmConfig {
  return Boolean(config?.enabled && config.baseUrl && config.apiKey && config.model);
}

export function mergeLlmConfigPatch(current: LlmConfig, patch: Partial<LlmConfig>) {
  return {
    ...current,
    ...patch,
    baseUrl: trimTrailingSlash((patch.baseUrl ?? current.baseUrl ?? "").trim()),
    apiKey: (patch.apiKey ?? current.apiKey ?? "").trim(),
    model: (patch.model ?? current.model ?? "").trim(),
    timeoutMs: Number.isFinite(Number(patch.timeoutMs)) ? Number(patch.timeoutMs) : current.timeoutMs,
    enabled: typeof patch.enabled === "boolean" ? patch.enabled : current.enabled,
  } satisfies LlmConfig;
}

export function sanitizeLlmConfig(config: LlmConfig) {
  return {
    enabled: config.enabled,
    baseUrl: config.baseUrl,
    apiKey: config.apiKey,
    model: config.model,
    timeoutMs: config.timeoutMs,
    hasApiKey: Boolean(config.apiKey),
  };
}

function buildChatUrl(config: LlmConfig) {
  return `${trimTrailingSlash(config.baseUrl)}/chat/completions`;
}

function normalizeStringList(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => String(item).trim()).filter(Boolean).slice(0, 8);
}

function pickString(...values: unknown[]) {
  for (const value of values) {
    const normalized = String(value ?? "").trim();
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

function coerceEntityKnowledgeCandidate(parsed: Record<string, unknown>) {
  const displayLatinName = pickString(
    parsed.displayLatinName,
    parsed.nameLatin,
    parsed.originalName,
    parsed.latinName,
    /^[A-Za-z]/.test(String(parsed.normalizedTitle ?? "").trim()) ? parsed.normalizedTitle : "",
  );
  const displayFullName = pickString(parsed.displayFullName, parsed.fullName, parsed.fullChineseName, parsed.chineseFullName);
  const displayName = pickString(parsed.displayName, parsed.commonChineseName, parsed.chineseName, parsed.commonName, displayFullName);
  const candidate = {
    summary: pickString(parsed.summary, parsed.description),
    country: pickString(parsed.country, parsed.nationality),
    birthYear: Number.isFinite(Number(parsed.birthYear)) ? Number(parsed.birthYear) : undefined,
    deathYear: Number.isFinite(Number(parsed.deathYear)) ? Number(parsed.deathYear) : undefined,
    displayName,
    displayFullName,
    displayLatinName,
    aliases: normalizeStringList(parsed.aliases),
    abbreviations: normalizeStringList(parsed.abbreviations),
    confidence: Number.isFinite(Number(parsed.confidence)) ? Number(parsed.confidence) : undefined,
    rationale: pickString(parsed.rationale, parsed.reason),
  } satisfies LlmEntityKnowledgeCandidate;

  return candidate;
}

function hasUsefulEntityKnowledge(candidate: LlmEntityKnowledgeCandidate | null, input?: EntityKnowledgePromptInput) {
  if (!candidate) {
    return false;
  }
  const primaryIdentityScore = [
    candidate.displayName,
    candidate.displayFullName,
    candidate.summary,
    candidate.country,
    candidate.birthYear,
    candidate.deathYear,
    candidate.aliases?.length,
    candidate.abbreviations?.length,
  ].filter(Boolean).length;
  if (primaryIdentityScore === 0) {
    return false;
  }

  const expectsChineseNaming = containsCjk(input?.knownDisplayName || "") || containsCjk(input?.knownDisplayFullName || "");
  if (expectsChineseNaming) {
    const hasChinesePrimary = containsCjk(candidate.displayName || "") || containsCjk(candidate.displayFullName || "");
    if (!hasChinesePrimary) {
      return false;
    }
  }

  return true;
}

function coerceWorkKnowledgeCandidate(parsed: Record<string, unknown>) {
  return {
    titleLatin: pickString(parsed.titleLatin, parsed.originalTitle, parsed.englishTitle, parsed.normalizedTitle),
    catalogue: pickString(parsed.catalogue, parsed.catalogNumber, parsed.opus),
    summary: pickString(parsed.summary, parsed.description),
    aliases: normalizeStringList(parsed.aliases),
    confidence: Number.isFinite(Number(parsed.confidence)) ? Number(parsed.confidence) : undefined,
    rationale: pickString(parsed.rationale, parsed.reason),
  } satisfies LlmWorkKnowledgeCandidate;
}

function hasUsefulWorkKnowledge(candidate: LlmWorkKnowledgeCandidate | null) {
  if (!candidate) {
    return false;
  }
  return Boolean(candidate.titleLatin || candidate.catalogue || candidate.summary || candidate.aliases?.length);
}

function buildEntityKnowledgeMessages(input: EntityKnowledgePromptInput, attempt: "primary" | "repair") {
  const jsonTemplate = {
    summary: "short Chinese summary under 80 Chinese characters, or empty string",
    country: "English country name such as Austria, or empty string",
    birthYear: "number or null",
    deathYear: "number or null",
    displayName: "preferred short Chinese display name, e.g. 贝多芬 / 布鲁克纳 / 老克莱伯",
    displayFullName: "full Chinese name, not just surname",
    displayLatinName: "full Latin / English / original-language name",
    aliases: ["common Chinese aliases, old translations, nicknames, max 4"],
    abbreviations: ["abbreviations for orchestras / ensembles, max 4"],
    confidence: "decimal between 0 and 1",
    rationale: "one short Chinese sentence explaining certainty and uncertainty",
  };
  const task = {
    entityType: input.entityType,
    title: input.title,
    roles: input.roles,
    known: {
      displayName: input.knownDisplayName || "",
      displayFullName: input.knownDisplayFullName || "",
      displayLatinName: input.knownDisplayLatinName || "",
      aliases: input.knownAliases || [],
      abbreviations: input.knownAbbreviations || [],
    },
    schema: jsonTemplate,
  };

  const system =
    attempt === "primary"
      ? "You normalize classical-music catalog entities. Return exactly one JSON object and nothing else. No markdown. No explanations. Keys allowed only: summary,country,birthYear,deathYear,displayName,displayFullName,displayLatinName,aliases,abbreviations,confidence,rationale. displayName must be a common short Chinese name. displayFullName must be the full Chinese name. displayLatinName must be the full Latin or original-language name. If uncertain, leave fields empty and lower confidence."
      : "Your previous answer was unusable. Return exactly one JSON object with only these keys: summary,country,birthYear,deathYear,displayName,displayFullName,displayLatinName,aliases,abbreviations,confidence,rationale. No markdown. No explanations. If a short Chinese name is known, infer the full Chinese name. Never repeat the English name in displayFullName.";

  const user =
    attempt === "primary"
      ? `Normalize this entity and return exactly one JSON object.\n${JSON.stringify(task, null, 2)}`
      : `Repair the previous output and strictly follow the JSON schema. Focus on Chinese short name, Chinese full name, and Latin/original full name.\n${JSON.stringify(task, null, 2)}`;

  return [
    { role: "system", content: system },
    { role: "user", content: user },
  ];
}

function buildWorkKnowledgeMessages(input: WorkKnowledgePromptInput, attempt: "primary" | "repair") {
  const jsonTemplate = {
    titleLatin: "canonical Latin-script or English work title, or empty string",
    catalogue: "catalogue / opus string such as Op. 64, BWV 1007, or empty string",
    summary: "short Chinese summary under 80 Chinese characters, or empty string",
    aliases: ["common alternative Chinese titles or catalog forms, max 4"],
    confidence: "decimal between 0 and 1",
    rationale: "one short Chinese sentence explaining certainty and uncertainty",
  };
  const task = {
    title: input.title,
    composerName: input.composerName,
    composerLatinName: input.composerLatinName || "",
    groupPath: input.groupPath || [],
    known: {
      titleLatin: input.knownTitleLatin || "",
      catalogue: input.knownCatalogue || "",
      summary: input.knownSummary || "",
      aliases: input.knownAliases || [],
    },
    schema: jsonTemplate,
  };

  const system =
    attempt === "primary"
      ? "You normalize classical music works. Return exactly one JSON object and nothing else. No markdown. No explanations. Keys allowed only: titleLatin,catalogue,summary,aliases,confidence,rationale. If uncertain, leave the field empty instead of guessing. titleLatin must be a commonly used Latin-script or English work title."
      : "Your previous answer was unusable. Return exactly one JSON object with only these keys: titleLatin,catalogue,summary,aliases,confidence,rationale. No markdown. No explanations. If you are unsure, leave the field empty.";

  const user =
    attempt === "primary"
      ? `Normalize this classical music work and return exactly one JSON object.\n${JSON.stringify(task, null, 2)}`
      : `Repair the previous output and strictly follow the JSON schema.\n${JSON.stringify(task, null, 2)}`;

  return [
    { role: "system", content: system },
    { role: "user", content: user },
  ];
}

function coerceProposalReview(parsed: Record<string, unknown>) {
  const verdict =
    parsed.verdict === "reject"
      ? "reject"
      : parsed.verdict === "needs-attention"
        ? "needs-attention"
        : parsed.status === "needs-attention"
          ? "needs-attention"
          : "accept";
  const reasons = normalizeStringList(parsed.reasons ?? parsed.issues).slice(0, 5);
  const rejectBecause = pickString(parsed.rejectBecause, parsed.blockReason);
  const normalizedValue =
    parsed.normalizedValue && typeof parsed.normalizedValue === "object" && !Array.isArray(parsed.normalizedValue)
      ? (parsed.normalizedValue as Record<string, unknown>)
      : undefined;
  const issues =
    verdict === "reject"
      ? normalizeStringList([rejectBecause, ...reasons]).slice(0, 5)
      : verdict === "needs-attention"
        ? reasons
        : [];

  return {
    verdict,
    status: verdict === "accept" ? "ok" : "needs-attention",
    issues,
    reasons,
    confidence: Number.isFinite(Number(parsed.confidence)) ? Number(parsed.confidence) : undefined,
    rationale: pickString(parsed.rationale, parsed.reason),
    rejectBecause,
    normalizedValue,
  } satisfies LlmProposalReview;
}

function buildProposalReviewMessages(input: ProposalReviewPromptInput) {
  const task = {
    entityType: input.entityType,
    title: input.title,
    roles: input.roles || [],
    current: input.current,
    preview: input.preview,
    fields: input.fields,
    sources: input.sources,
    evidence: input.evidence || [],
    schema: {
      status: "ok or needs-attention",
      issues: ["short Chinese review issues, max 4"],
      confidence: "decimal between 0 and 1",
      rationale: "one short English or Chinese sentence",
    },
  };

  return [
    {
      role: "system",
      content:
        "You review classical music catalog patches. Decide whether the proposal is grounded, internally consistent, and appropriate for the entity role. Return exactly one JSON object with only: verdict,status,issues,reasons,confidence,rationale,rejectBecause,normalizedValue. verdict must be one of accept, needs-attention, reject. issues and reasons must be short Chinese phrases. normalizedValue must be an object containing only field names and normalized candidate values when a safer normalized suggestion exists. If the proposal is acceptable, return verdict accept, status ok, and empty issues.",
    },
    {
      role: "user",
      content: `Review this proposal and return exactly one JSON object.\n${JSON.stringify(task, null, 2)}`,
    },
  ];
}

async function requestEntityKnowledgeCandidate(
  config: LlmConfig,
  input: EntityKnowledgePromptInput,
  fetchImpl: typeof fetch,
  attempt: "primary" | "repair",
) {
  const abortable = createAbortController(getEffectiveTimeoutMs(config, "entity-knowledge"));
  const requestModel = getEffectiveModel(config, "entity-knowledge");

  try {
    const payload = {
      model: requestModel,
      messages: buildEntityKnowledgeMessages(input, attempt),
      temperature: 0,
      max_tokens: 480,
      ...(attempt === "primary" && !looksReasoningModel(requestModel) ? { response_format: { type: "json_object" as const } } : {}),
    };
    const response = await fetchImpl(buildChatUrl(config), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify(payload),
      signal: abortable.controller.signal,
    });

    const rawPayload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return null;
    }

    const finalContent = String(rawPayload?.choices?.[0]?.message?.content ?? "").trim();
    const reasoningContent = String(rawPayload?.choices?.[0]?.message?.reasoning_content ?? "").trim();
    const content = finalContent || reasoningContent;
    const parsed = parseJsonFromText<Record<string, unknown>>(content);
    if (!parsed) {
      return null;
    }
    const normalized = coerceEntityKnowledgeCandidate(parsed);
    return hasUsefulEntityKnowledge(normalized, input) ? normalized : null;
  } catch {
    return null;
  } finally {
    abortable.clear();
  }
}

async function requestProposalReview(
  config: LlmConfig,
  input: ProposalReviewPromptInput,
  fetchImpl: typeof fetch,
) {
  const abortable = createAbortController(getEffectiveTimeoutMs(config, "proposal-review"));
  const requestModel = getEffectiveModel(config, "proposal-review");

  try {
    const response = await fetchImpl(buildChatUrl(config), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({
        model: requestModel,
        messages: buildProposalReviewMessages(input),
        temperature: 0,
        max_tokens: 320,
        ...(!looksReasoningModel(requestModel) ? { response_format: { type: "json_object" as const } } : {}),
      }),
      signal: abortable.controller.signal,
    });

    const rawPayload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return null;
    }

    const finalContent = String(rawPayload?.choices?.[0]?.message?.content ?? "").trim();
    const reasoningContent = String(rawPayload?.choices?.[0]?.message?.reasoning_content ?? "").trim();
    const content = finalContent || reasoningContent;
    const parsed = parseJsonFromText<Record<string, unknown>>(content);
    if (!parsed) {
      return null;
    }

    return coerceProposalReview(parsed);
  } catch {
    return null;
  } finally {
    abortable.clear();
  }
}

async function requestWorkKnowledgeCandidate(
  config: LlmConfig,
  input: WorkKnowledgePromptInput,
  fetchImpl: typeof fetch,
  attempt: "primary" | "repair",
) {
  const abortable = createAbortController(getEffectiveTimeoutMs(config, "entity-knowledge"));
  const requestModel = getEffectiveModel(config, "entity-knowledge");

  try {
    const payload = {
      model: requestModel,
      messages: buildWorkKnowledgeMessages(input, attempt),
      temperature: 0,
      max_tokens: 420,
      ...(attempt === "primary" && !looksReasoningModel(requestModel) ? { response_format: { type: "json_object" as const } } : {}),
    };
    const response = await fetchImpl(buildChatUrl(config), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify(payload),
      signal: abortable.controller.signal,
    });

    const rawPayload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return null;
    }

    const finalContent = String(rawPayload?.choices?.[0]?.message?.content ?? "").trim();
    const reasoningContent = String(rawPayload?.choices?.[0]?.message?.reasoning_content ?? "").trim();
    const content = finalContent || reasoningContent;
    const parsed = parseJsonFromText<Record<string, unknown>>(content);
    if (!parsed) {
      return null;
    }
    const normalized = coerceWorkKnowledgeCandidate(parsed);
    return hasUsefulWorkKnowledge(normalized) ? normalized : null;
  } catch {
    return null;
  } finally {
    abortable.clear();
  }
}

export async function testOpenAiCompatibleConfig(config: LlmConfig, fetchImpl: typeof fetch = fetch): Promise<LlmTestResult> {
  if (!config.baseUrl || !config.model || !config.apiKey) {
    return {
      ok: false,
      message: "请先填写 base URL、API key 和 model。",
    };
  }

  const abortable = createAbortController(getEffectiveTimeoutMs(config, "connectivity"));

  try {
    const response = await fetchImpl(buildChatUrl(config), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({
        model: config.model,
        messages: [
          {
            role: "system",
            content: "You are a concise assistant.",
          },
          {
            role: "user",
            content: "Reply with OK only.",
          },
        ],
        temperature: 0,
      }),
      signal: abortable.controller.signal,
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return {
        ok: false,
        message: payload?.error?.message || `请求失败：HTTP ${response.status}`,
      };
    }

    const output = String(payload?.choices?.[0]?.message?.content ?? "").trim();
    return {
      ok: true,
      message: "连接测试成功。",
      model: payload?.model || config.model,
      output,
    };
  } catch (error) {
    return {
      ok: false,
      message: error instanceof Error ? error.message : String(error),
    };
  } finally {
    abortable.clear();
  }
}

export async function generateConciseChineseSummary(options: {
  config: LlmConfig;
  title: string;
  sourceText: string;
  fetchImpl?: typeof fetch;
}) {
  const { config, title, sourceText } = options;
  if (!isLlmConfigured(config) || !sourceText.trim()) {
    return "";
  }

  const abortable = createAbortController(getEffectiveTimeoutMs(config, "summary"));
  const requestModel = getEffectiveModel(config, "summary");

  try {
    const response = await (options.fetchImpl ?? fetch)(buildChatUrl(config), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({
        model: requestModel,
        messages: [
          {
            role: "system",
            content: "你是一名严谨的古典音乐资料整理助手。请把给定资料压缩为不超过 80 字的中文简介，不要编造事实，不要加入资料中没有出现的信息。",
          },
          {
            role: "user",
            content: `条目：${title}\n资料：${sourceText}`,
          },
        ],
        temperature: 0.1,
      }),
      signal: abortable.controller.signal,
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return "";
    }

    return String(payload?.choices?.[0]?.message?.content ?? "").trim();
  } catch {
    return "";
  } finally {
    abortable.clear();
  }
}

export async function generateEntityKnowledgeCandidate(options: {
  config: LlmConfig;
  title: string;
  entityType: "composer" | "person";
  roles?: string[];
  knownDisplayName?: string;
  knownDisplayFullName?: string;
  knownDisplayLatinName?: string;
  knownAliases?: string[];
  knownAbbreviations?: string[];
  fetchImpl?: typeof fetch;
}) {
  const {
    config,
    title,
    entityType,
    roles = [],
    knownDisplayName = "",
    knownDisplayFullName = "",
    knownDisplayLatinName = "",
    knownAliases = [],
    knownAbbreviations = [],
  } = options;
  if (!isLlmConfigured(config) || !title.trim()) {
    return null;
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const promptInput: EntityKnowledgePromptInput = {
    title,
    entityType,
    roles,
    knownDisplayName,
    knownDisplayFullName,
    knownDisplayLatinName,
    knownAliases,
    knownAbbreviations,
  };

  const primary = await requestEntityKnowledgeCandidate(config, promptInput, fetchImpl, "primary");
  if (hasUsefulEntityKnowledge(primary, promptInput)) {
    return primary;
  }

  return requestEntityKnowledgeCandidate(config, promptInput, fetchImpl, "repair");
}

export async function generateWorkKnowledgeCandidate(options: {
  config: LlmConfig;
  title: string;
  composerName: string;
  composerLatinName?: string;
  groupPath?: string[];
  knownTitleLatin?: string;
  knownCatalogue?: string;
  knownSummary?: string;
  knownAliases?: string[];
  fetchImpl?: typeof fetch;
}) {
  const {
    config,
    title,
    composerName,
    composerLatinName = "",
    groupPath = [],
    knownTitleLatin = "",
    knownCatalogue = "",
    knownSummary = "",
    knownAliases = [],
  } = options;
  if (!isLlmConfigured(config) || !title.trim() || !composerName.trim()) {
    return null;
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const promptInput: WorkKnowledgePromptInput = {
    title,
    composerName,
    composerLatinName,
    groupPath,
    knownTitleLatin,
    knownCatalogue,
    knownSummary,
    knownAliases,
  };

  const primary = await requestWorkKnowledgeCandidate(config, promptInput, fetchImpl, "primary");
  if (hasUsefulWorkKnowledge(primary)) {
    return primary;
  }

  return requestWorkKnowledgeCandidate(config, promptInput, fetchImpl, "repair");
}

export async function reviewAutomationProposalWithLlm(options: {
  config: LlmConfig;
  entityType: "composer" | "person" | "work";
  title: string;
  roles?: string[];
  current: Record<string, unknown>;
  preview: Record<string, unknown>;
  fields: Array<{ path: string; before: unknown; after: unknown }>;
  sources: string[];
  evidence?: Array<{ field?: string; sourceLabel?: string; sourceUrl?: string; confidence?: number }>;
  fetchImpl?: typeof fetch;
}) {
  if (!isLlmConfigured(options.config) || !options.fields.length) {
    return null;
  }

  return requestProposalReview(
    options.config,
    {
      entityType: options.entityType,
      title: options.title,
      roles: options.roles || [],
      current: options.current,
      preview: options.preview,
      fields: options.fields,
      sources: options.sources,
      evidence: options.evidence,
    },
    options.fetchImpl ?? fetch,
  );
}
