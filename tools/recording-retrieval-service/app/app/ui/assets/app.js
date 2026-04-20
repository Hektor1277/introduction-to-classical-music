const form = document.querySelector("#request-form");
const analyzeButton = document.querySelector("#analyze-button");
const refreshButton = document.querySelector("#refresh-button");
const searchButton = document.querySelector("#search-button");

const openHighQualityButton = document.querySelector("#open-high-quality");
const openStreamingButton = document.querySelector("#open-streaming");
const openLlmConfigButton = document.querySelector("#open-llm-config");
const openOrchestraAliasesButton = document.querySelector("#open-orchestra-aliases");
const openPersonAliasesButton = document.querySelector("#open-person-aliases");

const highQualityPath = document.querySelector("#high-quality-path");
const streamingPath = document.querySelector("#streaming-path");
const llmConfigPath = document.querySelector("#llm-config-path");
const orchestraAliasPath = document.querySelector("#orchestra-alias-path");
const personAliasPath = document.querySelector("#person-alias-path");

const statusBar = document.querySelector("#status-bar");
const variantTabs = document.querySelector("#variant-tabs");
const entryTitle = document.querySelector("#entry-title");
const entrySubtitle = document.querySelector("#entry-subtitle");
const entryBadge = document.querySelector("#entry-badge");
const linkList = document.querySelector("#link-list");
const warningList = document.querySelector("#warning-list");
const coverFrame = document.querySelector("#cover-frame");
const coverImage = document.querySelector("#cover-image");
const coverCaption = document.querySelector("#cover-caption");
const rawTextInput = form.elements.rawText;

const primaryPersonLabel = document.querySelector("#primary-person-label");
const primaryPersonLatinLabel = document.querySelector("#primary-person-latin-label");
const secondaryPersonField = document.querySelector("#secondary-person-field");
const secondaryPersonLatinField = document.querySelector("#secondary-person-latin-field");
const secondaryPersonLabel = document.querySelector("#secondary-person-label");
const secondaryPersonLatinLabel = document.querySelector("#secondary-person-latin-label");
const groupNameLabel = document.querySelector("#group-name-label");
const groupNameLatinLabel = document.querySelector("#group-name-latin-label");

const displayPrimaryPersonLabel = document.querySelector("#display-primary-person-label");
const displayPrimaryPersonLatinLabel = document.querySelector("#display-primary-person-latin-label");
const displaySecondaryPersonLabel = document.querySelector("#display-secondary-person-label");
const displaySecondaryPersonLatinLabel = document.querySelector("#display-secondary-person-latin-label");
const displayGroupNameLabel = document.querySelector("#display-group-name-label");
const displayGroupNameLatinLabel = document.querySelector("#display-group-name-latin-label");

const STATUS_TEXT = {
  idle: "状态：等待操作",
  analyzing: "状态：正在分析原始文本",
  refreshed: "状态：已刷新右侧条目预览",
  searching: "状态：正在执行版本搜索",
  finished: "状态：检索完成",
  openSuccess: "状态：已打开配置文档",
  errorPrefix: "状态：操作失败 - ",
};

const WORK_TYPE_PROFILES = {
  orchestral: {
    primaryLabel: "人物 / 指挥",
    primaryLatinLabel: "人物 / 指挥 Latin",
    secondaryLabel: "第二关键信息",
    secondaryLatinLabel: "第二关键信息 Latin",
    secondaryVisible: false,
    groupLabel: "团体 / 乐团",
    groupLatinLabel: "团体 / 乐团 Latin",
    rawHint: "建议格式：作曲家 | 作品 | 指挥 | 乐团 | 年份；缺失项写 -",
    primaryRole: "conductor",
    secondaryRole: "",
    groupRole: "orchestra",
  },
  concerto: {
    primaryLabel: "人物 / 独奏者",
    primaryLatinLabel: "人物 / 独奏者 Latin",
    secondaryLabel: "人物 / 指挥",
    secondaryLatinLabel: "人物 / 指挥 Latin",
    secondaryVisible: true,
    groupLabel: "团体 / 乐团",
    groupLatinLabel: "团体 / 乐团 Latin",
    rawHint: "建议格式：作曲家 | 作品 | 独奏者 | 指挥 | 乐团 | 年份；缺失项写 -",
    primaryRole: "soloist",
    secondaryRole: "conductor",
    groupRole: "orchestra",
  },
  opera_vocal: {
    primaryLabel: "人物 / 指挥",
    primaryLatinLabel: "人物 / 指挥 Latin",
    secondaryLabel: "人物 / 重要歌手",
    secondaryLatinLabel: "人物 / 重要歌手 Latin",
    secondaryVisible: true,
    groupLabel: "团体 / 剧团或乐团",
    groupLatinLabel: "团体 / 剧团或乐团 Latin",
    rawHint: "建议格式：作曲家 | 作品 | 指挥 | 重要歌手 | 剧团或乐团 | 年份；缺失项写 -",
    primaryRole: "conductor",
    secondaryRole: "singer",
    groupRole: "ensemble",
  },
  chamber_solo: {
    primaryLabel: "人物 / 主奏或组合",
    primaryLatinLabel: "人物 / 主奏或组合 Latin",
    secondaryLabel: "人物 / 协作人员",
    secondaryLatinLabel: "人物 / 协作人员 Latin",
    secondaryVisible: true,
    groupLabel: "团体 / 组合",
    groupLatinLabel: "团体 / 组合 Latin",
    rawHint: "建议格式：作曲家 | 作品 | 主奏或组合 | 协作人员 | 年份；缺失项写 -",
    primaryRole: "instrumentalist",
    secondaryRole: "instrumentalist",
    groupRole: "ensemble",
  },
  unknown: {
    primaryLabel: "人物 / 关键信息 1",
    primaryLatinLabel: "人物 / 关键信息 1 Latin",
    secondaryLabel: "人物 / 关键信息 2",
    secondaryLatinLabel: "人物 / 关键信息 2 Latin",
    secondaryVisible: true,
    groupLabel: "团体 / 关键信息",
    groupLatinLabel: "团体 / 关键信息 Latin",
    rawHint: "建议格式：作曲家 | 作品 | 人物 1 | 人物 2 | 团体 | 年份；缺失项写 -",
    primaryRole: "person",
    secondaryRole: "person",
    groupRole: "group",
  },
};

const state = {
  profileTargets: null,
  workspace: null,
  variants: [],
  activeVariantId: null,
};

function compact(value) {
  return String(value || "").trim();
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function applyWorkTypeProfile(workType) {
  const profile = WORK_TYPE_PROFILES[workType] || WORK_TYPE_PROFILES.unknown;
  primaryPersonLabel.textContent = profile.primaryLabel;
  primaryPersonLatinLabel.textContent = profile.primaryLatinLabel;
  secondaryPersonLabel.textContent = profile.secondaryLabel;
  secondaryPersonLatinLabel.textContent = profile.secondaryLatinLabel;
  groupNameLabel.textContent = profile.groupLabel;
  groupNameLatinLabel.textContent = profile.groupLatinLabel;
  displayPrimaryPersonLabel.textContent = profile.primaryLabel;
  displayPrimaryPersonLatinLabel.textContent = profile.primaryLatinLabel;
  displaySecondaryPersonLabel.textContent = profile.secondaryLabel;
  displaySecondaryPersonLatinLabel.textContent = profile.secondaryLatinLabel;
  displayGroupNameLabel.textContent = profile.groupLabel;
  displayGroupNameLatinLabel.textContent = profile.groupLatinLabel;
  secondaryPersonField.hidden = !profile.secondaryVisible;
  secondaryPersonLatinField.hidden = !profile.secondaryVisible;
  rawTextInput.placeholder = profile.rawHint;
  return profile;
}

function splitLinks(value) {
  return compact(value)
    .split(/\s+/)
    .map((item) => compact(item))
    .filter((item) => /^https?:\/\//i.test(item))
    .map((url) => ({ platform: "other", url, title: "" }));
}

function collectWorkspaceState() {
  const formData = new FormData(form);
  return {
    rawText: compact(formData.get("rawText")),
    title: compact(formData.get("title")),
    primaryPerson: compact(formData.get("primaryPerson")),
    primaryPersonLatin: compact(formData.get("primaryPersonLatin")),
    secondaryPerson: compact(formData.get("secondaryPerson")),
    secondaryPersonLatin: compact(formData.get("secondaryPersonLatin")),
    groupName: compact(formData.get("groupName")),
    groupNameLatin: compact(formData.get("groupNameLatin")),
    composerName: compact(formData.get("composerName")),
    composerNameLatin: compact(formData.get("composerNameLatin")),
    workTitle: compact(formData.get("workTitle")),
    workTitleLatin: compact(formData.get("workTitleLatin")),
    catalogue: compact(formData.get("catalogue")),
    performanceDateText: compact(formData.get("performanceDateText")),
    existingLinksText: compact(formData.get("existingLinks")),
    workTypeHint: compact(formData.get("workTypeHint")) || "unknown",
  };
}

function buildSourceLine(workspace) {
  return (
    workspace.rawText ||
    [
      workspace.composerName || workspace.composerNameLatin,
      workspace.workTitle || workspace.workTitleLatin,
      workspace.primaryPerson || workspace.primaryPersonLatin,
      workspace.secondaryPerson || workspace.secondaryPersonLatin,
      workspace.groupName || workspace.groupNameLatin,
      workspace.performanceDateText,
    ]
      .filter(Boolean)
      .join(" | ")
  );
}

function buildDraftEntry(workspace) {
  const title =
    compact(workspace.title) ||
    [
      workspace.primaryPerson || workspace.primaryPersonLatin,
      workspace.secondaryPerson || workspace.secondaryPersonLatin,
      workspace.groupName || workspace.groupNameLatin,
      workspace.workTitle || workspace.workTitleLatin,
      workspace.performanceDateText,
    ]
      .filter(Boolean)
      .join(" - ") ||
    "Untitled Recording";

  return {
    rawText: workspace.rawText,
    title,
    primaryPerson: workspace.primaryPerson,
    primaryPersonLatin: workspace.primaryPersonLatin,
    secondaryPerson: workspace.secondaryPerson,
    secondaryPersonLatin: workspace.secondaryPersonLatin,
    groupName: workspace.groupName,
    groupNameLatin: workspace.groupNameLatin,
    composerName: workspace.composerName,
    composerNameLatin: workspace.composerNameLatin,
    workTitle: workspace.workTitle,
    workTitleLatin: workspace.workTitleLatin,
    catalogue: workspace.catalogue,
    performanceDateText: workspace.performanceDateText,
    sourceLine: buildSourceLine(workspace),
    existingLinks: splitLinks(workspace.existingLinksText),
    workTypeHint: workspace.workTypeHint,
  };
}

function applyDraftToWorkspace(draft) {
  form.elements.title.value = compact(draft.title);
  form.elements.primaryPerson.value = compact(draft.primaryPerson);
  form.elements.primaryPersonLatin.value = compact(draft.primaryPersonLatin);
  form.elements.secondaryPerson.value = compact(draft.secondaryPerson);
  form.elements.secondaryPersonLatin.value = compact(draft.secondaryPersonLatin);
  form.elements.groupName.value = compact(draft.groupName);
  form.elements.groupNameLatin.value = compact(draft.groupNameLatin);
  form.elements.composerName.value = compact(draft.composerName);
  form.elements.composerNameLatin.value = compact(draft.composerNameLatin);
  form.elements.workTitle.value = compact(draft.workTitle);
  form.elements.workTitleLatin.value = compact(draft.workTitleLatin);
  form.elements.catalogue.value = compact(draft.catalogue);
  form.elements.performanceDateText.value = compact(draft.performanceDateText);
}

async function requestTextAnalysis(workspace) {
  const response = await fetch("/ui/analyze-text", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      rawText: workspace.rawText,
      workTypeHint: workspace.workTypeHint,
    }),
  });
  if (!response.ok) {
    throw new Error(`文本分析失败：HTTP ${response.status}`);
  }
  return response.json();
}

function buildCredits(draft) {
  const profile = WORK_TYPE_PROFILES[draft.workTypeHint] || WORK_TYPE_PROFILES.unknown;
  const credits = [];
  const pushCredit = (role, displayName, label) => {
    if (!compact(displayName) && !compact(label)) {
      return;
    }
    credits.push({
      role,
      displayName: compact(displayName) || compact(label),
      label: compact(label) || compact(displayName),
    });
  };

  pushCredit(profile.primaryRole, draft.primaryPerson, draft.primaryPersonLatin);
  if (profile.secondaryRole) {
    pushCredit(profile.secondaryRole, draft.secondaryPerson, draft.secondaryPersonLatin);
  }
  pushCredit(profile.groupRole, draft.groupName, draft.groupNameLatin);
  return credits;
}

function buildRequestPayload(draft) {
  const timestamp = Date.now();
  return {
    requestId: `standalone-${timestamp}`,
    source: {
      kind: "owner-entity-check",
      ownerRunId: `standalone-run-${timestamp}`,
      requestedBy: "owner-tool",
    },
    items: [
      {
        itemId: `standalone-item-${timestamp}`,
        recordingId: `standalone-item-${timestamp}`,
        workId: "standalone-work",
        composerId: "standalone-composer",
        workTypeHint: draft.workTypeHint,
        sourceLine: draft.sourceLine,
        seed: {
          title: draft.title,
          composerName: draft.composerName,
          composerNameLatin: draft.composerNameLatin,
          workTitle: draft.workTitle,
          workTitleLatin: draft.workTitleLatin,
          catalogue: draft.catalogue,
          performanceDateText: draft.performanceDateText,
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          credits: buildCredits(draft),
          links: draft.existingLinks,
          notes: draft.rawText,
        },
        requestedFields: [
          "links",
          "performanceDateText",
          "venueText",
          "albumTitle",
          "label",
          "releaseDate",
          "notes",
          "images",
        ],
      },
    ],
    options: {
      maxConcurrency: 1,
      timeoutMs: 45000,
      returnPartialResults: true,
    },
  };
}

function updateStatus(text) {
  statusBar.textContent = text;
}

function formatSubtitle(entry) {
  return [
    entry.primaryPerson || entry.primaryPersonLatin,
    entry.secondaryPerson || entry.secondaryPersonLatin,
    entry.groupName || entry.groupNameLatin,
    entry.composerName || entry.composerNameLatin,
    entry.workTitle || entry.workTitleLatin,
    entry.performanceDateText,
  ]
    .filter(Boolean)
    .join(" | ");
}

function setFieldValue(field, value) {
  const node = document.querySelector(`#field-${field}`);
  if (node) {
    node.textContent = compact(value) || "—";
  }
}

function renderCover(variant) {
  const image = variant.cover || null;
  if (!image?.src) {
    coverFrame.classList.add("empty");
    coverImage.hidden = true;
    coverImage.removeAttribute("src");
    coverCaption.textContent = "";
    return;
  }
  coverFrame.classList.remove("empty");
  coverImage.hidden = false;
  coverImage.src = image.src;
  coverImage.alt = compact(image.title) || "版本封面";
  coverCaption.textContent = [image.title, image.attribution].filter(Boolean).join(" | ");
}

function buildPreviewVariant(workspace) {
  const draft = buildDraftEntry(workspace);
  return {
    id: "preview",
    tabLabel: "当前条目",
    badge: "preview",
    title: draft.title,
    subtitle: formatSubtitle(draft),
    ...draft,
    venueText: "",
    albumTitle: "",
    label: "",
    releaseDate: "",
    notes: draft.rawText,
    links: draft.existingLinks,
    warnings: ["尚未开始搜索。"],
    cover: null,
  };
}

function pickCover(itemResult) {
  return itemResult?.result?.images?.[0] || itemResult?.imageCandidates?.[0] || null;
}

function buildPrimaryVariant(workspace, itemResult) {
  const draft = buildDraftEntry(workspace);
  const result = itemResult.result || {};
  const firstResultLink = result.links?.[0] || null;
  return {
    id: "primary",
    tabLabel: "整合结果",
    badge: itemResult.status,
    title: draft.title,
    subtitle: formatSubtitle({
      ...draft,
      performanceDateText: result.performanceDateText || draft.performanceDateText,
    }),
    ...draft,
    performanceDateText: result.performanceDateText || draft.performanceDateText,
    venueText: result.venueText || "",
    albumTitle: result.albumTitle || firstResultLink?.title || "",
    label: result.label || firstResultLink?.sourceLabel || "",
    releaseDate: result.releaseDate || "",
    notes: result.notes || draft.rawText || "",
    links: result.links || [],
    warnings: itemResult.warnings || [],
    cover: pickCover(itemResult),
  };
}

function deriveVariantLabel(candidate, draft, index) {
  const title = compact(candidate.title).toLowerCase();
  const hints = [
    draft.groupName,
    draft.groupNameLatin,
    draft.secondaryPerson,
    draft.secondaryPersonLatin,
    draft.primaryPerson,
    draft.primaryPersonLatin,
  ].filter(Boolean);
  for (const hint of hints) {
    if (title.includes(hint.toLowerCase())) {
      return hint;
    }
  }
  const parts = compact(candidate.title)
    .split(/[-|/]/)
    .map((item) => compact(item))
    .filter(Boolean);
  return parts[1] || parts[0] || candidate.sourceLabel || candidate.platform || `候选 ${index + 1}`;
}

function buildCandidateVariants(workspace, itemResult) {
  const draft = buildDraftEntry(workspace);
  const candidates = itemResult.linkCandidates || [];
  const images = itemResult.imageCandidates || [];
  return candidates.map((candidate, index) => ({
    id: `candidate-${index}`,
    tabLabel: deriveVariantLabel(candidate, draft, index),
    badge: "候选",
    title: candidate.title || draft.title,
    subtitle: [
      candidate.sourceLabel,
      candidate.platform,
      candidate.confidence != null ? `confidence=${candidate.confidence}` : "",
    ]
      .filter(Boolean)
      .join(" | "),
    ...draft,
    venueText: "",
    albumTitle: candidate.title || "",
    label: candidate.sourceLabel || "",
    releaseDate: "",
    notes: candidate.url || "",
    links: [candidate],
    warnings: ["这是单条候选版本，尚未被最终采纳。"],
    cover: images.find((image) => compact(image.sourceUrl) === compact(candidate.url)) || images[0] || null,
  }));
}

function renderVariant(variant) {
  const profile = applyWorkTypeProfile(variant.workTypeHint || collectWorkspaceState().workTypeHint);
  entryTitle.textContent = compact(variant.title) || "尚未生成条目";
  entrySubtitle.textContent = compact(variant.subtitle) || "暂无附加信息。";
  entryBadge.textContent = compact(variant.badge) || "draft";

  setFieldValue("composerName", variant.composerName);
  setFieldValue("composerNameLatin", variant.composerNameLatin);
  setFieldValue("workTitle", variant.workTitle);
  setFieldValue("workTitleLatin", variant.workTitleLatin);
  setFieldValue("primaryPerson", variant.primaryPerson);
  setFieldValue("primaryPersonLatin", variant.primaryPersonLatin);
  setFieldValue("secondaryPerson", profile.secondaryVisible ? variant.secondaryPerson : "");
  setFieldValue("secondaryPersonLatin", profile.secondaryVisible ? variant.secondaryPersonLatin : "");
  setFieldValue("groupName", variant.groupName);
  setFieldValue("groupNameLatin", variant.groupNameLatin);
  setFieldValue("workTypeHint", variant.workTypeHint);
  setFieldValue("performanceDateText", variant.performanceDateText);
  setFieldValue("venueText", variant.venueText);
  setFieldValue("albumTitle", variant.albumTitle);
  setFieldValue("label", variant.label);
  setFieldValue("releaseDate", variant.releaseDate);
  setFieldValue("notes", variant.notes);
  renderCover(variant);

  const links = variant.links || [];
  if (!links.length) {
    linkList.className = "chip-list empty-list";
    linkList.innerHTML = "<li>暂无链接</li>";
  } else {
    linkList.className = "chip-list";
    linkList.innerHTML = links
      .map((link) => {
        const title = escapeHtml(compact(link.title) || compact(link.url));
        const meta = [compact(link.sourceLabel), compact(link.platform)].filter(Boolean).join(" | ");
        return `<li><span>${title}</span><a href="${escapeHtml(link.url)}" target="_blank" rel="noreferrer">${escapeHtml(link.url)}</a><small>${escapeHtml(meta)}</small></li>`;
      })
      .join("");
  }

  const warnings = variant.warnings || [];
  if (!warnings.length) {
    warningList.className = "plain-list empty-list";
    warningList.innerHTML = "<li>暂无提示</li>";
  } else {
    warningList.className = "plain-list";
    warningList.innerHTML = warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join("");
  }
}

function renderVariantTabs() {
  variantTabs.innerHTML = "";
  for (const variant of state.variants) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `variant-tab${variant.id === state.activeVariantId ? " active" : ""}`;
    button.textContent = variant.tabLabel;
    button.addEventListener("click", () => activateVariant(variant.id));
    variantTabs.appendChild(button);
  }
}

function activateVariant(variantId) {
  state.activeVariantId = variantId;
  renderVariantTabs();
  const variant = state.variants.find((item) => item.id === variantId) || state.variants[0];
  if (variant) {
    renderVariant(variant);
  }
}

function setVariants(variants) {
  state.variants = variants;
  state.activeVariantId = variants[0]?.id || null;
  renderVariantTabs();
  if (state.activeVariantId) {
    activateVariant(state.activeVariantId);
  }
}

function syncWorkspaceAndPreview() {
  const workspace = collectWorkspaceState();
  state.workspace = workspace;
  setVariants([buildPreviewVariant(workspace)]);
}

async function openProfile(group) {
  const response = await fetch(`/ui/open-profile/${group}`, { method: "POST" });
  if (!response.ok) {
    throw new Error(`打开配置文档失败：HTTP ${response.status}`);
  }
  updateStatus(STATUS_TEXT.openSuccess);
}

async function loadProfileTargets() {
  const response = await fetch("/ui/profile-targets");
  if (!response.ok) {
    throw new Error(`加载配置文档失败：HTTP ${response.status}`);
  }
  state.profileTargets = await response.json();
  highQualityPath.textContent = `高质量来源文档：${state.profileTargets.highQuality.path}`;
  streamingPath.textContent = `资源平台文档：${state.profileTargets.streaming.path}`;
  llmConfigPath.textContent = `LLM 配置：${state.profileTargets.llmConfig.path}`;
  orchestraAliasPath.textContent = `乐团缩写文档：${state.profileTargets.orchestraAliases.path}`;
  personAliasPath.textContent = `人物映射文档：${state.profileTargets.personAliases.path}`;
}

async function runSearch() {
  const workspace = collectWorkspaceState();
  const draft = buildDraftEntry(workspace);
  updateStatus(STATUS_TEXT.searching);
  const createResponse = await fetch("/v1/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(buildRequestPayload(draft)),
  });
  if (!createResponse.ok) {
    throw new Error(`创建任务失败：HTTP ${createResponse.status}`);
  }
  const accepted = await createResponse.json();
  const resultResponse = await waitForResults(accepted.jobId);
  const itemResult = resultResponse.items?.[0];
  const variants = itemResult
    ? [buildPrimaryVariant(workspace, itemResult), ...buildCandidateVariants(workspace, itemResult)]
    : [buildPreviewVariant(workspace)];
  state.workspace = workspace;
  setVariants(variants);
  updateStatus(STATUS_TEXT.finished);
}

async function waitForResults(jobId) {
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    const statusResponse = await fetch(`/v1/jobs/${jobId}`);
    if (!statusResponse.ok) {
      throw new Error(`读取任务状态失败：HTTP ${statusResponse.status}`);
    }
    const statusPayload = await statusResponse.json();
    if (["succeeded", "partial", "failed", "canceled", "timed_out"].includes(statusPayload.status)) {
      const resultsResponse = await fetch(`/v1/jobs/${jobId}/results`);
      if (!resultsResponse.ok) {
        throw new Error(`读取任务结果失败：HTTP ${resultsResponse.status}`);
      }
      return resultsResponse.json();
    }
    await new Promise((resolve) => window.setTimeout(resolve, 800));
  }
  throw new Error("等待搜索结果超时");
}

analyzeButton.addEventListener("click", async () => {
  try {
    const workspace = collectWorkspaceState();
    updateStatus(STATUS_TEXT.analyzing);
    const result = await requestTextAnalysis(workspace);
    applyDraftToWorkspace({
      ...workspace,
      ...result,
    });
    syncWorkspaceAndPreview();
    updateStatus(STATUS_TEXT.refreshed);
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

refreshButton.addEventListener("click", () => {
  syncWorkspaceAndPreview();
  updateStatus(STATUS_TEXT.refreshed);
});

searchButton.addEventListener("click", async () => {
  try {
    await runSearch();
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

openHighQualityButton.addEventListener("click", async () => {
  try {
    await openProfile("high-quality");
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

openStreamingButton.addEventListener("click", async () => {
  try {
    await openProfile("streaming");
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

openLlmConfigButton.addEventListener("click", async () => {
  try {
    await openProfile("llm-config");
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

openOrchestraAliasesButton.addEventListener("click", async () => {
  try {
    await openProfile("orchestra-aliases");
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

openPersonAliasesButton.addEventListener("click", async () => {
  try {
    await openProfile("person-aliases");
  } catch (error) {
    updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
  }
});

form.elements.workTypeHint.addEventListener("change", (event) => {
  applyWorkTypeProfile(event.target.value || "unknown");
  syncWorkspaceAndPreview();
});

for (const element of Array.from(form.elements)) {
  if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement || element instanceof HTMLSelectElement) {
    element.addEventListener("input", () => {
      state.workspace = collectWorkspaceState();
    });
  }
}

applyWorkTypeProfile(form.elements.workTypeHint.value || "orchestral");
syncWorkspaceAndPreview();
loadProfileTargets().catch((error) => {
  updateStatus(`${STATUS_TEXT.errorPrefix}${error.message}`);
});
