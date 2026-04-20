export function createEmptyActiveEntity() {
  return { type: "", id: "" };
}

const compact = (value) => String(value ?? "").trim();
const normalizeWorkComparableText = (value) =>
  compact(value)
    .toLowerCase()
    .replace(/[，,:：;；()[\]{}]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
const escapeRegExp = (value) => String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const stripCatalogueFromWorkSegment = (segment, catalogue) => {
  const normalizedSegment = compact(segment);
  const normalizedCatalogue = compact(catalogue);
  if (!normalizedSegment || !normalizedCatalogue) {
    return normalizedSegment;
  }
  if (normalizeWorkComparableText(normalizedSegment) === normalizeWorkComparableText(normalizedCatalogue)) {
    return "";
  }
  const escapedCatalogue = escapeRegExp(normalizedCatalogue).replace(/\s+/g, "\\s+");
  const trailingPatterns = [
    new RegExp(`(?:\\s*[,，:：;；/|·-]\\s*|\\s+)\\(?${escapedCatalogue}\\)?$`, "i"),
    new RegExp(`\\(${escapedCatalogue}\\)$`, "i"),
  ];
  return trailingPatterns.reduce((currentValue, pattern) => currentValue.replace(pattern, "").trim(), normalizedSegment);
};
const buildWorkDisplayParts = (work, composers = []) => {
  if (!work) {
    return [];
  }
  const composer = composers.find((item) => compact(item?.id) === compact(work?.composerId));
  const parts = [
    compact(work?.title || work?.id || ""),
    stripCatalogueFromWorkSegment(work?.titleLatin || "", work?.catalogue || ""),
    compact(work?.catalogue || ""),
    compact(composer?.name || ""),
    compact(composer?.nameLatin || ""),
  ].filter(Boolean);
  return [...new Set(parts)];
};
const buildWorkInferenceText = (work = {}) =>
  [compact(work?.title || ""), stripCatalogueFromWorkSegment(work?.titleLatin || "", work?.catalogue || ""), compact(work?.catalogue || "")]
    .filter(Boolean)
    .join(" ");
const workPathInstrumentRules = [
  { pattern: /(钢琴|piano)/i, path: ["协奏曲", "钢琴协奏曲"] },
  { pattern: /(小提琴|violin)/i, path: ["协奏曲", "小提琴协奏曲"] },
  { pattern: /(大提琴|cello)/i, path: ["协奏曲", "大提琴协奏曲"] },
  { pattern: /(中提琴|viola)/i, path: ["协奏曲", "中提琴协奏曲"] },
  { pattern: /(长笛|flute)/i, path: ["协奏曲", "长笛协奏曲"] },
  { pattern: /(单簧管|clarinet)/i, path: ["协奏曲", "单簧管协奏曲"] },
  { pattern: /(双簧管|oboe)/i, path: ["协奏曲", "双簧管协奏曲"] },
  { pattern: /(大提琴|bassoon)/i, path: ["协奏曲", "巴松协奏曲"] },
  { pattern: /(小号|trumpet)/i, path: ["协奏曲", "小号协奏曲"] },
  { pattern: /(圆号|horn)/i, path: ["协奏曲", "圆号协奏曲"] },
];

export function buildInferredWorkGroupPath(work = {}) {
  const text = buildWorkInferenceText(work);
  if (!text) {
    return [];
  }

  if (/(协奏曲|concerto|concertante)/i.test(text)) {
    return workPathInstrumentRules.find((rule) => rule.pattern.test(text))?.path || ["协奏曲"];
  }

  if (/(歌剧|opera)/i.test(text)) {
    return ["歌剧与声乐", "歌剧"];
  }
  if (/(安魂曲|requiem)/i.test(text)) {
    return ["歌剧与声乐", "安魂曲"];
  }
  if (/(弥撒|mass)/i.test(text)) {
    return ["歌剧与声乐", "弥撒"];
  }
  if (/(清唱剧|神剧|oratorio|cantata)/i.test(text)) {
    return ["歌剧与声乐", "清唱剧"];
  }
  if (/(声乐|vocal|lied|song cycle)/i.test(text)) {
    return ["歌剧与声乐"];
  }

  if (/(交响曲|symphon)/i.test(text)) {
    return ["交响曲"];
  }
  if (/(交响诗|tone poem)/i.test(text)) {
    return ["管弦乐", "交响诗"];
  }
  if (/(序曲|overture)/i.test(text)) {
    return ["管弦乐", "序曲"];
  }
  if (/(组曲|suite)/i.test(text)) {
    return ["管弦乐", "组曲"];
  }
  if (/(舞剧|芭蕾|ballet)/i.test(text)) {
    return ["管弦乐", "舞剧与芭蕾"];
  }
  if (/(交响|管弦|orchestral)/i.test(text)) {
    return ["管弦乐"];
  }

  if (/(奏鸣曲|sonata)/i.test(text)) {
    return ["室内乐与独奏", "奏鸣曲"];
  }
  if (/(四重奏|quartet)/i.test(text)) {
    return ["室内乐与独奏", "四重奏"];
  }
  if (/(五重奏|quintet)/i.test(text)) {
    return ["室内乐与独奏", "五重奏"];
  }
  if (/(三重奏|trio)/i.test(text)) {
    return ["室内乐与独奏", "三重奏"];
  }
  if (/(二重奏|duo)/i.test(text)) {
    return ["室内乐与独奏", "二重奏"];
  }
  if (/(独奏|solo|partita)/i.test(text)) {
    return ["室内乐与独奏", "独奏"];
  }
  if (/(室内乐|chamber)/i.test(text)) {
    return ["室内乐与独奏"];
  }

  return [];
}
const personRoleLabels = {
  composer: "作曲家",
  conductor: "指挥",
  orchestra: "乐团",
  soloist: "独奏",
  singer: "歌手",
  ensemble: "组合",
  chorus: "合唱",
  instrumentalist: "器乐",
};
const escapeHtml = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

const buildBatchRelationLabel = (item) => {
  const primary = compact(item?.name || item?.title || item?.id || "");
  const secondary = compact(item?.nameLatin || "");
  if (!primary) {
    return "";
  }
  return secondary && secondary !== primary ? `${primary} / ${secondary}` : primary;
};

export function buildComposerOptionLabel(item) {
  return buildBatchRelationLabel(item);
}

export function getProposalModeAttributes(options = {}) {
  if (options.mode === "batch") {
    return {
      imageSelectAttr: "data-batch-image-select",
      actionAttr: "data-owner-proposal-action",
      proposalIdAttr: "data-owner-proposal-id",
      proposalTargetIdAttr: "data-owner-proposal-target-id",
      inputAttr: "data-batch-proposal-field-input",
      fieldPathAttr: "data-batch-proposal-field-path",
      imageUploadAttr: "data-batch-image-upload",
    };
  }
  if (options.inline) {
    return {
      imageSelectAttr: "data-inline-image-select",
      actionAttr: "data-owner-proposal-action",
      proposalIdAttr: "data-owner-proposal-id",
      proposalTargetIdAttr: "data-owner-proposal-target-id",
      inputAttr: "data-inline-proposal-field-input",
      fieldPathAttr: "data-inline-proposal-field-path",
      imageUploadAttr: "data-inline-image-upload",
    };
  }
  return {
    imageSelectAttr: "data-image-select",
    actionAttr: "data-owner-proposal-action",
    proposalIdAttr: "data-owner-proposal-id",
    proposalTargetIdAttr: "data-owner-proposal-target-id",
    inputAttr: "data-proposal-field-input",
    fieldPathAttr: "data-proposal-field-path",
    imageUploadAttr: "data-image-upload",
  };
}

/**
 * @param {{ dataset?: Record<string, string> } | null | undefined} [button]
 * @param {{ dataset?: Record<string, string> } | null | undefined} [card]
 */
export function resolveProposalActionContext(button = null, card = null) {
  const buttonDataset = button?.dataset || {};
  const cardDataset = card?.dataset || {};
  return {
    proposalId: compact(buttonDataset.ownerProposalTargetId || cardDataset.ownerProposalId || ""),
    action: compact(buttonDataset.ownerProposalAction || ""),
    mode: compact(cardDataset.ownerProposalMode || "review"),
    runId: compact(cardDataset.ownerRunId || ""),
  };
}

export function buildWorkOptionLabel(work, composers = []) {
  return buildWorkDisplayParts(work, composers).join(" / ");
}

export function buildBatchWorkOptionLabel(work) {
  if (!work) {
    return "";
  }
  return [
    compact(work?.title || work?.id || ""),
    stripCatalogueFromWorkSegment(work?.titleLatin || "", work?.catalogue || ""),
    compact(work?.catalogue || ""),
  ]
    .filter(Boolean)
    .join(" / ");
}

export function buildPreferredWorkLabel(work, composers = []) {
  return buildWorkDisplayParts(work, composers).join(" / ");
}

export function buildSearchResultBadges(item = {}) {
  const type = compact(item.type);
  const roles = Array.isArray(item.roles) ? item.roles.map((role) => compact(role)).filter(Boolean) : [];
  if (type === "composer") {
    const badges = ["人物", "作曲家"];
    roles.forEach((role) => {
      if (personRoleLabels[role] && !badges.includes(personRoleLabels[role])) {
        badges.push(personRoleLabels[role]);
      }
    });
    return badges;
  }
  if (type === "site") {
    return ["网站文本"];
  }
  if (type === "work") {
    return ["作品"];
  }
  if (type === "recording") {
    return ["版本"];
  }
  if (type === "person") {
    const badges = [];
    const isGroup = roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role));
    badges.push(isGroup ? "团体" : "人物");
    roles.forEach((role) => {
      if (personRoleLabels[role] && !badges.includes(personRoleLabels[role])) {
        badges.push(personRoleLabels[role]);
      }
    });
    return badges;
  }
  return [type || "条目"];
}

export function filterMergeTargetOptions(options = [], query = "") {
  const normalizedQuery = compact(query).toLowerCase();
  if (!normalizedQuery) {
    return [...options];
  }
  return options.filter((option) => compact(option?.label).toLowerCase().includes(normalizedQuery));
}

export function buildBatchPreviewShellHtml(listHtml, detailHtml) {
  return `
      <div class="owner-batch-preview-shell">
        <div class="owner-batch-preview-shell__list">${listHtml}</div>
        <div class="owner-batch-preview-shell__detail">${detailHtml}</div>
      </div>`;
}

function countBatchDraftEntities(draftEntities = {}) {
  return {
    composers: draftEntities.composers?.length || 0,
    people: draftEntities.people?.length || 0,
    works: draftEntities.works?.length || 0,
    recordings: draftEntities.recordings?.length || 0,
  };
}

export function buildBatchResultSummary(action, result = {}) {
  const session = result.session || {};
  return {
    action,
    sessionId: session.id || "",
    status: session.status || "",
    counts: countBatchDraftEntities(session.draftEntities),
    warnings: Array.isArray(session.warnings) ? session.warnings : [],
    runId: result.run?.id || session.runId || "",
  };
}

export function buildRecordingLinkChipLabel(link, index, links = []) {
  const platform = compact(link?.platform) || (compact(link?.linkType) === "local" ? "local" : "other");
  const comparablePlatform = platform.toLowerCase();
  const samePlatformLinks = links.filter((item) => compact(item?.platform).toLowerCase() === comparablePlatform);
  if (samePlatformLinks.length <= 1) {
    return compact(link?.linkType) === "local" ? `${platform} (local)` : platform;
  }
  const currentIndex =
    links.filter((item, itemIndex) => compact(item?.platform).toLowerCase() === comparablePlatform && itemIndex <= index).length || 1;
  const numberedPlatform = `${platform}${currentIndex}`;
  return compact(link?.linkType) === "local" ? `${numberedPlatform} (local)` : numberedPlatform;
}

export function buildRecordingLinkEditorHtml(links = [], emptyMessage = "暂无资源链接。") {
  if (!Array.isArray(links) || links.length === 0) {
    return `<p class="owner-empty">${escapeHtml(emptyMessage)}</p>`;
  }
  return links
    .map(
      (link, index) => `
        <button
          type="button"
          class="owner-link-chip"
          data-recording-link-index="${escapeHtml(index)}"
          title="${escapeHtml(link?.title || link?.url || link?.localPath || "")}"
        >${escapeHtml(buildRecordingLinkChipLabel(link, index, links))}</button>`,
    )
    .join("");
}

export function buildBatchRelationOptions(entryType, field, library = {}, draftEntities = {}, currentValue = "") {
  const sourceItems =
    entryType === "work" && field === "composerId"
      ? [...(draftEntities.composers || []).map((entry) => entry?.entity || entry), ...(library.composers || [])]
      : entryType === "recording" && field === "workId"
        ? [...(draftEntities.works || []).map((entry) => entry?.entity || entry), ...(library.works || [])]
        : [];
  const composers = [...(draftEntities.composers || []).map((entry) => entry?.entity || entry), ...(library.composers || [])];

  const options = [{ value: "", label: "请选择" }];
  const seenIds = new Set();

  sourceItems.forEach((item) => {
    const id = compact(item?.id);
    const label = buildBatchRelationLabel(item);
    if (!id || !label || seenIds.has(id)) {
      return;
    }
    seenIds.add(id);
    options.push({
      value: id,
      label: entryType === "recording" && field === "workId" ? buildWorkOptionLabel(item, composers) : label,
    });
  });

  const normalizedCurrentValue = compact(currentValue);
  if (normalizedCurrentValue && !seenIds.has(normalizedCurrentValue)) {
    options.push({
      value: normalizedCurrentValue,
      label: `当前关联（${normalizedCurrentValue}）`,
    });
  }

  return options;
}

export function selectBatchSessionAfterRefresh(sessions = [], currentSessionId = "", preferEmptyState = false) {
  if (preferEmptyState || !Array.isArray(sessions) || sessions.length === 0) {
    return null;
  }
  return (
    sessions.find((session) => session.id === currentSessionId) ||
    sessions.find((session) => session.status !== "applied" && session.status !== "abandoned") ||
    sessions[0] ||
    null
  );
}
