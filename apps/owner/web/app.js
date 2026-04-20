import {
  buildBatchPreviewShellHtml,
  buildBatchRelationOptions,
  buildBatchResultSummary,
  buildInferredWorkGroupPath,
  buildBatchWorkOptionLabel,
  buildComposerOptionLabel,
  buildPreferredWorkLabel,
  buildSearchResultBadges,
  buildRecordingLinkChipLabel,
  buildRecordingLinkEditorHtml,
  createEmptyActiveEntity,
  filterMergeTargetOptions,
  getProposalModeAttributes,
  resolveProposalActionContext,
  selectBatchSessionAfterRefresh,
} from "./ui-helpers.js";
import {
  REVIEW_PAGE_SIZE,
  applyProposalDraft,
  buildBlockedReviewActionMessage,
  buildDataAttributeSelector,
  buildExcerpt,
  filterPendingProposalsForDisplay,
  getBlockedProposalsForReviewAction,
  getProposalApplyBlockers,
  getProposalsForReviewAction,
  hasProposalDraftChanges,
  isProposalDirectlyApplicable,
  paginateItems,
  resolveProposalDraft,
} from "./review-utils.js";

const state = {
  library: null,
  articles: [],
  site: null,
  runs: [],
  dataIssues: [],
  llmConfig: null,
  llmHasStoredKey: false,
  libraryMeta: null,
  activeRunId: "",
  activeLogRunId: "",
  activeJobId: "",
  activeJobItemId: "",
  activeJob: null,
  activeEntity: { type: "site", id: "site" },
  activeSelectionPreview: { total: 0, groups: [] },
  selectionState: {},
  jobPollTimer: null,
  inlineCheck: {
    entityType: "",
    entityId: "",
    jobId: "",
    runId: "",
  },
  batchSession: null,
  batchDraftEntities: null,
  batchSelectedEntry: { type: "recording", index: 0 },
  batchPreferEmptyState: false,
  articleTab: "create",
  activeArticleId: "",
  selectionFilters: {},
  proposalDrafts: {},
  proposalLinkCandidateDismissals: {},
  reviewPage: 1,
  recordingLinkDialogContext: { form: null, index: -1 },
  infoPanelTextDialogContext: null,
  confirmDialogContext: null,
};

const desktopLauncher = window.desktopLauncher || null;

const resultPanel = document.querySelector("#result-panel");
const statsPanel = document.querySelector("#owner-stats");
const issuesPanel = document.querySelector("#owner-issues");
const issueCount = document.querySelector("#owner-issue-count");
const libraryStatus = document.querySelector("#owner-library-status");
const rebuildButton = document.querySelector("#rebuild-button");
const libraryOpenButton = document.querySelector("#library-open-button");
const libraryImportButton = document.querySelector("#library-import-button");
const libraryExportButton = document.querySelector("#library-export-button");
const refreshButton = document.querySelector("#refresh-button");
const siteForm = document.querySelector("[data-site-form]");
const entityForms = [...document.querySelectorAll("[data-entity-form]")];
const viewTabs = [...document.querySelectorAll("[data-view-tab]")];
const viewPanels = [...document.querySelectorAll("[data-view-panel]")];
const detailTabs = [...document.querySelectorAll("[data-detail-tab]")];
const entityCheckButtons = [...document.querySelectorAll("[data-entity-check]")];
const searchInput = document.querySelector("#owner-search-input");
const searchType = document.querySelector("#owner-search-type");
const searchButton = document.querySelector("#owner-search-button");
const searchResults = document.querySelector("#owner-search-results");
const searchCount = document.querySelector("#owner-search-count");
const batchFileInput = document.querySelector("#batch-file-input");
const batchFileStatus = document.querySelector("#batch-file-status");
const batchProgressStatus = document.querySelector("#batch-progress-status");
const batchComposerSelect = document.querySelector("#batch-composer-select");
const batchWorkSelect = document.querySelector("#batch-work-select");
const batchWorkTypeSelect = document.querySelector("#batch-work-type-select");
const batchSourceText = document.querySelector("#batch-source-text");
const batchAnalyzeButton = document.querySelector("#batch-analyze-button");
const batchConfirmCreateButton = document.querySelector("#batch-confirm-create-button");
const batchCheckButton = document.querySelector("#batch-check-button");
const batchApplyButton = document.querySelector("#batch-apply-button");
const batchAbandonButton = document.querySelector("#batch-abandon-button");
const batchAbandonUnconfirmedButton = document.querySelector("#batch-abandon-unconfirmed-button");
const batchInlineStatus = document.querySelector("#batch-inline-status");
const batchSessionSummary = document.querySelector("#batch-session-summary");
const batchPreviewCount = document.querySelector("#batch-preview-count");
const batchPreviewList = document.querySelector("#batch-preview-list");
const batchReviewStatus = document.querySelector("#batch-review-status");
const batchReviewList = document.querySelector("#batch-review-list");
const articleTabs = [...document.querySelectorAll("[data-article-tab]")];
const articlePanels = [...document.querySelectorAll("[data-article-panel]")];
const articleSearchInput = document.querySelector("#article-search-input");
const articleSearchResults = document.querySelector("#article-search-results");
const articleForm = document.querySelector("#article-form");
const articleImageInput = document.querySelector("#article-image-input");
const articlePreviewButton = document.querySelector("#article-preview-button");
const articleSaveButton = document.querySelector("#article-save-button");
const articleResetButton = document.querySelector("#article-reset-button");
const articleDeleteButton = document.querySelector("#article-delete-button");
const articlePreviewStatus = document.querySelector("#article-preview-status");
const articlePreviewRender = document.querySelector("#article-preview-render");
const markdownToolbar = document.querySelector(".owner-markdown-toolbar");
const automationCheckForm = document.querySelector("#automation-check-form");
const previewAutomationSelectionButton = document.querySelector("#preview-automation-selection");
const runAutomationButton = document.querySelector("#run-automation-button");
const cancelAutomationButton = document.querySelector("#cancel-automation-button");
const automationSelectionPreview = document.querySelector("#automation-selection-preview");
const automationPreviewTotal = document.querySelector("#automation-preview-total");
const automationJobStatus = document.querySelector("#automation-job-status");
const automationJobProgressBar = document.querySelector("#automation-job-progress-bar");
const automationJobProgressText = document.querySelector("#automation-job-progress-text");
const automationJobMetrics = document.querySelector("#automation-job-metrics");
const automationJobItems = document.querySelector("#automation-job-items");
const automationJobDetail = document.querySelector("#automation-job-detail");
const inlineCheckPanel = document.querySelector("#owner-inline-check-panel");
const inlineCheckTitle = document.querySelector("#owner-inline-check-title");
const inlineCheckSubtitle = document.querySelector("#owner-inline-check-subtitle");
const inlineCheckContent = document.querySelector("#owner-inline-check-content");
const inlineCheckCloseButton = document.querySelector("#owner-inline-check-close");
const detailCard = document.querySelector(".owner-card--detail");
const reviewRunSelect = document.querySelector("#review-run-select");
const proposalReviewList = document.querySelector("#proposal-review-list");
const reviewPaginationTop = document.querySelector("#review-pagination-top");
const reviewPaginationBottom = document.querySelector("#review-pagination-bottom");
const logRunSelect = document.querySelector("#log-run-select");
const runLogPanel = document.querySelector("#run-log-panel");
const applyConfirmedButton = document.querySelector("#apply-confirmed-button");
const ignorePendingButton = document.querySelector("#ignore-pending-button");
const applyPageConfirmedButton = document.querySelector("#apply-page-confirmed-button");
const ignorePagePendingButton = document.querySelector("#ignore-page-pending-button");
const applyConfirmedButtonFooter = document.querySelector("#apply-confirmed-button-footer");
const ignorePendingButtonFooter = document.querySelector("#ignore-pending-button-footer");
const applyPageConfirmedButtonFooter = document.querySelector("#apply-page-confirmed-button-footer");
const ignorePagePendingButtonFooter = document.querySelector("#ignore-page-pending-button-footer");
const llmConfigForm = document.querySelector("#llm-config-form");
const llmConfigSummary = document.querySelector("#llm-config-summary");
const llmPanelToggle = document.querySelector("#llm-panel-toggle");
const llmTestResult = document.querySelector("#llm-test-result");
const saveLlmConfigButton = document.querySelector("#save-llm-config");
const testLlmConfigButton = document.querySelector("#test-llm-config");
const textDialog = document.querySelector("#owner-text-dialog");
const textDialogTitle = document.querySelector("#owner-text-dialog-title");
const textDialogContent = document.querySelector("#owner-text-dialog-content");
const textDialogClose = document.querySelector("#owner-text-dialog-close");
const textDialogSave = document.querySelector("#owner-text-dialog-save");
const linkDialog = document.querySelector("#owner-link-dialog");
const linkDialogTitle = document.querySelector("#owner-link-dialog-title");
const linkDialogForm = document.querySelector("#owner-link-dialog-form");
const linkDialogClose = document.querySelector("#owner-link-dialog-close");
const linkDialogOpen = document.querySelector("#owner-link-dialog-open");
const linkDialogSave = document.querySelector("#owner-link-dialog-save");
const linkDialogDelete = document.querySelector("#owner-link-dialog-delete");
const linkDialogMeta = document.querySelector("#owner-link-dialog-meta");
const previewDialog = document.querySelector("#owner-preview-dialog");
const previewDialogTitle = document.querySelector("#owner-preview-dialog-title");
const previewDialogContent = document.querySelector("#owner-preview-dialog-content");
const previewDialogClose = document.querySelector("#owner-preview-dialog-close");
const confirmDialog = document.querySelector("#owner-confirm-dialog");
const confirmDialogTitle = document.querySelector("#owner-confirm-dialog-title");
const confirmDialogContent = document.querySelector("#owner-confirm-dialog-content");
const confirmDialogClose = document.querySelector("#owner-confirm-dialog-close");
const confirmDialogCancel = document.querySelector("#owner-confirm-dialog-cancel");
const confirmDialogConfirm = document.querySelector("#owner-confirm-dialog-confirm");
const articlePreviewDialog = document.querySelector("#owner-article-preview-dialog");
const articlePreviewClose = document.querySelector("#owner-article-preview-close");
const GROUP_ROLE_VALUES = new Set(["orchestra", "ensemble", "chorus", "instrumentalist"]);
const PERSON_ROLE_VALUES = new Set(["composer", "conductor", "soloist", "singer", "instrumentalist"]);
const GROUP_FORM_ROLE_LABELS = {
  orchestra: "乐团",
  ensemble: "组合",
  chorus: "合唱",
  instrumentalist: "器乐团体",
};
const PERSON_FORM_ROLE_LABELS = {
  composer: "作曲家",
  conductor: "指挥",
  soloist: "独奏",
  singer: "歌手",
  instrumentalist: "器乐",
};

const normalizeEntityActionButtons = () => {
  entityForms.forEach((form) => {
    const resetButton = form.querySelector('[data-action="reset"]');
    const saveButton = form.querySelector('[data-action="save"]');
    const deleteButton = form.querySelector('[data-action="delete"]');
    const previewButton = form.querySelector('[data-action="preview"]');
    if (resetButton) {
      resetButton.textContent = "新建条目";
    }
    if (saveButton) {
      saveButton.textContent = "保存条目";
    }
    if (deleteButton) {
      deleteButton.textContent = "删除条目";
    }
    if (previewButton) {
      previewButton.remove();
    }
  });
};

const llmDraftStorageKey = "owner.llm.apiKey";
const collapsedPanelStoragePrefix = "owner.collapsed";
const proposalDraftStoragePrefix = "owner.reviewDraft";
const artistRoles = ["soloist", "singer", "ensemble", "chorus", "instrumentalist"];
const entityTypeLabels = {
  site: "网站文本",
  composer: "作曲家",
  person: "人物",
  work: "作品",
  recording: "版本",
};
const normalizeSearchTypeOptions = () => {
  if (!searchType) {
    return;
  }
  if (searchInput) {
    searchInput.placeholder = "搜索网站文本、作曲家、人物、团体、作品或版本";
  }
  searchType.innerHTML = `
    <option value="">全部类型</option>
    <option value="site">网站文本</option>
    <option value="composer">作曲家</option>
    <option value="person">人物</option>
    <option value="group">团体</option>
    <option value="work">作品</option>
    <option value="recording">版本</option>
  `;
};

const getVisibleDetailTabForEntity = (entityType, entity = null) => {
  if (entityType === "composer") {
    return "person";
  }
  if (entityType === "person") {
    return entity?.roles?.some((role) => GROUP_ROLE_VALUES.has(role)) ? "group" : "person";
  }
  return entityType;
};

const normalizeDetailTabs = () => {
  const tabsHost = document.querySelector(".owner-tabs--inner");
  if (!tabsHost) {
    return;
  }
  const composerButton = tabsHost.querySelector('[data-detail-tab="composer"]');
  const personButton = tabsHost.querySelector('[data-detail-tab="person"]');
  if (composerButton) {
    composerButton.hidden = true;
    composerButton.setAttribute("aria-hidden", "true");
    composerButton.tabIndex = -1;
  }
  if (personButton) {
    personButton.textContent = "人物";
  }
  if (!tabsHost.querySelector('[data-detail-tab="group"]')) {
    const groupButton = document.createElement("button");
    groupButton.type = "button";
    groupButton.dataset.detailTab = "group";
    groupButton.textContent = "团体";
    personButton?.insertAdjacentElement("afterend", groupButton);
    groupButton.addEventListener("click", () => setActiveDetailTab("group"));
  }
};

const setLeadingLabelText = (control, text) => {
  const label = control?.closest("label");
  if (!label) {
    return;
  }
  [...label.childNodes].forEach((node) => {
    if (node !== control) {
      node.remove();
    }
  });
  label.insertBefore(document.createTextNode(text), control);
};

const configurePersonFormMode = (mode = "person") => {
  const form = document.querySelector('[data-entity-form="person"]');
  if (!form) {
    return;
  }
  form.dataset.entityMode = mode;
  const lifeRangeInput = form.elements.lifeRange;
  if (lifeRangeInput) {
    setLeadingLabelText(lifeRangeInput, mode === "group" ? "建立时间 - 解散时间" : "生卒年");
    lifeRangeInput.placeholder = mode === "group" ? "1882-" : "1918-1990";
  }
  const summaryInput = form.elements.summary;
  if (summaryInput) {
    setLeadingLabelText(summaryInput, mode === "group" ? "团体简介" : "简介");
  }
  const mediaHint = form.querySelector(".owner-form__hint");
  if (mediaHint) {
    mediaHint.textContent =
      mode === "group" ? "乐团、组合与合唱等团体可在这里手动上传更合适的图片。" : "人物可在这里手动上传更合适的图片。";
  }
  const visibleRoles = mode === "group" ? GROUP_ROLE_VALUES : PERSON_ROLE_VALUES;
  const labels = mode === "group" ? GROUP_FORM_ROLE_LABELS : PERSON_FORM_ROLE_LABELS;
  [...form.querySelectorAll('input[name="roles"]')].forEach((input) => {
    const wrapper = input.closest("label");
    if (!wrapper) {
      return;
    }
    const visible = visibleRoles.has(input.value);
    wrapper.hidden = !visible;
    wrapper.setAttribute("aria-hidden", String(!visible));
    const textNode = [...wrapper.childNodes].find((node) => node.nodeType === Node.TEXT_NODE);
    if (textNode) {
      textNode.textContent = ` ${labels[input.value] || input.value}`;
    } else {
      wrapper.append(document.createTextNode(` ${labels[input.value] || input.value}`));
    }
  });
};
normalizeEntityActionButtons();
normalizeSearchTypeOptions();
normalizeDetailTabs();
const automationCategoryLabels = {
  composer: "作曲家",
  conductor: "指挥",
  orchestra: "乐团",
  artist: "艺术家",
  recording: "版本",
};
const reviewStateLabels = {
  unseen: "未查看",
  viewed: "已查看",
  edited: "已修改未确认",
  confirmed: "已确认",
  discarded: "已放弃",
};
const jobStatusLabels = {
  queued: "排队中",
  preparing: "准备中",
  running: "运行中",
  completed: "已完成",
  cancelled: "已取消",
};
const jobItemStatusLabels = {
  queued: "未执行",
  running: "执行中",
  succeeded: "成功",
  failed: "失败",
  skipped: "跳过",
  "completed-nochange": "无新增",
  "needs-attention": "待关注",
};

const compact = (value) => String(value ?? "").trim();
const isLocalResourceLink = (link) => compact(link?.linkType) === "local";
const getResourceLinkTarget = (link) => (isLocalResourceLink(link) ? compact(link?.localPath) : compact(link?.url));
const normalizeStructuredLink = (link = {}) => {
  const url = compact(link?.url);
  const localPath = compact(link?.localPath || link?.path);
  const linkType = compact(link?.linkType) === "local" || (!url && localPath) ? "local" : "external";
  return {
    platform: compact(link?.platform || (linkType === "local" ? "local" : "other")) || "other",
    url,
    localPath,
    title: compact(link?.title),
    linkType,
    visibility: linkType === "local" ? "local-only" : compact(link?.visibility) === "local-only" ? "local-only" : "public",
  };
};
const buildResourceLinkLine = (link) => {
  const normalized = normalizeStructuredLink(link);
  return [
    normalized.platform,
    getResourceLinkTarget(normalized),
    normalized.title || "",
    normalized.linkType,
    normalized.visibility,
  ].join(" | ");
};
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
const buildWorkSlugSource = (title, titleLatin, catalogue) =>
  [compact(title), stripCatalogueFromWorkSegment(titleLatin || "", catalogue || ""), compact(catalogue)].filter(Boolean).join(" ");
const createSlugLike = (value) => {
  const slug = String(value ?? "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’'".·]/g, "")
    .replace(/[\/_,:;()[\]{}]+/g, "-")
    .replace(/[^\p{Letter}\p{Number}-]+/gu, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return slug || "item";
};
const escapeHtml = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
const formatValue = (value) => {
  if (Array.isArray(value)) {
    return value.length ? value.join(" / ") : "未填写";
  }
  if (value && typeof value === "object") {
    return JSON.stringify(value);
  }
  const normalized = compact(value ?? "");
  return normalized || "未填写";
};
const clipText = (value, maxLength = 160) => {
  const normalized = compact(value);
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength).trim()}…` : normalized;
};
const emptyInfoPanel = () => ({
  text: "",
  articleId: "",
  collectionLinks: [],
});
const encodeDataText = (value) => encodeURIComponent(String(value ?? ""));
const decodeDataText = (value) => {
  try {
    return decodeURIComponent(value || "");
  } catch {
    return String(value ?? "");
  }
};
const toggleBusyState = (button, pending, pendingText = "处理中…") => {
  if (!button) {
    return;
  }
  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent || "";
  }
  button.disabled = pending;
  button.textContent = pending ? pendingText : button.dataset.defaultLabel;
};
const withBusyButton = async (button, pendingText, action) => {
  toggleBusyState(button, true, pendingText);
  try {
    return await action();
  } finally {
    toggleBusyState(button, false);
  }
};
const getCollapsedStorageKey = (panelId) => `${collapsedPanelStoragePrefix}.${panelId}`;
const isPanelCollapsed = (panelId) => {
  try {
    return window.localStorage.getItem(getCollapsedStorageKey(panelId)) === "1";
  } catch {
    return false;
  }
};
const setPanelCollapsed = (panelId, collapsed) => {
  try {
    window.localStorage.setItem(getCollapsedStorageKey(panelId), collapsed ? "1" : "0");
  } catch {
    // ignore localStorage failures in owner tool
  }
};
const applyCollapsedPanelState = (panelId) => {
  const panel = document.querySelector(`[data-collapsible-panel="${panelId}"]`);
  const toggle = document.querySelector(`[data-collapsible-toggle="${panelId}"]`);
  if (!panel || !toggle) {
    return;
  }
  const collapsed = isPanelCollapsed(panelId);
  panel.hidden = collapsed;
  toggle.textContent = collapsed ? "展开" : "折叠";
};
const toggleCollapsedPanel = (panelId) => {
  const nextCollapsed = !isPanelCollapsed(panelId);
  setPanelCollapsed(panelId, nextCollapsed);
  applyCollapsedPanelState(panelId);
};
const hostLabel = (value) => {
  try {
    return new URL(value).hostname.replace(/^www\./, "");
  } catch {
    return compact(value).slice(0, 48) || "未标注来源";
  }
};
const parseStructuredLinks = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => normalizeStructuredLink(item)).filter((item) => getResourceLinkTarget(item));
  }
  if (typeof value === "string") {
    const normalized = compact(value);
    if (!normalized) {
      return [];
    }
    try {
      return parseStructuredLinks(JSON.parse(normalized));
    } catch {
      return [normalizeStructuredLink({ platform: "other", url: normalized, title: "", linkType: "external", visibility: "public" })];
    }
  }
  return [];
};
const normalizeInfoPanel = (infoPanel) => {
  const collectionLinks = parseStructuredLinks(infoPanel?.collectionLinks || infoPanel?.collectionUrl || []);
  return {
    text: compact(infoPanel?.text),
    articleId: compact(infoPanel?.articleId),
    collectionLinks,
  };
};
const renderInfoPanelState = (form, infoPanel) => {
  if (!form?.elements) {
    return;
  }
  const normalized = normalizeInfoPanel(
    infoPanel || {
      text: form.elements.infoPanelText?.value,
      articleId: form.elements.infoPanelArticleId?.value,
      collectionLinks: form.elements.infoPanelCollectionLinks?.value,
    },
  );
  if (form.elements.infoPanelText) {
    form.elements.infoPanelText.value = normalized.text;
  }
  if (form.elements.infoPanelArticleId) {
    form.elements.infoPanelArticleId.value = normalized.articleId;
  }
  if (form.elements.infoPanelCollectionLinks) {
    form.elements.infoPanelCollectionLinks.value = JSON.stringify(normalized.collectionLinks);
  }
  const summary = form.querySelector("[data-info-panel-text-summary]");
  if (summary) {
    summary.textContent = normalized.text ? clipText(normalized.text, 180) : "尚未填写导览文本。";
  }
  const editor = form.querySelector("[data-info-panel-links-editor]");
  if (editor) {
    editor.innerHTML = normalized.collectionLinks.length
      ? normalized.collectionLinks
          .map(
            (link, index) =>
              `<button type="button" class="owner-link-chip" data-info-panel-link-edit="${index}" title="${escapeHtml(link.title || getResourceLinkTarget(link))}">${escapeHtml(
                buildRecordingLinkChipLabel(link, index, normalized.collectionLinks),
              )}</button>`,
          )
          .join("")
      : '<p class="owner-empty">尚未添加合集链接。</p>';
  }
};
const readInfoPanelLinksFromForm = (form) => parseStructuredLinks(form?.elements?.infoPanelCollectionLinks?.value || "");
const openInfoPanelTextDialog = (form) => {
  state.infoPanelTextDialogContext = form;
  textDialogTitle.textContent = "编辑导览文本";
  textDialogContent.value = form?.elements?.infoPanelText?.value || "";
  textDialogContent.readOnly = false;
  textDialogSave.hidden = false;
  textDialog.showModal();
};
const saveInfoPanelTextDialog = () => {
  const form = state.infoPanelTextDialogContext;
  if (!form?.elements?.infoPanelText) {
    return;
  }
  form.elements.infoPanelText.value = textDialogContent.value || "";
  renderInfoPanelState(form);
  textDialog.close();
};
const openPreviewDialog = (title, html) => {
  previewDialogTitle.textContent = title || "页面预览";
  previewDialogContent.innerHTML = html || '<p class="owner-empty">暂无可预览内容。</p>';
  previewDialog.showModal();
};
const showConfirmDialog = ({ title, message, confirmText = "确认删除", onConfirm }) => {
  state.confirmDialogContext = { onConfirm };
  confirmDialogTitle.textContent = title || "确认操作";
  confirmDialogContent.innerHTML = `<p>${escapeHtml(message || "确认继续吗？")}</p>`;
  confirmDialogConfirm.textContent = confirmText;
  confirmDialog.showModal();
};
const getManagedComposers = (library = state.library) => {
  const composerMap = new Map();
  [...(library?.composers || []), ...((library?.people || []).filter((item) => item?.roles?.includes("composer")))].forEach((item) => {
    if (item?.id && !composerMap.has(item.id)) {
      composerMap.set(item.id, item);
    }
  });
  return [...composerMap.values()];
};
const getEntityCollection = (entityType, library = state.library) => {
  if (entityType === "composer") return getManagedComposers(library);
  if (entityType === "recording") return library?.recordings || [];
  if (entityType === "work") return library?.works || [];
  if (entityType === "site") return state.site ? [state.site] : [];
  return library?.people || [];
};
const getEntityByTypeAndId = (entityType, entityId, library = state.library) =>
  getEntityCollection(entityType, library).find((item) => item.id === entityId);
const parseLines = (value, mapper) =>
  compact(value)
    .split("\n")
    .map((line) => mapper(line.trim()))
    .filter(Boolean);
const formatLines = (items, mapper) => (items || []).map(mapper).join("\n");
const parsePipeLine = (value) => String(value ?? "").split("|").map((part) => part.trim());
const parseRecordingImageLines = (value) =>
  parseLines(value, (line) => {
    const [src, alt = "", kind = "other", sourceUrl = "", sourceKind = ""] = parsePipeLine(line);
    if (!src) {
      return null;
    }
    return { src, alt, kind, sourceUrl, sourceKind };
  });
const formatRecordingImageLine = (item) =>
  [item?.src || "", item?.alt || "", item?.kind || "other", item?.sourceUrl || "", item?.sourceKind || ""].join(" | ");
const parseIntegerString = (value) => {
  const normalized = compact(value);
  return normalized ? normalized : "";
};
const getDisplayTitle = (entity) =>
  (entity?.aliases || []).find((value) => /[\u3400-\u9fff]/.test(value) && compact(value).length < compact(entity?.name).length) ||
  entity?.name ||
  "";
const getEntityScopedProposals = (run, entityType, entityId) =>
  filterPendingProposalsForDisplay(run?.proposals || []).filter(
    (proposal) => proposal.entityType === entityType && proposal.entityId === entityId,
  );
const getReviewRunById = (runId) => state.runs.find((run) => run.id === runId) || null;
const getEntityBucket = (entityType) => {
  if (entityType === "composer") return "composers";
  if (entityType === "person") return "people";
  if (entityType === "recording") return "recordings";
  return "misc";
};
const getEditableFieldDefinitions = (entityType) => {
  if (["composer", "person"].includes(entityType)) {
    return [
      { path: "name", label: "中文全名" },
      { path: "nameLatin", label: "英文 / 原文全名" },
      { path: "country", label: "国家" },
      { path: "lifeRange", label: "生卒年" },
      { path: "aliases", label: "别名" },
      { path: "summary", label: "简介" },
    ];
  }
  if (entityType === "work") {
    return [
      { path: "title", label: "作品标题" },
      { path: "titleLatin", label: "英文 / 原文标题" },
      { path: "catalogue", label: "作品号 / Catalogue" },
      { path: "aliases", label: "别名" },
      { path: "summary", label: "简介" },
    ];
  }
  if (entityType === "recording") {
    return [
      { path: "title", label: "版本标题" },
      { path: "performanceDateText", label: "演出时间" },
      { path: "venueText", label: "地点" },
      { path: "albumTitle", label: "专辑名称" },
      { path: "label", label: "发行商" },
      { path: "releaseDate", label: "发行日期" },
      { path: "links", label: "资源链接" },
      { path: "images", label: "图片" },
      { path: "notes", label: "备注" },
    ];
  }
  return [];
};
const getValueByPath = (target, path) => {
  if (!target || !path) {
    return "";
  }
  if (path === "lifeRange") {
    return buildLifeRangeValue(target);
  }
  const imageMatch = /^images\[(\d+)\]\.(.+)$/.exec(path);
  if (imageMatch) {
    const image = target.images?.[Number(imageMatch[1])];
    return image?.[imageMatch[2]] ?? "";
  }
  return target[path] ?? "";
};
const buildEditableFieldDescriptors = (proposal, entityType, entity) => {
  const definitions = getEditableFieldDefinitions(entityType);
  const existingByPath = new Map((proposal.fields || []).map((field) => [field.path, field]));
  const descriptors = [];
  for (const definition of definitions) {
    if (definition.path === "lifeRange") {
      const existingBirth = existingByPath.get("birthYear");
      const existingDeath = existingByPath.get("deathYear");
      const before = buildLifeRangeValue(entity);
      const after = buildLifeRangeValue({
        birthYear: existingBirth?.after ?? entity?.birthYear,
        deathYear: existingDeath?.after ?? entity?.deathYear,
      });
      descriptors.push({
        path: "lifeRange",
        label: definition.label || definition.path,
        before,
        after,
      });
      existingByPath.delete("birthYear");
      existingByPath.delete("deathYear");
      continue;
    }
    const existing = existingByPath.get(definition.path);
    if (existing) {
      descriptors.push({ ...existing, label: definition.label || existing.label || definition.path });
      existingByPath.delete(definition.path);
      continue;
    }
    descriptors.push({
      path: definition.path,
      label: definition.label || definition.path,
      before: getValueByPath(entity, definition.path),
      after: getValueByPath(entity, definition.path),
    });
  }
  for (const field of existingByPath.values()) {
    descriptors.push(field);
  }
  return descriptors;
};
const readFileAsBase64 = (file) =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      resolve(result.includes(",") ? result.split(",").at(-1) || "" : result);
    };
    reader.onerror = () => reject(reader.error || new Error("读取文件失败"));
    reader.readAsDataURL(file);
  });
const renderFormImagePreview = (entityType, asset) => {
  const preview = document.querySelector(buildDataAttributeSelector("data-image-preview", entityType));
  if (!preview) {
    return;
  }
  const primaryRecordingImage = asset?.images?.[0] || null;
  const src = compact(asset?.src || asset?.avatarSrc || primaryRecordingImage?.src);
  const alt = compact(
    asset?.alt ||
      primaryRecordingImage?.alt ||
      asset?.name ||
      "条目图片",
  );
  preview.innerHTML = src
    ? `<img src="${escapeHtml(src)}" alt="${escapeHtml(alt)}" />`
    : "<span>暂无图片</span>";
};
const syncEntityImageFields = (form, asset) => {
  if (!form?.elements) {
    return;
  }
  if (form.dataset.entityForm === "recording" && form.elements.images) {
    const currentImages = parseRecordingImageLines(form.elements.images.value);
    const nextImage = {
      src: compact(asset?.src),
      alt: compact(asset?.alt || form.elements.title?.value || "版本图片"),
      kind: currentImages[0]?.kind || "cover",
      sourceUrl: compact(asset?.sourceUrl),
      sourceKind: compact(asset?.sourceKind),
    };
    form.elements.images.value = formatLines(
      [nextImage, ...currentImages.filter((item, index) => index > 0 || item.src !== nextImage.src)],
      formatRecordingImageLine,
    );
    return;
  }
  if (form.elements.avatarSrc) {
    form.elements.avatarSrc.value = compact(asset?.src);
  }
  if (form.elements.imageSourceUrl) {
    form.elements.imageSourceUrl.value = compact(asset?.sourceUrl);
  }
  if (form.elements.imageSourceKind) {
    form.elements.imageSourceKind.value = compact(asset?.sourceKind);
  }
  if (form.elements.imageAttribution) {
    form.elements.imageAttribution.value = compact(asset?.attribution || asset?.alt);
  }
  if (form.elements.imageUpdatedAt) {
    form.elements.imageUpdatedAt.value = asset?.updatedAt || new Date().toISOString();
  }
};
const uploadEntityImage = async (entityType, form, file) => {
  const fileName = compact(file?.name || "upload.jpg");
  if (!fileName) {
    throw new Error("未选择图片文件。");
  }
  const slug =
    compact(form?.elements?.slug?.value) ||
    compact(form?.elements?.existingId?.value) ||
    compact(form?.elements?.name?.value) ||
    entityType;
  const contentBase64 = await readFileAsBase64(file);
  const result = await fetchJson("/api/assets/upload", {
    method: "POST",
    body: JSON.stringify({
      bucket: getEntityBucket(entityType),
      slug,
      fileName,
      contentBase64,
    }),
  });
  const uploadedAsset = result.asset || {};
  const asset = {
    src: uploadedAsset.src || "",
    sourceUrl: "",
    sourceKind: uploadedAsset.imageSourceKind || "manual",
    attribution: uploadedAsset.imageAttribution || fileName,
    alt: compact(form?.elements?.name?.value),
    updatedAt: uploadedAsset.imageUpdatedAt || new Date().toISOString(),
  };
  syncEntityImageFields(form, asset);
  renderFormImagePreview(entityType, asset);
  setResult({ uploaded: true, entityType, src: uploadedAsset.src || "" });
};

const setResult = (value) => {
  if (typeof value === "string") {
    resultPanel.textContent = value;
    return;
  }

  const safeValue = JSON.parse(JSON.stringify(value, (key, currentValue) => {
    if (key === "apiKey" && currentValue) {
      return "***";
    }
    return currentValue;
  }));
  resultPanel.textContent = JSON.stringify(safeValue, null, 2);
};

const fetchJson = async (url, options = {}) => {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || "请求失败");
  }
  return payload;
};

const buildAffectedPathsPreviewHtml = (result, entityType) => {
  const paths = Array.isArray(result?.affectedPaths) ? result.affectedPaths : [];
  const title = entityTypeLabels[entityType] || entityType || "条目";
  return `
    <section class="owner-dialog-preview">
      <p>以下页面会受到当前${escapeHtml(title)}修改影响：</p>
      ${
        paths.length
          ? `<ul>${paths
              .map(
                (pathValue) =>
                  `<li><a href="${escapeHtml(pathValue)}" target="_blank" rel="noreferrer">${escapeHtml(pathValue)}</a></li>`,
              )
              .join("")}</ul>`
          : "<p>本次未解析到受影响页面路径。</p>"
      }
    </section>`;
};

const deleteEntity = async (form) => {
  const entityType = form?.dataset?.entityForm;
  const entityId = compact(form?.elements?.existingId?.value);
  if (!entityType || !entityId) {
    throw new Error("当前没有可删除的条目。");
  }
  const result = await fetchJson(`/api/entity/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`, {
    method: "DELETE",
  });
  await refreshAll();
  await performSearch();
  resetEntityForm(form);
  setActiveDetailTab(getVisibleDetailTabForEntity(entityType), {
    panel: entityType === "composer" ? "person" : undefined,
  });
  setResult(result);
};

const mergeEntity = async (form) => {
  const entityType = form?.dataset?.entityForm || "";
  const duplicateId = compact(form?.elements?.existingId?.value || "");
  const targetId = compact(form?.elements?.mergeTargetId?.value || "");
  if (!supportsManualMerge(entityType)) {
    throw new Error("当前条目类型不支持手动合并。");
  }
  if (!duplicateId) {
    throw new Error("请先载入重复条目并选择主条目。");
  }
  if (!targetId) {
    throw new Error("请先选择主条目。");
  }
  const result = await fetchJson(`/api/entity/${encodeURIComponent(entityType)}/${encodeURIComponent(duplicateId)}/merge`, {
    method: "POST",
    body: JSON.stringify({ targetId }),
  });
  await refreshAll();
  await performSearch();
  await loadEntity(entityType, targetId);
  setActiveDetailTab(getVisibleDetailTabForEntity(entityType, getEntityByTypeAndId(entityType, targetId)), {
    panel: entityType === "composer" ? "composer" : undefined,
  });
  setResult(result);
};

const setActiveView = (viewName) => {
  viewTabs.forEach((button) => button.classList.toggle("is-active", button.dataset.viewTab === viewName));
  viewPanels.forEach((panel) => panel.classList.toggle("is-active", panel.dataset.viewPanel === viewName));
  if (viewName === "review") {
    void renderReviewRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
  }
  if (viewName === "logs") {
    void renderLogRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
  }
  if (viewName === "batch" && state.batchSession) {
    void renderBatchReview().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
  }
};

const setActiveDetailTab = (tabName, options = {}) => {
  const requestedTab = tabName || "site";
  const resolvedPanel = options.panel || (requestedTab === "group" ? "person" : requestedTab);
  [...document.querySelectorAll("[data-detail-tab]")].forEach((button) => {
    button.classList.toggle("is-active", button.dataset.detailTab === requestedTab);
  });
  [...document.querySelectorAll("[data-detail-panel]")].forEach((panel) => {
    const active = panel.dataset.detailPanel === resolvedPanel;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  });
  configurePersonFormMode(requestedTab === "group" ? "group" : "person");
  entityForms.forEach((form) => resetMergeControlsState(form));
};

const setActiveArticleTab = (tabName) => {
  state.articleTab = tabName;
  articleTabs.forEach((button) => button.classList.toggle("is-active", button.dataset.articleTab === tabName));
  articlePanels.forEach((panel) => {
    const active = panel.dataset.articlePanel === tabName;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  });
};

viewTabs.forEach((button) => button.addEventListener("click", () => setActiveView(button.dataset.viewTab)));
detailTabs.forEach((button) => button.addEventListener("click", () => setActiveDetailTab(button.dataset.detailTab)));
articleTabs.forEach((button) => button.addEventListener("click", () => setActiveArticleTab(button.dataset.articleTab)));

const populateOptions = (select, items, formatLabel) => {
  if (!select) {
    return;
  }
  const previousValue = select.value;
  select.innerHTML =
    '<option value="">请选择</option>' +
    items.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(formatLabel(item))}</option>`).join("");
  if (items.some((item) => item.id === previousValue)) {
    select.value = previousValue;
  }
};

const populateArticleOptions = () => {
  const selects = [...document.querySelectorAll('select[name="infoPanelArticleId"]')];
  selects.forEach((select) => {
    const previousValue = select.value;
    select.innerHTML =
      '<option value="">不设置</option>' +
      state.articles.map((article) => `<option value="${escapeHtml(article.id)}">${escapeHtml(article.title)}</option>`).join("");
    if (state.articles.some((article) => article.id === previousValue)) {
      select.value = previousValue;
    }
  });
};

const supportsManualMerge = (entityType) => ["composer", "person", "work"].includes(entityType);
const getMergeOptionLabel = (entityType, entity) => {
  if (!entity) {
    return "";
  }
  if (entityType === "work") {
    return getWorkDisplayLabel(entity);
  }
  return buildComposerOptionLabel(entity);
};
const getMergeTargetOptions = (entityType, currentId = "") => {
  if (!supportsManualMerge(entityType)) {
    return [];
  }
  const items =
    entityType === "composer"
      ? getManagedComposers()
      : entityType === "person"
        ? state.library?.people || []
        : state.library?.works || [];
  return items
    .filter((item) => item?.id && item.id !== currentId)
    .map((item) => {
      const label = getMergeOptionLabel(entityType, item);
      return label
        ? {
            value: item.id,
            label,
          }
        : null;
    })
    .filter(Boolean)
    .sort((left, right) => left.label.localeCompare(right.label, "zh-Hans-CN"));
};
const resetMergeControlsState = (form, { closePanel = true } = {}) => {
  if (!form) {
    return;
  }
  if (form.elements.mergeTargetId) {
    form.elements.mergeTargetId.value = "";
  }
  const host = form.querySelector("[data-merge-host]");
  if (!host) {
    return;
  }
  const searchInputControl = host.querySelector("[data-merge-target-search]");
  const panel = host.querySelector("[data-merge-target-panel]");
  const trigger = host.querySelector("[data-merge-target-toggle]");
  if (searchInputControl) {
    searchInputControl.value = "";
  }
  if (closePanel && panel) {
    panel.hidden = true;
  }
  if (trigger) {
    trigger.setAttribute("aria-expanded", "false");
  }
};
const ensureMergeControls = (form) => {
  if (!form || form.querySelector("[data-merge-host]")) {
    return form?.querySelector("[data-merge-host]") || null;
  }
  const actions = form.querySelector(".owner-form__actions");
  if (!actions) {
    return null;
  }
  const mergeHost = document.createElement("section");
  mergeHost.className = "owner-merge-controls";
  mergeHost.dataset.mergeHost = "true";
  mergeHost.hidden = true;
  mergeHost.innerHTML = `
    <div class="owner-card__header">
      <h3>合并到主条目</h3>
    </div>
    <div class="owner-merge-combobox">
      <button
        type="button"
        class="owner-merge-combobox__trigger"
        data-merge-target-toggle
        aria-expanded="false"
      >请选择主条目</button>
      <div class="owner-merge-combobox__panel" data-merge-target-panel hidden>
        <input type="search" data-merge-target-search placeholder="搜索主条目" />
        <div class="owner-merge-combobox__results" data-merge-target-results></div>
      </div>
    </div>
    <input type="hidden" name="mergeTargetId" />
    <p class="owner-form__hint">当前加载条目会作为被合并条目，主条目保留已填写字段，缺失字段由当前条目补齐。</p>
    <button type="button" data-action="merge" class="owner-button--danger" disabled>合并条目</button>
  `;
  actions.before(mergeHost);
  return mergeHost;
};
const renderMergeControls = (form) => {
  if (!form) {
    return;
  }
  const entityType = form.dataset.entityForm || "";
  const host = ensureMergeControls(form);
  if (!host) {
    return;
  }
  if (!supportsManualMerge(entityType)) {
    host.hidden = true;
    return;
  }
  const currentId = compact(form.elements.existingId?.value || "");
  const searchInputControl = host.querySelector("[data-merge-target-search]");
  const trigger = host.querySelector("[data-merge-target-toggle]");
  const panel = host.querySelector("[data-merge-target-panel]");
  const resultsPanel = host.querySelector("[data-merge-target-results]");
  const mergeButton = form.querySelector('[data-action="merge"]');
  const mergeTargetField = form.elements.mergeTargetId;
  if (!currentId) {
    host.hidden = true;
    resetMergeControlsState(form);
    if (resultsPanel) {
      resultsPanel.innerHTML = "";
    }
    if (mergeButton) {
      mergeButton.disabled = true;
    }
    return;
  }
  host.hidden = false;
  const query = compact(searchInputControl?.value || "");
  const options = filterMergeTargetOptions(getMergeTargetOptions(entityType, currentId), query);
  const selectedId = compact(mergeTargetField?.value || "");
  const selectedOption = options.find((option) => option.value === selectedId) || getMergeTargetOptions(entityType, currentId).find((option) => option.value === selectedId);
  if (trigger) {
    trigger.textContent = selectedOption?.label || "请选择主条目";
    trigger.setAttribute("aria-expanded", String(Boolean(panel && !panel.hidden)));
  }
  if (mergeButton) {
    mergeButton.disabled = !selectedId;
  }
  if (resultsPanel) {
    resultsPanel.innerHTML =
      options.length > 0
        ? options
            .map(
              (option) => `
                <button
                  type="button"
                  class="owner-merge-combobox__option${option.value === selectedId ? " is-selected" : ""}"
                  data-merge-target-option="${escapeHtml(option.value)}"
                >${escapeHtml(option.label)}</button>`,
            )
            .join("")
        : '<p class="owner-empty">没有匹配主条目。</p>';
  }
};

const ensureRelatedEntityControls = (form) => {
  if (!form || form.querySelector("[data-related-entity-host]")) {
    return form?.querySelector("[data-related-entity-host]") || null;
  }
  const actions = form.querySelector(".owner-form__actions");
  if (!actions) {
    return null;
  }
  const host = document.createElement("section");
  host.className = "owner-merge-controls";
  host.dataset.relatedEntityHost = "true";
  host.hidden = true;
  host.innerHTML = `
    <div class="owner-card__header">
      <h3>关联条目</h3>
      <span data-related-entity-count>0 条</span>
    </div>
    <label>
      直接关联条目
      <select data-related-entity-select>
        <option value="">当前没有直接关联条目</option>
      </select>
    </label>
    <div class="owner-form__actions owner-form__actions--compact">
      <button type="button" data-related-entity-jump disabled>跳转查看</button>
    </div>
    <p class="owner-form__hint">这里只显示一级直接关联。先跳转并解除关联，再处理删除或合并。</p>
  `;
  const mergeHost = form.querySelector("[data-merge-host]");
  if (mergeHost) {
    mergeHost.before(host);
  } else {
    actions.before(host);
  }
  const select = host.querySelector("[data-related-entity-select]");
  const jumpButton = host.querySelector("[data-related-entity-jump]");
  const syncJumpState = () => {
    const option = select?.selectedOptions?.[0];
    const relatedType = option?.dataset?.entityType || "";
    const relatedId = option?.value || "";
    jumpButton.disabled = !relatedType || !relatedId;
    jumpButton.dataset.relatedEntityType = relatedType;
    jumpButton.dataset.relatedEntityId = relatedId;
  };
  select?.addEventListener("change", syncJumpState);
  jumpButton?.addEventListener("click", () => {
    const relatedType = compact(jumpButton.dataset.relatedEntityType || "");
    const relatedId = compact(jumpButton.dataset.relatedEntityId || "");
    if (!relatedType || !relatedId) {
      return;
    }
    void loadEntity(relatedType, relatedId);
  });
  return host;
};

const renderRelatedEntityControls = (form, relatedEntities = []) => {
  if (!form) {
    return;
  }
  const host = ensureRelatedEntityControls(form);
  if (!host) {
    return;
  }
  const currentId = compact(form.elements.existingId?.value || "");
  const normalizedEntities = Array.isArray(relatedEntities)
    ? relatedEntities.filter((item) => compact(item?.id) && compact(item?.entityType))
    : [];
  if (!currentId) {
    host.hidden = true;
    return;
  }
  host.hidden = false;
  const count = host.querySelector("[data-related-entity-count]");
  const select = host.querySelector("[data-related-entity-select]");
  const jumpButton = host.querySelector("[data-related-entity-jump]");
  if (count) {
    count.textContent = `${normalizedEntities.length} 条`;
  }
  if (select) {
    const groupedEntities = normalizedEntities.reduce((groups, item) => {
      const key = compact(item.groupLabel || item.entityType || "其他");
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(item);
      return groups;
    }, new Map());
    select.innerHTML = normalizedEntities.length
      ? [...groupedEntities.entries()]
          .map(
            ([groupLabel, items]) => `
              <optgroup label="${escapeHtml(groupLabel)}">
                ${items
                  .map(
                    (item) => `
                      <option value="${escapeHtml(item.id)}" data-entity-type="${escapeHtml(item.entityType)}">
                        ${escapeHtml(item.label || item.title || item.id)}
                      </option>`,
                  )
                  .join("")}
              </optgroup>`,
          )
          .join("")
      : '<option value="">当前没有直接关联条目</option>';
  }
  const option = select?.selectedOptions?.[0];
  const relatedType = option?.dataset?.entityType || "";
  const relatedId = option?.value || "";
  if (jumpButton) {
    jumpButton.disabled = !relatedType || !relatedId;
    jumpButton.dataset.relatedEntityType = relatedType;
    jumpButton.dataset.relatedEntityId = relatedId;
  }
};

const deriveGroupPath = (work) => {
  if (!work || !state.library) {
    return [];
  }
  const groups = (work.groupIds || [])
    .map((groupId) => state.library.workGroups.find((group) => group.id === groupId))
    .filter(Boolean)
    .sort((left, right) => left.path.length - right.path.length);
  return groups.at(-1)?.path || [];
};

const getComposerById = (composerId, library = state.library) => getManagedComposers(library).find((item) => item.id === composerId) || null;
const getWorkById = (workId, library = state.library) => (library?.works || []).find((item) => item.id === workId) || null;
const getRecordingComposerIdFromWork = (workId, library = state.library) => getWorkById(workId, library)?.composerId || "";
const getWorkDisplayLabel = (work, library = state.library) => buildPreferredWorkLabel(work, getManagedComposers(library));
const populateRecordingComposerOptions = (select) => {
  populateOptions(select, [...getManagedComposers()].sort((a, b) => buildComposerOptionLabel(a).localeCompare(buildComposerOptionLabel(b), "zh-Hans-CN")), buildComposerOptionLabel);
  if (select && !select.value) {
    select.innerHTML = select.innerHTML.replace('>请选择<', '>请选择作曲家<');
  }
};
const populateWorkSelectOptions = (select, composerId, selectedWorkId = "", prompts = {}) => {
  if (!select) {
    return;
  }
  const formatLabel = select.id === "batch-work-select" ? buildBatchWorkOptionLabel : getWorkDisplayLabel;
  const works = [...(state.library?.works || [])]
    .filter((item) => !composerId || item.composerId === composerId)
    .sort((a, b) => formatLabel(a).localeCompare(formatLabel(b), "zh-Hans-CN"));
  select.disabled = !composerId;
  select.innerHTML =
    `<option value="">${composerId ? prompts.ready || "请选择作品" : prompts.empty || "请先选择作曲家"}</option>` +
    works.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(formatLabel(item))}</option>`).join("");
  if (works.some((item) => item.id === selectedWorkId)) {
    select.value = selectedWorkId;
  }
};
const populateRecordingWorkOptions = (form, composerId, selectedWorkId = "") =>
  populateWorkSelectOptions(form?.elements?.workId, composerId, selectedWorkId, {
    ready: "请选择作品",
    empty: "请先选择作曲家",
  });

const getConductors = () => (state.library?.people || []).filter((person) => person.roles.includes("conductor"));
const getOrchestras = () => (state.library?.people || []).filter((person) => person.roles.includes("orchestra"));
const getArtists = () =>
  (state.library?.people || []).filter((person) => person.roles.some((role) => artistRoles.includes(role)));
const RECORDING_CREDIT_ROLE_OPTIONS = [
  ["conductor", "指挥"],
  ["orchestra", "乐团"],
  ["chorus", "合唱"],
  ["ensemble", "组合"],
  ["soloist", "独奏"],
  ["singer", "歌手"],
  ["instrumentalist", "器乐"],
];
const RECORDING_CREDIT_ROLE_COMPAT = {
  conductor: ["conductor"],
  orchestra: ["orchestra"],
  chorus: ["chorus"],
  ensemble: ["ensemble", "chorus", "orchestra"],
  soloist: ["soloist", "instrumentalist"],
  singer: ["singer"],
  instrumentalist: ["instrumentalist", "soloist"],
};

const parseRecordingCreditLines = (value) =>
  parseLines(value, (line) => {
    const [role, displayName = "", personId = ""] = line.split("|").map((part) => part.trim());
    if (!role || !displayName) {
      return null;
    }
    return { role, displayName, personId };
  });

const serializeRecordingCredits = (credits) =>
  formatLines(
    (credits || []).filter((credit) => compact(credit?.role) && compact(credit?.displayName)),
    (item) => [item.role, item.displayName, item.personId || ""].join(" | "),
  );

const getRecordingCreditRowsHost = (form) => form?.querySelector?.("[data-recording-credit-rows]") || null;

const getPeopleByRecordingCreditRole = (role) => {
  const acceptedRoles = new Set(RECORDING_CREDIT_ROLE_COMPAT[role] || [role]);
  return [...(state.library?.people || [])]
    .filter((person) => person.roles?.some((personRole) => acceptedRoles.has(personRole)))
    .sort((a, b) => buildComposerOptionLabel(a).localeCompare(buildComposerOptionLabel(b), "zh-Hans-CN"));
};

const buildRecordingCreditDisplayName = (role, personId, fallback = "") => {
  const person = (state.library?.people || []).find((item) => item.id === personId);
  if (!person) {
    return compact(fallback);
  }
  if (["orchestra", "ensemble", "chorus"].includes(role)) {
    return compact(person.name || fallback);
  }
  return compact(person.name || fallback);
};

const buildRecordingCreditRowHtml = (credit, index) => {
  const selectedRole = compact(credit?.role || "soloist");
  const selectedPersonId = compact(credit?.personId || "");
  const roleOptions = RECORDING_CREDIT_ROLE_OPTIONS.map(
    ([value, label]) => `<option value="${escapeHtml(value)}" ${value === selectedRole ? "selected" : ""}>${escapeHtml(label)}</option>`,
  ).join("");
  const personOptions = [
    '<option value="">手动填写</option>',
    ...getPeopleByRecordingCreditRole(selectedRole).map(
      (person) =>
        `<option value="${escapeHtml(person.id)}" ${person.id === selectedPersonId ? "selected" : ""}>${escapeHtml(buildComposerOptionLabel(person))}</option>`,
    ),
  ].join("");
  return `
    <div class="owner-recording-credit-row" data-recording-credit-row data-recording-credit-index="${index}">
      <label>
        <span>角色</span>
        <select data-recording-credit-role>${roleOptions}</select>
      </label>
      <label>
        <span>关联人物 / 团体</span>
        <select data-recording-credit-person-id>${personOptions}</select>
      </label>
      <label class="owner-recording-credit-row__display">
        <span>显示名称</span>
        <input data-recording-credit-display value="${escapeHtml(credit?.displayName || "")}" placeholder="可手动填写显示名称" />
      </label>
      <button type="button" class="owner-recording-credit-row__remove" data-recording-credit-remove="${index}">删除</button>
    </div>
  `;
};

const syncPrimaryRecordingCreditSelects = (form, credits) => {
  if (form.elements.conductorPersonId) {
    form.elements.conductorPersonId.value = compact(
      (credits || []).find((credit) => credit.role === "conductor" && compact(credit.personId))?.personId || "",
    );
  }
  if (form.elements.orchestraPersonId) {
    form.elements.orchestraPersonId.value = compact(
      (credits || []).find((credit) => credit.role === "orchestra" && compact(credit.personId))?.personId || "",
    );
  }
};

const renderRecordingCreditEditor = (form, credits = parseRecordingCreditLines(form?.elements?.credits?.value || "")) => {
  const host = getRecordingCreditRowsHost(form);
  if (!host) {
    return;
  }
  host.innerHTML = (credits || []).length
    ? credits.map((credit, index) => buildRecordingCreditRowHtml(credit, index)).join("")
    : '<p class="owner-empty">当前没有参与署名。可通过上方快捷字段或“新增署名行”添加。</p>';
};

const syncRecordingCreditsField = (form, credits, { rerender = true } = {}) => {
  if (!form?.elements?.credits) {
    return;
  }
  const normalizedCredits = (credits || [])
    .map((credit) => ({
      role: compact(credit?.role),
      personId: compact(credit?.personId || ""),
      displayName: compact(credit?.displayName || buildRecordingCreditDisplayName(credit?.role, credit?.personId, "")),
    }))
    .filter((credit) => credit.role && credit.displayName);
  form.elements.credits.value = serializeRecordingCredits(normalizedCredits);
  syncPrimaryRecordingCreditSelects(form, normalizedCredits);
  if (rerender) {
    renderRecordingCreditEditor(form, normalizedCredits);
  }
};

const readRecordingCreditsFromEditor = (form) => {
  const rows = [...(getRecordingCreditRowsHost(form)?.querySelectorAll?.("[data-recording-credit-row]") || [])];
  if (!rows.length) {
    return parseRecordingCreditLines(form?.elements?.credits?.value || "");
  }
  return rows
    .map((row) => {
      const role = compact(row.querySelector("[data-recording-credit-role]")?.value || "");
      const personId = compact(row.querySelector("[data-recording-credit-person-id]")?.value || "");
      const displayInput = row.querySelector("[data-recording-credit-display]");
      const displayName = compact(displayInput?.value || buildRecordingCreditDisplayName(role, personId, ""));
      return role && displayName ? { role, personId, displayName } : null;
    })
    .filter(Boolean);
};

const upsertPrimaryRecordingCredit = (form, role, personId) => {
  const credits = readRecordingCreditsFromEditor(form);
  const nextPersonId = compact(personId || "");
  const firstIndex = credits.findIndex((credit) => credit.role === role);
  if (!nextPersonId) {
    if (firstIndex >= 0) {
      credits.splice(firstIndex, 1);
    }
    syncRecordingCreditsField(form, credits);
    return;
  }
  const nextCredit = {
    role,
    personId: nextPersonId,
    displayName: buildRecordingCreditDisplayName(role, nextPersonId, ""),
  };
  if (firstIndex >= 0) {
    credits[firstIndex] = nextCredit;
  } else {
    credits.unshift(nextCredit);
  }
  syncRecordingCreditsField(form, credits);
};

const renderIssues = () => {
  issueCount.textContent = `${state.dataIssues.length} 条`;
  issuesPanel.innerHTML =
    state.dataIssues
      .slice(0, 12)
      .map(
        (issue) => `
          <button
            type="button"
            class="owner-issue-item owner-issue-item--jump"
            data-issue-entity-type="${escapeHtml(issue.entityType)}"
            data-issue-entity-id="${escapeHtml(issue.entityId)}"
          >
            <span class="owner-pill">${escapeHtml(issue.category)}</span>
            <p>${escapeHtml(issue.message)}</p>
          </button>`,
      )
      .join("") || '<p class="owner-empty">当前未发现明显规范问题。</p>';
};

const formatDateTime = (value) => {
  const normalized = compact(value);
  if (!normalized) {
    return "未构建";
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return normalized;
  }
  return parsed.toLocaleString("zh-CN", { hour12: false });
};

const renderLibraryStatus = () => {
  if (!libraryStatus) {
    return;
  }
  const meta = state.libraryMeta;
  if (!meta?.manifest) {
    libraryStatus.textContent = "\u5f53\u524d\u6d3b\u52a8\u5e93\u4fe1\u606f\u5c1a\u672a\u52a0\u8f7d\u3002";
    return;
  }
  const counts = meta.counts || {};
  const summary = [
    meta.manifest.libraryName || "\u672a\u547d\u540d\u5e93",
    Number.isFinite(Number(counts.total)) ? `\u5171 ${counts.total} \u6761` : "",
    meta.lastBuiltAt ? `\u6700\u8fd1\u6784\u5efa ${formatDateTime(meta.lastBuiltAt)}` : "",
  ].filter(Boolean);
  libraryStatus.textContent = summary.join(" / ");
};

const refreshOverview = () => {
  if (!state.library) {
    return;
  }
  const composers = [...getManagedComposers()].sort((a, b) => buildComposerOptionLabel(a).localeCompare(buildComposerOptionLabel(b), "zh-Hans-CN"));
  const stats = [
    ["作曲家", composers.length],
    ["人物", state.library.people.length],
    ["作品", state.library.works.length],
    ["版本", state.library.recordings.length],
    ["指挥", getConductors().length],
    ["乐团", getOrchestras().length],
    ["艺术家", getArtists().length],
    ["自动检查 run", state.runs.length],
  ];
  statsPanel.innerHTML = stats.map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd>`).join("");
  renderIssues();

  const works = [...state.library.works].sort((a, b) => getWorkDisplayLabel(a).localeCompare(getWorkDisplayLabel(b), "zh-Hans-CN"));
  const recordings = [...state.library.recordings].sort((a, b) => a.title.localeCompare(b.title, "zh-Hans-CN"));

  populateOptions(document.querySelector('[data-entity-form="work"] select[name="composerId"]'), composers, buildComposerOptionLabel);
  populateRecordingComposerOptions(document.querySelector('[data-entity-form="recording"] select[name="selectedComposerId"]'));
  populateOptions(document.querySelector('[data-entity-form="recording"] select[name="conductorPersonId"]'), getConductors(), buildComposerOptionLabel);
  populateOptions(document.querySelector('[data-entity-form="recording"] select[name="orchestraPersonId"]'), getOrchestras(), buildComposerOptionLabel);
  const recordingForm = document.querySelector('[data-entity-form="recording"]');
  populateRecordingWorkOptions(recordingForm, recordingForm?.elements?.selectedComposerId?.value || "", recordingForm?.elements?.workId?.value || "");
  if (recordingForm?.elements?.credits) {
    syncRecordingCreditsField(recordingForm, parseRecordingCreditLines(recordingForm.elements.credits.value));
  }
  populateRecordingComposerOptions(batchComposerSelect);
  populateWorkSelectOptions(batchWorkSelect, batchComposerSelect?.value || "", batchWorkSelect?.value || "", {
    ready: "请选择批量作品",
    empty: "请先选择批量作曲家",
  });
  populateOptions(automationCheckForm?.elements.composerId, composers, buildComposerOptionLabel);
  populateOptions(automationCheckForm?.elements.workId, works, getWorkDisplayLabel);
  populateOptions(automationCheckForm?.elements.conductorId, getConductors(), buildComposerOptionLabel);
  populateOptions(automationCheckForm?.elements.artistId, getArtists(), (item) => `${buildComposerOptionLabel(item)} (${item.roles.join("/")})`);
  populateOptions(automationCheckForm?.elements.orchestraId, getOrchestras(), buildComposerOptionLabel);
  populateOptions(automationCheckForm?.elements.recordingId, recordings, (item) => item.title);
  populateArticleOptions();
};

const fillSiteForm = (site) => {
  if (!siteForm) {
    return;
  }
  siteForm.elements.title.value = site?.title || "";
  siteForm.elements.subtitle.value = site?.subtitle || "";
  siteForm.elements.description.value = site?.description || "";
  siteForm.elements.heroIntro.value = site?.heroIntro || "";
  siteForm.elements.composerDirectoryIntro.value = site?.composerDirectoryIntro || "";
  siteForm.elements.conductorDirectoryIntro.value = site?.conductorDirectoryIntro || "";
  siteForm.elements.searchIntro.value = site?.searchIntro || "";
  siteForm.elements.about.value = (site?.about || []).join("\n");
  siteForm.elements.contactLabel.value = site?.contact?.label || "";
  siteForm.elements.contactValue.value = site?.contact?.value || "";
  siteForm.elements.copyrightNotice.value = site?.copyrightNotice || "";
};

const fillNamedEntityForm = (form, entity) => {
  form.elements.existingId.value = entity?.id || "";
  form.elements.name.value = entity?.name || "";
  form.elements.nameLatin.value = entity?.nameLatin || "";
  form.elements.country.value = buildCountryInputValue(entity);
  form.elements.birthYear.value = entity?.birthYear || "";
  form.elements.deathYear.value = entity?.deathYear || "";
  if (form.elements.lifeRange) {
    form.elements.lifeRange.value = buildLifeRangeValue(entity);
  }
  form.elements.avatarSrc.value = entity?.avatarSrc || "";
  form.elements.imageSourceUrl.value = entity?.imageSourceUrl || "";
  form.elements.imageSourceKind.value = entity?.imageSourceKind || "";
  form.elements.imageAttribution.value = entity?.imageAttribution || "";
  form.elements.imageUpdatedAt.value = entity?.imageUpdatedAt || "";
  form.elements.slug.value = entity?.slug || "";
  form.elements.sortKey.value = entity?.sortKey || "";
  form.elements.aliases.value = (entity?.aliases || []).join("\n");
  form.elements.summary.value = entity?.summary || "";
  renderInfoPanelState(form, entity?.infoPanel);
  resetMergeControlsState(form);
  renderMergeControls(form);
  const deleteButton = form.querySelector('[data-action="delete"]');
  if (deleteButton) {
    deleteButton.hidden = !entity?.id;
  }
  renderFormImagePreview(form.dataset.entityForm, entity);
};

const fillComposerForm = (form, entity) => {
  fillNamedEntityForm(form, entity);
  [...form.querySelectorAll('input[name="roles"]')].forEach((input) => {
    input.checked = entity?.roles?.includes(input.value) || input.value === "composer";
  });
};

const fillPersonForm = (form, entity) => {
  fillNamedEntityForm(form, entity);
  [...form.querySelectorAll('input[name="roles"]')].forEach((input) => {
    input.checked = entity?.roles?.includes(input.value) || false;
  });
};

const fillWorkForm = (form, entity) => {
  const preferredSlugSource = buildWorkSlugSource(entity?.title, entity?.titleLatin, entity?.catalogue);
  const preferredSlug = createSlugLike(preferredSlugSource || entity?.title || "");
  const legacySlug = createSlugLike(entity?.title || "");
  form.elements.existingId.value = entity?.id || "";
  form.elements.composerId.value = entity?.composerId || "";
  form.elements.title.value = entity?.title || "";
  form.elements.titleLatin.value = entity?.titleLatin || "";
  form.elements.catalogue.value = entity?.catalogue || "";
  form.elements.groupPath.value = deriveGroupPath(entity).join(" / ");
  form.elements.slug.value = !entity?.slug || entity.slug === legacySlug ? preferredSlug : entity.slug;
  form.elements.sortKey.value = entity?.sortKey || "";
  form.elements.aliases.value = (entity?.aliases || []).join("\n");
  form.elements.summary.value = entity?.summary || "";
  renderInfoPanelState(form, entity?.infoPanel);
  resetMergeControlsState(form);
  renderMergeControls(form);
  const deleteButton = form.querySelector('[data-action="delete"]');
  if (deleteButton) {
    deleteButton.hidden = !entity?.id;
  }
};

const fillRecordingForm = (form, entity) => {
  form.elements.existingId.value = entity?.id || "";
  const selectedComposerId = getRecordingComposerIdFromWork(entity?.workId || "");
  if (form.elements.selectedComposerId) {
    form.elements.selectedComposerId.value = selectedComposerId;
  }
  populateRecordingWorkOptions(form, selectedComposerId, entity?.workId || "");
  if (form.elements.workTypeHint) {
    form.elements.workTypeHint.value = entity?.workTypeHint || "unknown";
  }
  if (form.elements.conductorPersonId) {
    form.elements.conductorPersonId.value = entity?.credits?.find((credit) => credit.role === "conductor")?.personId || "";
  }
  if (form.elements.orchestraPersonId) {
    form.elements.orchestraPersonId.value = entity?.credits?.find((credit) => credit.role === "orchestra")?.personId || "";
  }
  form.elements.title.value = entity?.title || "";
  form.elements.slug.value = entity?.slug || "";
  form.elements.sortKey.value = entity?.sortKey || "";
  form.elements.isPrimaryRecommendation.checked = Boolean(entity?.isPrimaryRecommendation);
  if (form.elements.eventMeta) {
    form.elements.eventMeta.value = buildRecordingEventMetaValue(entity);
  }
  form.elements.performanceDateText.value = entity?.performanceDateText || "";
  form.elements.venueText.value = entity?.venueText || "";
  form.elements.albumTitle.value = entity?.albumTitle || "";
  form.elements.label.value = entity?.label || "";
  form.elements.releaseDate.value = entity?.releaseDate || "";
  form.elements.notes.value = entity?.notes || "";
  form.elements.images.value = formatLines(entity?.images || [], formatRecordingImageLine);
  syncRecordingCreditsField(form, entity?.credits || []);
  syncRecordingLinksField(form, entity?.links || []);
  renderInfoPanelState(form, entity?.infoPanel);
  const deleteButton = form.querySelector('[data-action="delete"]');
  if (deleteButton) {
    deleteButton.hidden = !entity?.id;
  }
  renderFormImagePreview("recording", entity);
};

const formFillers = {
  composer: fillComposerForm,
  person: fillPersonForm,
  work: fillWorkForm,
  recording: fillRecordingForm,
};

const buildSitePayload = () => ({
  title: compact(siteForm.elements.title.value),
  subtitle: compact(siteForm.elements.subtitle.value),
  description: compact(siteForm.elements.description.value),
  heroIntro: compact(siteForm.elements.heroIntro.value),
  composerDirectoryIntro: compact(siteForm.elements.composerDirectoryIntro.value),
  conductorDirectoryIntro: compact(siteForm.elements.conductorDirectoryIntro.value),
  searchIntro: compact(siteForm.elements.searchIntro.value),
  about: parseLines(siteForm.elements.about.value, (line) => line),
  contact: {
    label: compact(siteForm.elements.contactLabel.value),
    value: compact(siteForm.elements.contactValue.value),
  },
  copyrightNotice: compact(siteForm.elements.copyrightNotice.value),
});

const buildNamedEntityPayload = (form) => ({
  id: compact(form.elements.existingId.value) || undefined,
  name: compact(form.elements.name.value),
  nameLatin: compact(form.elements.nameLatin.value),
  country: compact(form.elements.country.value),
  birthYear: parseIntegerString(parseLifeRangeInput(form.elements.lifeRange?.value || "").birthYear || form.elements.birthYear.value),
  deathYear: parseIntegerString(parseLifeRangeInput(form.elements.lifeRange?.value || "").deathYear || form.elements.deathYear.value),
  avatarSrc: compact(form.elements.avatarSrc.value),
  imageSourceUrl: compact(form.elements.imageSourceUrl.value),
  imageSourceKind: compact(form.elements.imageSourceKind.value),
  imageAttribution: compact(form.elements.imageAttribution.value),
  imageUpdatedAt: compact(form.elements.imageUpdatedAt.value),
  slug: compact(form.elements.slug.value),
  sortKey: compact(form.elements.sortKey.value),
  aliases: parseLines(form.elements.aliases.value, (line) => line),
  summary: compact(form.elements.summary.value),
  infoPanel: {
    text: compact(form.elements.infoPanelText.value),
    articleId: compact(form.elements.infoPanelArticleId.value),
    collectionLinks: readInfoPanelLinksFromForm(form),
  },
});

const buildComposerPayload = (form) => ({
  ...buildNamedEntityPayload(form),
  roles: [...form.querySelectorAll('input[name="roles"]:checked')].map((input) => input.value),
});

const buildPersonPayload = (form) => ({
  ...buildNamedEntityPayload(form),
  roles: [...form.querySelectorAll('input[name="roles"]:checked')].map((input) => input.value),
});

const buildWorkPayload = (form) => {
  const title = compact(form.elements.title.value);
  const titleLatin = compact(form.elements.titleLatin.value);
  const catalogue = compact(form.elements.catalogue.value);
  const preservedGroupPath = compact(form.elements.groupPath.value)
    .split("/")
    .map((item) => item.trim())
    .filter(Boolean);

  return {
    id: compact(form.elements.existingId.value) || undefined,
    composerId: compact(form.elements.composerId.value),
    title,
    titleLatin,
    catalogue,
    groupPath: preservedGroupPath.length ? preservedGroupPath : buildInferredWorkGroupPath({ title, titleLatin, catalogue }),
    slug: compact(form.elements.slug.value) || createSlugLike(buildWorkSlugSource(title, titleLatin, catalogue)),
    sortKey: compact(form.elements.sortKey.value),
    aliases: parseLines(form.elements.aliases.value, (line) => line),
    summary: compact(form.elements.summary.value),
    infoPanel: {
      text: compact(form.elements.infoPanelText.value),
      articleId: compact(form.elements.infoPanelArticleId.value),
      collectionLinks: readInfoPanelLinksFromForm(form),
    },
  };
};

const buildRecordingPayload = (form) => ({
  ...parseRecordingEventMetaInput(form.elements.eventMeta?.value || ""),
  id: compact(form.elements.existingId.value) || undefined,
  workId: compact(form.elements.workId.value),
  workTypeHint: compact(form.elements.workTypeHint?.value || "unknown"),
  conductorPersonId: compact(form.elements.conductorPersonId?.value || ""),
  orchestraPersonId: compact(form.elements.orchestraPersonId?.value || ""),
  title: compact(form.elements.title.value),
  slug: compact(form.elements.slug.value),
  sortKey: compact(form.elements.sortKey.value),
  isPrimaryRecommendation: form.elements.isPrimaryRecommendation.checked,
  performanceDateText:
    parseRecordingEventMetaInput(form.elements.eventMeta?.value || "").performanceDateText || compact(form.elements.performanceDateText.value),
  venueText: parseRecordingEventMetaInput(form.elements.eventMeta?.value || "").venueText || compact(form.elements.venueText.value),
  albumTitle: compact(form.elements.albumTitle.value),
  label: compact(form.elements.label.value),
  releaseDate: compact(form.elements.releaseDate.value),
  notes: compact(form.elements.notes.value),
  images: parseRecordingImageLines(form.elements.images.value),
  credits: readRecordingCreditsFromEditor(form),
  links: parseLines(form.elements.links.value, (line) => {
    const [platform, url, title = ""] = line.split("|").map((part) => part.trim());
    if (!platform || !url) {
      return null;
    }
    return { platform, url, title };
  }),
  infoPanel: {
    text: compact(form.elements.infoPanelText.value),
    articleId: compact(form.elements.infoPanelArticleId.value),
    collectionLinks: readInfoPanelLinksFromForm(form),
  },
});

const getActiveRecordingForm = () => document.querySelector('[data-entity-form="recording"]');
const resolveRecordingLinkHost = (target) =>
  target?.closest?.("[data-recording-link-host], [data-info-panel-link-host]") || target?.closest?.("form") || null;
const resolveInfoPanelForm = (host) => (host?.elements ? host : host?.closest?.("form")) || null;
const getRecordingLinksField = (host) =>
  host?.elements?.links ||
  host?.querySelector?.('[name="links"], [data-inline-entity-field="links"], [data-batch-draft-field="links"], [data-proposal-link-input]');
const getInfoPanelLinksField = (host) =>
  resolveInfoPanelForm(host)?.elements?.infoPanelCollectionLinks || host?.querySelector?.('[name="infoPanelCollectionLinks"]');
const getStructuredLinkMode = (host) =>
  host?.matches?.("[data-info-panel-link-host]") || (!getRecordingLinksField(host) && getInfoPanelLinksField(host)) ? "info-panel" : "recording";
const readRecordingLinksFromHost = (host) =>
  getStructuredLinkMode(host) === "info-panel"
    ? parseStructuredLinks(getInfoPanelLinksField(host)?.value || "")
    : parseBatchLinkLines(getRecordingLinksField(host)?.value || "");

const resetRecordingLinkDialogButtons = () => {
  linkDialogSave.textContent = "保存链接";
  linkDialogDelete.textContent = "删除链接";
};

const setLinkDialogMeta = (details = []) => {
  if (!linkDialogMeta) {
    return;
  }
  linkDialogMeta.textContent = details.join(" / ");
  linkDialogMeta.hidden = !linkDialogMeta.textContent;
};

const resetLinkDialogMeta = () => {
  setLinkDialogMeta([]);
};

const syncLinkDialogFieldState = () => {
  const linkType = compact(linkDialogForm?.elements?.linkType?.value || "external") === "local" ? "local" : "external";
  const urlField = linkDialogForm?.querySelector?.('[data-link-dialog-field="url"]');
  const localPathField = linkDialogForm?.querySelector?.('[data-link-dialog-field="localPath"]');
  const browseButton = linkDialogForm?.querySelector?.('[data-link-dialog-action="browse-local"]');
  const visibilityInput = linkDialogForm?.elements?.visibility;
  if (urlField) {
    urlField.hidden = linkType === "local";
  }
  if (localPathField) {
    localPathField.hidden = linkType !== "local";
  }
  if (browseButton) {
    browseButton.hidden = linkType !== "local";
  }
  if (visibilityInput && linkType === "local") {
    visibilityInput.value = "local-only";
  }
  if (linkDialogOpen) {
    linkDialogOpen.textContent = linkType === "local" ? "打开文件" : "打开链接";
  }
};

const fillRecordingLinkDialog = (link) => {
  const normalized = normalizeStructuredLink(link);
  linkDialogForm.elements.platform.value = normalized.platform || "";
  linkDialogForm.elements.url.value = normalized.url || "";
  if (linkDialogForm.elements.localPath) {
    linkDialogForm.elements.localPath.value = normalized.localPath || "";
  }
  if (linkDialogForm.elements.linkType) {
    linkDialogForm.elements.linkType.value = normalized.linkType;
  }
  if (linkDialogForm.elements.visibility) {
    linkDialogForm.elements.visibility.value = normalized.visibility;
  }
  linkDialogForm.elements.title.value = normalized.title || "";
  syncLinkDialogFieldState();
};

const readRecordingLinkDialogValue = () =>
  normalizeStructuredLink({
    platform: compact(linkDialogForm.elements.platform.value),
    url: compact(linkDialogForm.elements.url.value),
    localPath: compact(linkDialogForm.elements.localPath?.value),
    title: compact(linkDialogForm.elements.title.value),
    linkType: compact(linkDialogForm.elements.linkType?.value || "external"),
    visibility: compact(linkDialogForm.elements.visibility?.value || "public"),
  });

const openManagedResourceLink = async (link) => {
  const normalized = normalizeStructuredLink(link);
  if (normalized.linkType === "local") {
    return fetchJson("/api/open-resource", {
      method: "POST",
      body: JSON.stringify(normalized),
    });
  }
  if (normalized.url) {
    window.open(normalized.url, "_blank", "noopener,noreferrer");
  }
  return { opened: Boolean(normalized.url) };
};

const pickLocalResourcePath = async () => {
  if (typeof desktopLauncher?.pickLocalResourceFile === "function") {
    const result = await desktopLauncher.pickLocalResourceFile();
    return compact(result?.path || "");
  }
  return compact(window.prompt("请输入本地文件完整路径。") || "");
};

const removeProposalLinkCandidateRow = (row) => {
  const candidateContainer = row?.closest?.(".owner-proposal__candidate-links");
  row?.remove?.();
  if (!candidateContainer?.querySelector?.(".owner-proposal__candidate-link-row")) {
    candidateContainer?.remove?.();
  }
};

const openProposalLinkCandidateDialog = (button) => {
  const host = resolveRecordingLinkHost(button);
  if (!host) {
    return;
  }
  const candidate = {
    runId: compact(button.dataset.proposalLinkCandidateRunId),
    proposalId: compact(button.dataset.proposalLinkCandidateProposalId),
    platform: compact(button.dataset.proposalLinkCandidatePlatform),
    url: compact(button.dataset.proposalLinkCandidateUrl),
    title: compact(button.dataset.proposalLinkCandidateTitle),
    sourceLabel: compact(button.dataset.proposalLinkCandidateSourceLabel),
    confidence: compact(button.dataset.proposalLinkCandidateConfidence),
    row: button.closest(".owner-proposal__candidate-link-row"),
  };
  state.recordingLinkDialogContext = {
    form: host,
    index: -1,
    mode: "recording",
    candidate,
  };
  linkDialogTitle.textContent = "确认候选资源链接";
  fillRecordingLinkDialog({
    platform: candidate.platform || "",
    url: candidate.url || "",
    localPath: "",
    title: candidate.title || "",
    linkType: "external",
    visibility: "public",
  });
  linkDialogSave.textContent = "确认添加候选链接";
  linkDialogDelete.textContent = "放弃候选链接";
  linkDialogDelete.hidden = false;
  setLinkDialogMeta(
    [
      candidate.sourceLabel ? `候选来源：${candidate.sourceLabel}` : "",
      candidate.confidence ? `置信度 ${candidate.confidence}` : "",
    ].filter(Boolean),
  );
  linkDialog.showModal();
};

const openRecordingLinkDialog = (host, index) => {
  const links = readRecordingLinksFromHost(host);
  const link = links[index] || { platform: "", url: "", localPath: "", title: "", linkType: "external", visibility: "public" };
  const mode = getStructuredLinkMode(host);
  resetRecordingLinkDialogButtons();
  resetLinkDialogMeta();
  state.recordingLinkDialogContext = { form: host, index, mode };
  linkDialogTitle.textContent =
    index >= 0
      ? `${mode === "info-panel" ? "编辑导览链接" : "编辑资源链接"}：${buildRecordingLinkChipLabel(link, index, links)}`
      : mode === "info-panel"
        ? "新增导览链接"
        : "新增资源链接";
  fillRecordingLinkDialog(link);
  linkDialogDelete.hidden = index < 0;
  linkDialog.showModal();
};

const saveRecordingLinkDialog = () => {
  const { form: host, index, mode, candidate } = state.recordingLinkDialogContext;
  if (!host) {
    return;
  }
  const dialogValue = readRecordingLinkDialogValue();
  void dialogValue;
  if (!platform || !url) {
    throw new Error("平台和 URL 不能为空。");
  }
  const links = readRecordingLinksFromHost(host);
  const nextLinks = [...links];
  const nextLink = { platform, url, title };
  if (index >= 0) {
    nextLinks[index] = nextLink;
  } else {
    nextLinks.push(nextLink);
  }
  if (mode === "info-panel") {
    const form = resolveInfoPanelForm(host);
    renderInfoPanelState(form, {
      text: form?.elements?.infoPanelText?.value,
      articleId: form?.elements?.infoPanelArticleId?.value,
      collectionLinks: nextLinks,
    });
  } else {
    syncRecordingLinksField(host, nextLinks);
    syncRecordingLinkHostState(host);
  }
  if (candidate?.url) {
    dismissProposalLinkCandidate(candidate.runId, candidate.proposalId, candidate.url);
    removeProposalLinkCandidateRow(candidate.row);
    setResult(`已接受候选链接：${candidate.url}`);
  }
  linkDialog.close();
};

const deleteRecordingLinkDialog = () => {
  const { form: host, index, mode, candidate } = state.recordingLinkDialogContext;
  if (candidate?.url) {
    dismissProposalLinkCandidate(candidate.runId, candidate.proposalId, candidate.url);
    removeProposalLinkCandidateRow(candidate.row);
    linkDialog.close();
    setResult(`已放弃候选链接：${candidate.url}`);
    return;
  }
  if (!host || index < 0) {
    return;
  }
  const links = readRecordingLinksFromHost(host);
  const nextLinks = links.filter((_, currentIndex) => currentIndex !== index);
  if (mode === "info-panel") {
    const form = resolveInfoPanelForm(host);
    renderInfoPanelState(form, {
      text: form?.elements?.infoPanelText?.value,
      articleId: form?.elements?.infoPanelArticleId?.value,
      collectionLinks: nextLinks,
    });
  } else {
    syncRecordingLinksField(host, nextLinks);
    syncRecordingLinkHostState(host);
  }
  linkDialog.close();
};

const commitRecordingLinkDialog = () => {
  const { form: host, index, mode, candidate } = state.recordingLinkDialogContext;
  if (!host) {
    return;
  }
  const nextLink = readRecordingLinkDialogValue();
  if (!nextLink.platform) {
    throw new Error("平台不能为空。");
  }
  if (nextLink.linkType === "local" && !nextLink.localPath) {
    throw new Error("本地文件路径不能为空。");
  }
  if (nextLink.linkType !== "local" && !nextLink.url) {
    throw new Error("URL 不能为空。");
  }
  const links = readRecordingLinksFromHost(host);
  const nextLinks = [...links];
  if (index >= 0) {
    nextLinks[index] = nextLink;
  } else {
    nextLinks.push(nextLink);
  }
  if (mode === "info-panel") {
    const form = resolveInfoPanelForm(host);
    renderInfoPanelState(form, {
      text: form?.elements?.infoPanelText?.value,
      articleId: form?.elements?.infoPanelArticleId?.value,
      collectionLinks: nextLinks,
    });
  } else {
    syncRecordingLinksField(host, nextLinks);
    syncRecordingLinkHostState(host);
  }
  if (candidate?.url) {
    dismissProposalLinkCandidate(candidate.runId, candidate.proposalId, candidate.url);
    removeProposalLinkCandidateRow(candidate.row);
    setResult(`已接受候选链接：${candidate.url}`);
  }
  linkDialog.close();
};

const payloadBuilders = {
  composer: buildComposerPayload,
  person: buildPersonPayload,
  work: buildWorkPayload,
  recording: buildRecordingPayload,
};

const formatBatchCreditLines = (items) =>
  formatLines(items || [], (item) => [item?.role || "", item?.displayName || "", item?.personId || ""].join(" | "));
const parseBatchCreditLines = (value) =>
  parseLines(value, (line) => {
    const [role, displayName = "", personId = ""] = parsePipeLine(line);
    if (!role || !displayName) {
      return null;
    }
    return { role, displayName, personId };
  });
const formatBatchLinkLines = (items) =>
  formatLines(items || [], (item) => buildResourceLinkLine(item));
const parseBatchLinkLines = (value) =>
  parseLines(value, (line) => {
    const [platform, target = "", title = "", linkType = "", visibility = ""] = parsePipeLine(line);
    const normalized = normalizeStructuredLink({
      platform,
      title,
      url: compact(linkType) === "local" ? "" : target,
      localPath: compact(linkType) === "local" ? target : "",
      linkType,
      visibility,
    });
    if (!normalized.platform || !getResourceLinkTarget(normalized)) {
      return null;
    }
    return normalized;
  });
const renderRecordingLinksEditor = (host, links) => {
  const container = host?.querySelector?.("[data-recording-links-editor]");
  if (!container) {
    return;
  }
  container.innerHTML = buildRecordingLinkEditorHtml(links);
};
const syncRecordingLinksField = (host, links) => {
  const field = getRecordingLinksField(host);
  if (!field) {
    return;
  }
  field.value = formatBatchLinkLines(links || []);
  renderRecordingLinksEditor(host, links || []);
};
const syncRecordingLinkHostState = (host) => {
  const field = getRecordingLinksField(host);
  if (!field) {
    return;
  }
  if (field.dataset.batchDraftType) {
    readBatchDraftEntities();
    return;
  }
  if (field.dataset.proposalFieldInput) {
    const run = getReviewRunById(reviewRunSelect.value || state.activeRunId);
    const proposal = run?.proposals?.find((item) => item.id === field.dataset.proposalFieldInput);
    if (proposal) {
      rememberProposalDraftFromForm(proposal, { runId: run.id });
      field.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
    }
    return;
  }
  if (field.dataset.batchProposalFieldInput && state.batchSession?.runId) {
    const proposal = state.batchSession.run?.proposals?.find((item) => item.id === field.dataset.batchProposalFieldInput);
    if (proposal) {
      rememberProposalDraftFromForm(proposal, {
        mode: "batch",
        runId: state.batchSession.runId,
        library: state.batchSession.draftLibrary,
      });
      field.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
    }
    return;
  }
  if (field.dataset.inlineProposalFieldInput && state.inlineCheck.runId) {
    const run = getReviewRunById(state.inlineCheck.runId);
    const proposal = run?.proposals?.find((item) => item.id === field.dataset.inlineProposalFieldInput);
    if (proposal) {
      rememberProposalDraftFromForm(proposal, {
        inline: true,
        runId: state.inlineCheck.runId,
      });
      field.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
    }
  }
};
const cloneBatchDraftEntities = (draftEntities) =>
  structuredClone(
    draftEntities || {
      composers: [],
      people: [],
      works: [],
      recordings: [],
    },
  );
const batchDraftFieldConfigs = {
  composer: [
    { field: "name", label: "中文全名" },
    { field: "nameLatin", label: "英文 / 原文全名" },
    { field: "country", label: "国家" },
    { field: "aliases", label: "别名（含简称 / 缩写）", multiline: true, rows: 3 },
    { field: "summary", label: "简介", multiline: true, rows: 4 },
  ],
  person: [
    { field: "name", label: "中文全名" },
    { field: "nameLatin", label: "英文 / 原文全名" },
    { field: "country", label: "国家" },
    { field: "roles", label: "角色（每行一个）", multiline: true, rows: 3 },
    { field: "aliases", label: "别名（含简称 / 缩写）", multiline: true, rows: 3 },
    { field: "summary", label: "简介", multiline: true, rows: 4 },
  ],
  work: [
    { field: "composerId", label: "所属作曲家" },
    { field: "title", label: "作品标题" },
    { field: "titleLatin", label: "英文 / 原文标题" },
    { field: "catalogue", label: "作品号 / Catalogue" },
    { field: "aliases", label: "别名", multiline: true, rows: 3 },
    { field: "summary", label: "简介", multiline: true, rows: 4 },
  ],
  recording: [
    { field: "selectedComposerId", label: "所属作曲家" },
    { field: "workId", label: "所属作品" },
    { field: "title", label: "版本标题" },
    { field: "performanceDateText", label: "演出时间" },
    { field: "venueText", label: "地点" },
    { field: "albumTitle", label: "专辑名称" },
    { field: "label", label: "发行商" },
    { field: "releaseDate", label: "发行日期" },
    { field: "notes", label: "备注", multiline: true, rows: 4 },
    { field: "credits", label: "演出信息（role | displayName | personId）", multiline: true, rows: 4 },
    { field: "links", label: "资源链接（platform | target | title | linkType | visibility）", multiline: true, rows: 4 },
  ],
};
const getBatchDraftFieldValue = (entryType, entity, field, entry = null) => {
  if (entryType === "recording" && field === "selectedComposerId") {
    return compact(entry?.draftState?.selectedComposerId || getRecordingComposerIdFromWork(entity?.workId || ""));
  }
  const currentValue = entity?.[field];
  if (field === "aliases" || field === "roles") {
    return Array.isArray(currentValue) ? currentValue.join("\n") : "";
  }
  if (field === "credits") {
    return formatBatchCreditLines(currentValue);
  }
  if (field === "links") {
    return formatBatchLinkLines(currentValue);
  }
  return compact(currentValue);
};
const applyBatchDraftFieldValue = (entryType, entry, field, rawValue) => {
  const entity = entry?.entity;
  if (!entity) {
    return;
  }
  if (entryType === "recording" && field === "selectedComposerId") {
    const selectedComposerId = compact(rawValue);
    entry.draftState = {
      ...(entry.draftState || {}),
      selectedComposerId,
    };
    if (!selectedComposerId) {
      entity.workId = "";
      return;
    }
    const currentWork = getWorkById(entity.workId);
    if (currentWork?.composerId !== selectedComposerId) {
      entity.workId = "";
    }
    return;
  }
  if (entryType === "recording" && field === "workId") {
    entity[field] = compact(rawValue);
    const currentWork = getWorkById(entity.workId);
    entry.draftState = {
      ...(entry.draftState || {}),
      selectedComposerId: currentWork?.composerId || compact(entry?.draftState?.selectedComposerId || ""),
    };
    return;
  }
  if (field === "aliases" || field === "roles") {
    entity[field] = parseLines(rawValue, (line) => line);
    return;
  }
  if (field === "credits") {
    entity[field] = parseBatchCreditLines(rawValue);
    return;
  }
  if (field === "links") {
    entity[field] = parseBatchLinkLines(rawValue);
    return;
  }
  entity[field] = compact(rawValue);
};
const buildBatchDraftFieldHtml = (entryType, entryIndex, config, value, draftEntities, entry = null) => {
  const entity = entry?.entity || {};
  const baseAttributes = `data-batch-draft-type="${escapeHtml(entryType)}" data-batch-draft-index="${escapeHtml(entryIndex)}" data-batch-draft-field="${escapeHtml(config.field)}"`;
  if (entryType === "recording" && config.field === "selectedComposerId") {
    const composerOptions = buildBatchRelationOptions("work", "composerId", state.library || {}, draftEntities || {}, value)
      .map(
        (option) =>
          `<option value="${escapeHtml(option.value)}" ${option.value === value ? "selected" : ""}>${escapeHtml(option.label || "请选择作曲家")}</option>`,
      )
      .join("");
    return `
      <label class="owner-batch-draft__field">
        <span>${escapeHtml(config.label)}</span>
        <select ${baseAttributes}>${composerOptions}</select>
      </label>`;
  }
  if ((entryType === "work" && config.field === "composerId") || (entryType === "recording" && config.field === "workId")) {
    const normalizedValue = value;
    const selectedComposerId = entryType === "recording" ? getBatchDraftFieldValue(entryType, entity, "selectedComposerId", entry) : "";
    const sourceOptions =
      entryType === "recording" && config.field === "workId"
        ? buildBatchRelationOptions(
            entryType,
            config.field,
            {
              ...(state.library || {}),
              works: (state.library?.works || []).filter((item) => !selectedComposerId || item.composerId === selectedComposerId),
            },
            {
              ...(draftEntities || {}),
              works: (draftEntities?.works || []).filter((item) => !selectedComposerId || item?.entity?.composerId === selectedComposerId),
            },
            normalizedValue,
          )
        : buildBatchRelationOptions(entryType, config.field, state.library || {}, draftEntities || {}, normalizedValue);
    const options = sourceOptions
      .map(
        (option) =>
          `<option value="${escapeHtml(option.value)}" ${option.value === normalizedValue ? "selected" : ""}>${escapeHtml(option.label)}</option>`,
      )
      .join("");
    return `
      <label class="owner-batch-draft__field">
        <span>${escapeHtml(config.label)}</span>
        <select ${baseAttributes} ${entryType === "recording" && config.field === "workId" && !selectedComposerId ? "disabled" : ""}>${options}</select>
      </label>`;
  }
  if (entryType === "recording" && config.field === "links") {
    const links = parseBatchLinkLines(value);
    return `
      <section class="owner-batch-draft__field owner-batch-draft__field--full owner-structured-links" data-recording-link-host>
        <div class="owner-card__header">
          <span>${escapeHtml(config.label)}</span>
          <button type="button" data-recording-link-action="add">新增链接</button>
        </div>
        <input type="hidden" ${baseAttributes} value="${escapeHtml(value)}" />
        <div data-recording-links-editor class="owner-link-editor-list">${buildRecordingLinkEditorHtml(links)}</div>
      </section>`;
  }
  if (config.multiline) {
    return `
      <label class="owner-batch-draft__field owner-batch-draft__field--full">
        <span>${escapeHtml(config.label)}</span>
        <textarea ${baseAttributes} rows="${config.rows || 4}">${escapeHtml(value)}</textarea>
      </label>`;
  }
  return `
    <label class="owner-batch-draft__field">
      <span>${escapeHtml(config.label)}</span>
      <input ${baseAttributes} value="${escapeHtml(value)}" />
    </label>`;
};
const buildBatchDraftEntryHtml = (entryType, entry, index) => {
  const entity = entry?.entity || {};
  const title =
    entityTypeLabels[entryType] === "版本"
      ? entity.title || `版本 ${index + 1}`
      : entity.name || entity.title || `${entityTypeLabels[entryType] || entryType} ${index + 1}`;
  const fieldHtml = (batchDraftFieldConfigs[entryType] || [])
    .map((config) =>
      buildBatchDraftFieldHtml(
        entryType,
        index,
        config,
        getBatchDraftFieldValue(entryType, entity, config.field, entry),
        state.batchDraftEntities || state.batchSession?.draftEntities,
        entry,
      ),
    )
    .join("");
  return `
    <article class="owner-batch-draft">
      <div class="owner-card__header">
        <div>
          <h4>${escapeHtml(title)}</h4>
          <p>${escapeHtml(entry?.sourceLine || "自动生成草稿")}</p>
        </div>
        <span class="owner-pill">${escapeHtml(entityTypeLabels[entryType] || entryType)}</span>
      </div>
      ${
        entry?.notes?.length
          ? `<ul class="owner-batch-draft__notes">${entry.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}</ul>`
          : ""
      }
      <div class="owner-batch-draft__grid">${fieldHtml}</div>
    </article>`;
};

const resetArticleForm = () => {
  articleForm.reset();
  articleForm.elements.existingId.value = "";
  state.activeArticleId = "";
  articleDeleteButton.hidden = true;
  articleDeleteButton.disabled = true;
  articleSaveButton.textContent = "创建专栏";
  articlePreviewStatus.textContent = "等待输入";
  articlePreviewRender.innerHTML = "";
};

const renderArticlePreviewModel = (preview) => {
  if (!preview || preview.isEmpty) {
    articlePreviewRender.innerHTML = '<p class="owner-empty">等待输入。</p>';
    return;
  }
  articlePreviewRender.innerHTML = `
    <article class="owner-article-preview-card">
      <header class="owner-article-preview-card__header">
        ${preview.title ? `<h1>${escapeHtml(preview.title)}</h1>` : ""}
        ${preview.summary ? `<p class="owner-article-preview-card__summary">${escapeHtml(preview.summary)}</p>` : ""}
      </header>
      <div class="article-body owner-article-preview__body">${preview.bodyHtml || "<p>暂无正文。</p>"}</div>
    </article>`;
};

const fillArticleForm = (article) => {
  if (!article) {
    resetArticleForm();
    return;
  }
  articleForm.elements.existingId.value = article.id || "";
  articleForm.elements.title.value = article.title || "";
  articleForm.elements.slug.value = article.slug || "";
  articleForm.elements.summary.value = article.summary || "";
  articleForm.elements.showOnHome.checked = Boolean(article.showOnHome);
  articleForm.elements.markdown.value = article.markdown || "";
  state.activeArticleId = article.id || "";
  articleDeleteButton.hidden = !state.activeArticleId;
  articleDeleteButton.disabled = !state.activeArticleId;
  articleSaveButton.textContent = "保存修改";
};

const buildArticlePayload = () => ({
  id: compact(articleForm.elements.existingId.value) || undefined,
  title: compact(articleForm.elements.title.value),
  slug: compact(articleForm.elements.slug.value) || createSlugLike(compact(articleForm.elements.title.value) || "article"),
  summary: compact(articleForm.elements.summary.value),
  showOnHome: Boolean(articleForm.elements.showOnHome.checked),
  markdown: articleForm.elements.markdown.value || "",
});

const resetEntityForm = (form) => {
  form.reset();
  if (form.elements.existingId) {
    form.elements.existingId.value = "";
  }
  if (form.elements.selectedComposerId) {
    populateRecordingComposerOptions(form.elements.selectedComposerId);
    populateRecordingWorkOptions(form, "", "");
  }
  if (form.elements.mergeTargetId) {
    form.elements.mergeTargetId.value = "";
  }
  resetMergeControlsState(form);
  state.activeEntity = createEmptyActiveEntity();
  renderInfoPanelState(form, emptyInfoPanel());
  const deleteButton = form.querySelector('[data-action="delete"]');
  if (deleteButton) {
    deleteButton.hidden = true;
  }
  renderFormImagePreview(form.dataset.entityForm, null);
  if (form.dataset.entityForm === "recording") {
    syncRecordingLinksField(form, []);
    syncRecordingCreditsField(form, []);
  }
  clearInlineCheck();
  renderMergeControls(form);
  renderRelatedEntityControls(form, []);
};

const clearInlineCheck = () => {
  state.inlineCheck = {
    entityType: "",
    entityId: "",
    jobId: "",
    runId: "",
  };
  inlineCheckPanel.hidden = true;
  inlineCheckTitle.textContent = "当前条目自动检查";
  inlineCheckSubtitle.textContent = "将在当前条目内展示检查进度与候选。";
  inlineCheckContent.innerHTML = "";
  detailCard?.classList.remove("is-inline-check-active");
};

const openInlineCheck = (entityType, entityId, title) => {
  state.inlineCheck = {
    ...state.inlineCheck,
    entityType,
    entityId,
  };
  inlineCheckTitle.textContent = `${title || "当前条目"} · 自动检查`;
  inlineCheckSubtitle.textContent = "检查进度、错误与候选审查会直接显示在此处，不再跳转批量检查页面。";
  inlineCheckPanel.hidden = false;
  detailCard?.classList.add("is-inline-check-active");
};

const resolveInlineCheck = (mode) => {
  setResult({
    inlineCheckResolved: true,
    mode,
    entityType: state.inlineCheck.entityType,
    entityId: state.inlineCheck.entityId,
    runId: state.inlineCheck.runId,
  });
  clearInlineCheck();
};
const buildLifeRangeValue = (entity) => {
  const birth = compact(entity?.birthYear);
  const death = compact(entity?.deathYear);
  if (!birth && !death) {
    return "";
  }
  return `${birth || ""}-${death || ""}`;
};
const getCountryValues = (entity) =>
  [...new Set([...(Array.isArray(entity?.countries) ? entity.countries : []), compact(entity?.country)].filter(Boolean))];
const buildCountryInputValue = (entity) => getCountryValues(entity).join(" / ");
const buildCountrySummaryValue = (entity) => buildCountryInputValue(entity);
const parseLifeRangeInput = (value) => {
  const normalized = compact(value);
  if (!normalized) {
    return { birthYear: "", deathYear: "" };
  }
  const [birth = "", death = ""] = normalized.split("-").map((part) => part.trim());
  return {
    birthYear: birth,
    deathYear: death,
  };
};
const buildRecordingEventMetaValue = (entity) => {
  const dateText = compact(entity?.performanceDateText);
  const venueText = compact(entity?.venueText);
  if (!dateText && !venueText) {
    return "";
  }
  return [dateText, venueText].filter(Boolean).join(" / ");
};
const parseRecordingEventMetaInput = (value) => {
  const normalized = compact(value);
  if (!normalized) {
    return { performanceDateText: "", venueText: "" };
  }
  const parts = normalized
    .split("/")
    .map((part) => compact(part))
    .filter(Boolean);
  if (parts.length <= 1) {
    return {
      performanceDateText: parts[0] || "",
      venueText: "",
    };
  }
  return {
    performanceDateText: parts[0] || "",
    venueText: parts.slice(1).join(" / "),
  };
};
const buildInlineEntityEditorHtml = (entityType, entity) => {
  if (!entity) {
    return "";
  }
  const imageAsset = getEntityImageAsset(entity);
  const imagePreview = imageAsset?.src
    ? `<img src="${escapeHtml(imageAsset.src)}" alt="${escapeHtml(imageAsset.alt || "条目图片")}" />`
    : "<span>暂无图片</span>";
  if (["composer", "person"].includes(entityType)) {
    return `
      <section class="owner-inline-editor">
        <div class="owner-card__header"><h4>手动微调</h4><span>可直接修改字段并上传图片</span></div>
        <div class="owner-inline-editor__grid owner-inline-editor__grid--named">
          <label><span>中文全名</span><input data-inline-entity-field="name" value="${escapeHtml(entity.name || "")}" /></label>
          <label><span>英文 / 原文全名</span><input data-inline-entity-field="nameLatin" value="${escapeHtml(entity.nameLatin || "")}" /></label>
          <label><span>生卒年</span><input data-inline-entity-field="lifeRange" value="${escapeHtml(buildLifeRangeValue(entity))}" placeholder="1824-1896" /></label>
          <label><span>国家 / 地区</span><input data-inline-entity-field="country" value="${escapeHtml(buildCountryInputValue(entity))}" placeholder="Argentina / Israel / Palestine" /></label>
          <label class="owner-inline-editor__field--full"><span>别名（每行一个）</span><textarea data-inline-entity-field="aliases">${escapeHtml((entity.aliases || []).join("\n"))}</textarea></label>
          <label class="owner-inline-editor__field--full"><span>简介</span><textarea data-inline-entity-field="summary">${escapeHtml(entity.summary || "")}</textarea></label>
        </div>
        <aside class="owner-inline-editor__media">
          <div class="owner-image-preview owner-image-preview--large" data-inline-entity-image-preview>${imagePreview}</div>
          <label class="owner-upload-button">
            <span>上传图片</span>
            <input type="file" accept="image/*" data-inline-entity-image-upload hidden />
          </label>
        </aside>
        <div class="owner-inline-check__actions">
          <button type="button" data-inline-run-action="save-entity">保存手动修改</button>
        </div>
      </section>`;
  }
  if (entityType === "work") {
    return `
      <section class="owner-inline-editor">
        <div class="owner-card__header"><h4>手动微调</h4><span>可直接补充作品信息</span></div>
        <div class="owner-inline-editor__grid">
          <label><span>作品标题</span><input data-inline-entity-field="title" value="${escapeHtml(entity.title || "")}" /></label>
          <label><span>英文 / 原文标题</span><input data-inline-entity-field="titleLatin" value="${escapeHtml(entity.titleLatin || "")}" /></label>
          <label><span>作品号 / Catalogue</span><input data-inline-entity-field="catalogue" value="${escapeHtml(entity.catalogue || "")}" /></label>
          <label class="owner-inline-editor__field--full"><span>别名（每行一个）</span><textarea data-inline-entity-field="aliases">${escapeHtml((entity.aliases || []).join("\n"))}</textarea></label>
          <label class="owner-inline-editor__field--full"><span>简介</span><textarea data-inline-entity-field="summary">${escapeHtml(entity.summary || "")}</textarea></label>
        </div>
        <div class="owner-inline-check__actions">
          <button type="button" data-inline-run-action="save-entity">保存手动修改</button>
        </div>
      </section>`;
  }
  if (entityType === "recording") {
    const recordingImage = entity.images?.[0] || null;
    const linksValue = formatBatchLinkLines(entity.links || []);
    return `
      <section class="owner-inline-editor">
        <div class="owner-card__header"><h4>手动微调</h4><span>可直接补充版本信息与图片</span></div>
        <div class="owner-inline-editor__grid owner-inline-editor__grid--recording">
          <label><span>版本标题</span><input data-inline-entity-field="title" value="${escapeHtml(entity.title || "")}" /></label>
          <label class="owner-inline-editor__field--full"><span>演出时间 / 地点</span><input data-inline-entity-field="eventMeta" value="${escapeHtml(buildRecordingEventMetaValue(entity))}" placeholder="1984 / Chicago" /></label>
          <label><span>专辑名称</span><input data-inline-entity-field="albumTitle" value="${escapeHtml(entity.albumTitle || "")}" /></label>
          <label><span>发行商</span><input data-inline-entity-field="label" value="${escapeHtml(entity.label || "")}" /></label>
          <label><span>发行日期</span><input data-inline-entity-field="releaseDate" value="${escapeHtml(entity.releaseDate || "")}" /></label>
          <section class="owner-inline-editor__field--full owner-structured-links" data-recording-link-host>
            <div class="owner-card__header">
              <h4>资源链接</h4>
              <button type="button" data-recording-link-action="add">新增链接</button>
            </div>
            <input type="hidden" data-inline-entity-field="links" value="${escapeHtml(linksValue)}" />
            <div data-recording-links-editor class="owner-link-editor-list">${buildRecordingLinkEditorHtml(entity.links || [])}</div>
          </section>
          <label class="owner-inline-editor__field--full"><span>备注</span><textarea data-inline-entity-field="notes">${escapeHtml(entity.notes || "")}</textarea></label>
        </div>
        <aside class="owner-inline-editor__media">
          <div class="owner-image-preview owner-image-preview--large" data-inline-entity-image-preview>${recordingImage?.src ? `<img src="${escapeHtml(recordingImage.src)}" alt="${escapeHtml(recordingImage.alt || entity.title || "版本图片")}" />` : "<span>暂无图片</span>"}</div>
          <label class="owner-upload-button">
            <span>上传图片</span>
            <input type="file" accept="image/*" data-inline-entity-image-upload hidden />
          </label>
        </aside>
        <div class="owner-inline-check__actions">
          <button type="button" data-inline-run-action="save-entity">保存手动修改</button>
        </div>
      </section>`;
  }
  return "";
};
const buildInlineEntityPayload = (entityType, container) => {
  const entity = getEntityByTypeAndId(entityType, state.inlineCheck.entityId);
  if (!entity || !container) {
    throw new Error("当前条目不存在，无法保存修改。");
  }
  if (["composer", "person"].includes(entityType)) {
    const lifeRange = parseLifeRangeInput(container.querySelector('[data-inline-entity-field="lifeRange"]')?.value || "");
    const payload = {
      ...entity,
      name: compact(container.querySelector('[data-inline-entity-field="name"]')?.value || entity.name || ""),
      nameLatin: compact(container.querySelector('[data-inline-entity-field="nameLatin"]')?.value || entity.nameLatin || ""),
      country: compact(container.querySelector('[data-inline-entity-field="country"]')?.value || buildCountryInputValue(entity) || ""),
      birthYear: parseIntegerString(lifeRange.birthYear),
      deathYear: parseIntegerString(lifeRange.deathYear),
      aliases: parseLines(container.querySelector('[data-inline-entity-field="aliases"]')?.value || "", (line) => line),
      summary: compact(container.querySelector('[data-inline-entity-field="summary"]')?.value || ""),
    };
    if (entityType === "person") {
      payload.roles = entity.roles || [];
    }
    return payload;
  }
  if (entityType === "work") {
    return {
      ...entity,
      title: compact(container.querySelector('[data-inline-entity-field="title"]')?.value || entity.title || ""),
      titleLatin: compact(container.querySelector('[data-inline-entity-field="titleLatin"]')?.value || entity.titleLatin || ""),
      catalogue: compact(container.querySelector('[data-inline-entity-field="catalogue"]')?.value || entity.catalogue || ""),
      aliases: parseLines(container.querySelector('[data-inline-entity-field="aliases"]')?.value || "", (line) => line),
      summary: compact(container.querySelector('[data-inline-entity-field="summary"]')?.value || ""),
    };
  }
  if (entityType === "recording") {
    const eventMeta = parseRecordingEventMetaInput(container.querySelector('[data-inline-entity-field="eventMeta"]')?.value || "");
    return {
      ...entity,
      title: compact(container.querySelector('[data-inline-entity-field="title"]')?.value || entity.title || ""),
      performanceDateText: eventMeta.performanceDateText || entity.performanceDateText || "",
      venueText: eventMeta.venueText || entity.venueText || "",
      albumTitle: compact(container.querySelector('[data-inline-entity-field="albumTitle"]')?.value || entity.albumTitle || ""),
      label: compact(container.querySelector('[data-inline-entity-field="label"]')?.value || entity.label || ""),
      releaseDate: compact(container.querySelector('[data-inline-entity-field="releaseDate"]')?.value || entity.releaseDate || ""),
      links: parseBatchLinkLines(container.querySelector('[data-inline-entity-field="links"]')?.value || ""),
      notes: compact(container.querySelector('[data-inline-entity-field="notes"]')?.value || entity.notes || ""),
    };
  }
  return entity;
};
const saveInlineEntityEdits = async () => {
  const { entityType, entityId } = state.inlineCheck;
  if (!entityType || !entityId) {
    throw new Error("当前没有正在检查的条目。");
  }
  const editor = inlineCheckContent.querySelector(".owner-inline-editor");
  if (!editor) {
    throw new Error("当前没有可保存的手动编辑区。");
  }
  const payload = buildInlineEntityPayload(entityType, editor);
  const result = await fetchJson(`/api/save/${encodeURIComponent(entityType)}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await refreshAll();
  await loadEntity(entityType, result.entity?.id || entityId);
  if (state.activeJob) {
    await renderInlineCheckJob(state.activeJob);
  }
  setResult({ inlineEntitySaved: true, entityType, entityId: result.entity?.id || entityId });
};
const uploadInlineEntityImage = async (file) => {
  const { entityType, entityId } = state.inlineCheck;
  if (!entityType || !entityId) {
    throw new Error("当前没有正在检查的条目。");
  }
  const contentBase64 = await readFileAsBase64(file);
  const result = await fetchJson("/api/assets/upload", {
    method: "POST",
    body: JSON.stringify({
      bucket: getEntityBucket(entityType),
      slug: entityId,
      fileName: file.name || "upload.jpg",
      contentBase64,
    }),
  });
  const asset = {
    src: result.asset?.src || "",
    sourceUrl: "",
    sourceKind: result.asset?.imageSourceKind || "manual",
    attribution: result.asset?.imageAttribution || file.name || "",
    alt: entityId,
    updatedAt: result.asset?.imageUpdatedAt || new Date().toISOString(),
  };
  const preview = inlineCheckContent.querySelector("[data-inline-entity-image-preview]");
  if (preview) {
    preview.innerHTML = asset.src ? `<img src="${escapeHtml(asset.src)}" alt="${escapeHtml(asset.alt || "条目图片")}" />` : "<span>暂无图片</span>";
  }
  const entity = getEntityByTypeAndId(entityType, entityId);
  if (entityType === "recording") {
    entity.images = [{ src: asset.src, alt: file.name || entity.title || "版本图片", kind: "cover", sourceUrl: "", sourceKind: "manual" }, ...(entity.images || []).slice(1)];
  } else {
    entity.avatarSrc = asset.src;
    entity.imageSourceUrl = "";
    entity.imageSourceKind = "manual";
    entity.imageAttribution = file.name || "";
    entity.imageUpdatedAt = asset.updatedAt;
  }
  setResult({ inlineEntityImageUploaded: true, entityType, entityId, src: asset.src });
};

const buildSummaryFieldHtml = (label, value, options = {}) => {
  const normalized = compact(value);
  if (!normalized) {
    return "";
  }
  const { excerptLength = 120, allowDialog = false } = options;
  const excerpt = buildExcerpt(normalized, excerptLength);
  const detailButton =
    allowDialog && excerpt.truncated
      ? `<button type="button" class="owner-link-button" data-open-text="${escapeHtml(
          encodeDataText(normalized),
        )}" data-open-text-title="${escapeHtml(label)}">查看完整内容</button>`
      : "";

  return `
    <div class="owner-entity-summary__item">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(excerpt.text || normalized)}</strong>
      ${detailButton}
    </div>`;
};

const buildLifeSpan = (entity) => {
  const birth = compact(entity?.birthYear);
  const death = compact(entity?.deathYear);
  if (!birth && !death) {
    return "";
  }
  return `${birth || "?"}-${death || "?"}`;
};

const getEntityImageAsset = (entity) => {
  const src = compact(entity?.avatarSrc || entity?.images?.[0]?.src);
  if (!src) {
    return null;
  }
  return {
    src,
    alt: compact(entity?.images?.[0]?.alt || entity?.name || "条目图片"),
    attribution: compact(entity?.imageAttribution || entity?.images?.[0]?.alt),
    sourceKind: compact(entity?.imageSourceKind || entity?.images?.[0]?.sourceKind),
    sourceUrl: compact(entity?.imageSourceUrl || entity?.images?.[0]?.sourceUrl),
  };
};

const buildEntitySummaryHtml = (entity, options = {}) => {
  if (!entity) {
    return "";
  }
  const imageAsset = getEntityImageAsset(entity);
  if ("workId" in entity && "title" in entity) {
    const work = getEntityByTypeAndId("work", entity.workId);
    const rows = [
      ["所属作品", work ? getWorkDisplayLabel(work) : entity.workId],
      ["版本标题", entity.title],
      ["路径别名（Slug）", entity.slug],
      ["排序键", entity.sortKey],
      ["演出时间 / 地点", buildRecordingEventMetaValue(entity)],
      ["专辑 / 发行", [entity.albumTitle, entity.label, entity.releaseDate].filter(Boolean).join(" / ")],
      ["资源链接", (entity.links || []).map((item) => item.platform || hostLabel(getResourceLinkTarget(item))).filter(Boolean).join(" / ")],
      ["备注", entity.notes],
    ]
      .filter(([, value]) => compact(value))
      .map(([label, value]) =>
        buildSummaryFieldHtml(label, formatValue(value), {
          excerptLength: label === "备注" ? 64 : 72,
          allowDialog: label === "备注" || String(formatValue(value)).length > 120,
          ...options,
        }),
      )
      .join("");
    return rows
      ? `<section class="owner-entity-summary">
          <h4>当前条目信息</h4>
          <div class="owner-entity-summary__layout ${imageAsset ? "has-image" : ""}">
            ${
              imageAsset
                ? `<div class="owner-entity-summary__media">
                     <div class="owner-image-preview">
                       <img src="${escapeHtml(imageAsset.src)}" alt="${escapeHtml(imageAsset.alt)}" />
                     </div>
                   </div>`
                : ""
            }
            <div class="owner-entity-summary__grid">${rows}</div>
          </div>
        </section>`
      : "";
  }
  const rows = [
    ["中文全名", entity.name],
    ["英文 / 原文全名", entity.nameLatin],
    ["路径别名（Slug）", entity.slug],
    ["排序键", entity.sortKey],
    ["生卒年", buildLifeSpan(entity)],
    ["国家", buildCountrySummaryValue(entity)],
    ["别名", (entity.aliases || []).join(" / ")],
    ["简介", entity.summary],
  ]
    .filter(([, value]) => compact(value))
    .map(([label, value]) =>
      buildSummaryFieldHtml(label, formatValue(value), {
        excerptLength: label === "简介" ? 64 : 72,
        allowDialog: label === "简介" || String(formatValue(value)).length > 120,
        ...options,
      }),
    )
    .join("");
  return rows
    ? `<section class="owner-entity-summary">
        <h4>当前条目信息</h4>
        <div class="owner-entity-summary__layout ${imageAsset ? "has-image" : ""}">
          ${
            imageAsset
              ? `<div class="owner-entity-summary__media">
                   <div class="owner-image-preview">
                     <img src="${escapeHtml(imageAsset.src)}" alt="${escapeHtml(imageAsset.alt)}" />
                   </div>
                 </div>`
              : ""
          }
          <div class="owner-entity-summary__grid">${rows}</div>
        </div>
      </section>`
    : "";
};

const buildProposalEvidenceHtml = (proposal) => {
  if (proposal?.entityType === "recording" || !proposal?.evidence?.length) {
    return "";
  }
  return `
    <section class="owner-proposal__warnings">
      <h4>证据来源</h4>
      <ul>${proposal.evidence
        .map(
          (item) =>
            `<li><strong>${escapeHtml(item.field)}</strong>：${escapeHtml(item.sourceLabel || hostLabel(item.sourceUrl) || "未标注来源")} / 置信度 ${escapeHtml(
              `${Math.round((item.confidence || 0) * 100)}%`,
            )}</li>`,
        )
        .join("")}</ul>
    </section>`;
};

const buildProposalLinkCandidatesHtml = (proposal) => {
  if (!proposal?.linkCandidates?.length) {
    return "";
  }
  return `
    <section class="owner-proposal__warnings">
      <h4>资源来源候选</h4>
      <div class="owner-link-editor-list">
        ${proposal.linkCandidates
          .map((candidate, index) => {
            const label = buildRecordingLinkChipLabel(candidate, index, proposal.linkCandidates);
            const title = [candidate.title, candidate.sourceLabel, candidate.confidence ? `置信度 ${Math.round(candidate.confidence * 100)}%` : ""]
              .filter(Boolean)
              .join("\n");
            return `<a class="owner-link-chip" href="${escapeHtml(candidate.url)}" target="_blank" rel="noreferrer" title="${escapeHtml(title)}">${escapeHtml(
              label,
            )}</a>`;
          })
          .join("")}
      </div>
    </section>`;
};

const recordingProviderStatusLabels = {
  unavailable: "不可用",
  queued: "已排队",
  running: "运行中",
  partial: "部分完成",
  succeeded: "已完成",
  failed: "失败",
  timed_out: "超时",
  canceled: "已取消",
};

const getProposalLinkCandidateDismissalKey = (runId, proposalId) => `${compact(runId)}:${compact(proposalId)}`;
const getDismissedProposalLinkCandidateUrls = (runId, proposalId) =>
  state.proposalLinkCandidateDismissals[getProposalLinkCandidateDismissalKey(runId, proposalId)] || [];
const rememberDismissedProposalLinkCandidateUrls = (runId, proposalId, urls) => {
  const key = getProposalLinkCandidateDismissalKey(runId, proposalId);
  const normalized = [...new Set((urls || []).map((value) => compact(value)).filter(Boolean))];
  if (normalized.length) {
    state.proposalLinkCandidateDismissals[key] = normalized;
    return normalized;
  }
  delete state.proposalLinkCandidateDismissals[key];
  return [];
};
const dismissProposalLinkCandidate = (runId, proposalId, url) =>
  rememberDismissedProposalLinkCandidateUrls(runId, proposalId, [
    ...getDismissedProposalLinkCandidateUrls(runId, proposalId),
    url,
  ]);
const getVisibleProposalLinkCandidates = (proposal, runId = "") => {
  const hiddenUrls = new Set(getDismissedProposalLinkCandidateUrls(runId, proposal?.id));
  return (proposal?.linkCandidates || []).filter((candidate) => !hiddenUrls.has(compact(candidate?.url)));
};

const buildRecordingProviderStatusHtml = (run, proposal) => {
  if (proposal?.entityType !== "recording") {
    return "";
  }
  if (!run?.provider) {
    return `
      <section class="owner-proposal__warnings owner-proposal__warnings--status">
        <h4>版本检查调用</h4>
        <ul>
          <li>未检测到外部版本检索工具状态。</li>
          <li>当前候选可能来自历史结果或旧链路，请先人工复核后再确认。</li>
        </ul>
      </section>`;
  }
  const provider = run.provider;
  const statusLabel = recordingProviderStatusLabels[provider.status] || provider.status || "unknown";
  const healthLine =
    provider.status === "succeeded" || provider.status === "partial" || provider.status === "running" || provider.status === "queued"
      ? "链路状态：外部版本检索工具已接通。"
      : "链路状态：外部版本检索工具返回异常，请优先人工复核。";
  const lines = [
    `已调用外部版本检索工具：${provider.providerName}`,
    `当前状态：${statusLabel}`,
    healthLine,
    provider.error ? `错误信息：${provider.error}` : "",
  ].filter(Boolean);
  return `
    <section class="owner-proposal__warnings owner-proposal__warnings--status">
      <h4>版本检查调用</h4>
      <ul>${lines.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
    </section>`;
};

const buildEditableProposalLinkCandidatesHtml = (proposal, options = {}) => {
  const visibleCandidates = getVisibleProposalLinkCandidates(proposal, options.runId);
  if (!visibleCandidates.length) {
    return "";
  }
  return `
    <div class="owner-proposal__candidate-links">
      <h5>资源来源候选</h5>
      <div class="owner-proposal__candidate-link-list">
        ${visibleCandidates
          .map((candidate, index) => {
            const label = buildRecordingLinkChipLabel(candidate, index, visibleCandidates);
            const title = [candidate.title, candidate.sourceLabel, candidate.confidence ? `置信度 ${Math.round(candidate.confidence * 100)}%` : ""]
              .filter(Boolean)
              .join("\n");
            return `
              <div class="owner-proposal__candidate-link-row">
                <button
                  type="button"
                  class="owner-link-chip"
                  data-proposal-link-candidate-edit
                  data-proposal-link-candidate-run-id="${escapeHtml(options.runId || "")}"
                  data-proposal-link-candidate-proposal-id="${escapeHtml(proposal.id)}"
                  data-proposal-link-candidate-platform="${escapeHtml(candidate.platform || "")}"
                  data-proposal-link-candidate-url="${escapeHtml(candidate.url || "")}"
                  data-proposal-link-candidate-title="${escapeHtml(candidate.title || "")}"
                  data-proposal-link-candidate-source-label="${escapeHtml(candidate.sourceLabel || hostLabel(candidate.sourceUrl) || "")}"
                  data-proposal-link-candidate-confidence="${escapeHtml(
                    candidate.confidence ? `${Math.round(candidate.confidence * 100)}%` : "",
                  )}"
                  title="${escapeHtml(title)}"
                >${escapeHtml(label)}</button>
              </div>`;
          })
          .join("")}
      </div>
    </div>`;
};

const formatImageCandidateLabel = (candidate, index) => {
  const title = clipText(compact(candidate.title) || "图片候选", 24);
  const sourceKind = compact(candidate.sourceKind) || "unknown";
  const host = compact(hostLabel(candidate.sourceUrl));
  const size =
    candidate.width && candidate.height ? `${candidate.width}×${candidate.height}` : "";
  const attribution = clipText(compact(candidate.attribution), 18);
  const segments = [
    `${index + 1}. ${title}`,
    attribution && attribution !== title ? attribution : "",
    sourceKind,
    host,
    size,
  ].filter(Boolean);
  return segments.join(" / ");
};

const buildOwnerRemoteImagePreviewUrl = (value) => {
  const normalized = compact(value);
  if (!normalized) {
    return "";
  }
  if (/^https?:\/\//i.test(normalized)) {
    return `/api/remote-image?url=${encodeURIComponent(normalized)}`;
  }
  if (normalized.startsWith("//")) {
    return `/api/remote-image?url=${encodeURIComponent(`${window.location.protocol}${normalized}`)}`;
  }
  return normalized;
};

const buildImageCandidateCardHtml = (candidate, proposal, index) => `
  <div class="owner-proposal__image ${candidate.id === proposal.selectedImageCandidateId ? "is-selected" : ""}">
    <div class="owner-proposal__image-grid">
      <img src="${escapeHtml(buildOwnerRemoteImagePreviewUrl(candidate.src))}" alt="${escapeHtml(candidate.title || proposal.summary)}" />
      <div>
        <strong>${escapeHtml(formatImageCandidateLabel(candidate, index))}${candidate.id === proposal.selectedImageCandidateId ? "（当前选中）" : ""}</strong>
        <p>${escapeHtml(candidate.sourceKind)} / ${escapeHtml(candidate.attribution || "未标注来源")}</p>
        <p>${escapeHtml(hostLabel(candidate.sourceUrl))}</p>
      </div>
    </div>
  </div>`;

const buildImageCandidatesHtml = (proposal, options = {}) => {
  const { imageSelectAttr, imageUploadAttr } = getProposalModeAttributes(options);
  const imageCandidates = proposal.imageCandidates || [];
  const selectedId = compact(proposal.selectedImageCandidateId);
  const visibleCandidates = selectedId ? imageCandidates.filter((candidate) => candidate.id === selectedId) : imageCandidates;
  const imageOptions = [
    `<option value="" ${!selectedId ? "selected" : ""}>请选择（显示全部候选）</option>`,
    ...imageCandidates.map(
      (candidate, index) => `
        <option value="${escapeHtml(candidate.id)}" ${candidate.id === selectedId ? "selected" : ""}>
          ${escapeHtml(formatImageCandidateLabel(candidate, index))}
        </option>`,
    ),
  ].join("");

  return `<div class="owner-proposal__images">
    <label>图片候选<select ${imageSelectAttr}="${escapeHtml(proposal.id)}">${imageOptions}</select></label>
    ${
      imageCandidates.length
        ? visibleCandidates
            .map((candidate) =>
              buildImageCandidateCardHtml(
                candidate,
                proposal,
                imageCandidates.findIndex((item) => item.id === candidate.id),
              ),
            )
            .join("")
        : '<p class="owner-proposal__hint">当前没有自动抓取到可用图片候选，可直接手动上传。</p>'
    }
    <div class="owner-proposal__image-upload">
      <label class="owner-upload-button">
        <span>上传图片</span>
        <input type="file" accept="image/*" ${imageUploadAttr}="${escapeHtml(proposal.id)}" hidden />
      </label>
    </div>
  </div>`;
};

const proposalFieldUsesTextarea = (field) => {
  const sample = field.after ?? field.before;
  return (
    Array.isArray(sample) ||
    typeof sample === "object" ||
    /summary|aliases|notes|description|about|images|credits|links/i.test(field.path)
  );
};

const formatProposalFieldInputValue = (field) => {
  const sample = field.after ?? field.before;
  if (field.path === "lifeRange") {
    return compact(sample);
  }
  if (field.path === "links") {
    return formatBatchLinkLines(sample);
  }
  if (field.path === "credits") {
    return formatBatchCreditLines(sample);
  }
  if (field.path === "images") {
    return formatLines(sample || [], formatRecordingImageLine);
  }
  if (Array.isArray(sample)) {
    return sample.join("\n");
  }
  if (sample && typeof sample === "object") {
    return JSON.stringify(sample, null, 2);
  }
  return compact(sample);
};

const formatProposalFieldOriginalValue = (field) => {
  if (!field) {
    return "";
  }
  if (field.path === "links" || field.path === "credits" || field.path === "images") {
    return formatProposalFieldInputValue({ ...field, after: undefined });
  }
  return formatValue(field.before);
};

const parseProposalFieldInputValue = (field, rawValue) => {
  const sample = field.after ?? field.before;
  const normalized = String(rawValue ?? "");
  if (field.path === "lifeRange") {
    return compact(normalized);
  }
  if (field.path === "links") {
    return parseBatchLinkLines(normalized);
  }
  if (field.path === "credits") {
    return parseBatchCreditLines(normalized);
  }
  if (field.path === "images") {
    return parseRecordingImageLines(normalized);
  }
  if (Array.isArray(sample) || /aliases/i.test(field.path)) {
    return normalized
      .split(/\r?\n/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  if (typeof sample === "number") {
    return compact(normalized) ? Number(compact(normalized)) : "";
  }
  if (typeof sample === "boolean") {
    return ["1", "true", "yes", "on"].includes(compact(normalized).toLowerCase());
  }
  if (sample && typeof sample === "object") {
    try {
      return JSON.parse(normalized);
    } catch {
      return normalized;
    }
  }
  return normalized.trim();
};

const buildProposalFieldEditorHtml = (proposal, field, options = {}) => {
  const { inputAttr, fieldPathAttr } = getProposalModeAttributes(options);
  if (field.path === "links") {
    const fieldValue = formatProposalFieldInputValue(field);
    const oldValue = escapeHtml(formatProposalFieldOriginalValue(field));
    const candidateHtml = buildEditableProposalLinkCandidatesHtml(proposal, options);
    return `
      <section class="owner-proposal__editor-field owner-proposal__editor-field--full owner-structured-links" data-recording-link-host>
        <div class="owner-card__header">
          <span>${escapeHtml(field.label || field.path)}</span>
          <button type="button" data-recording-link-action="add">新增链接</button>
        </div>
        <input
          type="hidden"
          ${inputAttr}="${escapeHtml(proposal.id)}"
          ${fieldPathAttr}="${escapeHtml(field.path)}"
          data-proposal-link-input
          value="${escapeHtml(fieldValue)}"
        />
        <div data-recording-links-editor class="owner-link-editor-list">${buildRecordingLinkEditorHtml(parseBatchLinkLines(fieldValue))}</div>
        ${candidateHtml}
        <p class="owner-proposal__hint">原值：${oldValue}</p>
      </section>`;
  }
  const multiline = proposalFieldUsesTextarea(field);
  const fieldValue = escapeHtml(formatProposalFieldInputValue(field));
  const oldValue = escapeHtml(formatProposalFieldOriginalValue(field));
  const fieldClass = multiline ? "owner-proposal__editor-field owner-proposal__editor-field--full" : "owner-proposal__editor-field";
  const rows = /summary|notes|description|about/i.test(field.path) ? 6 : 4;

  return `
    <label class="${fieldClass}">
      <span>${escapeHtml(field.label || field.path)}</span>
      ${multiline
        ? `<textarea ${inputAttr}="${escapeHtml(proposal.id)}" ${fieldPathAttr}="${escapeHtml(field.path)}" rows="${rows}">${fieldValue}</textarea>`
        : `<input ${inputAttr}="${escapeHtml(proposal.id)}" ${fieldPathAttr}="${escapeHtml(field.path)}" value="${fieldValue}" />`}
      <p class="owner-proposal__hint">原值：${oldValue}</p>
    </label>`;
};

const buildComparableProposal = (proposal, library = state.library) => {
  if (!proposal) {
    return proposal;
  }
  const entity = getEntityByTypeAndId(proposal.entityType, proposal.entityId, library);
  return {
    ...proposal,
    fields: buildEditableFieldDescriptors(proposal, proposal.entityType, entity),
  };
};

const buildProposalCardsHtml = (run, proposals, options = {}) => {
  const { inline = false, allowDirectApply = true } = options;
  const { imageSelectAttr, actionAttr, proposalIdAttr, proposalTargetIdAttr } = getProposalModeAttributes(options);
  void imageSelectAttr;
  void proposalIdAttr;
  const proposalLibrary = options.library || state.library;
  return (
    proposals
      .map((proposal) => {
        const comparableProposal = buildComparableProposal(proposal, proposalLibrary);
        const draft = getProposalDraft(run.id, proposal.id);
        const effectiveProposal = draft ? applyProposalDraft(comparableProposal, draft) : comparableProposal;
        const entity = getEntityByTypeAndId(effectiveProposal.entityType, effectiveProposal.entityId, proposalLibrary);
        void entity;
        const editableFields = effectiveProposal.fields || [];
        const applyBlockers = getProposalApplyBlockers(effectiveProposal);
        const canApply = allowDirectApply && isProposalDirectlyApplicable(effectiveProposal);
        const sourceLabels = [...new Set((effectiveProposal.sources || []).map(hostLabel))];
        const summaryExcerpt = buildExcerpt(effectiveProposal.summary, 84);
        const summaryAction =
          summaryExcerpt.truncated
            ? `<button type="button" class="owner-link-button" data-open-text="${escapeHtml(
                encodeDataText(effectiveProposal.summary),
              )}" data-open-text-title="候选摘要">查看完整内容</button>`
            : "";
        const proposalClasses = [
          "owner-proposal",
          `owner-proposal--${escapeHtml(effectiveProposal.reviewState || "unseen")}`,
          applyBlockers.length ? "owner-proposal--blocked" : "",
          draft && hasProposalDraftChanges(comparableProposal, draft) ? "owner-proposal--dirty" : "",
        ]
          .filter(Boolean)
          .join(" ");
        const proposalMode = options.mode || (inline ? "inline" : "review");
        const proposalContextReady = Boolean(compact(proposal.id) && compact(run.id) && compact(proposalMode));

        return `
          <article
            class="${proposalClasses}"
            data-owner-proposal-mode="${escapeHtml(proposalMode)}"
            data-owner-run-id="${escapeHtml(run.id)}"
            data-owner-proposal-id="${escapeHtml(proposal.id)}"
          >
            <div class="owner-proposal__headline">
              <div>
                <span class="owner-pill">${escapeHtml(reviewStateLabels[effectiveProposal.reviewState || "unseen"] || effectiveProposal.reviewState || "未查看")}</span>
                <span class="owner-pill">${escapeHtml(effectiveProposal.risk)}</span>
                <span class="owner-pill">${escapeHtml(effectiveProposal.status || "pending")}</span>
                ${draft && hasProposalDraftChanges(comparableProposal, draft) ? '<span class="owner-pill">草稿未保存</span>' : ""}
              </div>
              <div class="owner-proposal__headline-copy">
                <strong>${escapeHtml(summaryExcerpt.text || effectiveProposal.summary)}</strong>
                ${summaryAction}
              </div>
            </div>
            ${
              sourceLabels.length
                ? `<div class="owner-source-pills">${sourceLabels.map((label) => `<span class="owner-pill">${escapeHtml(label)}</span>`).join("")}</div>`
                : ""
            }
            ${
              editableFields.length
                ? `<section class="owner-proposal__editor">
                    <h4>候选修改</h4>
                    <div class="owner-proposal__editor-grid">${editableFields
                      .map((field) => buildProposalFieldEditorHtml(effectiveProposal, field, { ...options, runId: run.id }))
                      .join("")}</div>
                  </section>`
                : ""
            }
            ${buildRecordingProviderStatusHtml(run, effectiveProposal)}
            ${buildImageCandidatesHtml(effectiveProposal, options)}
            ${buildProposalEvidenceHtml(effectiveProposal)}
            ${
              applyBlockers.length
                ? `<section class="owner-proposal__warnings owner-proposal__warnings--blocked">
                    <h4>阻止应用</h4>
                    <ul>${applyBlockers.map((reason) => `<li>${escapeHtml(reason)}</li>`).join("")}</ul>
                  </section>`
                : ""
            }
            ${
              effectiveProposal.warnings?.length
                ? `<section class="owner-proposal__warnings">
                    <h4>提示</h4>
                    <ul>${effectiveProposal.warnings
                      .map((warning) => `<li>${escapeHtml(clipText(warning, 180))}</li>`)
                      .join("")}</ul>
                  </section>`
                : ""
            }
            <div class="owner-form__actions">
              <button type="button" ${actionAttr}="viewed" ${proposalTargetIdAttr}="${escapeHtml(proposal.id)}" ${proposalContextReady ? "" : "disabled"}>标记已读</button>
              <button type="button" ${actionAttr}="confirm" ${proposalTargetIdAttr}="${escapeHtml(proposal.id)}" ${proposalContextReady ? "" : "disabled"}>确认采用</button>
              <button type="button" ${actionAttr}="discard" ${proposalTargetIdAttr}="${escapeHtml(proposal.id)}" ${proposalContextReady ? "" : "disabled"}>放弃</button>
              ${canApply ? `<button type="button" ${actionAttr}="apply" ${proposalTargetIdAttr}="${escapeHtml(proposal.id)}" ${proposalContextReady ? "" : "disabled"}>直接应用</button>` : ""}
            </div>
          </article>`;
      })
      .join("") || '<p class="owner-empty">当前没有候选条目。</p>'
  );
};

const renderReviewPagination = (pageData) => {
  if (!pageData || pageData.totalPages <= 1) {
    return "";
  }
  return `
    <div class="owner-review-pagination__summary">
      第 ${pageData.page} / ${pageData.totalPages} 页
      <span>共 ${pageData.totalItems} 条，本页 ${pageData.items.length} 条</span>
    </div>
    <div class="owner-review-pagination__actions">
      <button type="button" data-review-page-action="prev" ${pageData.page <= 1 ? "disabled" : ""}>上一页</button>
      <button type="button" data-review-page-action="next" ${pageData.page >= pageData.totalPages ? "disabled" : ""}>下一页</button>
    </div>`;
};

const getRunPageProposals = (run) =>
  paginateItems(filterPendingProposalsForDisplay(run?.proposals || []), state.reviewPage, REVIEW_PAGE_SIZE);

const renderInlineCheckJob = async (job) => {
  if (!job || !state.inlineCheck.entityType || !state.inlineCheck.entityId) {
    return;
  }
  const activeEntity = getEntityByTypeAndId(state.inlineCheck.entityType, state.inlineCheck.entityId);
  const currentItem =
    job.items?.find((item) => item.entityId === state.inlineCheck.entityId) ||
    job.items?.find((item) => item.entityId === job.selectedItemId) ||
    null;
  const hasRun = Boolean(job.run?.id);
  let proposals = [];
  if (hasRun) {
    const payload = await fetchJson(`/api/automation/runs/${encodeURIComponent(job.run.id)}`);
    const run = payload.run;
    state.inlineCheck.runId = run.id;
    proposals = getEntityScopedProposals(run, state.inlineCheck.entityType, state.inlineCheck.entityId);
  }
  const showResolutionActions = hasRun && proposals.length === 0 && Boolean(currentItem);

  inlineCheckContent.innerHTML = `
    <section class="owner-inline-check__summary">
      <div class="owner-card__header">
        <strong>${escapeHtml(currentItem?.label || getDisplayTitle(activeEntity) || "当前条目")}</strong>
        <span class="owner-pill owner-pill--job owner-pill--${escapeHtml(currentItem?.status || job.status)}">${escapeHtml(
          currentItem ? describeJobItemStatus(currentItem) : describeJobStatus(job),
        )}</span>
      </div>
      <ul>
        <li>任务状态：${escapeHtml(describeJobStatus(job))}</li>
        <li>进度：${escapeHtml(`${job.progress.processed} / ${job.progress.total}`)}</li>
        <li>成功：${escapeHtml(job.progress.succeeded)}，无新增：${escapeHtml(job.progress.unchanged || 0)}，待关注：${escapeHtml(job.progress.attention || 0)}，失败：${escapeHtml(job.progress.failed)}，跳过：${escapeHtml(job.progress.skipped)}</li>
      </ul>
      ${buildInlineOutcomeHtml(currentItem, proposals)}
      ${
        hasRun && proposals.length
          ? `<section class="owner-job-detail__section">
              <div class="owner-inline-check__proposal-list">${buildProposalCardsHtml(job.run, proposals, { inline: true })}</div>
            </section>`
          : ""
      }
      ${
        currentItem?.errors?.length
          ? `<section class="owner-job-detail__section owner-job-detail__section--error">
              <h4>错误</h4>
              <ul>${currentItem.errors.map((message) => `<li>${escapeHtml(clipText(message, 220))}</li>`).join("")}</ul>
            </section>`
          : ""
      }
      ${
        !proposals.length && showResolutionActions
          ? `<div class="owner-inline-check__actions">
              <button type="button" data-inline-run-action="acknowledge">确认本次检查结论</button>
              <button type="button" data-inline-run-action="dismiss">放弃并关闭</button>
            </div>`
          : ""
      }
      <section class="owner-job-detail__section">
        <h4>执行流程</h4>
        <div class="owner-inline-check__event-list">
          ${
            currentItem?.events?.length
              ? currentItem.events
                  .map(
                    (event) => `
                      <article class="owner-inline-check__event">
                        <strong>${escapeHtml(event.phase)}</strong>
                        <p>${escapeHtml(clipText(event.message, 220))}</p>
                        <small>${escapeHtml(event.timestamp.replace("T", " ").replace("Z", ""))}</small>
                      </article>`,
                  )
                  .join("")
              : '<p class="owner-empty">当前还没有可展示的事件。</p>'
          }
        </div>
      </section>
    </section>`;
};

const renderSearchResults = (results) => {
  searchCount.textContent = `${results.length} 条`;
  searchResults.innerHTML =
    results
      .map(
        (item) => `
          <article class="owner-result-item">
            <div class="owner-result-item__badges">${buildSearchResultBadges(item)
              .map((badge) => `<span class="owner-pill">${escapeHtml(badge)}</span>`)
              .join("")}</div>
            <h3>${escapeHtml(item.title)}</h3>
            ${item.subtitle ? `<p class="owner-result-item__subtitle">${escapeHtml(item.subtitle)}</p>` : ""}
            <button type="button" data-load-type="${escapeHtml(item.type)}" data-load-id="${escapeHtml(item.id)}">载入详情</button>
          </article>`,
      )
      .join("") || '<p class="owner-empty">没有匹配条目。</p>';
};

const loadEntity = async (entityType, entityId) => {
  if (entityType === "site") {
    fillSiteForm(state.site);
    state.activeEntity = { type: "site", id: "site" };
    setActiveDetailTab("site");
    clearInlineCheck();
    return;
  }
  const { entity, relatedEntities = [] } = await fetchJson(`/api/entity/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`);
  const form = document.querySelector(`[data-entity-form="${entityType}"]`);
  if (!form) {
    return;
  }
  formFillers[entityType]?.(form, entity);
  renderRelatedEntityControls(form, relatedEntities);
  state.activeEntity = { type: entityType, id: entityId };
  setActiveDetailTab(getVisibleDetailTabForEntity(entityType, entity), {
    panel: entityType === "composer" ? "composer" : undefined,
  });
  clearInlineCheck();
  setResult({ loaded: entityType, id: entityId });
};

const performSearch = async () => {
  const query = compact(searchInput.value);
  const type = compact(searchType.value);
  const params = new URLSearchParams();
  if (query) {
    params.set("q", query);
  }
  if (type) {
    params.set("type", type);
  }
  const { results } = await fetchJson(`/api/search?${params.toString()}`);
  renderSearchResults(results);
};

const renderSelectionPreview = () => {
  const preview = state.activeSelectionPreview;
  automationPreviewTotal.textContent = `${preview.total} 条`;
  automationSelectionPreview.innerHTML =
    preview.groups
      .map((group) => {
        const selectedIds = state.selectionState[group.category] || new Set();
        const filterText = compact(state.selectionFilters[group.category] || "").toLowerCase();
        const visibleItems = group.items.filter((item) =>
          !filterText ? true : [item.label, item.description].join(" ").toLowerCase().includes(filterText),
        );
        const chips = visibleItems
          .map((item) => {
            const selected = selectedIds.has(item.entityId);
            return `
              <button
                type="button"
                class="owner-chip ${selected ? "is-selected" : "is-excluded"}"
                data-selection-category="${escapeHtml(group.category)}"
                data-selection-id="${escapeHtml(item.entityId)}"
                title="${escapeHtml(item.description || "")}" 
              >
                <span>${escapeHtml(item.label)}</span>
                ${item.description ? `<small>${escapeHtml(item.description)}</small>` : ""}
              </button>`;
          })
          .join("");
        return `
          <section class="owner-selection-group">
            <div class="owner-card__header">
              <h4>${escapeHtml(automationCategoryLabels[group.category] || group.category)}</h4>
              <span>${group.items.length} 条</span>
            </div>
            <div class="owner-selection-group__toolbar">
              <input
                type="search"
                class="owner-selection-group__search"
                placeholder="筛选当前分组"
                data-selection-filter="${escapeHtml(group.category)}"
                value="${escapeHtml(state.selectionFilters[group.category] || "")}"
              />
              <div class="owner-selection-group__actions">
                <button type="button" data-selection-action="toggle-all" data-selection-category-action="${escapeHtml(group.category)}">全选 / 全不选</button>
                <button type="button" data-selection-action="invert" data-selection-category-action="${escapeHtml(group.category)}">反选</button>
              </div>
            </div>
            <div class="owner-chip-list owner-chip-list--scrollable">${chips || '<p class="owner-empty">当前筛选没有命中条目。</p>'}</div>
          </section>`;
      })
      .join("") || '<p class="owner-empty">当前筛选范围没有命中任何条目。</p>';
};

const hydrateSelectionState = (preview) => {
  state.activeSelectionPreview = preview;
  state.selectionState = Object.fromEntries(
    preview.groups.map((group) => [group.category, new Set(group.items.map((item) => item.entityId))]),
  );
  renderSelectionPreview();
};

const getCheckedCategories = () =>
  [...automationCheckForm.querySelectorAll('input[name="categories"]:checked')].map((input) => input.value);

const buildCheckFilterRequest = () => ({
  categories: getCheckedCategories(),
  composerIds: compact(automationCheckForm.elements.composerId.value) ? [compact(automationCheckForm.elements.composerId.value)] : [],
  workIds: compact(automationCheckForm.elements.workId.value) ? [compact(automationCheckForm.elements.workId.value)] : [],
  conductorIds: compact(automationCheckForm.elements.conductorId.value) ? [compact(automationCheckForm.elements.conductorId.value)] : [],
  artistIds: compact(automationCheckForm.elements.artistId.value) ? [compact(automationCheckForm.elements.artistId.value)] : [],
  orchestraIds: compact(automationCheckForm.elements.orchestraId.value) ? [compact(automationCheckForm.elements.orchestraId.value)] : [],
  recordingIds: compact(automationCheckForm.elements.recordingId.value) ? [compact(automationCheckForm.elements.recordingId.value)] : [],
});

const buildSelectionRequest = () => {
  const request = {
    categories: getCheckedCategories(),
    composerIds: [],
    conductorIds: [],
    orchestraIds: [],
    artistIds: [],
    recordingIds: [],
    workIds: [],
  };

  for (const group of state.activeSelectionPreview.groups) {
    const selectedIds = [...(state.selectionState[group.category] || new Set())];
    if (group.category === "composer") request.composerIds = selectedIds;
    if (group.category === "conductor") request.conductorIds = selectedIds;
    if (group.category === "orchestra") request.orchestraIds = selectedIds;
    if (group.category === "artist") request.artistIds = selectedIds;
    if (group.category === "recording") request.recordingIds = selectedIds;
  }

  return request;
};

const refreshSelectionPreview = async () => {
  const request = buildCheckFilterRequest();
  const { preview } = await fetchJson("/api/automation/selection-preview", {
    method: "POST",
    body: JSON.stringify(request),
  });
  hydrateSelectionState(preview);
  setResult({ preview });
};

const describeJobStatus = (job) => {
  if (!job) return "尚未启动";
  if (job.status === "completed" && job.progress.failed > 0) return "部分失败（已完成）";
  if (job.status === "completed" && (job.progress.attention || 0) > 0) return "已完成（含待关注项）";
  if (job.status === "completed" && (job.progress.unchanged || 0) > 0 && job.progress.succeeded === 0) return "已完成（无新增）";
  return jobStatusLabels[job.status] || job.status;
};

const describeJobItemStatus = (item) => {
  if (!item) return "未执行";
  return jobItemStatusLabels[item.status] || item.status || "未执行";
};

const buildInlineOutcomeHtml = (item, proposals) => {
  if (!item) {
    return "";
  }
  const resultItems = [];
  let sectionTitle = "检查结果";
  if (item.status === "completed-nochange") {
    sectionTitle = "复核结论";
    resultItems.push("本次自动检查未生成新增候选，当前条目已较完整。");
  } else if (item.status === "needs-attention") {
    sectionTitle = "复核结论";
    resultItems.push("本次自动检查未能把条目补齐到规则要求，需要人工继续复核或补录。");
  } else if (item.status === "failed") {
    sectionTitle = "错误结论";
    resultItems.push("本次自动检查发生错误，未能生成可用结果。");
  } else if (item.status === "succeeded" && proposals.length) {
    sectionTitle = "待复核候选";
    resultItems.push(`本次自动检查生成了 ${proposals.length} 条候选，请在当前条目内确认或放弃。`);
  } else if (item.status === "running") {
    resultItems.push("当前条目仍在检查中，候选和事件会实时刷新到此处。");
  }
  if (item.reviewIssues?.length) {
    resultItems.push(...item.reviewIssues);
  }
  if (!resultItems.length) {
    return "";
  }
  return `
    <section class="owner-job-detail__section owner-inline-check__outcome owner-inline-check__outcome--${escapeHtml(item.status || "queued")}">
      <h4>${escapeHtml(sectionTitle)}</h4>
      <ul>${resultItems.map((message) => `<li>${escapeHtml(clipText(message, 220))}</li>`).join("")}</ul>
    </section>`;
};

const buildRecordingAuditHtml = (job, selectedItem) => {
  const audit = job?.recordingAudit;
  if (!audit?.summary) {
    return "";
  }
  const selectedAudit = audit.results?.find((result) => result.recordingId === selectedItem?.entityId) || null;
  const groupLabelMap = Object.fromEntries((audit.summary.groups || []).map((group) => [group.key, group.label]));
  const attentionGroups = (audit.summary.groups || []).filter((group) => (group.reviewStatusCounts?.["needs-attention"] || 0) > 0);
  const summaryLines = [
    `已纳入在线审计的录音条目：${audit.summary.totalTargets}`,
    `待关注样本：${audit.summary.reviewStatusCounts?.["needs-attention"] || 0}`,
    `高风险字段组：${
      attentionGroups.length
        ? attentionGroups.map((group) => `${group.label} ${group.reviewStatusCounts?.["needs-attention"] || 0}/${group.sampleCount}`).join("；")
        : "无"
    }`,
  ];
  const selectedLines = selectedAudit
    ? [
        `当前条目 provider 状态：${selectedAudit.providerStatus}`,
        `命中字段组：${selectedAudit.groupKeys.map((key) => groupLabelMap[key] || key).join(" / ") || "无"}`,
        ...selectedAudit.warnings.map((warning) => `警告：${warning}`),
        ...selectedAudit.issues.map((issue) => `问题：${issue}`),
      ]
    : [];

  return `
    <section class="owner-job-detail__section">
      <h4>录音在线审计</h4>
      <ul>${summaryLines.map((line) => `<li>${escapeHtml(clipText(line, 220))}</li>`).join("")}</ul>
      ${
        selectedLines.length
          ? `<ul>${selectedLines.map((line) => `<li>${escapeHtml(clipText(line, 220))}</li>`).join("")}</ul>`
          : ""
      }
    </section>`;
};

const renderJob = (job) => {
  state.activeJob = job || null;
  automationJobStatus.textContent = describeJobStatus(job);
  if (!job) {
    automationJobProgressBar.style.width = "0%";
    automationJobProgressText.textContent = "0 / 0";
    automationJobMetrics.innerHTML = "";
    automationJobItems.innerHTML = "";
    automationJobDetail.innerHTML = "";
    return;
  }

  const total = Math.max(job.progress.total, 1);
  const percent = Math.round((job.progress.processed / total) * 100);
  automationJobProgressBar.style.width = `${percent}%`;
  automationJobProgressText.textContent = `${job.progress.processed} / ${job.progress.total}`;
  automationJobMetrics.innerHTML = `
    <article><strong>${job.progress.succeeded}</strong><span>成功</span></article>
    <article><strong>${job.progress.unchanged || 0}</strong><span>无新增</span></article>
    <article><strong>${job.progress.attention || 0}</strong><span>待关注</span></article>
    <article><strong>${job.progress.failed}</strong><span>失败</span></article>
    <article><strong>${job.progress.skipped}</strong><span>跳过</span></article>
    <article><strong>${job.selection.total}</strong><span>总条目</span></article>
  `;

  const selectedItemId =
    state.activeJobItemId && job.items.some((item) => item.entityId === state.activeJobItemId)
      ? state.activeJobItemId
      : job.currentItem?.entityId || job.selectedItemId || job.items[0]?.entityId || "";
  state.activeJobItemId = selectedItemId;

  automationJobItems.innerHTML =
    job.items
      .map((item) => {
        return `
          <button
            type="button"
            class="owner-job-tile owner-job-tile--${escapeHtml(item.status)} ${item.entityId === selectedItemId ? "is-selected" : ""}"
            data-job-item-id="${escapeHtml(item.entityId)}"
          >
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(automationCategoryLabels[item.category] || item.category)}</span>
            <small>${escapeHtml(describeJobItemStatus(item))}</small>
          </button>`;
      })
      .join("") || '<p class="owner-empty">当前没有任务条目。</p>';

  const selectedItem = job.items.find((item) => item.entityId === selectedItemId);
  if (!selectedItem) {
    automationJobDetail.innerHTML = '<p class="owner-empty">点击左侧任务方块查看详情。</p>';
    return;
  }

  automationJobDetail.innerHTML = `
    <article class="owner-job-detail-card">
      <div class="owner-card__header">
        <div>
          <h3>${escapeHtml(selectedItem.label)}</h3>
          <p>${escapeHtml(selectedItem.description || `${automationCategoryLabels[selectedItem.category]} 条目`)}</p>
        </div>
        <span class="owner-pill owner-pill--job owner-pill--${escapeHtml(selectedItem.status)}">${escapeHtml(describeJobItemStatus(selectedItem))}</span>
      </div>
      ${buildInlineOutcomeHtml(selectedItem, [])}
      ${buildRecordingAuditHtml(job, selectedItem)}
      ${
        selectedItem.reviewIssues?.length
          ? `<section class="owner-job-detail__section">
              <h4>自动复查</h4>
              <ul>${selectedItem.reviewIssues.map((issue) => `<li>${escapeHtml(issue)}</li>`).join("")}</ul>
            </section>`
          : ""
      }
      ${
        selectedItem.errors?.length
          ? `<section class="owner-job-detail__section owner-job-detail__section--error">
              <h4>错误</h4>
              <ul>${selectedItem.errors.map((message) => `<li>${escapeHtml(clipText(message, 220))}</li>`).join("")}</ul>
            </section>`
          : ""
      }
      <section class="owner-job-detail__section">
        <h4>执行流程</h4>
        <div class="owner-job-detail__events">
          ${
            selectedItem.events.length
              ? selectedItem.events
                  .map(
                    (event) => `
                      <article class="owner-job-detail__event">
                        <strong>${escapeHtml(event.phase)}</strong>
                        <p>${escapeHtml(clipText(event.message, 220))}</p>
                        <small>${escapeHtml(event.timestamp.replace("T", " ").replace("Z", ""))}</small>
                      </article>`,
                  )
                  .join("")
              : '<p class="owner-empty">暂无细节事件。</p>'
          }
        </div>
      </section>
    </article>`;
};

const renderArticleSearchResults = () => {
  const query = compact(articleSearchInput?.value || "").toLowerCase();
  const filteredArticles = state.articles.filter((article) => {
    if (!query) {
      return true;
    }
    return [article.title, article.slug, article.summary, article.markdown].join(" ").toLowerCase().includes(query);
  });
  articleSearchResults.innerHTML =
    filteredArticles
      .map(
        (article) => `
          <article class="owner-result-item">
            <span class="owner-pill">专栏</span>
            <h3>${escapeHtml(article.title)}</h3>
            ${article.summary ? `<p class="owner-result-item__subtitle">${escapeHtml(clipText(article.summary, 120))}</p>` : ""}
            <div class="owner-form__actions owner-form__actions--compact">
              <button type="button" data-preview-article-id="${escapeHtml(article.id)}">预览</button>
              <button type="button" data-load-article-id="${escapeHtml(article.id)}">载入编辑</button>
            </div>
          </article>`,
      )
      .join("") || '<p class="owner-empty">没有匹配的专栏。</p>';
};

const previewArticleInDialog = async (article) => {
  if (!article) {
    throw new Error("专栏不存在或已被删除。");
  }
  const { preview } = await fetchJson("/api/articles/preview", {
    method: "POST",
    body: JSON.stringify({
      id: article.id || "",
      title: article.title || "",
      slug: article.slug || "",
      summary: article.summary || "",
      markdown: article.markdown || "",
    }),
  });
  renderArticlePreviewModel(preview);
  articlePreviewStatus.textContent = "已生成预览";
  articlePreviewDialog.showModal();
};

const refreshArticlePreview = async () => {
  const payload = buildArticlePayload();
  if (!compact(payload.markdown) && !compact(payload.title) && !compact(payload.summary)) {
    articlePreviewStatus.textContent = "等待输入";
    articlePreviewRender.innerHTML = '<p class="owner-empty">等待输入。</p>';
    return;
  }
  const { preview } = await fetchJson("/api/articles/preview", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  articlePreviewStatus.textContent = payload.id ? "已更新预览" : "预览已生成";
  renderArticlePreviewModel(preview);
};

const uploadArticleImage = async (file) => {
  const contentBase64 = await readFileAsBase64(file);
  const slug = compact(articleForm.elements.slug.value) || compact(articleForm.elements.title.value) || "article";
  const result = await fetchJson("/api/assets/upload", {
    method: "POST",
    body: JSON.stringify({
      bucket: "articles",
      slug,
      fileName: file.name || "article-image.jpg",
      contentBase64,
    }),
  });
  const src = compact(result.asset?.src);
  if (!src) {
    throw new Error("图片上传失败，未返回路径。");
  }
  const markdownInput = articleForm.elements.markdown;
  const alt = compact(articleForm.elements.title.value) || "专栏图片";
  const current = markdownInput.value || "";
  markdownInput.value = `${current}${current.endsWith("\n") || !current ? "" : "\n"}\n![${alt}](${src}){size=large}\n`;
  await refreshArticlePreview();
  setResult({ uploadedArticleImage: src });
};

const saveArticle = async () => {
  const payload = buildArticlePayload();
  if (!payload.title) {
    throw new Error("请先填写专栏标题。");
  }
  const existingId = compact(payload.id || "");
  const result = await fetchJson(existingId ? `/api/articles/${encodeURIComponent(existingId)}` : "/api/articles", {
    method: existingId ? "PUT" : "POST",
    body: JSON.stringify(payload),
  });
  await refreshAll();
  const savedArticle = result.article || state.articles.find((article) => article.id === existingId) || null;
  if (savedArticle) {
    fillArticleForm(savedArticle);
  } else if (result.article) {
    fillArticleForm(result.article);
  }
  articlePreviewStatus.textContent = result.mode === "updated" ? "已保存修改" : "已创建专栏";
  renderArticlePreviewModel(result.preview);
  setActiveView("articles");
  setActiveArticleTab("create");
  setResult(result);
};

const deleteArticle = async () => {
  const articleId = compact(articleForm.elements.existingId.value);
  if (!articleId) {
    throw new Error("当前没有可删除的专栏。");
  }
  const result = await fetchJson(`/api/articles/${encodeURIComponent(articleId)}`, {
    method: "DELETE",
  });
  await refreshAll();
  resetArticleForm();
  setActiveView("articles");
  setActiveArticleTab("list");
  setResult(result);
};

const getBatchFlatEntries = (draftEntities) =>
  ["composer", "person", "work", "recording"].flatMap((entryType) =>
    (draftEntities?.[`${entryType}s`] || []).map((entry, index) => ({
      entryType,
      entry,
      index,
      key: `${entryType}:${index}:${entry?.draftId || entry?.entity?.id || index}`,
    })),
  );

const getBatchSelectedDescriptor = (draftEntities) => {
  const entries = getBatchFlatEntries(draftEntities);
  if (!entries.length) {
    state.batchSelectedEntry = { type: "recording", index: 0 };
    return null;
  }
  const current = state.batchSelectedEntry || {};
  const matched =
    entries.find((item) => item.entryType === current.type && item.index === current.index) ||
    entries.find((item) => item.entry.reviewState !== "discarded") ||
    entries[0];
  state.batchSelectedEntry = { type: matched.entryType, index: matched.index };
  return matched;
};

const getBatchReviewStateLabel = (reviewState) =>
  reviewState === "confirmed" ? "已确认" : reviewState === "discarded" ? "已放弃" : "未确认";

const buildBatchEntryTitle = (entryType, entity, index) => {
  if (entryType === "recording") {
    return entity?.title || `版本 ${index + 1}`;
  }
  return entity?.name || entity?.title || `${entityTypeLabels[entryType] || entryType} ${index + 1}`;
};

const buildBatchDraftCardHtml = (entryType, entry, index, selected) => {
  const entity = entry?.entity || {};
  const title = buildBatchEntryTitle(entryType, entity, index);
  const summary =
    entryType === "recording"
      ? [buildRecordingEventMetaValue(entity), entity.albumTitle, entity.label].filter(Boolean).join(" / ")
      : entity.nameLatin || buildCountrySummaryValue(entity) || "";
  return `
    <button
      type="button"
      class="owner-batch-item ${selected ? "is-selected" : ""}"
      data-batch-entry-select="${escapeHtml(entryType)}:${index}"
    >
      <div class="owner-batch-item__meta">
        <span class="owner-pill">${escapeHtml(entityTypeLabels[entryType] || entryType)}</span>
        <span class="owner-pill owner-pill--subtle">${escapeHtml(getBatchReviewStateLabel(entry.reviewState))}</span>
      </div>
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(entry.sourceLine || summary || "自动生成草稿")}</span>
      ${summary && summary !== entry.sourceLine ? `<small>${escapeHtml(summary)}</small>` : ""}
    </button>`;
};

const buildBatchDetailPanelHtml = (descriptor) => {
  if (!descriptor) {
    return '<p class="owner-empty">暂无可编辑条目。</p>';
  }
  const { entryType, entry, index } = descriptor;
  const entity = entry?.entity || {};
  const title = buildBatchEntryTitle(entryType, entity, index);
  const fieldHtml = (batchDraftFieldConfigs[entryType] || [])
    .map((config) =>
      buildBatchDraftFieldHtml(
        entryType,
        index,
        config,
        getBatchDraftFieldValue(entryType, entity, config.field, entry),
        state.batchDraftEntities || state.batchSession?.draftEntities,
        entry,
      ),
    )
    .join("");
  return `
    <article class="owner-batch-draft owner-batch-draft--detail">
      <div class="owner-card__header">
        <div>
          <h4>${escapeHtml(title)}</h4>
          <p>${escapeHtml(entry?.sourceLine || "自动生成草稿")}</p>
        </div>
        <span class="owner-pill">${escapeHtml(getBatchReviewStateLabel(entry.reviewState))}</span>
      </div>
      ${
        entry?.notes?.length
          ? `<ul class="owner-batch-draft__notes">${entry.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}</ul>`
          : ""
      }
      <div class="owner-form__actions owner-form__actions--compact">
        <button type="button" data-batch-entry-state="confirmed">确认本条</button>
        <button type="button" data-batch-entry-state="discarded">标记放弃</button>
        <button type="button" data-batch-entry-state="unconfirmed">恢复未确认</button>
      </div>
      <div class="owner-batch-draft__grid">${fieldHtml}</div>
    </article>`;
};

const readBatchDraftEntities = () => {
  const nextDraftEntities = cloneBatchDraftEntities(state.batchDraftEntities || state.batchSession?.draftEntities);
  document.querySelectorAll("[data-batch-draft-type][data-batch-draft-index][data-batch-draft-field]").forEach((input) => {
    const entryType = input.dataset.batchDraftType;
    const entryIndex = Number(input.dataset.batchDraftIndex);
    const field = input.dataset.batchDraftField;
    const entry = nextDraftEntities?.[`${entryType}s`]?.[entryIndex];
    if (!entry?.entity || !field) {
      return;
    }
    applyBatchDraftFieldValue(entryType, entry, field, input.value);
  });
  state.batchDraftEntities = nextDraftEntities;
  return nextDraftEntities;
};

const updateBatchEntryReviewState = (reviewState) => {
  const draftEntities = readBatchDraftEntities();
  const descriptor = getBatchSelectedDescriptor(draftEntities);
  if (!descriptor) {
    return;
  }
  draftEntities[`${descriptor.entryType}s`][descriptor.index].reviewState = reviewState;
  state.batchDraftEntities = draftEntities;
  renderBatchSession();
};

const renderBatchSession = () => {
  const session = state.batchSession;
  const draftEntities = state.batchDraftEntities || session?.draftEntities || null;
  if (!session || !draftEntities) {
    batchInlineStatus.textContent = "等待分析。";
    batchSessionSummary.innerHTML = "等待分析。";
    batchPreviewCount.textContent = "0 条";
    batchPreviewList.innerHTML = '<p class="owner-empty">分析完成后，这里会显示待创建条目及预填内容。</p>';
    batchReviewStatus.textContent = "尚未执行";
    batchReviewList.innerHTML = '<p class="owner-empty">尚未生成自动检查候选。</p>';
    batchConfirmCreateButton.disabled = true;
    batchCheckButton.disabled = true;
    batchApplyButton.disabled = true;
    batchAbandonButton.disabled = true;
    batchAbandonUnconfirmedButton.disabled = true;
    return;
  }

  const entries = getBatchFlatEntries(draftEntities);
  const selected = getBatchSelectedDescriptor(draftEntities);
  const counts = entries.reduce(
    (accumulator, item) => {
      accumulator[item.entry.reviewState] += 1;
      return accumulator;
    },
    { confirmed: 0, unconfirmed: 0, discarded: 0 },
  );
  batchPreviewCount.textContent = `${entries.length} 条`;
  batchPreviewList.innerHTML = entries.length
    ? buildBatchPreviewShellHtml(
        entries.map((item) => buildBatchDraftCardHtml(item.entryType, item.entry, item.index, item.key === selected?.key)).join(""),
        buildBatchDetailPanelHtml(selected),
      )
    : '<p class="owner-empty">当前分析结果没有生成可创建条目。</p>';

  batchSessionSummary.innerHTML = `
    <div><strong>${escapeHtml(session.sourceFileName || "直接输入文本")}</strong></div>
    <div>状态：${escapeHtml(session.status)}${session.llmUsed ? " / 已启用 LLM 辅助" : " / 纯规则模式"}</div>
    <div>已选作曲家：${escapeHtml(buildComposerOptionLabel(getComposerById(session.selectedComposerId || session.composerId) || { name: "", nameLatin: "" }))}</div>
    <div>已选作品：${escapeHtml(getWorkDisplayLabel(getWorkById(session.selectedWorkId || session.workId) || { title: "", titleLatin: "", composerId: "" }, state.library || { composers: [] }))}</div>
    <div>版本模板：${escapeHtml(session.workTypeHint || "unknown")}</div>
    <div>草稿统计：已确认 ${counts.confirmed} / 未确认 ${counts.unconfirmed} / 已放弃 ${counts.discarded}</div>
    <div>条目数量：人物 ${draftEntities.people.length} / 作品 ${draftEntities.works.length} / 版本 ${draftEntities.recordings.length}</div>
    ${
      session.warnings?.length
        ? `<div>警告：${session.warnings.map((item) => escapeHtml(item)).join("；")}</div>`
        : "<div>警告：无</div>"
    }
    ${
      session.parseNotes?.length
        ? `<div>分析备注：${session.parseNotes.map((item) => escapeHtml(item)).join("；")}</div>`
        : ""
    }`;

  batchInlineStatus.textContent =
    session.status === "checked"
      ? `已完成自动检查。当前已确认 ${counts.confirmed} 条，可确认应用；也可放弃未确认条目后继续。`
      : session.status === "created"
        ? `已保存批量草稿。当前已确认 ${counts.confirmed} 条，只有这些条目会进入自动检查。`
        : `分析完成。当前共 ${entries.length} 条草稿，默认都为未确认，请逐条复核后再继续。`;

  batchConfirmCreateButton.disabled = !["analyzed", "created", "checked"].includes(session.status) || entries.length === 0;
  batchCheckButton.disabled = !["created", "checked"].includes(session.status) || counts.confirmed === 0;
  batchApplyButton.disabled = session.status !== "checked" || counts.confirmed === 0;
  batchAbandonButton.disabled = !entries.length;
  batchAbandonUnconfirmedButton.disabled = counts.unconfirmed + counts.discarded === 0;
};

const renderBatchReview = async () => {
  const session = state.batchSession;
  if (!session?.runId) {
    batchReviewStatus.textContent =
      session?.status === "checked" ? "未产生候选" : session ? "尚未执行自动检查" : "尚未执行";
    batchReviewList.innerHTML = '<p class="owner-empty">自动检查完成后，这里会显示候选审查卡片。</p>';
    return;
  }
  const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(session.runId)}`);
  state.batchSession = {
    ...state.batchSession,
    runId: run.id,
    run,
  };
  const pendingProposals = filterPendingProposalsForDisplay(run.proposals || []);
  batchReviewStatus.textContent = `候选 ${run.summary.total} 条 / 待处理 ${run.summary.pending} 条 / 已应用 ${run.summary.applied} 条 / 已忽略 ${run.summary.ignored} 条`;
  batchReviewList.innerHTML = buildProposalCardsHtml(run, pendingProposals, {
    mode: "batch",
    allowDirectApply: false,
    library: state.batchSession.draftLibrary,
  });
};

const refreshBatchState = async () => {
  const { sessions } = await fetchJson("/api/batch-import/sessions");
  const preferredSession = selectBatchSessionAfterRefresh(
    sessions,
    state.batchSession?.id || "",
    state.batchPreferEmptyState,
  );
  state.batchPreferEmptyState = false;
  if (!preferredSession) {
    state.batchSession = null;
    state.batchDraftEntities = null;
    renderBatchSession();
    await renderBatchReview();
    return;
  }
  state.batchSession = preferredSession;
  state.batchDraftEntities = cloneBatchDraftEntities(preferredSession.draftEntities);
  if (!compact(batchSourceText.value)) {
    batchSourceText.value = preferredSession.sourceText || "";
  }
  if (batchComposerSelect) {
    batchComposerSelect.value = preferredSession.selectedComposerId || preferredSession.composerId || "";
  }
  populateWorkSelectOptions(batchWorkSelect, batchComposerSelect?.value || "", preferredSession.selectedWorkId || preferredSession.workId || "", {
    ready: "请选择批量作品",
    empty: "请先选择批量作曲家",
  });
  if (batchWorkTypeSelect) {
    batchWorkTypeSelect.value = preferredSession.workTypeHint || "unknown";
  }
  renderBatchSession();
  await renderBatchReview();
};

const runBatchAnalyze = async () => {
  const sourceText = batchSourceText.value || "";
  if (!compact(sourceText)) {
    throw new Error("请先输入或上传批量更新文本。");
  }
  if (!compact(batchComposerSelect?.value || "")) {
    throw new Error("请先选择批量导入作曲家。");
  }
  if (!compact(batchWorkSelect?.value || "")) {
    throw new Error("请先选择批量导入作品。");
  }
  const result = await fetchJson("/api/batch-import/analyze", {
    method: "POST",
    body: JSON.stringify({
      sourceText,
      sourceFileName: batchFileInput?.files?.[0]?.name || "",
      selectedComposerId: batchComposerSelect?.value || "",
      selectedWorkId: batchWorkSelect?.value || "",
      workTypeHint: batchWorkTypeSelect?.value || "unknown",
    }),
  });
  state.batchSession = result.session;
  state.batchDraftEntities = cloneBatchDraftEntities(result.session?.draftEntities);
  state.batchSelectedEntry = { type: "recording", index: 0 };
  renderBatchSession();
  await renderBatchReview();
  batchInlineStatus.textContent = "分析完成，请逐条检查并确认需要保留的条目。";
  batchProgressStatus.textContent = `状态：分析完成，生成 ${getBatchFlatEntries(state.batchDraftEntities).length} 条草稿。`;
  setActiveView("batch");
  setResult(buildBatchResultSummary("analyze", result));
};

const confirmBatchCreate = async () => {
  if (!state.batchSession?.id) {
    throw new Error("当前没有可确认的批量草稿。");
  }
  const result = await fetchJson(`/api/batch-import/${encodeURIComponent(state.batchSession.id)}/confirm-create`, {
    method: "POST",
    body: JSON.stringify({
      draftEntities: readBatchDraftEntities(),
    }),
  });
  state.batchSession = result.session;
  state.batchDraftEntities = cloneBatchDraftEntities(result.session?.draftEntities);
  await refreshLibrary();
  renderBatchSession();
  await renderBatchReview();
  batchInlineStatus.textContent = "批量草稿已保存。只有已确认条目会进入自动检查。";
  setResult(buildBatchResultSummary("confirm-create", result));
};

const runBatchCheck = async () => {
  if (!state.batchSession?.id) {
    throw new Error("请先完成分析并确认创建。");
  }
  const result = await fetchJson(`/api/batch-import/${encodeURIComponent(state.batchSession.id)}/check`, {
    method: "POST",
    body: JSON.stringify({
      draftEntities: readBatchDraftEntities(),
    }),
  });
  state.batchSession = result.session;
  state.batchDraftEntities = cloneBatchDraftEntities(result.session?.draftEntities);
  renderBatchSession();
  await renderBatchReview();
  batchInlineStatus.textContent = result.run ? "自动检查已完成，请在下方复核候选。" : "当前没有已确认条目可供自动检查。";
  batchProgressStatus.textContent = result.run ? "状态：自动检查完成。" : "状态：没有可检查的已确认条目。";
  setResult(buildBatchResultSummary("check", result));
};

const applyBatchSession = async () => {
  if (!state.batchSession?.id) {
    throw new Error("当前没有可应用的批量会话。");
  }
  const result = await fetchJson(`/api/batch-import/${encodeURIComponent(state.batchSession.id)}/apply`, {
    method: "POST",
    body: JSON.stringify({
      draftEntities: readBatchDraftEntities(),
    }),
  });
  await refreshAll();
  batchInlineStatus.textContent = "已应用本批次中所有已确认条目。";
  batchProgressStatus.textContent = "状态：已应用当前批次。";
  setActiveView("batch");
  setResult(buildBatchResultSummary("apply", result));
};

const abandonBatchSession = async () => {
  if (!state.batchSession?.id) {
    throw new Error("当前没有可放弃的批量会话。");
  }
  const result = await fetchJson(`/api/batch-import/${encodeURIComponent(state.batchSession.id)}/abandon`, {
    method: "POST",
  });
  state.batchSession = null;
  state.batchDraftEntities = null;
  state.batchPreferEmptyState = true;
  renderBatchSession();
  await refreshAll();
  batchInlineStatus.textContent = "已放弃整个批量会话。";
  batchProgressStatus.textContent = "状态：已放弃当前批次。";
  setActiveView("batch");
  setResult(buildBatchResultSummary("abandon", result));
};

const abandonBatchUnconfirmed = async () => {
  if (!state.batchSession?.id) {
    throw new Error("当前没有可处理的批量会话。");
  }
  const result = await fetchJson(`/api/batch-import/${encodeURIComponent(state.batchSession.id)}/abandon-unconfirmed`, {
    method: "POST",
    body: JSON.stringify({
      draftEntities: readBatchDraftEntities(),
    }),
  });
  state.batchSession = result.session;
  state.batchDraftEntities = cloneBatchDraftEntities(result.session?.draftEntities);
  renderBatchSession();
  await renderBatchReview();
  batchInlineStatus.textContent = "已清理未确认和已标记放弃的条目。";
  batchProgressStatus.textContent = "状态：已清理未确认条目。";
  setResult(buildBatchResultSummary("abandon-unconfirmed", result));
};

const refreshLibrary = async () => {
  const payload = await fetchJson("/api/library");
  state.library = payload.library;
  state.articles = payload.articles || [];
  state.dataIssues = payload.dataIssues || [];
  state.libraryMeta = payload.libraryMeta || null;
  renderLibraryStatus();
  refreshOverview();
  renderArticleSearchResults();
};

const refreshSite = async () => {
  const { site } = await fetchJson("/api/site");
  state.site = site;
  fillSiteForm(site);
};

const readStoredLlmApiKey = () => {
  try {
    return window.localStorage.getItem(llmDraftStorageKey) || "";
  } catch {
    return "";
  }
};

const rememberLlmApiKey = (value) => {
  const normalized = compact(value);
  try {
    if (normalized) {
      window.localStorage.setItem(llmDraftStorageKey, normalized);
    }
  } catch {
    // ignore local storage failures in local owner tool
  }
  return normalized;
};

const getProposalDraftStateKey = (runId, proposalId) => `${compact(runId)}::${compact(proposalId)}`;
const getProposalDraftStorageKey = (runId, proposalId) =>
  `${proposalDraftStoragePrefix}:${compact(runId)}:${compact(proposalId)}`;
const normalizeProposalDraft = (draft = {}) => {
  const safeDraft = draft && typeof draft === "object" ? draft : {};
  const normalized = {
    selectedImageCandidateId:
      typeof safeDraft.selectedImageCandidateId === "string" ? safeDraft.selectedImageCandidateId : undefined,
    fieldsPatchMap:
      safeDraft.fieldsPatchMap && typeof safeDraft.fieldsPatchMap === "object"
        ? Object.fromEntries(Object.entries(safeDraft.fieldsPatchMap))
        : {},
  };
  return normalized;
};
const readStoredProposalDraft = (runId, proposalId) => {
  try {
    const stored = window.localStorage.getItem(getProposalDraftStorageKey(runId, proposalId));
    if (!stored) {
      return null;
    }
    return normalizeProposalDraft(JSON.parse(stored));
  } catch {
    return null;
  }
};
const getProposalDraft = (runId, proposalId) => {
  const stateKey = getProposalDraftStateKey(runId, proposalId);
  if (state.proposalDrafts[stateKey]) {
    return state.proposalDrafts[stateKey];
  }
  const stored = readStoredProposalDraft(runId, proposalId);
  if (stored) {
    state.proposalDrafts[stateKey] = stored;
    return stored;
  }
  return null;
};
const rememberProposalDraft = (runId, proposalId, draft = {}) => {
  const normalized = normalizeProposalDraft(draft);
  const stateKey = getProposalDraftStateKey(runId, proposalId);
  state.proposalDrafts[stateKey] = normalized;
  try {
    window.localStorage.setItem(getProposalDraftStorageKey(runId, proposalId), JSON.stringify(normalized));
  } catch {
    // ignore local storage failures in local owner tool
  }
  return normalized;
};
const clearProposalDraft = (runId, proposalId) => {
  const stateKey = getProposalDraftStateKey(runId, proposalId);
  delete state.proposalDrafts[stateKey];
  try {
    window.localStorage.removeItem(getProposalDraftStorageKey(runId, proposalId));
  } catch {
    // ignore local storage failures in local owner tool
  }
};

const setLlmConfigForm = (config) => {
  const storedDraft = readStoredLlmApiKey();
  const existingInput = llmConfigForm.elements.apiKey.value;
  const resolvedApiKey = config?.apiKey || existingInput || storedDraft || "";

  state.llmConfig = {
    ...(config || {}),
    apiKey: resolvedApiKey,
  };
  state.llmHasStoredKey = Boolean(config?.hasApiKey || resolvedApiKey);

  llmConfigForm.elements.enabled.checked = Boolean(config?.enabled);
  llmConfigForm.elements.baseUrl.value = config?.baseUrl || "";
  llmConfigForm.elements.apiKey.value = resolvedApiKey;
  llmConfigForm.elements.apiKey.placeholder = state.llmHasStoredKey ? "已保存密钥，如需更换可直接覆盖" : "请输入 API key";
  llmConfigForm.elements.model.value = config?.model || "";
  llmConfigForm.elements.timeoutMs.value = config?.timeoutMs || "";

  if (resolvedApiKey) {
    rememberLlmApiKey(resolvedApiKey);
  }

  if (!config?.enabled) {
    llmConfigSummary.textContent = "未启用，自动检查将回退到纯规则模式";
  } else if (!state.llmHasStoredKey) {
    llmConfigSummary.textContent = `${config?.model || "未指定模型"} · 已启用，但缺少密钥`;
  } else {
    llmConfigSummary.textContent = `${config?.model || "未指定模型"} · 已启用，已保存密钥`;
  }
};

const openTextDialog = (title, content) => {
  textDialogTitle.textContent = title || "完整内容";
  textDialogContent.value = content || "";
  textDialogContent.readOnly = true;
  textDialogSave.hidden = true;
  if (typeof textDialog.showModal === "function") {
    textDialog.showModal();
  } else {
    textDialog.setAttribute("open", "open");
  }
};

const closeTextDialog = () => {
  state.infoPanelTextDialogContext = null;
  if (typeof textDialog.close === "function") {
    textDialog.close();
  } else {
    textDialog.removeAttribute("open");
  }
};

const buildLlmConfigPayload = () => {
  const apiKey = compact(llmConfigForm.elements.apiKey.value) || compact(state.llmConfig?.apiKey || "") || readStoredLlmApiKey();
  return {
    enabled: llmConfigForm.elements.enabled.checked,
    baseUrl: compact(llmConfigForm.elements.baseUrl.value),
    ...(apiKey ? { apiKey } : {}),
    model: compact(llmConfigForm.elements.model.value),
    timeoutMs: Number(compact(llmConfigForm.elements.timeoutMs.value) || 0) || undefined,
  };
};

const refreshLlmConfig = async () => {
  const { config } = await fetchJson("/api/automation/llm/config");
  setLlmConfigForm(config);
};

const renderReviewRun = async () => {
  const runId = reviewRunSelect.value || state.activeRunId;
  if (!runId) {
    proposalReviewList.innerHTML = '<p class="owner-empty">暂无候选 run。</p>';
    reviewPaginationTop.innerHTML = "";
    reviewPaginationBottom.innerHTML = "";
    return;
  }
  const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(runId)}`);
  state.activeRunId = run.id;
  reviewRunSelect.value = run.id;
  const pageData = getRunPageProposals(run);
  state.reviewPage = pageData.page;
  reviewPaginationTop.innerHTML = renderReviewPagination(pageData);
  reviewPaginationBottom.innerHTML = renderReviewPagination(pageData);
  proposalReviewList.innerHTML = buildProposalCardsHtml(run, pageData.items, { inline: false });
};

const renderLogRun = async () => {
  const runId = logRunSelect.value || state.activeLogRunId;
  if (!runId) {
    runLogPanel.innerHTML = '<p class="owner-empty">暂无 run 日志。</p>';
    return;
  }
  const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(runId)}`);
  state.activeLogRunId = run.id;
  logRunSelect.value = run.id;
  const notes = run.notes?.length
    ? run.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")
    : "<li>当前没有额外说明。</li>";
  const snapshots = run.snapshots?.length
    ? run.snapshots
        .map(
          (snapshot) => `
            <article class="owner-snapshot">
              <h4>${escapeHtml(snapshot.proposalId)}</h4>
              <p>${escapeHtml(snapshot.entityType)} / ${escapeHtml(snapshot.entityId)}</p>
              <button type="button" data-snapshot-revert="${escapeHtml(snapshot.id)}">回滚此快照</button>
            </article>`,
        )
        .join("")
    : '<p class="owner-empty">当前没有可回滚快照。</p>';
  runLogPanel.innerHTML = `
    <article class="owner-log-entry">
      <h3>${escapeHtml(run.id)}</h3>
      <p>总候选：${run.summary.total} / 待处理：${run.summary.pending} / 已应用：${run.summary.applied} / 已忽略：${run.summary.ignored}</p>
      <ul>${notes}</ul>
      <div class="owner-log-entry__snapshots">${snapshots}</div>
    </article>`;
};

const refreshRuns = async () => {
  const { runs } = await fetchJson("/api/automation/runs");
  state.runs = runs;
  const optionsHtml =
    '<option value="">请选择</option>' +
    runs.map((run) => `<option value="${escapeHtml(run.id)}">${escapeHtml(`${run.id} / ${run.summary.pending}`)}</option>`).join("");
  reviewRunSelect.innerHTML = optionsHtml;
  logRunSelect.innerHTML = optionsHtml;

  if (state.activeRunId && runs.some((run) => run.id === state.activeRunId)) {
    reviewRunSelect.value = state.activeRunId;
  } else if (runs[0]) {
    state.activeRunId = runs[0].id;
    reviewRunSelect.value = runs[0].id;
  }
  if (state.activeLogRunId && runs.some((run) => run.id === state.activeLogRunId)) {
    logRunSelect.value = state.activeLogRunId;
  } else if (runs[0]) {
    state.activeLogRunId = runs[0].id;
    logRunSelect.value = runs[0].id;
  }

  await Promise.all([renderReviewRun(), renderLogRun()]);
};

const refreshAll = async () => {
  await Promise.all([refreshLibrary(), refreshSite(), refreshRuns(), refreshLlmConfig(), refreshBatchState()]);
};

const getSelectedImageCandidateId = (proposalId, selectors) => {
  for (const selector of selectors) {
    const select = document.querySelector(buildDataAttributeSelector(selector, proposalId));
    if (select) {
      return select.value;
    }
  }
  return "";
};
const getProposalFieldInputValue = (proposalId, fieldPath, options = {}) => {
  const { inputAttr: proposalAttr, fieldPathAttr: fieldAttr } = getProposalModeAttributes(options);
  const selector = `[${proposalAttr}="${String(proposalId)}"][${fieldAttr}="${String(fieldPath).replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"]`;
  const input = document.querySelector(selector);
  return input ? input.value : undefined;
};
const buildProposalPatchMap = (proposal, options = {}) => {
  const patchMap = {};
  const entity = getEntityByTypeAndId(proposal.entityType, proposal.entityId, options.library || state.library);
  for (const field of buildEditableFieldDescriptors(proposal, proposal.entityType, entity)) {
    const rawValue = getProposalFieldInputValue(proposal.id, field.path, options);
    if (rawValue === undefined) {
      continue;
    }
    if (field.path === "lifeRange") {
      const parsed = parseLifeRangeInput(rawValue);
      patchMap.birthYear = parseIntegerString(parsed.birthYear);
      patchMap.deathYear = parseIntegerString(parsed.deathYear);
      continue;
    }
    patchMap[field.path] = parseProposalFieldInputValue(field, rawValue);
  }
  return patchMap;
};
const captureProposalDraft = (proposal, options = {}) => {
  const selectedImageCandidateId =
    options.imageCandidateId ??
    getSelectedImageCandidateId(proposal.id, [
      getProposalModeAttributes(options).imageSelectAttr,
      "data-image-select",
      "data-inline-image-select",
      "data-batch-image-select",
    ]);
  return normalizeProposalDraft({
    selectedImageCandidateId,
    fieldsPatchMap: buildProposalPatchMap(proposal, options),
  });
};
const rememberProposalDraftFromForm = (proposal, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  if (!runId || !proposal?.id) {
    return null;
  }
  const draft = captureProposalDraft(proposal, options);
  return rememberProposalDraft(runId, proposal.id, draft);
};

const syncUpdatedRunState = async (run, options = {}) => {
  state.activeRunId = run.id;
  if (options.mode === "batch" && state.batchSession) {
    state.batchSession = {
      ...state.batchSession,
      runId: run.id,
      run,
    };
    await renderBatchReview();
    return;
  }
  if (options.inline) {
    state.inlineCheck.runId = run.id;
    await renderInlineCheckJob({ ...(state.activeJob || {}), run });
    return;
  }
  await refreshRuns();
};

const persistProposalDraftIfNeeded = async (proposal, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  if (!runId || !proposal?.id) {
    return { runId, persisted: false };
  }
  const comparableProposal = buildComparableProposal(proposal);
  const storedDraft = getProposalDraft(runId, proposal.id);
  const liveDraft = captureProposalDraft(comparableProposal, options);
  const draft = resolveProposalDraft(comparableProposal, liveDraft, storedDraft);
  if (draft && draft !== storedDraft) {
    rememberProposalDraft(runId, proposal.id, draft);
  }
  if (!hasProposalDraftChanges(comparableProposal, draft)) {
    clearProposalDraft(runId, proposal.id);
    return {
      runId,
      selectedImageCandidateId: draft?.selectedImageCandidateId || comparableProposal.selectedImageCandidateId || "",
      persisted: false,
    };
  }

  const { run } = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposal.id)}/edit`, {
    method: "POST",
    body: JSON.stringify({
      runId,
      fieldsPatchMap: draft?.fieldsPatchMap || {},
      selectedImageCandidateId: draft?.selectedImageCandidateId || "",
    }),
  });

  clearProposalDraft(runId, proposal.id);
  clearProposalDraft(run.id, proposal.id);
  await syncUpdatedRunState(run, options);
  return {
    runId: run.id,
    selectedImageCandidateId: draft?.selectedImageCandidateId || "",
    persisted: true,
  };
};
const persistProposalDrafts = async (run, proposals, options = {}) => {
  let currentRun = run;
  let currentRunId = run?.id || options.runId || "";
  for (const proposal of proposals) {
    const currentProposal = currentRun?.proposals?.find((item) => item.id === proposal.id) || proposal;
    const result = await persistProposalDraftIfNeeded(currentProposal, {
      ...options,
      runId: currentRunId,
    });
    currentRunId = result.runId || currentRunId;
    if (!result.persisted) {
      continue;
    }
    const payload = await fetchJson(`/api/automation/runs/${encodeURIComponent(currentRunId)}`);
    currentRun = payload.run;
  }
  return {
    run: currentRun,
    runId: currentRunId,
  };
};
const editProposal = async (proposal, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  const selectedImageCandidateId =
    options.imageCandidateId ?? captureProposalDraft(proposal, options).selectedImageCandidateId;
  const fieldsPatchMap = options.fieldsPatchMap || buildProposalPatchMap(proposal, options);
  const { run } = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposal.id)}/edit`, {
    method: "POST",
    body: JSON.stringify({
      runId,
      fieldsPatchMap,
      selectedImageCandidateId,
    }),
  });
  await syncUpdatedRunState(run, options);
  clearProposalDraft(runId, proposal.id);
  clearProposalDraft(run.id, proposal.id);
  setResult({ editedProposal: proposal.id, fields: Object.keys(fieldsPatchMap).length, inline: Boolean(options.inline) });
};
const uploadProposalImage = async (proposalId, file, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  const contentBase64 = await readFileAsBase64(file);
  const { run } = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposalId)}/upload-image`, {
    method: "POST",
    body: JSON.stringify({
      runId,
      fileName: file.name || "upload.jpg",
      contentBase64,
    }),
  });
  await syncUpdatedRunState(run, options);
  setResult({ uploadedProposalImage: proposalId, fileName: file.name || "upload.jpg" });
};

const getCurrentReviewPageProposalIds = () => {
  const run = getReviewRunById(reviewRunSelect.value || state.activeRunId);
  if (!run) {
    return [];
  }
  return getRunPageProposals(run).items.map((proposal) => proposal.id);
};
void [
  saveRecordingLinkDialog,
  buildBatchDraftEntryHtml,
  buildInlineEntityEditorHtml,
  buildEntitySummaryHtml,
  buildProposalLinkCandidatesHtml,
  getCurrentReviewPageProposalIds,
];

const getReviewActionProposals = (run, action, scope = "all") =>
  getProposalsForReviewAction(run?.proposals || [], action, {
    scope,
    scopeIds: scope === "page" ? getRunPageProposals(run).items.map((proposal) => proposal.id) : [],
  });

const getBlockedReviewActionProposals = (run, action, scope = "all") =>
  getBlockedProposalsForReviewAction(run?.proposals || [], action, {
    scope,
    scopeIds: scope === "page" ? getRunPageProposals(run).items.map((proposal) => proposal.id) : [],
  });

const assertNoBlockedReviewActionProposals = (run, action, scope = "all") => {
  const blocked = getBlockedReviewActionProposals(run, action, scope);
  if (blocked.length) {
    throw new Error(buildBlockedReviewActionMessage(blocked, action));
  }
  return blocked;
};

const updateProposalState = async (proposalId, reviewState, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  const imageCandidateId =
    options.imageCandidateId ||
    getSelectedImageCandidateId(proposalId, ["data-image-select", "data-inline-image-select", "data-batch-image-select"]);
  const payload = {
    runId,
    reviewState,
    selectedImageCandidateId: imageCandidateId,
  };
  const { run } = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposalId)}/review-state`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await syncUpdatedRunState(run, options);
  setResult({ proposalId, reviewState });
};

const applyProposal = async (proposalId, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  const imageCandidateId =
    options.imageCandidateId ||
    getSelectedImageCandidateId(proposalId, ["data-image-select", "data-inline-image-select", "data-batch-image-select"]);
  const result = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposalId)}/apply`, {
    method: "POST",
    body: JSON.stringify({
      runId,
      imageCandidateId,
    }),
  });
  await refreshAll();
  if (!options.inline && state.inlineCheck.runId && state.inlineCheck.runId === runId) {
    clearInlineCheck();
  }
  if (options.inline && state.activeEntity.type && state.activeEntity.id) {
    await loadEntity(state.activeEntity.type, state.activeEntity.id);
    setActiveView("detail");
  }
  setResult(result);
};

const ignoreProposal = async (proposalId, options = {}) => {
  const runId = options.runId || reviewRunSelect.value || state.inlineCheck.runId;
  const result = await fetchJson(`/api/automation/proposals/${encodeURIComponent(proposalId)}/ignore`, {
    method: "POST",
    body: JSON.stringify({ runId }),
  });
  if (options.inline || options.mode === "batch") {
    if (result.run) {
      await syncUpdatedRunState(result.run, options);
    }
    if (options.inline) {
      await loadEntity(state.activeEntity.type, state.activeEntity.id);
      setActiveView("detail");
    }
  } else {
    await refreshRuns();
    if (state.inlineCheck.runId && state.inlineCheck.runId === runId) {
      clearInlineCheck();
    }
  }
  setResult(result);
};

const buildUnifiedProposalOptions = (card) => {
  const mode = compact(card?.dataset?.ownerProposalMode || "review");
  const runId = compact(card?.dataset?.ownerRunId || "");
  if (mode === "inline") {
    return { inline: true, runId };
  }
  if (mode === "batch") {
    return { mode: "batch", runId, library: state.batchSession?.draftLibrary };
  }
  return { runId };
};

const handleUnifiedProposalAction = async (button) => {
  const card = button.closest("article[data-owner-proposal-id]");
  const { proposalId, action, mode, runId } = resolveProposalActionContext(button, card);
  const options = buildUnifiedProposalOptions(card);
  if (!proposalId || !action || !mode || !runId || !options.runId) {
    throw new Error("候选上下文已失效，请刷新后重试。");
  }
  const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(options.runId)}`);
  const proposal = run?.proposals?.find((item) => item.id === proposalId);
  if (!proposal) {
    throw new Error("候选不存在或已失效。");
  }
  const persistedDraft = await persistProposalDraftIfNeeded(proposal, options);
  const nextRunId = persistedDraft.runId || run.id;
  const nextImageCandidateId = persistedDraft.selectedImageCandidateId || proposal.selectedImageCandidateId || "";
  const nextOptions = {
    ...options,
    runId: nextRunId,
    imageCandidateId: nextImageCandidateId,
  };
  if (action === "viewed") {
    await updateProposalState(proposalId, "viewed", nextOptions);
    return;
  }
  if (action === "confirm") {
    await updateProposalState(proposalId, "confirmed", nextOptions);
    return;
  }
  if (action === "discard") {
    await ignoreProposal(proposalId, nextOptions);
    return;
  }
  if (action === "apply") {
    const blockers = getProposalApplyBlockers(proposal);
    if (blockers.length) {
      throw new Error(
        buildBlockedReviewActionMessage([{ proposal, reasons: blockers }], "apply-confirmed"),
      );
    }
    await applyProposal(proposalId, nextOptions);
  }
};

const getProposalActionPendingText = (action) => {
  if (action === "viewed") {
    return "标记中…";
  }
  if (action === "confirm") {
    return "确认中…";
  }
  if (action === "discard") {
    return "放弃中…";
  }
  if (action === "apply") {
    return "应用中…";
  }
  return "处理中…";
};

const startJobPolling = (jobId, options = {}) => {
  const mode = options.mode || "checks";
  const entityType = options.entityType || "";
  const entityId = options.entityId || "";
  if (state.jobPollTimer) {
    clearInterval(state.jobPollTimer);
  }
  state.activeJobId = jobId;

  const poll = async () => {
    const { job } = await fetchJson(`/api/automation/jobs/${encodeURIComponent(jobId)}`);
    renderJob(job);
    if (mode === "inline" && entityType && entityId) {
      if (state.inlineCheck.entityType !== entityType || state.inlineCheck.entityId !== entityId) {
        openInlineCheck(entityType, entityId, getDisplayTitle(getEntityByTypeAndId(entityType, entityId)));
      }
      state.inlineCheck.jobId = job.id;
      if (job.run?.id) {
        state.inlineCheck.runId = job.run.id;
      }
      await renderInlineCheckJob(job);
    }
    if (["completed", "cancelled"].includes(job.status)) {
      clearInterval(state.jobPollTimer);
      state.jobPollTimer = null;
      if (job.run?.id) {
        state.activeRunId = job.run.id;
        state.activeLogRunId = job.run.id;
      }
      await refreshRuns();
      if (job.run?.id && mode !== "inline") {
        setActiveView("review");
      }
    }
  };

  void poll();
  state.jobPollTimer = setInterval(() => {
    void poll().catch((error) => {
      clearInterval(state.jobPollTimer);
      state.jobPollTimer = null;
      setResult(error instanceof Error ? error.message : String(error));
    });
  }, 1200);
};

const runAutomationJob = async () => {
  if (!state.activeSelectionPreview.total) {
    await refreshSelectionPreview();
  }
  const request = buildSelectionRequest();
  if (!request.categories.length) {
    throw new Error("请先勾选需要自动检查的类别。");
  }
  const { job } = await fetchJson("/api/automation/jobs", {
    method: "POST",
    body: JSON.stringify(request),
  });
  renderJob(job);
  setActiveView("checks");
  startJobPolling(job.id);
  setResult({ jobId: job.id, request });
};

const runSingleEntityCheck = async (entityType) => {
  const form = document.querySelector(`[data-entity-form="${entityType}"]`);
  const entityId = compact(form?.elements.existingId?.value || "");
  if (!entityId) {
    throw new Error("请先载入已有条目，再执行单条自动检查。");
  }
  setActiveView("detail");
  setActiveDetailTab(getVisibleDetailTabForEntity(entityType, getEntityByTypeAndId(entityType, entityId)), {
    panel: entityType === "composer" ? "composer" : undefined,
  });
  const { job } = await fetchJson(`/api/automation/entity-check/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`, {
    method: "POST",
  });
  openInlineCheck(entityType, entityId, getDisplayTitle(getEntityByTypeAndId(entityType, entityId)));
  inlineCheckContent.innerHTML = '<p class="owner-empty">正在为当前条目启动自动检查...</p>';
  renderJob(job);
  startJobPolling(job.id, { mode: "inline", entityType, entityId });
  setResult({ jobId: job.id, entityType, entityId });
};

const applyInlineConfirmedProposals = async () => {
  if (!state.inlineCheck.runId || !state.inlineCheck.entityType || !state.inlineCheck.entityId) {
    throw new Error("当前没有可处理的单条检查结果。");
  }
  let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(state.inlineCheck.runId)}`);
  ({ run } = await persistProposalDrafts(
    run,
    getEntityScopedProposals(run, state.inlineCheck.entityType, state.inlineCheck.entityId),
    { inline: true, runId: run.id },
  ));
  const scopedProposals = getEntityScopedProposals(run, state.inlineCheck.entityType, state.inlineCheck.entityId);
  const blocked = scopedProposals
    .filter((proposal) => proposal.reviewState === "confirmed" && proposal.status === "pending")
    .map((proposal) => ({ proposal, reasons: getProposalApplyBlockers(proposal) }))
    .filter((entry) => entry.reasons.length > 0);
  if (blocked.length) {
    throw new Error(buildBlockedReviewActionMessage(blocked, "apply-confirmed"));
  }
  const proposals = scopedProposals.filter(
    (proposal) => proposal.reviewState === "confirmed" && proposal.status === "pending" && isProposalDirectlyApplicable(proposal),
  );
  if (!proposals.length) {
    throw new Error("当前条目没有已确认且可应用的候选。");
  }
  for (const proposal of proposals) {
    await applyProposal(proposal.id, {
      inline: true,
      runId: run.id,
      imageCandidateId: proposal.selectedImageCandidateId || "",
    });
  }
};

const discardInlinePendingProposals = async () => {
  if (!state.inlineCheck.runId || !state.inlineCheck.entityType || !state.inlineCheck.entityId) {
    throw new Error("当前没有可处理的单条检查结果。");
  }
  let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(state.inlineCheck.runId)}`);
  ({ run } = await persistProposalDrafts(
    run,
    getEntityScopedProposals(run, state.inlineCheck.entityType, state.inlineCheck.entityId),
    { inline: true, runId: run.id },
  ));
  const proposals = getEntityScopedProposals(run, state.inlineCheck.entityType, state.inlineCheck.entityId).filter(
    (proposal) => proposal.status === "pending",
  );
  if (!proposals.length) {
    throw new Error("当前条目没有待放弃的候选。");
  }
  for (const proposal of proposals) {
    await ignoreProposal(proposal.id, { inline: true, runId: run.id });
  }
};

const applyCurrentPageConfirmedProposals = async () => {
  if (!reviewRunSelect.value) {
    throw new Error("请先选择候选 run。");
  }
  let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
  assertNoBlockedReviewActionProposals(run, "apply-confirmed", "page");
  ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "apply-confirmed", "page"), {
    runId: run.id,
  }));
  assertNoBlockedReviewActionProposals(run, "apply-confirmed", "page");
  const proposals = getReviewActionProposals(run, "apply-confirmed", "page");
  if (!proposals.length) {
    throw new Error("当前页没有已确认且可应用的候选。");
  }
  for (const proposal of proposals) {
    await applyProposal(proposal.id, {
      runId: run.id,
      imageCandidateId: proposal.selectedImageCandidateId || "",
    });
  }
};

const ignoreCurrentPagePendingProposals = async () => {
  if (!reviewRunSelect.value) {
    throw new Error("请先选择候选 run。");
  }
  let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
  ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "ignore-pending", "page"), {
    runId: run.id,
  }));
  const proposals = getReviewActionProposals(run, "ignore-pending", "page");
  if (!proposals.length) {
    throw new Error("当前页没有待放弃的候选。");
  }
  for (const proposal of proposals) {
    await ignoreProposal(proposal.id, { runId: run.id });
  }
};

const refreshSelectionPreviewDebounced = (() => {
  let timer = 0;
  return () => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => {
      void refreshSelectionPreview().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
    }, 250);
  };
})();

searchResults.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-load-type]");
  if (!button) {
    return;
  }
  try {
    await loadEntity(button.dataset.loadType, button.dataset.loadId);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

issuesPanel.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-issue-entity-type]");
  if (!button) {
    return;
  }
  try {
    setActiveView("detail");
    await loadEntity(button.dataset.issueEntityType, button.dataset.issueEntityId);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

automationSelectionPreview.addEventListener("click", (event) => {
  const actionButton = event.target.closest("[data-selection-action]");
  if (actionButton) {
    const category = actionButton.dataset.selectionCategoryAction;
    const group = state.activeSelectionPreview.groups.find((item) => item.category === category);
    if (!group) {
      return;
    }
    const current = new Set(state.selectionState[category] || []);
    if (actionButton.dataset.selectionAction === "toggle-all") {
      if (current.size === group.items.length) {
        state.selectionState[category] = new Set();
      } else {
        state.selectionState[category] = new Set(group.items.map((item) => item.entityId));
      }
    } else if (actionButton.dataset.selectionAction === "invert") {
      state.selectionState[category] = new Set(
        group.items.map((item) => item.entityId).filter((entityId) => !current.has(entityId)),
      );
    }
    renderSelectionPreview();
    return;
  }
  const chip = event.target.closest("[data-selection-category]");
  if (!chip) {
    return;
  }
  const category = chip.dataset.selectionCategory;
  const entityId = chip.dataset.selectionId;
  const current = state.selectionState[category] || new Set();
  if (current.has(entityId)) {
    current.delete(entityId);
  } else {
    current.add(entityId);
  }
  state.selectionState[category] = current;
  renderSelectionPreview();
});

automationSelectionPreview.addEventListener("input", (event) => {
  const input = event.target.closest("[data-selection-filter]");
  if (!input) {
    return;
  }
  state.selectionFilters[input.dataset.selectionFilter] = input.value || "";
  renderSelectionPreview();
});

automationJobItems.addEventListener("click", (event) => {
  const button = event.target.closest("[data-job-item-id]");
  if (!button || !state.activeJob) {
    return;
  }
  state.activeJobItemId = button.dataset.jobItemId;
  renderJob(state.activeJob);
});

proposalReviewList.addEventListener("click", (event) => {
  const textButton = event.target.closest("[data-open-text]");
  if (!textButton) {
    return;
  }
  openTextDialog(textButton.dataset.openTextTitle || "完整内容", decodeDataText(textButton.dataset.openText));
});

batchReviewList.addEventListener("click", (event) => {
  const textButton = event.target.closest("[data-open-text]");
  if (!textButton) {
    return;
  }
  openTextDialog(textButton.dataset.openTextTitle || "完整内容", decodeDataText(textButton.dataset.openText));
});

proposalReviewList.addEventListener("input", (event) => {
  const proposalField = event.target.closest("[data-proposal-field-input]");
  if (!proposalField) {
    return;
  }
  const run = getReviewRunById(reviewRunSelect.value || state.activeRunId);
  const proposal = run?.proposals?.find((item) => item.id === proposalField.dataset.proposalFieldInput);
  if (proposal) {
    rememberProposalDraftFromForm(proposal, { runId: run.id });
    proposalField.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
  }
});

proposalReviewList.addEventListener("change", async (event) => {
  const select = event.target.closest("[data-image-select]");
  const proposalField = event.target.closest("[data-proposal-field-input]");
  const uploadInput = event.target.closest("[data-image-upload]");
  try {
    if (select) {
      const run = getReviewRunById(reviewRunSelect.value || state.activeRunId);
      const proposal = run?.proposals?.find((item) => item.id === select.dataset.imageSelect);
      if (proposal) {
        await editProposal(proposal, { imageCandidateId: select.value });
      }
      return;
    }
    if (proposalField) {
      const run = getReviewRunById(reviewRunSelect.value || state.activeRunId);
      const proposal = run?.proposals?.find((item) => item.id === proposalField.dataset.proposalFieldInput);
      if (proposal) {
        rememberProposalDraftFromForm(proposal, { runId: run.id });
        proposalField.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
      }
      return;
    }
    if (uploadInput?.files?.[0]) {
      await uploadProposalImage(uploadInput.dataset.imageUpload, uploadInput.files[0]);
      uploadInput.value = "";
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

batchReviewList.addEventListener("input", (event) => {
  const proposalField = event.target.closest("[data-batch-proposal-field-input]");
  if (!proposalField || !state.batchSession?.runId) {
    return;
  }
  const proposal = state.batchSession.run?.proposals?.find((item) => item.id === proposalField.dataset.batchProposalFieldInput);
  if (proposal) {
    rememberProposalDraftFromForm(proposal, {
      mode: "batch",
      runId: state.batchSession.runId,
      library: state.batchSession.draftLibrary,
    });
    proposalField.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
  }
});

batchReviewList.addEventListener("change", async (event) => {
  const select = event.target.closest("[data-batch-image-select]");
  const proposalField = event.target.closest("[data-batch-proposal-field-input]");
  const uploadInput = event.target.closest("[data-batch-image-upload]");
  try {
    if (select && state.batchSession?.runId) {
      const proposal = state.batchSession.run?.proposals?.find((item) => item.id === select.dataset.batchImageSelect);
      if (proposal) {
        await editProposal(proposal, {
          mode: "batch",
          imageCandidateId: select.value,
          runId: state.batchSession.runId,
          library: state.batchSession.draftLibrary,
        });
      }
      return;
    }
    if (proposalField && state.batchSession?.runId) {
      const proposal = state.batchSession.run?.proposals?.find((item) => item.id === proposalField.dataset.batchProposalFieldInput);
      if (proposal) {
        rememberProposalDraftFromForm(proposal, {
          mode: "batch",
          runId: state.batchSession.runId,
          library: state.batchSession.draftLibrary,
        });
        proposalField.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
      }
      return;
    }
    if (uploadInput?.files?.[0] && state.batchSession?.runId) {
      await uploadProposalImage(uploadInput.dataset.batchImageUpload, uploadInput.files[0], {
        mode: "batch",
        runId: state.batchSession.runId,
        library: state.batchSession.draftLibrary,
      });
      uploadInput.value = "";
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

inlineCheckContent.addEventListener("click", async (event) => {
  const textButton = event.target.closest("[data-open-text]");
  if (textButton) {
    openTextDialog(textButton.dataset.openTextTitle || "完整内容", decodeDataText(textButton.dataset.openText));
    return;
  }
  const runButton = event.target.closest("[data-inline-run-action]");
  if (runButton) {
    try {
      if (runButton.dataset.inlineRunAction === "apply-confirmed") {
        await applyInlineConfirmedProposals();
      }
      if (runButton.dataset.inlineRunAction === "discard-pending") {
        await discardInlinePendingProposals();
      }
      if (runButton.dataset.inlineRunAction === "acknowledge") {
        resolveInlineCheck("acknowledged");
      }
      if (runButton.dataset.inlineRunAction === "dismiss") {
        resolveInlineCheck("dismissed");
      }
      if (runButton.dataset.inlineRunAction === "save-entity") {
        await saveInlineEntityEdits();
      }
    } catch (error) {
      setResult(error instanceof Error ? error.message : String(error));
    }
    return;
  }
});

reviewPaginationTop.addEventListener("click", (event) => {
  const button = event.target.closest("[data-review-page-action]");
  if (!button) {
    return;
  }
  if (button.dataset.reviewPageAction === "prev") {
    state.reviewPage = Math.max(1, state.reviewPage - 1);
  }
  if (button.dataset.reviewPageAction === "next") {
    state.reviewPage += 1;
  }
  void renderReviewRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
});

reviewPaginationBottom.addEventListener("click", (event) => {
  const button = event.target.closest("[data-review-page-action]");
  if (!button) {
    return;
  }
  if (button.dataset.reviewPageAction === "prev") {
    state.reviewPage = Math.max(1, state.reviewPage - 1);
  }
  if (button.dataset.reviewPageAction === "next") {
    state.reviewPage += 1;
  }
  void renderReviewRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
});

inlineCheckContent.addEventListener("change", async (event) => {
  const select = event.target.closest("[data-inline-image-select]");
  const proposalField = event.target.closest("[data-inline-proposal-field-input]");
  const uploadInput = event.target.closest("[data-inline-image-upload]");
  const entityImageUploadInput = event.target.closest("[data-inline-entity-image-upload]");
  try {
    if (select) {
      const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(state.inlineCheck.runId)}`);
      const proposal = run?.proposals?.find((item) => item.id === select.dataset.inlineImageSelect);
      if (proposal) {
        await editProposal(proposal, { inline: true, runId: run.id, imageCandidateId: select.value });
      }
      return;
    }
    if (proposalField) {
      const { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(state.inlineCheck.runId)}`);
      const proposal = run?.proposals?.find((item) => item.id === proposalField.dataset.inlineProposalFieldInput);
      if (proposal) {
        rememberProposalDraftFromForm(proposal, { inline: true, runId: run.id });
      }
      return;
    }
    if (uploadInput?.files?.[0]) {
      await uploadProposalImage(uploadInput.dataset.inlineImageUpload, uploadInput.files[0], {
        inline: true,
        runId: state.inlineCheck.runId,
      });
      uploadInput.value = "";
      return;
    }
    if (entityImageUploadInput?.files?.[0]) {
      await uploadInlineEntityImage(entityImageUploadInput.files[0]);
      entityImageUploadInput.value = "";
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

inlineCheckContent.addEventListener("input", (event) => {
  const proposalField = event.target.closest("[data-inline-proposal-field-input]");
  if (!proposalField || !state.inlineCheck.runId) {
    return;
  }
  const run = getReviewRunById(state.inlineCheck.runId);
  const proposal = run?.proposals?.find((item) => item.id === proposalField.dataset.inlineProposalFieldInput);
  if (proposal) {
    rememberProposalDraftFromForm(proposal, {
      inline: true,
      runId: state.inlineCheck.runId,
    });
    proposalField.closest(".owner-proposal")?.classList.add("owner-proposal--dirty");
  }
});

runLogPanel.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-snapshot-revert]");
  if (!button) {
    return;
  }
  try {
    const result = await fetchJson(`/api/automation/snapshots/${encodeURIComponent(button.dataset.snapshotRevert)}/revert`, {
      method: "POST",
      body: JSON.stringify({ runId: logRunSelect.value }),
    });
    await refreshAll();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

entityForms.forEach((form) => {
  form.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-action]");
    if (!button) {
      return;
    }
    const entityType = form.dataset.entityForm;
    try {
      if (button.dataset.action === "reset") {
        resetEntityForm(form);
        setResult(`已清空 ${entityTypeLabels[entityType]} 表单。`);
        return;
      }
      if (button.dataset.action === "merge") {
        const duplicateLabel = compact(form.elements.name?.value || form.elements.title?.value || form.elements.existingId?.value);
        const targetId = compact(form.elements.mergeTargetId?.value || "");
        const targetEntity = getEntityByTypeAndId(entityType, targetId);
        showConfirmDialog({
          title: "合并条目",
          message: `确认将“${duplicateLabel || "当前条目"}”合并到“${
            targetEntity ? getMergeOptionLabel(entityType, targetEntity) : targetId || "主条目"
          }”吗？此操作会迁移引用并删除当前重复条目。`,
          confirmText: "合并条目",
          onConfirm: async () => {
            await withBusyButton(button, "合并中…", () => mergeEntity(form));
          },
        });
        return;
      }
      if (button.dataset.action === "delete") {
        const entityLabel = compact(form.elements.name?.value || form.elements.title?.value || form.elements.existingId?.value);
        showConfirmDialog({
          title: "删除条目",
          message: `确认删除“${entityLabel || "当前条目"}”吗？此操作不可撤销。`,
          confirmText: "确认删除",
          onConfirm: async () => {
            await withBusyButton(button, "删除中…", () => deleteEntity(form));
          },
        });
        return;
      }
      const payload = payloadBuilders[entityType](form);
      if (entityType === "work" && !compact(payload.composerId)) {
        throw new Error("请先选择所属作曲家。");
      }
      if (entityType === "recording" && !compact(payload.workId)) {
        throw new Error("请先选择所属作品。");
      }
      if (button.dataset.action === "preview") {
        const result = await fetchJson(`/api/preview/${encodeURIComponent(entityType)}`, {
          method: "POST",
          body: JSON.stringify(payload),
        });
        openPreviewDialog(`${entityTypeLabels[entityType]}页面预览`, buildAffectedPathsPreviewHtml(result, entityType));
        setResult(result);
        return;
      }
      const result = await fetchJson(`/api/save/${encodeURIComponent(entityType)}`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      await refreshAll();
      if (result.entity?.id) {
        await loadEntity(entityType, result.entity.id);
      }
      setResult(result);
    } catch (error) {
      setResult(error instanceof Error ? error.message : String(error));
    }
  });
  form.addEventListener("change", async (event) => {
    const selectedComposerSelect = event.target.closest('select[name="selectedComposerId"]');
    const conductorSelect = event.target.closest('select[name="conductorPersonId"]');
    const orchestraSelect = event.target.closest('select[name="orchestraPersonId"]');
    const creditRoleSelect = event.target.closest("[data-recording-credit-role]");
    const creditPersonSelect = event.target.closest("[data-recording-credit-person-id]");
    const uploadInput = event.target.closest("[data-image-upload]");
    if (selectedComposerSelect) {
      populateRecordingWorkOptions(form, selectedComposerSelect.value, "");
      return;
    }
    if (conductorSelect) {
      upsertPrimaryRecordingCredit(form, "conductor", conductorSelect.value);
      return;
    }
    if (orchestraSelect) {
      upsertPrimaryRecordingCredit(form, "orchestra", orchestraSelect.value);
      return;
    }
    if (creditRoleSelect) {
      const credits = readRecordingCreditsFromEditor(form);
      syncRecordingCreditsField(form, credits);
      return;
    }
    if (creditPersonSelect) {
      const row = creditPersonSelect.closest("[data-recording-credit-row]");
      const role = compact(row?.querySelector("[data-recording-credit-role]")?.value || "");
      const displayInput = row?.querySelector("[data-recording-credit-display]");
      if (displayInput && compact(creditPersonSelect.value)) {
        displayInput.value = buildRecordingCreditDisplayName(role, creditPersonSelect.value, displayInput.value);
      }
      syncRecordingCreditsField(form, readRecordingCreditsFromEditor(form), { rerender: false });
      syncPrimaryRecordingCreditSelects(form, readRecordingCreditsFromEditor(form));
      return;
    }
    if (!uploadInput?.files?.[0]) {
      return;
    }
    try {
      await uploadEntityImage(form.dataset.entityForm, form, uploadInput.files[0]);
      uploadInput.value = "";
    } catch (error) {
      setResult(error instanceof Error ? error.message : String(error));
    }
  });
  form.addEventListener("input", (event) => {
    const mergeSearchInput = event.target.closest("[data-merge-target-search]");
    const creditDisplayInput = event.target.closest("[data-recording-credit-display]");
    if (!mergeSearchInput) {
      if (!creditDisplayInput) {
        return;
      }
      syncRecordingCreditsField(form, readRecordingCreditsFromEditor(form), { rerender: false });
      syncPrimaryRecordingCreditSelects(form, readRecordingCreditsFromEditor(form));
      return;
    }
    renderMergeControls(form);
  });
  form.addEventListener("click", (event) => {
    const addCreditButton = event.target.closest("[data-recording-credit-add]");
    if (addCreditButton) {
      const credits = readRecordingCreditsFromEditor(form);
      credits.push({ role: "soloist", personId: "", displayName: "" });
      syncRecordingCreditsField(form, credits);
      return;
    }
    const removeCreditButton = event.target.closest("[data-recording-credit-remove]");
    if (removeCreditButton) {
      const credits = readRecordingCreditsFromEditor(form);
      credits.splice(Number(removeCreditButton.dataset.recordingCreditRemove || -1), 1);
      syncRecordingCreditsField(form, credits);
      return;
    }
    const mergeToggleButton = event.target.closest("[data-merge-target-toggle]");
    if (mergeToggleButton) {
      const host = form.querySelector("[data-merge-host]");
      const panel = host?.querySelector("[data-merge-target-panel]");
      const nextOpen = Boolean(panel?.hidden);
      if (panel) {
        panel.hidden = !nextOpen;
      }
      mergeToggleButton.setAttribute("aria-expanded", String(nextOpen));
      if (nextOpen) {
        host?.querySelector("[data-merge-target-search]")?.focus();
      }
      return;
    }
    const mergeTargetButton = event.target.closest("[data-merge-target-option]");
    if (mergeTargetButton && form.elements.mergeTargetId) {
      form.elements.mergeTargetId.value = mergeTargetButton.dataset.mergeTargetOption || "";
      const host = form.querySelector("[data-merge-host]");
      const panel = host?.querySelector("[data-merge-target-panel]");
      if (panel) {
        panel.hidden = true;
      }
      renderMergeControls(form);
      return;
    }
    const textButton = event.target.closest('[data-info-panel-action="edit-text"]');
    if (textButton) {
      event.preventDefault();
      event.stopPropagation();
      openInfoPanelTextDialog(form);
      return;
    }
    const infoPanelLinkButton = event.target.closest("[data-info-panel-link-edit]");
    if (infoPanelLinkButton) {
      event.preventDefault();
      event.stopPropagation();
      openRecordingLinkDialog(resolveRecordingLinkHost(infoPanelLinkButton), Number(infoPanelLinkButton.dataset.infoPanelLinkEdit || -1));
      return;
    }
  });
});

entityCheckButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await runSingleEntityCheck(button.dataset.entityCheck);
    } catch (error) {
      setResult(error instanceof Error ? error.message : String(error));
    }
  });
});

batchComposerSelect?.addEventListener("change", () => {
  populateWorkSelectOptions(batchWorkSelect, batchComposerSelect.value || "", "", {
    ready: "请选择批量作品",
    empty: "请先选择批量作曲家",
  });
});

batchFileInput.addEventListener("change", async () => {
  const file = batchFileInput.files?.[0];
  if (!file) {
    batchFileStatus.textContent = "尚未选择文件";
    return;
  }
  try {
    batchFileStatus.textContent = `已选择：${file.name}`;
    batchSourceText.value = await file.text();
    setResult({ loadedBatchSource: file.name, length: batchSourceText.value.length });
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

batchPreviewList.addEventListener("input", () => {
  if (!state.batchSession) {
    return;
  }
  readBatchDraftEntities();
});

batchPreviewList.addEventListener("change", () => {
  if (!state.batchSession) {
    return;
  }
  readBatchDraftEntities();
  renderBatchSession();
});

batchPreviewList.addEventListener("click", (event) => {
  const selectButton = event.target.closest("[data-batch-entry-select]");
  const stateButton = event.target.closest("[data-batch-entry-state]");
  if (selectButton) {
    const [type, indexValue] = String(selectButton.dataset.batchEntrySelect || "").split(":");
    state.batchSelectedEntry = {
      type,
      index: Number(indexValue || 0),
    };
    renderBatchSession();
    return;
  }
  if (stateButton) {
    updateBatchEntryReviewState(stateButton.dataset.batchEntryState);
  }
});

batchAnalyzeButton.addEventListener("click", async () => {
  try {
    batchProgressStatus.textContent = "状态：正在分析…";
    await withBusyButton(batchAnalyzeButton, "分析中…", () => runBatchAnalyze());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
    batchProgressStatus.textContent = "状态：分析失败";
  }
});

batchConfirmCreateButton.addEventListener("click", async () => {
  try {
    await withBusyButton(batchConfirmCreateButton, "保存中…", () => confirmBatchCreate());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

batchCheckButton.addEventListener("click", async () => {
  try {
    batchProgressStatus.textContent = "状态：正在自动检查…";
    await withBusyButton(batchCheckButton, "检查中…", () => runBatchCheck());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
    batchProgressStatus.textContent = "状态：自动检查失败";
  }
});

batchApplyButton.addEventListener("click", async () => {
  try {
    await withBusyButton(batchApplyButton, "应用中…", () => applyBatchSession());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

batchAbandonButton.addEventListener("click", async () => {
  try {
    await withBusyButton(batchAbandonButton, "放弃中…", () => abandonBatchSession());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

batchAbandonUnconfirmedButton.addEventListener("click", async () => {
  try {
    await withBusyButton(batchAbandonUnconfirmedButton, "处理中…", () => abandonBatchUnconfirmed());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

articleSearchInput.addEventListener("input", () => {
  renderArticleSearchResults();
});

articleSearchResults.addEventListener("click", (event) => {
  const previewButton = event.target.closest("[data-preview-article-id]");
  if (previewButton) {
    const article = state.articles.find((item) => item.id === previewButton.dataset.previewArticleId);
    void previewArticleInDialog(article).catch((error) => setResult(error instanceof Error ? error.message : String(error)));
    return;
  }
  const button = event.target.closest("[data-load-article-id]");
  if (!button) {
    return;
  }
  const article = state.articles.find((item) => item.id === button.dataset.loadArticleId);
  if (!article) {
    setResult("专栏不存在或已被删除。");
    return;
  }
  fillArticleForm(article);
  void refreshArticlePreview().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
  setActiveView("articles");
  setActiveArticleTab("create");
});

document.addEventListener("click", (event) => {
  const proposalActionButton = event.target.closest("[data-owner-proposal-action]");
  if (proposalActionButton) {
    event.preventDefault();
    event.stopPropagation();
    void withBusyButton(proposalActionButton, getProposalActionPendingText(proposalActionButton.dataset.ownerProposalAction), () =>
      handleUnifiedProposalAction(proposalActionButton),
    ).catch((error) => setResult(error instanceof Error ? error.message : String(error)));
    return;
  }
  const proposalLinkCandidateButton = event.target.closest("[data-proposal-link-candidate-edit]");
  if (proposalLinkCandidateButton) {
    event.preventDefault();
    openProposalLinkCandidateDialog(proposalLinkCandidateButton);
    return;
  }
  const addButton = event.target.closest("[data-recording-link-action='add']");
  if (addButton) {
    openRecordingLinkDialog(resolveRecordingLinkHost(addButton) || getActiveRecordingForm(), -1);
    return;
  }
  const infoPanelAddButton = event.target.closest("[data-info-panel-link-action='add']");
  if (infoPanelAddButton) {
    openRecordingLinkDialog(resolveRecordingLinkHost(infoPanelAddButton), -1);
    return;
  }
  const linkButton = event.target.closest("[data-recording-link-index]");
  if (linkButton) {
    openRecordingLinkDialog(
      resolveRecordingLinkHost(linkButton) || getActiveRecordingForm(),
      Number(linkButton.dataset.recordingLinkIndex || -1),
    );
    return;
  }
  const infoPanelLinkButton = event.target.closest("[data-info-panel-link-edit]");
  if (infoPanelLinkButton) {
    openRecordingLinkDialog(resolveRecordingLinkHost(infoPanelLinkButton), Number(infoPanelLinkButton.dataset.infoPanelLinkEdit || -1));
  }
});

markdownToolbar?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-markdown-insert], [data-markdown-wrap]");
  if (!button) {
    return;
  }
  const textarea = articleForm?.elements?.markdown;
  if (!textarea) {
    return;
  }
  const start = textarea.selectionStart ?? textarea.value.length;
  const end = textarea.selectionEnd ?? textarea.value.length;
  const selected = textarea.value.slice(start, end);
  if (button.dataset.markdownWrap) {
    const wrapper = button.dataset.markdownWrap;
    textarea.setRangeText(`${wrapper}${selected || "文本"}${wrapper}`, start, end, "end");
  } else {
    const insertion = button.dataset.markdownInsert || "";
    textarea.setRangeText(`${insertion}${selected}`, start, end, "end");
  }
  textarea.focus();
});

articlePreviewButton.addEventListener("click", async () => {
  try {
    await withBusyButton(articlePreviewButton, "预览中…", () => refreshArticlePreview());
    articlePreviewDialog.showModal();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

articleImageInput.addEventListener("change", async () => {
  const file = articleImageInput.files?.[0];
  if (!file) {
    return;
  }
  try {
    await uploadArticleImage(file);
    articleImageInput.value = "";
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

articleSaveButton.addEventListener("click", async () => {
  try {
    await withBusyButton(articleSaveButton, compact(articleForm.elements.existingId.value) ? "保存中…" : "创建中…", () => saveArticle());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

articleResetButton.addEventListener("click", () => {
  resetArticleForm();
  setActiveArticleTab("create");
  setResult("已放弃当前专栏编辑内容。");
});

articleDeleteButton.addEventListener("click", async () => {
  const articleLabel = compact(articleForm.elements.title.value || articleForm.elements.slug.value || "当前专栏");
  showConfirmDialog({
    title: "删除专栏",
    message: `确认删除“${articleLabel}”吗？此操作不可撤销。`,
    confirmText: "确认删除",
    onConfirm: async () => {
      await withBusyButton(articleDeleteButton, "删除中…", () => deleteArticle());
    },
  });
});

inlineCheckCloseButton.addEventListener("click", () => {
  clearInlineCheck();
});

searchButton.addEventListener("click", async () => {
  try {
    await performSearch();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    void performSearch().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
  }
});

previewAutomationSelectionButton.addEventListener("click", async () => {
  try {
    await refreshSelectionPreview();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

runAutomationButton.addEventListener("click", async () => {
  try {
    await runAutomationJob();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

cancelAutomationButton.addEventListener("click", async () => {
  if (!state.activeJobId) {
    setResult("当前没有正在运行的任务。");
    return;
  }
  try {
    const result = await fetchJson(`/api/automation/jobs/${encodeURIComponent(state.activeJobId)}/cancel`, {
      method: "POST",
    });
    renderJob(result.job);
    if (state.jobPollTimer) {
      clearInterval(state.jobPollTimer);
      state.jobPollTimer = null;
    }
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

applyConfirmedButton.addEventListener("click", async () => {
  if (!reviewRunSelect.value) {
    setResult("请先选择候选 run。");
    return;
  }
  try {
    let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
    assertNoBlockedReviewActionProposals(run, "apply-confirmed", "all");
    ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "apply-confirmed", "all"), {
      runId: run.id,
    }));
    assertNoBlockedReviewActionProposals(run, "apply-confirmed", "all");
    const result = await fetchJson(`/api/automation/runs/${encodeURIComponent(run.id)}/apply-confirmed`, {
      method: "POST",
    });
    await refreshAll();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

applyConfirmedButtonFooter.addEventListener("click", async () => {
  if (!reviewRunSelect.value) {
    setResult("请先选择候选 run。");
    return;
  }
  try {
    let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
    assertNoBlockedReviewActionProposals(run, "apply-confirmed", "all");
    ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "apply-confirmed", "all"), {
      runId: run.id,
    }));
    assertNoBlockedReviewActionProposals(run, "apply-confirmed", "all");
    const result = await fetchJson(`/api/automation/runs/${encodeURIComponent(run.id)}/apply-confirmed`, {
      method: "POST",
    });
    await refreshAll();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

ignorePendingButton.addEventListener("click", async () => {
  if (!reviewRunSelect.value) {
    setResult("请先选择候选 run。");
    return;
  }
  try {
    let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
    ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "ignore-pending", "all"), {
      runId: run.id,
    }));
    const result = await fetchJson(`/api/automation/runs/${encodeURIComponent(run.id)}/ignore-pending`, {
      method: "POST",
    });
    await refreshRuns();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

ignorePendingButtonFooter.addEventListener("click", async () => {
  if (!reviewRunSelect.value) {
    setResult("请先选择候选 run。");
    return;
  }
  try {
    let { run } = await fetchJson(`/api/automation/runs/${encodeURIComponent(reviewRunSelect.value)}`);
    ({ run } = await persistProposalDrafts(run, getReviewActionProposals(run, "ignore-pending", "all"), {
      runId: run.id,
    }));
    const result = await fetchJson(`/api/automation/runs/${encodeURIComponent(run.id)}/ignore-pending`, {
      method: "POST",
    });
    await refreshRuns();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

applyPageConfirmedButton.addEventListener("click", async () => {
  try {
    await applyCurrentPageConfirmedProposals();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

applyPageConfirmedButtonFooter.addEventListener("click", async () => {
  try {
    await applyCurrentPageConfirmedProposals();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

ignorePagePendingButton.addEventListener("click", async () => {
  try {
    await ignoreCurrentPagePendingProposals();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

ignorePagePendingButtonFooter.addEventListener("click", async () => {
  try {
    await ignoreCurrentPagePendingProposals();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

reviewRunSelect.addEventListener("change", () => {
  state.activeRunId = reviewRunSelect.value;
  state.reviewPage = 1;
  void renderReviewRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
});

logRunSelect.addEventListener("change", () => {
  state.activeLogRunId = logRunSelect.value;
  void renderLogRun().catch((error) => setResult(error instanceof Error ? error.message : String(error)));
});

[...automationCheckForm.querySelectorAll("input, select")].forEach((control) => {
  control.addEventListener("change", refreshSelectionPreviewDebounced);
});

saveLlmConfigButton.addEventListener("click", async () => {
  try {
    const payload = buildLlmConfigPayload();
    if (payload.apiKey) {
      rememberLlmApiKey(payload.apiKey);
    }
    const { config } = await fetchJson("/api/automation/llm/config", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setLlmConfigForm({ ...config, apiKey: payload.apiKey || config.apiKey || "" });
    setResult({
      saved: true,
      config: {
        enabled: config.enabled,
        baseUrl: config.baseUrl,
        model: config.model,
        timeoutMs: config.timeoutMs,
        hasApiKey: Boolean(config.hasApiKey || config.apiKey || payload.apiKey),
      },
    });
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

llmPanelToggle?.addEventListener("click", () => {
  toggleCollapsedPanel("llm-panel");
});

document.querySelectorAll("[data-collapsible-toggle]").forEach((button) => {
  if (button === llmPanelToggle) {
    return;
  }
  button.addEventListener("click", () => {
    toggleCollapsedPanel(button.dataset.collapsibleToggle);
  });
});

testLlmConfigButton.addEventListener("click", async () => {
  try {
    const payload = buildLlmConfigPayload();
    if (payload.apiKey) {
      rememberLlmApiKey(payload.apiKey);
    }
    llmTestResult.textContent = "正在测试连接...";
    const result = await fetchJson("/api/automation/llm/test", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (result.config) {
      setLlmConfigForm({ ...result.config, apiKey: payload.apiKey || result.config.apiKey || "" });
    }
    llmTestResult.textContent = JSON.stringify(result.result, null, 2);
    setResult({
      result: result.result,
      config: result.config
        ? {
            enabled: result.config.enabled,
            baseUrl: result.config.baseUrl,
            model: result.config.model,
            timeoutMs: result.config.timeoutMs,
            hasApiKey: Boolean(result.config.hasApiKey || result.config.apiKey || payload.apiKey),
          }
        : null,
    });
  } catch (error) {
    llmTestResult.textContent = error instanceof Error ? error.message : String(error);
    setResult(error instanceof Error ? error.message : String(error));
  }
});

rebuildButton.addEventListener("click", async () => {
  try {
    const result = await buildLocalLibrarySite();
    await refreshAll();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

libraryOpenButton?.addEventListener("click", async () => {
  try {
    const result = await openLocalLibrarySite();
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

libraryImportButton?.addEventListener("click", async () => {
  try {
    const result = await importManagedLibraryWithPicker();
    if (!result?.cancelled) {
      setResult(result);
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

libraryExportButton?.addEventListener("click", async () => {
  try {
    const result = await exportManagedLibraryWithPicker();
    if (!result?.cancelled) {
      setResult(result);
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

refreshButton.addEventListener("click", async () => {
  try {
    await refreshAll();
    setResult("数据已刷新。");
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

textDialogClose.addEventListener("click", () => {
  closeTextDialog();
});
textDialogSave?.addEventListener("click", () => {
  saveInfoPanelTextDialog();
});

textDialog.addEventListener("click", (event) => {
  const card = event.target.closest(".owner-dialog__card");
  if (!card) {
    closeTextDialog();
  }
});

linkDialogClose?.addEventListener("click", () => {
  resetLinkDialogMeta();
  linkDialog.close();
});

linkDialog?.addEventListener("click", (event) => {
  const card = event.target.closest(".owner-dialog__card");
  if (!card) {
    resetLinkDialogMeta();
    linkDialog.close();
  }
});

linkDialog?.addEventListener("close", () => {
  resetLinkDialogMeta();
});

linkDialogOpen?.addEventListener("click", async () => {
  try {
    await openManagedResourceLink(readRecordingLinkDialogValue());
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

linkDialogForm?.elements?.linkType?.addEventListener?.("change", () => {
  syncLinkDialogFieldState();
});

linkDialogForm?.querySelector?.('[data-link-dialog-action="browse-local"]')?.addEventListener?.("click", async () => {
  try {
    const selectedPath = await pickLocalResourcePath();
    if (!selectedPath) {
      return;
    }
    if (linkDialogForm.elements.localPath) {
      linkDialogForm.elements.localPath.value = selectedPath;
    }
    if (linkDialogForm.elements.platform && !compact(linkDialogForm.elements.platform.value)) {
      linkDialogForm.elements.platform.value = "local";
    }
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

previewDialogClose?.addEventListener("click", () => previewDialog.close());
previewDialog?.addEventListener("click", (event) => {
  const card = event.target.closest(".owner-dialog__card");
  if (!card) {
    previewDialog.close();
  }
});

confirmDialogClose?.addEventListener("click", () => confirmDialog.close());
confirmDialogCancel?.addEventListener("click", () => confirmDialog.close());
confirmDialogConfirm?.addEventListener("click", async () => {
  const context = state.confirmDialogContext;
  confirmDialog.close();
  state.confirmDialogContext = null;
  if (!context?.onConfirm) {
    return;
  }
  try {
    await context.onConfirm();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});
confirmDialog?.addEventListener("click", (event) => {
  const card = event.target.closest(".owner-dialog__card");
  if (!card) {
    confirmDialog.close();
  }
});

articlePreviewClose?.addEventListener("click", () => articlePreviewDialog.close());
articlePreviewDialog?.addEventListener("click", (event) => {
  const card = event.target.closest(".owner-dialog__card");
  if (!card) {
    articlePreviewDialog.close();
  }
});

linkDialogSave?.addEventListener("click", () => {
  try {
    commitRecordingLinkDialog();
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

linkDialogDelete?.addEventListener("click", () => {
  deleteRecordingLinkDialog();
});

siteForm.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-site-action]");
  if (!button) {
    return;
  }
  try {
    if (button.dataset.siteAction === "load") {
      await refreshSite();
      setResult("已重新载入网站文本。");
      return;
    }
    const result = await fetchJson("/api/site", {
      method: "POST",
      body: JSON.stringify(buildSitePayload()),
    });
    state.site = result.site;
    fillSiteForm(result.site);
    setResult(result);
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
});

const bootstrap = async () => {
  try {
    applyCollapsedPanelState("llm-panel");
    applyCollapsedPanelState("automation-preview-panel");
    applyCollapsedPanelState("automation-job-panel");
    await refreshAll();
    fillSiteForm(state.site);
    resetArticleForm();
    setActiveArticleTab("create");
    renderBatchSession();
    renderSearchResults([]);
    renderJob(null);
    await refreshSelectionPreview();
    setResult("维护工具已就绪。");
  } catch (error) {
    setResult(error instanceof Error ? error.message : String(error));
  }
};

const buildLocalLibrarySite = async () => {
  const result = await fetchJson("/api/library/build-site", { method: "POST" });
  state.libraryMeta = result.libraryMeta || state.libraryMeta;
  renderLibraryStatus();
  if (typeof desktopLauncher?.openLibrary === "function") {
    await desktopLauncher.openLibrary();
  } else if (result.siteUrl) {
    window.open(result.siteUrl, "_blank", "noopener,noreferrer");
  }
  return result;
};

const openLocalLibrarySite = async () => {
  const result = await fetchJson("/api/library/open-site", { method: "POST" });
  state.libraryMeta = result.libraryMeta || state.libraryMeta;
  renderLibraryStatus();
  if (typeof desktopLauncher?.openLibrary === "function") {
    await desktopLauncher.openLibrary();
  } else if (result.siteUrl) {
    window.open(result.siteUrl, "_blank", "noopener,noreferrer");
  }
  return result;
};

const importManagedLibraryWithPicker = async () => {
  const sourcePath =
    (typeof desktopLauncher?.pickLibraryFolder === "function"
      ? compact((await desktopLauncher.pickLibraryFolder())?.path || "")
      : "") || compact(window.prompt("\u8bf7\u8f93\u5165\u8981\u5bfc\u5165\u7684\u5e93\u76ee\u5f55\u8def\u5f84\uff08\u5305\u542b library.manifest.json \u7684\u76ee\u5f55\uff09\u3002") || "");
  if (!sourcePath) {
    return { cancelled: true };
  }
  const result = await fetchJson("/api/library/import", {
    method: "POST",
    body: JSON.stringify({ sourcePath }),
  });
  await refreshAll();
  return result;
};

const exportManagedLibrary = async () => {
  const destinationPath = compact(window.prompt("\u8bf7\u8f93\u5165\u8981\u5bfc\u51fa\u5230\u7684\u76ee\u5f55\u8def\u5f84\u3002") || "");
  if (!destinationPath) {
    return { cancelled: true };
  }
  return fetchJson("/api/library/export", {
    method: "POST",
    body: JSON.stringify({ destinationPath }),
  });
};

const exportManagedLibraryWithPicker = async () => {
  if (typeof desktopLauncher?.exportLibrary === "function") {
    return desktopLauncher.exportLibrary();
  }
  return exportManagedLibrary();
};

void bootstrap();
