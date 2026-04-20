// @ts-nocheck
import express from "express";
import path from "node:path";
import { access, readFile as readBinaryFile } from "node:fs/promises";

import {
  applyAutomationProposal,
  applyPendingAutomationProposals,
  canApplyAutomationProposal,
  collectAutomationProposalApplyBlockers,
  ignoreAutomationProposal,
  ignorePendingAutomationProposals,
  revertAutomationProposal,
  summarizeAutomationRun,
  updateAutomationProposalReview,
} from "../../../packages/automation/src/automation.js";
import { createAutomationJobManager } from "../../../packages/automation/src/automation-jobs.js";
import { buildArticlePreviewModel, validateArticles } from "../../../packages/data-core/src/articles.js";
import {
  deleteAutomationRun,
  findRunSnapshot,
  listAutomationRuns,
  loadAutomationRun,
  loadLlmConfig,
  loadRecordingRetrievalConfig,
  persistRemoteImageAsset,
  persistUploadedImageAsset,
  saveAutomationRun,
  saveLlmConfig,
} from "../../../packages/automation/src/automation-store.js";
import {
  analyzeBatchImport,
  type BatchDraftEntities,
} from "../../../packages/automation/src/batch-import.js";
import {
  deleteBatchImportSession,
  listBatchImportSessions,
  loadBatchImportSession,
  saveBatchImportSession,
} from "../../../packages/automation/src/batch-import-store.js";
import { buildRecordingDisplayTitle, collectLibraryDataIssues, getCountryText, getDisplayData, getWebsiteDisplay } from "../../../packages/shared/src/display.js";
import {
  getRecordingWorkTypeHintLabel,
  normalizeRecordingWorkTypeHintValue,
  resolveRecordingWorkTypeHintValue,
} from "../../../packages/shared/src/recording-rules.js";
import { defaultLlmConfig, mergeLlmConfigPatch, sanitizeLlmConfig, testOpenAiCompatibleConfig } from "../../../packages/automation/src/llm.js";
import { fetchWithWindowsFallback } from "../../../packages/automation/src/external-fetch.js";
import { resolveLibraryAssetPath } from "../../../packages/data-core/src/owner-assets.js";
import { getAffectedPaths, mergeLibraryEntities } from "../../../packages/automation/src/owner-tools.js";
import {
  loadArticlesFromDisk,
  loadLibraryFromDisk,
  loadSiteConfig,
  readGeneratedArticles,
  saveArticlesToDisk,
  saveLibraryToDisk,
  saveSiteConfig,
  writeGeneratedArtifacts,
} from "../../../packages/data-core/src/library-store.js";
import { validateLibrary } from "../../../packages/shared/src/schema.js";
import { getRuntimePaths } from "../../../packages/data-core/src/app-paths.js";
import { buildLibrarySite } from "../../../packages/data-core/src/site-build-runner.js";
import {
  bootstrapActiveLibrary,
  exportActiveLibraryBundle,
  getActiveLibrarySummary,
  importLibraryBundle,
} from "../../../packages/data-core/src/library-manager.js";
import {
  assertOwnerEntityCanDelete,
  buildOwnerEntity,
  collectOwnerEntityRelations,
  normalizeOwnerManagedLibrary,
  removeOwnerEntity,
} from "../../../packages/data-core/src/owner-entity-helpers.js";
import { createLocalSiteServer } from "../../../packages/data-core/src/local-site-server.js";
import { openTargetInShell } from "../../../packages/data-core/src/open-target.js";
import { createEntityId, createSlug } from "../../../packages/shared/src/slug.js";
import { mergeSiteConfigPatch } from "../../../packages/data-core/src/site-content.js";
import { runAutomationChecks } from "../../../packages/automation/src/automation-checks.js";
import { createHttpRecordingRetrievalProvider } from "../../../packages/automation/src/recording-retrieval.js";
import { mergeBatchSessionIntoLibrary, replaceBatchDraftEntities, resolveConfirmedBatchSelection } from "./batch-session-utils.js";
import { sanitizeAutomationRunProposalFields, sanitizeProposalPatchMap } from "./proposal-patch-utils.js";
import { loadReferenceRegistry } from "../../../packages/data-core/src/reference-registry.js";

const app = express();
const port = Number(process.env.OWNER_PORT || 4322);
const runtimePaths = getRuntimePaths();
const ownerDir = runtimePaths.ownerWebDir;
const sitePublicDir = runtimePaths.sitePublicDir;
const libraryAssetsDir = runtimePaths.library.assetsDir;
const templateDir = runtimePaths.templateDir;
const jobManager = createAutomationJobManager();
const localSiteServer = createLocalSiteServer({
  preferredPort: Number(process.env.LIBRARY_SITE_PORT || 4331),
});

async function ensureLibrarySiteReady(options = { buildIfMissing: false }) {
  const summary = await getActiveLibrarySummary();
  const indexPath = path.join(summary.buildSiteDir, "index.html");
  try {
    await access(indexPath);
  } catch {
    if (!options.buildIfMissing) {
      throw new Error("Local site has not been built yet");
    }
    await buildLibrarySite();
  }
  return localSiteServer.ensureStarted((await getActiveLibrarySummary()).buildSiteDir);
}

app.use(express.json({ limit: "8mb" }));
app.use((request, response, next) => {
  if (request.path === "/" || request.path.endsWith(".js") || request.path.endsWith(".css") || request.path.endsWith(".html")) {
    response.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  }
  next();
});
app.get("/favicon.ico", async (_request, response, next) => {
  try {
    const iconPath = path.join(sitePublicDir, "favicon.ico");
    response.type("image/x-icon").send(await readBinaryFile(iconPath));
  } catch (error) {
    next(error);
  }
});
app.get("/favicon.svg", async (_request, response, next) => {
  try {
    const iconPath = path.join(sitePublicDir, "favicon.svg");
    response.type("image/svg+xml").send(await readBinaryFile(iconPath, "utf8"));
  } catch (error) {
    next(error);
  }
});
app.get("/api/remote-image", async (request, response) => {
  try {
    const url = new URL(String(request.query.url || ""));
    if (!["http:", "https:"].includes(url.protocol)) {
      response.status(400).json({ error: "Unsupported image protocol" });
      return;
    }
    const upstream = await fetchWithWindowsFallback(url, {
      headers: {
        Accept: "image/*,*/*;q=0.8",
      },
    }, { fetchImpl: fetch });
    if (!upstream.ok) {
      response.status(upstream.status).json({ error: `Remote image request failed: ${upstream.status}` });
      return;
    }
    const contentType = upstream.headers.get("content-type") || "application/octet-stream";
    response.setHeader("Cache-Control", "public, max-age=1800");
    response.type(contentType).send(Buffer.from(await upstream.arrayBuffer()));
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});
app.post("/api/open-resource", async (request, response) => {
  try {
    const linkType = String(request.body?.linkType || "external").trim() === "local" ? "local" : "external";
    if (linkType === "local") {
      const localPath = path.resolve(String(request.body?.localPath || "").trim());
      if (!localPath) {
        response.status(400).json({ error: "Missing localPath" });
        return;
      }
      await access(localPath);
      await openTargetInShell(localPath);
      response.json({ opened: true, target: localPath, linkType });
      return;
    }

    const url = String(request.body?.url || "").trim();
    if (!url) {
      response.status(400).json({ error: "Missing url" });
      return;
    }
    new URL(url);
    await openTargetInShell(url);
    response.json({ opened: true, target: url, linkType });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});
app.use(express.static(ownerDir));
app.use("/template", express.static(templateDir));
app.get(/^\/library-assets\/(.+)$/, (request, response, next) => {
  const resolvedAssetPath = resolveLibraryAssetPath(libraryAssetsDir, request.path);
  if (!resolvedAssetPath) {
    response.status(404).json({ error: "Asset not found" });
    return;
  }

  response.sendFile(resolvedAssetPath, (error) => {
    if (error && !response.headersSent) {
      next(error);
    }
  });
});
app.use("/library-assets", express.static(libraryAssetsDir));

const automationFetch: typeof fetch = (input, init) => fetchWithWindowsFallback(input, init, { fetchImpl: fetch });

async function buildRecordingRunOptions(source = {}) {
  const config = await loadRecordingRetrievalConfig();
  if (!config.enabled) {
    return {};
  }
  return {
    recordingProvider: createHttpRecordingRetrievalProvider({ baseUrl: config.baseUrl }),
    recordingRequestOptions: {
      source,
      timeoutMs: config.timeoutMs,
    },
    recordingExecutionOptions: {
      timeoutMs: config.timeoutMs,
      pollIntervalMs: config.pollIntervalMs,
    },
  };
}


function parseStructuredLinks(value) {
  if (!value) {
    return [];
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => ({
        platform: String(item?.platform || "other").trim() || "other",
        url: String(item?.url || "").trim(),
        localPath: String(item?.localPath || item?.path || "").trim(),
        title: String(item?.title || "").trim(),
        linkType:
          String(item?.linkType || "").trim() === "local" || (!String(item?.url || "").trim() && String(item?.localPath || item?.path || "").trim())
            ? "local"
            : "external",
        visibility:
          String(item?.linkType || "").trim() === "local" || (!String(item?.url || "").trim() && String(item?.localPath || item?.path || "").trim())
            ? "local-only"
            : String(item?.visibility || "").trim() === "local-only"
              ? "local-only"
              : "public",
      }))
      .filter((item) => (item.linkType === "local" ? item.localPath : item.url));
  }
  if (typeof value === "string") {
    try {
      return parseStructuredLinks(JSON.parse(value));
    } catch {
      const legacyUrl = value.trim();
      return legacyUrl ? [{ platform: "other", url: legacyUrl, localPath: "", title: "", linkType: "external", visibility: "public" }] : [];
    }
  }
  return [];
}

function upsertCollection(collection, entity) {
  const index = collection.findIndex((item) => item.id === entity.id);
  if (index >= 0) {
    collection[index] = entity;
    return entity;
  }
  collection.push(entity);
  return entity;
}

function parseInfoPanel(payload) {
  const collectionLinks = parseStructuredLinks(payload?.infoPanel?.collectionLinks || payload?.infoPanelCollectionLinks);
  const legacyCollectionUrl = String(payload?.infoPanel?.collectionUrl || payload?.infoPanelCollectionUrl || "").trim();
  return {
    text: payload?.infoPanel?.text || payload?.infoPanelText || "",
    articleId: payload?.infoPanel?.articleId || payload?.infoPanelArticleId || "",
    collectionLinks: collectionLinks.length
      ? collectionLinks
      : legacyCollectionUrl
        ? [{ platform: "other", url: legacyCollectionUrl, localPath: "", title: "", linkType: "external", visibility: "public" }]
        : [],
  };
}

function assertEntityCanDelete(library, entityType, entityId) {
  return assertOwnerEntityCanDelete(library, entityType, entityId);
}

function removeEntityFromLibrary(library, entityType, entityId) {
  return removeOwnerEntity(library, entityType, entityId);
}

function normalizeWorkComparableText(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[,:;(){}\[\]]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value) {
  return String(value ?? "").replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
}

function stripCatalogueFromWorkSegment(segment, catalogue) {
  const normalizedSegment = String(segment ?? "").trim();
  const normalizedCatalogue = String(catalogue ?? "").trim();
  if (!normalizedSegment || !normalizedCatalogue) {
    return normalizedSegment;
  }
  if (normalizeWorkComparableText(normalizedSegment) === normalizeWorkComparableText(normalizedCatalogue)) {
    return "";
  }
  const escapedCatalogue = escapeRegExp(normalizedCatalogue).replace(/\s+/g, "\\s+");
  const trailingPatterns = [
    new RegExp("(?:\\s*[,;:/-]\\s*|\\s+)\\(?" + escapedCatalogue + "\\)?$", "i"),
    new RegExp("\\(" + escapedCatalogue + "\\)$", "i"),
  ];
  return trailingPatterns.reduce((currentValue, pattern) => currentValue.replace(pattern, "").trim(), normalizedSegment);
}

function buildWorkDisplayParts(work, composer = null) {
  return [
    String(work?.title ?? work?.id ?? "").trim(),
    stripCatalogueFromWorkSegment(work?.titleLatin ?? "", work?.catalogue ?? ""),
    String(work?.catalogue ?? "").trim(),
    String(composer?.name ?? "").trim(),
    String(composer?.nameLatin ?? "").trim(),
  ].filter(Boolean);
}

function normalizeRecordingWorkTypeHint(value) {
  return normalizeRecordingWorkTypeHintValue(value);
}

function getComposerById(library, composerId) {
  return (library.composers || []).find((composer) => composer.id === composerId) || null;
}

function getWorkById(library, workId) {
  return (library.works || []).find((work) => work.id === workId) || null;
}

function getWorkGroupsByWork(library, work) {
  const groupIds = Array.isArray(work?.groupIds) ? work.groupIds : [];
  return groupIds
    .map((groupId) => (library.workGroups || []).find((group) => group.id === groupId))
    .filter(Boolean);
}

function resolveRecordingWorkTypeHint(library, value, workId) {
  return resolveRecordingWorkTypeHintValue(value, getWorkById(library, workId), getWorkGroupsByWork(library, getWorkById(library, workId)));
}

function getWorkGroupLabel(library, work) {
  if (!work) {
    return "作品 / 未分类";
  }
  const relatedRecording = (library.recordings || []).find((item) => item.workId === work.id);
  const resolvedWorkType = resolveRecordingWorkTypeHint(
    library,
    relatedRecording?.workTypeHint,
    work.id,
  );
  return "作品 / " + getRecordingWorkTypeHintLabel(resolvedWorkType);
}

function buildBlockedProposalApplyMessage(blockedEntries, scopeLabel = "已确认候选") {
  if (!Array.isArray(blockedEntries) || blockedEntries.length === 0) {
    return "";
  }
  const details = blockedEntries
    .slice(0, 5)
    .map((entry) => {
      const summary = String(entry?.proposal?.summary || entry?.proposal?.id || "未命名候选").trim();
      const reasons = Array.isArray(entry?.reasons) ? entry.reasons.join("；") : "不允许直接应用";
      return summary + "：" + reasons;
    })
    .join("；");
  const suffix = blockedEntries.length > 5 ? "；其余 " + (blockedEntries.length - 5) + " 条请先逐条处理。" : "";
  return scopeLabel + "中包含 " + blockedEntries.length + " 条被阻止的候选：" + details + suffix;
}

function getPersonGroupLabel(person) {
  if ((person.roles || []).some((role) => role === "orchestra" || role === "ensemble" || role === "chorus")) {
    return "团体";
  }
  if ((person.roles || []).includes("composer")) {
    return "作曲家";
  }
  return "人物";
}

function getRelatedRecordingGroupLabel(library, recording) {
  const work = getWorkById(library, recording.workId);
  const composer = getComposerById(library, work?.composerId);
  const composerLabel = composer ? getWebsiteDisplay(composer).heading : "未知作曲家";
  const workLabel = String(work?.title || recording.title || "未知作品").trim();
  return "版本 / " + composerLabel + " / " + workLabel;
}

function buildArticle(articles, payload) {
  const timestamp = new Date().toISOString();
  const nextId = payload.id || createEntityId("article", payload.slug || payload.title);
  return {
    id: nextId,
    slug: payload.slug || createSlug(payload.title),
    title: payload.title,
    summary: payload.summary || "",
    markdown: payload.markdown || "",
    showOnHome: Boolean(payload.showOnHome),
    createdAt: payload.createdAt || articles.find((item) => item.id === nextId)?.createdAt || timestamp,
    updatedAt: timestamp,
  };
}

async function previewOrSave(entityType, payload, shouldSave) {
  const library = await loadLibraryFromDisk();
  const entity = buildOwnerEntity(library, entityType, {
    ...payload,
    infoPanel: parseInfoPanel(payload),
  });

  if (entityType === "composer") {
    upsertCollection(library.composers, entity);
  } else if (entityType === "person") {
    upsertCollection(library.people, entity);
  } else if (entityType === "work") {
    upsertCollection(library.works, entity);
  } else {
    upsertCollection(library.recordings, entity);
  }

  const validated = validateLibrary(normalizeOwnerManagedLibrary(library));
  const entityId = entity.id;
  const savedEntity =
    entityType === "composer"
      ? validated.composers.find((item) => item.id === entityId)
      : entityType === "person"
        ? validated.people.find((item) => item.id === entityId)
        : entityType === "work"
          ? validated.works.find((item) => item.id === entityId)
          : validated.recordings.find((item) => item.id === entityId);

  const affectedPaths = getAffectedPaths(validated, entityType, entityId);

  if (shouldSave) {
    await saveLibraryToDisk(validated);
    await writeGeneratedArtifacts();
  }

  return {
    entity: savedEntity,
    affectedPaths,
    saved: shouldSave,
  };
}

function buildBatchCheckRequest(session) {
  const selection = resolveConfirmedBatchSelection(session);
  const request = {
    categories: [],
    workIds: [],
    recordingIds: [],
  };
  if (selection.createdEntityRefs.recordings.length) {
    request.recordingIds = [...selection.createdEntityRefs.recordings];
  }
  request.categories = request.recordingIds.length ? ["recording"] : [];
  return request;
}

function buildBatchRecordingOverrides(session) {
  return Object.fromEntries(
    (session.draftEntities?.recordings || []).map((entry) => [
      entry.entity.id,
      {
        sourceLine: entry.sourceLine || "",
        workTypeHint: session.workTypeHint || "unknown",
      },
    ]),
  );
}

function entityCollectionByType(library, entityType) {
  if (entityType === "composer") return library.composers;
  if (entityType === "person") return library.people;
  if (entityType === "work") return library.works;
  return library.recordings;
}

function buildSearchResults(library, site, query, type) {
  const normalizedQuery = String(query || "").trim().toLowerCase();
  const composerById = new Map((library.composers || []).map((item) => [item.id, item]));
  const makeKeywords = (values) =>
    values
      .flatMap((value) => (Array.isArray(value) ? value : [value]))
      .filter(Boolean)
      .map((value) => String(value).toLowerCase());
  const matchesRequestedType = (entry) => {
    if (!type) {
      return true;
    }
    if (type === "composer") {
      return entry.roles?.includes("composer");
    }
    if (type === "person") {
      return entry.searchBucket === "person";
    }
    if (type === "group") {
      return entry.searchBucket === "group";
    }
    return entry.type === type || entry.searchBucket === type;
  };
  const buckets = [
    ["site", [{ id: "site", title: site.title, subtitle: site.subtitle || site.description || "", type: "site", searchBucket: "site" }]],
    [
      "person",
      library.people.map((item) => {
        const display = getDisplayData(item);
        const isGroup = item.roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role));
        const websiteDisplay = getWebsiteDisplay(item);
        return {
          id: item.id,
          title: websiteDisplay.heading,
          subtitle: [websiteDisplay.short, item.nameLatin || display.latin, (item.abbreviations || []).join(" / "), getCountryText(item)].filter(Boolean).join(" / "),
          type: "person",
          searchBucket: isGroup ? "group" : "person",
          roles: item.roles,
          keywords: makeKeywords([
            display.primary,
            display.full,
            display.latin,
            item.name,
            item.nameLatin,
            item.aliases,
            item.abbreviations,
            getCountryText(item),
            item.roles,
          ]),
        };
      }),
      ],
      [
        "work",
      library.works.map((item) => {
          const composer = library.composers.find((composerItem) => composerItem.id === item.composerId);
          const composerDisplay = composer?.name || composer?.nameLatin || "";
          return {
            id: item.id,
            title: buildWorkDisplayParts(item, composer).slice(0, 3).join(" / "),
            subtitle: [composerDisplay, composer?.nameLatin && composer?.nameLatin !== composerDisplay ? composer.nameLatin : ""].filter(Boolean).join(" / "),
            type: "work",
            searchBucket: "work",
            roles: [],
            keywords: makeKeywords([item.title, item.titleLatin, item.catalogue, item.aliases, composerDisplay, composer?.nameLatin]),
          };
        }),
      ],
      [
        "recording",
        library.recordings.map((item) => {
          const work = library.works.find((workItem) => workItem.id === item.workId);
          const composer = work ? composerById.get(work.composerId) : null;
          const title = buildRecordingDisplayTitle(item, library);
          return {
            id: item.id,
            title,
            subtitle: [work?.title, composer ? getWebsiteDisplay(composer).heading : "", item.albumTitle || item.performanceDateText || ""]
              .filter(Boolean)
              .join(" / "),
            type: "recording",
            searchBucket: "recording",
            roles: [],
            keywords: makeKeywords([
              title,
              item.title,
              work?.title,
              work?.titleLatin,
              work?.catalogue,
              composer?.name,
              composer?.nameLatin,
              item.albumTitle,
              item.performanceDateText,
              item.venueText,
              item.credits?.map((credit) => [credit.displayName, credit.label, credit.personId]),
            ]),
          };
        }),
      ],
    ];

  return buckets
    .flatMap(([, entries]) => entries)
    .filter((entry) => matchesRequestedType(entry))
    .filter((entry) => !normalizedQuery || [entry.title, entry.subtitle, ...(entry.keywords || [])].join(" ").toLowerCase().includes(normalizedQuery))
    .slice(0, 100);
}

function buildLegacyRelatedEntityReference(library, entityType, entity) {
  return buildRelatedEntityReference(library, entityType, entity);
}

function buildRelatedEntityReference(library, entityType, entity) {
  if (!entity) {
    return null;
  }

  if (entityType === "composer") {
    const display = getWebsiteDisplay(entity);
    return {
      entityType,
      id: entity.id,
      label: display.heading,
      title: display.heading,
      subtitle: [display.short, entity.nameLatin || display.latin, getCountryText(entity)].filter(Boolean).join(" / "),
      groupLabel: "作曲家",
    };
  }

  if (entityType === "person") {
    const display = getWebsiteDisplay(entity);
    return {
      entityType,
      id: entity.id,
      label: display.heading,
      title: display.heading,
      subtitle: [display.short, entity.nameLatin || display.latin, getCountryText(entity)].filter(Boolean).join(" / "),
      groupLabel: getPersonGroupLabel(entity),
    };
  }

  if (entityType === "work") {
    const composer = (library.composers || []).find((item) => item.id === entity.composerId);
    return {
      entityType,
      id: entity.id,
      label: [entity.title, entity.titleLatin, entity.catalogue].filter(Boolean).join(" / "),
      title: entity.title,
      subtitle: [composer ? getWebsiteDisplay(composer).heading : "", entity.titleLatin, entity.catalogue].filter(Boolean).join(" / "),
      groupLabel: getWorkGroupLabel(library, entity),
    };
  }

  return {
    entityType,
    id: entity.id,
    label: buildRecordingDisplayTitle(entity, library),
    title: buildRecordingDisplayTitle(entity, library),
    subtitle: [entity.performanceDateText || "", entity.venueText || "", entity.albumTitle || ""].filter(Boolean).join(" / "),
    groupLabel: getRelatedRecordingGroupLabel(library, entity),
  };
}

function collectRelatedEntities(library, entityType, entityId) {
  return collectOwnerEntityRelations(library, entityType, entityId);
}

function appendOrReplaceField(fields, nextField) {
  const index = fields.findIndex((field) => field.path === nextField.path);
  if (index >= 0) {
    fields[index] = nextField;
    return fields;
  }
  fields.push(nextField);
  return fields;
}

function getValueAtPath(target, path) {
  if (!target || !path) {
    return "";
  }
  const imageMatch = /^images\[(\d+)\]\.(.+)$/.exec(path);
  if (imageMatch) {
    return target.images?.[Number(imageMatch[1])]?.[imageMatch[2]] ?? "";
  }
  return target[path] ?? "";
}

function decodeBase64Bytes(contentBase64 = "") {
  return Uint8Array.from(Buffer.from(String(contentBase64 || ""), "base64"));
}

function bucketForEntityType(entityType) {
  if (entityType === "composer") return "composers";
  if (entityType === "person") return "people";
  if (entityType === "recording") return "recordings";
  return "misc";
}

async function saveValidatedLibrary(library) {
  const validated = validateLibrary(normalizeOwnerManagedLibrary(library));
  await saveLibraryToDisk(validated);
  await writeGeneratedArtifacts();
  return validated;
}

async function prepareProposalRunForApply(run, proposalId, fetchImpl) {
  const proposal = run.proposals.find((item) => item.id === proposalId);
  if (!proposal) {
    throw new Error("Proposal not found");
  }

  const proposalFields = [...proposal.fields];
  const candidate = proposal.imageCandidates?.find((item) => item.id === proposal.selectedImageCandidateId);
  if (candidate) {
    if (proposal.entityType === "composer" || proposal.entityType === "person") {
      const assetPath = await persistRemoteImageAsset({
        bucket: proposal.entityType === "composer" ? "composers" : "people",
        slug: proposal.entityId,
        sourceUrl: candidate.src,
        fetchImpl,
      });
      appendOrReplaceField(proposalFields, { path: "avatarSrc", before: "", after: assetPath });
      appendOrReplaceField(proposalFields, { path: "imageSourceUrl", before: "", after: candidate.sourceUrl });
      appendOrReplaceField(proposalFields, { path: "imageSourceKind", before: "", after: candidate.sourceKind });
      appendOrReplaceField(proposalFields, { path: "imageAttribution", before: "", after: candidate.attribution });
      appendOrReplaceField(proposalFields, { path: "imageUpdatedAt", before: "", after: new Date().toISOString() });
    }

    if (proposal.entityType === "recording") {
      const assetPath = await persistRemoteImageAsset({
        bucket: "recordings",
        slug: proposal.entityId,
        sourceUrl: candidate.src,
        fetchImpl,
      });
      appendOrReplaceField(proposalFields, {
        path: "images[0]",
        before: null,
        after: {
          src: assetPath,
          alt: candidate.title || proposal.summary,
          kind: proposal.summary.includes("现场") ? "performance" : "cover",
          sourceUrl: candidate.sourceUrl,
          sourceKind: candidate.sourceKind,
          attribution: candidate.attribution,
          updatedAt: new Date().toISOString(),
          width: candidate.width,
          height: candidate.height,
          score: candidate.score,
        },
      });
    }
  }

  return summarizeAutomationRun({
    ...run,
    proposals: run.proposals.map((item) =>
      item.id === proposal.id
        ? {
            ...item,
            fields: proposalFields,
          }
        : item,
    ),
  });
}

function buildEntityCheckRequest(entityType, entityId, library) {
  if (entityType === "composer") {
    return { categories: ["composer"], composerIds: [entityId] };
  }
  if (entityType === "person") {
    const person = library.people.find((item) => item.id === entityId);
    if (!person) {
      throw new Error("Entity not found");
    }
    if (person.roles.includes("conductor")) {
      return { categories: ["conductor"], conductorIds: [entityId] };
    }
    if (person.roles.includes("orchestra")) {
      return { categories: ["orchestra"], orchestraIds: [entityId] };
    }
    return { categories: ["artist"], artistIds: [entityId] };
  }
  if (entityType === "work") {
    return { categories: ["work"], workIds: [entityId] };
  }
  return { categories: ["recording"], recordingIds: [entityId] };
}

function persistRunOnComplete(job) {
  if (job.run) {
    return saveAutomationRun(job.run);
  }
}

app.get("/api/library", async (_request, response) => {
  try {
    const [library, articles, libraryMeta] = await Promise.all([loadLibraryFromDisk(), loadArticlesFromDisk(), getActiveLibrarySummary()]);
    response.json({
      library,
      articles,
      libraryMeta,
      dataIssues: collectLibraryDataIssues(library),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/library/import", async (request, response) => {
  try {
    const sourcePath = String(request.body?.sourcePath || "").trim();
    if (!sourcePath) {
      response.status(400).json({ error: "Missing library source path" });
      return;
    }
    const libraryMeta = await importLibraryBundle(sourcePath);
    response.json({
      imported: true,
      libraryMeta,
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/library/export", async (request, response) => {
  try {
    const destinationPath = String(request.body?.destinationPath || "").trim();
    if (!destinationPath) {
      response.status(400).json({ error: "Missing library export destination path" });
      return;
    }
    const result = await exportActiveLibraryBundle(destinationPath);
    response.json(result);
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/library/open-site", async (_request, response) => {
  try {
    const site = await ensureLibrarySiteReady({ buildIfMissing: true });
    response.json({
      opened: true,
      siteUrl: site.url,
      libraryMeta: await getActiveLibrarySummary(),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/library/build-site", async (_request, response) => {
  try {
    const buildResult = await buildLibrarySite();
    const site = await ensureLibrarySiteReady({ buildIfMissing: false });
    response.json({
      built: true,
      outputDir: buildResult.outputDir,
      siteUrl: site.url,
      libraryMeta: await getActiveLibrarySummary(),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/data-issues", async (_request, response) => {
  try {
    const library = normalizeOwnerManagedLibrary(await loadLibraryFromDisk());
    response.json({ issues: collectLibraryDataIssues(library) });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/search", async (request, response) => {
  try {
    const [library, site] = await Promise.all([loadLibraryFromDisk(), loadSiteConfig()]);
    response.json({
      results: buildSearchResults(library, site, String(request.query.q || ""), String(request.query.type || "")),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/entity/:entityType/:id", async (request, response) => {
  try {
    if (request.params.entityType === "site") {
      const site = await loadSiteConfig();
      response.json({ entity: site });
      return;
    }

    const library = normalizeOwnerManagedLibrary(await loadLibraryFromDisk());
    const collection = entityCollectionByType(library, request.params.entityType);
    const entity = collection.find((item) => item.id === request.params.id);
    if (!entity) {
      response.status(404).json({ error: "Entity not found" });
      return;
    }

    response.json({
      entity,
      relatedEntities: collectRelatedEntities(library, request.params.entityType, request.params.id),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/site", async (_request, response) => {
  try {
    const site = await loadSiteConfig();
    response.json({ site });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/site", async (request, response) => {
  try {
    const currentSite = await loadSiteConfig();
    const site = mergeSiteConfigPatch(currentSite, request.body ?? {});
    await saveSiteConfig(site);
    await writeGeneratedArtifacts();
    response.json({
      saved: true,
      site,
      affectedPaths: ["/", "/about/"],
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/rebuild", async (_request, response) => {
  try {
    const buildResult = await buildLibrarySite();
    const site = await ensureLibrarySiteReady({ buildIfMissing: false });
    const { indexes } = await writeGeneratedArtifacts();
    response.json({
      rebuilt: true,
      outputDir: buildResult.outputDir,
      siteUrl: site.url,
      stats: indexes.stats,
      libraryMeta: await getActiveLibrarySummary(),
    });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/preview/:entityType", async (request, response) => {
  try {
    const result = await previewOrSave(request.params.entityType, request.body, false);
    response.json(result);
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/save/:entityType", async (request, response) => {
  try {
    const result = await previewOrSave(request.params.entityType, request.body, true);
    response.json(result);
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.delete("/api/entity/:entityType/:id", async (request, response) => {
  try {
    const library = normalizeOwnerManagedLibrary(await loadLibraryFromDisk());
    assertEntityCanDelete(library, request.params.entityType, request.params.id);
    const nextLibrary = removeEntityFromLibrary(library, request.params.entityType, request.params.id);
    const validated = await saveValidatedLibrary(nextLibrary);
    response.json({
      deleted: true,
      entityType: request.params.entityType,
      entityId: request.params.id,
      affectedPaths: getAffectedPaths(validated, request.params.entityType, request.params.id),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    response.status(message === "Entity not found" ? 404 : 400).json({ error: message });
  }
});

app.post("/api/entity/:entityType/:id/merge", async (request, response) => {
  try {
    const entityType = String(request.params.entityType || "");
    const duplicateId = String(request.params.id || "");
    const primaryId = String(request.body?.targetId || "").trim();
    if (!primaryId) {
      response.status(400).json({ error: "Missing merge target id" });
      return;
    }
    if (!["composer", "person", "work"].includes(entityType)) {
      response.status(400).json({ error: "This entity type does not support manual merge." });
      return;
    }

    const library = await loadLibraryFromDisk();
    const mergedLibrary = mergeLibraryEntities(library, entityType as "composer" | "person" | "work", primaryId, duplicateId);
    const validated = await saveValidatedLibrary(mergedLibrary);
    response.json({
      merged: true,
      entityType,
      primaryId,
      duplicateId,
      affectedPaths: [...new Set(getAffectedPaths(validated, entityType as EditableEntityType, primaryId))],
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/automation/runs", async (_request, response) => {
  try {
    const runs = await listAutomationRuns();
    response.json({ runs });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/automation/runs/:runId", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.params.runId);
    response.json({ run });
  } catch (error) {
    response.status(404).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/batch-import/sessions", async (_request, response) => {
  try {
    const sessions = await listBatchImportSessions();
    response.json({ sessions });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/batch-import/:sessionId", async (request, response) => {
  try {
    const session = await loadBatchImportSession(request.params.sessionId);
    response.json({ session });
  } catch (error) {
    response.status(404).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/analyze", async (request, response) => {
  try {
    const library = await loadLibraryFromDisk();
    const llmConfig = await loadLlmConfig();
    const selectedComposerId = String(request.body?.selectedComposerId || "").trim();
    const selectedWorkId = String(request.body?.selectedWorkId || "").trim();
    const workTypeHint = String(request.body?.workTypeHint || "unknown").trim();
    const result = await analyzeBatchImport({
      sourceText: request.body?.sourceText || "",
      composerId: selectedComposerId,
      workId: selectedWorkId,
      workTypeHint,
      library,
      llmConfig,
      fetchImpl: automationFetch,
      referenceRegistry: await loadReferenceRegistry(),
    });

    const now = new Date().toISOString();
    const session = await saveBatchImportSession({
      id: `batch-${now.replace(/[:.]/g, "-")}`,
      createdAt: now,
      updatedAt: now,
      sourceText: request.body?.sourceText || "",
      sourceFileName: request.body?.sourceFileName || "",
      status: "analyzed",
      selectedComposerId: result.selectedComposerId,
      selectedWorkId: result.selectedWorkId,
      workTypeHint: result.workTypeHint,
      composerId: result.composerId,
      workId: result.workId,
      baseLibrary: library,
      draftLibrary: result.draftLibrary,
      draftEntities: result.draftEntities,
      createdEntityRefs: result.createdEntityRefs,
      warnings: result.warnings,
      parseNotes: result.parseNotes,
      llmUsed: result.llmUsed,
      recordingEnrichment: {
        providerName: "recording-retrieval-service",
        status: "queued",
        itemMap: Object.fromEntries((result.draftEntities.recordings || []).map((entry) => [entry.entity.id, entry.sourceLine])),
      },
      runId: "",
    });

    response.json({ session });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/:sessionId/confirm-create", async (request, response) => {
  try {
    const session = await loadBatchImportSession(request.params.sessionId);
    const replacedSession = replaceBatchDraftEntities(session, request.body?.draftEntities || session.draftEntities);
    const selection = resolveConfirmedBatchSelection(replacedSession);
    const library = await loadLibraryFromDisk();
    await saveValidatedLibrary(
      mergeBatchSessionIntoLibrary(library, {
        draftLibrary: selection.draftLibrary,
        createdEntityRefs: selection.createdEntityRefs,
      }),
    );
    const nextSession = await saveBatchImportSession({
      ...replacedSession,
      status: "created",
      createdEntityRefs: selection.createdEntityRefs,
    });
    response.json({ session: nextSession, createdEntityRefs: selection.createdEntityRefs });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/:sessionId/check", async (request, response) => {
  try {
    let session = await loadBatchImportSession(request.params.sessionId);
    if (request.body?.draftEntities) {
      session = replaceBatchDraftEntities(session, request.body.draftEntities);
    }
    const selection = resolveConfirmedBatchSelection(session);

    const llmConfig = await loadLlmConfig();
    const checkRequest = buildBatchCheckRequest(session);
    if (!checkRequest.categories.length) {
      const nextSession = await saveBatchImportSession({
        ...session,
        status: "checked",
        updatedAt: new Date().toISOString(),
        createdEntityRefs: selection.createdEntityRefs,
        runId: "",
        run: undefined,
      });
      response.json({ session: nextSession, run: null });
      return;
    }

    const run = await runAutomationChecks(
      selection.draftLibrary,
      checkRequest,
      automationFetch,
      llmConfig,
      {
        ...(await buildRecordingRunOptions({
          kind: "owner-batch-check",
          batchSessionId: session.id,
        })),
        recordingRequestOptions: {
          source: {
            kind: "owner-batch-check",
            batchSessionId: session.id,
          },
          overrides: buildBatchRecordingOverrides(session),
        },
      },
    );
    await saveAutomationRun(run);
    const nextSession = await saveBatchImportSession({
      ...session,
      status: "checked",
      updatedAt: new Date().toISOString(),
      createdEntityRefs: selection.createdEntityRefs,
      recordingEnrichment: run.provider
        ? {
            providerName: run.provider.providerName,
            providerJobId: run.provider.providerJobId,
            requestId: run.provider.requestId,
            submittedAt: run.provider.submittedAt,
            lastSyncedAt: run.provider.lastSyncedAt,
            status: run.provider.status,
            itemProgress: run.provider.progress,
            itemMap: Object.fromEntries(checkRequest.recordingIds.map((id) => [id, id])),
            error: run.provider.error,
          }
        : session.recordingEnrichment,
      runId: run.id,
      run,
    });
    response.json({ session: nextSession, run });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/:sessionId/apply", async (request, response) => {
  try {
    let session = await loadBatchImportSession(request.params.sessionId);
    if (request.body?.draftEntities) {
      session = replaceBatchDraftEntities(session, request.body.draftEntities);
    }
    const selection = resolveConfirmedBatchSelection(session);

    let draftLibrary = structuredClone(selection.draftLibrary);
    let run = session.runId ? await loadAutomationRun(session.runId) : session.run;

    if (run) {
      for (const proposal of run.proposals.filter((item) => item.reviewState === "confirmed" && item.status === "pending")) {
        const preparedRun = await prepareProposalRunForApply(run, proposal.id, automationFetch);
        const applied = applyAutomationProposal(draftLibrary, preparedRun, proposal.id);
        draftLibrary = applied.library;
        run = applied.run;
      }
      await saveAutomationRun(run);
    }

    const library = await loadLibraryFromDisk();
    const mergedLibrary = mergeBatchSessionIntoLibrary(library, {
      ...session,
      draftLibrary,
      run,
    });
    await saveValidatedLibrary(mergedLibrary);

    const nextSession = await saveBatchImportSession({
      ...session,
      draftLibrary,
      createdEntityRefs: selection.createdEntityRefs,
      runId: run?.id || session.runId,
      run,
      status: "applied",
      updatedAt: new Date().toISOString(),
    });

    response.json({
      session: nextSession,
      affectedPaths: [
        ...selection.createdEntityRefs.composers.flatMap((composerId) => getAffectedPaths(mergedLibrary, "composer", composerId)),
        ...selection.createdEntityRefs.people.flatMap((personId) => getAffectedPaths(mergedLibrary, "person", personId)),
        ...selection.createdEntityRefs.works.flatMap((workId) => getAffectedPaths(mergedLibrary, "work", workId)),
        ...getAffectedPaths(mergedLibrary, "work", nextSession.workId),
        ...selection.createdEntityRefs.recordings.map((recordingId) => `/recordings/${recordingId}/`),
      ],
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/:sessionId/abandon", async (request, response) => {
  try {
    const session = await loadBatchImportSession(request.params.sessionId);
    if (session.runId) {
      await deleteAutomationRun(session.runId);
    }
    await deleteBatchImportSession(request.params.sessionId);
    response.json({ abandoned: request.params.sessionId });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/batch-import/:sessionId/abandon-unconfirmed", async (request, response) => {
  try {
    let session = await loadBatchImportSession(request.params.sessionId);
    if (request.body?.draftEntities) {
      session = replaceBatchDraftEntities(session, request.body.draftEntities);
    }

    const prunedDraftEntities = {
      composers: (session.draftEntities.composers || []).filter((entry) => entry.reviewState === "confirmed"),
      people: (session.draftEntities.people || []).filter((entry) => entry.reviewState === "confirmed"),
      works: (session.draftEntities.works || []).filter((entry) => entry.reviewState === "confirmed"),
      recordings: (session.draftEntities.recordings || []).filter((entry) => entry.reviewState === "confirmed"),
    };
    let nextSession = replaceBatchDraftEntities(session, prunedDraftEntities);
    const selection = resolveConfirmedBatchSelection(nextSession);

    if (nextSession.runId) {
      await deleteAutomationRun(nextSession.runId);
      nextSession = {
        ...nextSession,
        runId: "",
        run: undefined,
      };
    }

    const savedSession = await saveBatchImportSession({
      ...nextSession,
      status: selection.createdEntityRefs.recordings.length || selection.createdEntityRefs.works.length || selection.createdEntityRefs.composers.length
        ? "created"
        : "analyzed",
      updatedAt: new Date().toISOString(),
      createdEntityRefs: selection.createdEntityRefs,
      draftLibrary: selection.draftLibrary,
    });
    response.json({ session: savedSession });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/selection-preview", async (request, response) => {
  try {
    const library = await loadLibraryFromDisk();
    const preview = jobManager.previewSelection(library, request.body ?? {});
    response.json({ preview });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/automation/jobs", async (_request, response) => {
  response.json({ jobs: jobManager.listJobs() });
});

app.post("/api/automation/jobs", async (request, response) => {
  try {
    const library = await loadLibraryFromDisk();
    const llmConfig = await loadLlmConfig();
    const runChecksOptions = await buildRecordingRunOptions({ kind: "owner-entity-check" });
    const job = jobManager.createJob({
      library,
      request: request.body ?? {},
      fetchImpl: automationFetch,
      llmConfig,
      maxConcurrency: 6,
      runChecksOptions,
      onCompleted: async (currentJob) => {
        if (!currentJob.run) {
          return;
        }
        const runWithNotes = summarizeAutomationRun({
          ...currentJob.run,
          notes: [
            ...currentJob.run.notes,
            ...currentJob.errors.map((item) => `[${item.entityType ?? "job"}] ${item.message}`),
            llmConfig.enabled
              ? "LLM 辅助：已启用 OpenAI-compatible 配置。"
              : "LLM 辅助：未启用，已回退到纯规则模式。",
          ],
        });
        currentJob.run = runWithNotes;
        await persistRunOnComplete(currentJob);
      },
    });

    response.json({ job });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/check", async (request, response) => {
  try {
    const library = await loadLibraryFromDisk();
    const llmConfig = await loadLlmConfig();
    const runChecksOptions = await buildRecordingRunOptions({ kind: "owner-entity-check" });
    const job = jobManager.createJob({
      library,
      request: request.body ?? {},
      fetchImpl: automationFetch,
      llmConfig,
      maxConcurrency: 6,
      runChecksOptions,
      onCompleted: persistRunOnComplete,
    });
    response.json({ job });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/automation/jobs/:jobId", async (request, response) => {
  const job = jobManager.getJob(request.params.jobId);
  if (!job) {
    response.status(404).json({ error: "Job not found" });
    return;
  }
  response.json({ job });
});

app.post("/api/automation/jobs/:jobId/cancel", async (request, response) => {
  const job = jobManager.cancelJob(request.params.jobId);
  if (!job) {
    response.status(404).json({ error: "Job not found" });
    return;
  }
  response.json({ job });
});

app.post("/api/automation/entity-check/:entityType/:id", async (request, response) => {
  try {
    const library = await loadLibraryFromDisk();
    const llmConfig = await loadLlmConfig();
    const runChecksOptions = await buildRecordingRunOptions({ kind: "owner-entity-check" });
    const job = jobManager.createJob({
      library,
      request: buildEntityCheckRequest(request.params.entityType, request.params.id, library),
      fetchImpl: automationFetch,
      llmConfig,
      maxConcurrency: 2,
      runChecksOptions,
      onCompleted: persistRunOnComplete,
    });
    response.json({ job });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/proposals/:proposalId/review-state", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.body?.runId);
    const nextRun = updateAutomationProposalReview(
      run,
      request.params.proposalId,
      request.body?.reviewState || "viewed",
      request.body?.selectedImageCandidateId || "",
    );
    await saveAutomationRun(nextRun);
    response.json({ run: nextRun });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/assets/upload", async (request, response) => {
  try {
    const bucket = String(request.body?.bucket || "misc");
    const slug = String(request.body?.slug || "asset");
    const fileName = String(request.body?.fileName || "upload.jpg");
    const contentBase64 = String(request.body?.contentBase64 || "");
    if (!contentBase64) {
      response.status(400).json({ error: "Missing file content" });
      return;
    }

    const src = await persistUploadedImageAsset({
      bucket,
      slug,
      fileName,
      bytes: decodeBase64Bytes(contentBase64),
    });

    response.json({
      asset: {
        src,
        imageSourceKind: "manual",
        imageSourceUrl: "",
        imageAttribution: fileName,
        imageUpdatedAt: new Date().toISOString(),
      },
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/articles", async (_request, response) => {
  try {
    const articles = await loadArticlesFromDisk();
    response.json({ articles });
  } catch (error) {
    response.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/articles/preview", async (request, response) => {
  try {
    response.json({
      preview: buildArticlePreviewModel({
        title: request.body?.title || "",
        summary: request.body?.summary || "",
        markdown: request.body?.markdown || "",
      }),
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/articles", async (request, response) => {
  try {
    const articles = await loadArticlesFromDisk();
    const article = buildArticle(articles, request.body || {});
    const validated = validateArticles(upsertCollection(articles, article) && articles);
    await saveArticlesToDisk(validated);
    await writeGeneratedArtifacts();
    response.json({
      mode: "created",
      article,
      affectedPaths: [`/columns/${article.slug}/`],
      preview: buildArticlePreviewModel(article),
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.put("/api/articles/:id", async (request, response) => {
  try {
    const articles = await loadArticlesFromDisk();
    const article = buildArticle(articles, {
      ...(request.body || {}),
      id: request.params.id,
    });
    const validated = validateArticles(upsertCollection(articles, article) && articles);
    await saveArticlesToDisk(validated);
    await writeGeneratedArtifacts();
    response.json({
      mode: "updated",
      article,
      affectedPaths: [`/columns/${article.slug}/`],
      preview: buildArticlePreviewModel(article),
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.delete("/api/articles/:id", async (request, response) => {
  try {
    const articles = await loadArticlesFromDisk();
    const nextArticles = articles.filter((item) => item.id !== request.params.id);
    await saveArticlesToDisk(nextArticles);
    await writeGeneratedArtifacts();
    response.json({ deleted: request.params.id });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/proposals/:proposalId/edit", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.body?.runId);
    const proposal = run.proposals.find((item) => item.id === request.params.proposalId);
    if (!proposal) {
      response.status(404).json({ error: "Proposal not found" });
      return;
    }

    const patchMap = sanitizeProposalPatchMap(
      proposal.entityType,
      request.body?.fieldsPatchMap && typeof request.body.fieldsPatchMap === "object" ? request.body.fieldsPatchMap : {},
    );
    const selectedImageCandidateId = typeof request.body?.selectedImageCandidateId === "string" ? request.body.selectedImageCandidateId : proposal.selectedImageCandidateId || "";
    const library = await loadLibraryFromDisk();
    const entity = entityCollectionByType(library, proposal.entityType).find((item) => item.id === proposal.entityId);

    const nextRun = summarizeAutomationRun({
      ...run,
      proposals: run.proposals.map((item) =>
        item.id === proposal.id
          ? {
              ...item,
              reviewState: "edited",
              status: item.status === "applied" ? "applied" : "pending",
              selectedImageCandidateId,
              fields: Object.entries(patchMap).reduce((fields, [path, after]) => {
                const existing = item.fields.find((field) => field.path === path);
                return appendOrReplaceField(fields, {
                  path,
                  before: existing ? existing.before : getValueAtPath(entity, path),
                  after,
                });
              }, [...item.fields]),
            }
          : item,
      ),
    });

    await saveAutomationRun(nextRun);
    response.json({ run: nextRun });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/proposals/:proposalId/upload-image", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.body?.runId);
    const proposal = run.proposals.find((item) => item.id === request.params.proposalId);
    if (!proposal) {
      response.status(404).json({ error: "Proposal not found" });
      return;
    }
    const fileName = String(request.body?.fileName || "upload.jpg");
    const contentBase64 = String(request.body?.contentBase64 || "");
    if (!contentBase64) {
      response.status(400).json({ error: "Missing file content" });
      return;
    }

    const src = await persistUploadedImageAsset({
      bucket: bucketForEntityType(proposal.entityType),
      slug: proposal.entityId,
      fileName,
      bytes: decodeBase64Bytes(contentBase64),
    });

    const candidateId = `${proposal.id}-manual-${Date.now()}`;
    const nextCandidate = {
      id: candidateId,
      src,
      sourceUrl: "",
      sourceKind: "manual",
      attribution: fileName,
      title: fileName,
      score: 100,
    };

    const nextRun = summarizeAutomationRun({
      ...run,
      proposals: run.proposals.map((item) =>
        item.id === proposal.id
          ? {
              ...item,
              reviewState: "edited",
              status: item.status === "applied" ? "applied" : "pending",
              selectedImageCandidateId: candidateId,
              imageCandidates: [...(item.imageCandidates || []), nextCandidate],
            }
          : item,
      ),
    });

    await saveAutomationRun(nextRun);
    response.json({ run: nextRun, candidate: nextCandidate });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/proposals/:proposalId/apply", async (request, response) => {
  try {
    const { runId, imageCandidateId } = request.body ?? {};
    const [library, run] = await Promise.all([loadLibraryFromDisk(), loadAutomationRun(runId)]);
    const proposal = run.proposals.find((item) => item.id === request.params.proposalId);
    if (!proposal) {
      response.status(404).json({ error: "Proposal not found" });
      return;
    }
    if (!canApplyAutomationProposal(proposal)) {
      response.status(400).json({
        error: buildBlockedProposalApplyMessage(
          [{ proposal, reasons: collectAutomationProposalApplyBlockers(proposal) }],
          "当前候选",
        ),
      });
      return;
    }

    let preparedRun = summarizeAutomationRun({
      ...run,
      proposals: run.proposals.map((item) =>
        item.id === proposal.id
          ? {
              ...item,
              reviewState: "confirmed",
              selectedImageCandidateId: imageCandidateId || item.selectedImageCandidateId || "",
            }
          : item,
      ),
    });

    preparedRun = await prepareProposalRunForApply(preparedRun, proposal.id, automationFetch);
    preparedRun = sanitizeAutomationRunProposalFields(preparedRun);
    const applied = applyAutomationProposal(library, preparedRun, proposal.id);
    await saveValidatedLibrary(applied.library);
    await saveAutomationRun(applied.run);

    response.json({
      run: applied.run,
      snapshot: applied.snapshot,
      affectedPaths: getAffectedPaths(applied.library, proposal.entityType, proposal.entityId),
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/proposals/:proposalId/ignore", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.body?.runId);
    const nextRun = ignoreAutomationProposal(
      updateAutomationProposalReview(run, request.params.proposalId, "discarded"),
      request.params.proposalId,
    );
    await saveAutomationRun(nextRun);
    response.json({ run: nextRun });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/runs/:runId/apply-confirmed", async (_request, response) => {
  try {
    const [library, run] = await Promise.all([loadLibraryFromDisk(), loadAutomationRun(_request.params.runId)]);
    const blockedProposals = run.proposals
      .filter((proposal) => proposal.status === "pending" && proposal.reviewState === "confirmed")
      .map((proposal) => ({
        proposal,
        reasons: collectAutomationProposalApplyBlockers(proposal),
      }))
      .filter((entry) => entry.reasons.length > 0);
    if (blockedProposals.length) {
      response.status(400).json({ error: buildBlockedProposalApplyMessage(blockedProposals) });
      return;
    }

    let preparedRun = run;
    for (const proposal of preparedRun.proposals) {
      if (proposal.status !== "pending" || proposal.reviewState !== "confirmed" || !canApplyAutomationProposal(proposal)) {
        continue;
      }
      preparedRun = await prepareProposalRunForApply(preparedRun, proposal.id, automationFetch);
    }

    const confirmReadyRun = sanitizeAutomationRunProposalFields(summarizeAutomationRun({
      ...preparedRun,
      proposals: preparedRun.proposals.map((proposal) =>
        proposal.status === "pending" && proposal.reviewState !== "confirmed" ? { ...proposal, status: "ignored" } : proposal,
      ),
    }));

    const applied = applyPendingAutomationProposals(library, confirmReadyRun);
    await saveValidatedLibrary(applied.library);
    await saveAutomationRun(applied.run);
    response.json({ run: applied.run, snapshots: applied.snapshots });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/runs/:runId/ignore-pending", async (request, response) => {
  try {
    const run = await loadAutomationRun(request.params.runId);
    const nextRun = ignorePendingAutomationProposals(run);
    await saveAutomationRun(nextRun);
    response.json({ run: nextRun });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/snapshots/:snapshotId/revert", async (request, response) => {
  try {
    const [library, run] = await Promise.all([loadLibraryFromDisk(), loadAutomationRun(request.body?.runId)]);
    const snapshot = findRunSnapshot(run, request.params.snapshotId);
    if (!snapshot) {
      response.status(404).json({ error: "Snapshot not found" });
      return;
    }

    const revertedLibrary = revertAutomationProposal(library, run, snapshot.id);
    const nextRun = summarizeAutomationRun({
      ...run,
      proposals: run.proposals.map((proposal) =>
        proposal.id === snapshot.proposalId ? { ...proposal, status: "pending", reviewState: "viewed" } : proposal,
      ),
      snapshots: run.snapshots.filter((item) => item.id !== snapshot.id),
    });

    await saveValidatedLibrary(revertedLibrary);
    await saveAutomationRun(nextRun);

    response.json({
      run: nextRun,
      affectedPaths: getAffectedPaths(revertedLibrary, snapshot.entityType, snapshot.entityId),
    });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/automation/llm/config", async (_request, response) => {
  const config = await loadLlmConfig();
  response.json({ config: sanitizeLlmConfig(config) });
});

app.post("/api/automation/llm/config", async (request, response) => {
  try {
    const current = await loadLlmConfig();
    const next = mergeLlmConfigPatch(current ?? defaultLlmConfig, request.body ?? {});
    await saveLlmConfig(next);
    response.json({ config: sanitizeLlmConfig(next) });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.post("/api/automation/llm/test", async (request, response) => {
  try {
    const current = await loadLlmConfig();
    const config = mergeLlmConfigPatch(current ?? defaultLlmConfig, request.body ?? {});
    const result = await testOpenAiCompatibleConfig(config, automationFetch);
    response.json({ result, config: sanitizeLlmConfig(config) });
  } catch (error) {
    response.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
});

async function startOwnerApp() {
  await bootstrapActiveLibrary({ defaultLibraryName: "\u6211\u7684\u8d44\u6599\u5e93", seedFromLegacy: false });
  app.listen(port, "127.0.0.1", () => {
    process.stdout.write(`Owner tool running at http://127.0.0.1:${port}\n`);
  });
}

void startOwnerApp().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});














