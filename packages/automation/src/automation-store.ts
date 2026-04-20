import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";

import { normalizeAutomationRun, type AutomationRun, type AutomationSnapshot } from "./automation.js";
import { defaultLlmConfig, mergeLlmConfigPatch, type LlmConfig } from "./llm.js";
import type { RecordingRetrievalProviderStatus } from "./recording-retrieval.js";
import { getRuntimePaths } from "../../data-core/src/app-paths.js";

function stripUtf8Bom(value: string) {
  return value.charCodeAt(0) === 0xfeff ? value.slice(1) : value;
}

export type RecordingRetrievalConfig = {
  enabled: boolean;
  baseUrl: string;
  timeoutMs: number;
  pollIntervalMs: number;
  expectedProtocolVersion: "v1";
  status: RecordingRetrievalProviderStatus | "";
};

export const defaultRecordingRetrievalConfig: RecordingRetrievalConfig = {
  enabled: true,
  baseUrl: process.env.RECORDING_RETRIEVAL_SERVICE_URL || "http://127.0.0.1:4780",
  timeoutMs: 180000,
  pollIntervalMs: 1200,
  expectedProtocolVersion: "v1",
  status: "",
};

function hashValue(value: string) {
  return createHash("sha1").update(value).digest("hex").slice(0, 10);
}

function sanitizeSegment(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5-]+/gi, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "") || "asset";
}

function extensionFromUrl(url: string) {
  try {
    const pathname = new URL(url).pathname;
    const extension = path.extname(pathname).toLowerCase();
    return extension && extension.length <= 5 ? extension : ".jpg";
  } catch {
    return ".jpg";
  }
}

async function ensureAutomationDirs() {
  const runtimePaths = getRuntimePaths();
  const runsDir = runtimePaths.library.runtimeAutomationRunsDir;
  const assetsDir = runtimePaths.library.assetsManagedDir;
  await fs.mkdir(runtimePaths.appData.rootDir, { recursive: true });
  await fs.mkdir(runsDir, { recursive: true });
  await fs.mkdir(assetsDir, { recursive: true });
}

function runPath(runId: string) {
  const runsDir = getRuntimePaths().library.runtimeAutomationRunsDir;
  return path.join(runsDir, `${runId}.json`);
}

export async function saveAutomationRun(run: AutomationRun) {
  await ensureAutomationDirs();
  const normalizedRun = normalizeAutomationRun(run);
  await fs.writeFile(runPath(normalizedRun.id), `${JSON.stringify(normalizedRun, null, 2)}\n`, "utf8");
  return normalizedRun;
}

export async function loadAutomationRun(runId: string) {
  await ensureAutomationDirs();
  const content = await fs.readFile(runPath(runId), "utf8");
  return normalizeAutomationRun(JSON.parse(content) as AutomationRun);
}

export async function deleteAutomationRun(runId: string) {
  await ensureAutomationDirs();
  await fs.rm(runPath(runId), { force: true });
}

export async function listAutomationRuns() {
  await ensureAutomationDirs();
  const runsDir = getRuntimePaths().library.runtimeAutomationRunsDir;
  const files = await fs.readdir(runsDir);
  const runs = await Promise.all(
    files
      .filter((file) => file.endsWith(".json"))
      .map(async (file) => {
        const content = await fs.readFile(path.join(runsDir, file), "utf8");
        return normalizeAutomationRun(JSON.parse(content) as AutomationRun);
      }),
  );
  return runs.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
}

export async function persistRemoteImageAsset(options: {
  bucket: string;
  slug: string;
  sourceUrl: string;
  fetchImpl?: typeof fetch;
}) {
  await ensureAutomationDirs();
  const fetchImpl = options.fetchImpl ?? fetch;
  const response = await fetchImpl(options.sourceUrl);
  if (!response.ok) {
    throw new Error(`Failed to download image: ${options.sourceUrl}`);
  }

  const bytes = Buffer.from(await response.arrayBuffer());
  const extension = extensionFromUrl(options.sourceUrl);
  const relativeDir = path.join(options.bucket, sanitizeSegment(options.slug));
  const fileName = `${sanitizeSegment(options.slug)}-${hashValue(options.sourceUrl)}${extension}`;
  const assetsDir = getRuntimePaths().library.assetsManagedDir;
  const outputDir = path.join(assetsDir, relativeDir);
  await fs.mkdir(outputDir, { recursive: true });
  const outputPath = path.join(outputDir, fileName);
  await fs.writeFile(outputPath, bytes);

  return `/${path.posix.join("library-assets", "managed", ...relativeDir.split(path.sep), fileName)}`;
}

export async function persistUploadedImageAsset(options: {
  bucket: string;
  slug: string;
  fileName: string;
  bytes: Uint8Array;
}) {
  await ensureAutomationDirs();
  const sourceName = options.fileName || `${options.slug}.jpg`;
  const extension = path.extname(sourceName).toLowerCase() || ".jpg";
  const relativeDir = path.join(options.bucket, sanitizeSegment(options.slug));
  const fileName = `${sanitizeSegment(options.slug)}-${hashValue(`${sourceName}-${Date.now()}`)}${extension}`;
  const assetsDir = getRuntimePaths().library.assetsManagedDir;
  const outputDir = path.join(assetsDir, relativeDir);
  await fs.mkdir(outputDir, { recursive: true });
  const outputPath = path.join(outputDir, fileName);
  await fs.writeFile(outputPath, Buffer.from(options.bytes));

  return `/${path.posix.join("library-assets", "managed", ...relativeDir.split(path.sep), fileName)}`;
}

export function getAutomationPaths() {
  const runtimePaths = getRuntimePaths();
  return {
    automationDir: runtimePaths.library.runtimeAutomationDir,
    runsDir: runtimePaths.library.runtimeAutomationRunsDir,
    settingsPath: runtimePaths.appData.secretsPath,
    recordingRetrievalSettingsPath: runtimePaths.appData.settingsPath,
    assetsDir: runtimePaths.library.assetsManagedDir,
  };
}

export function findRunSnapshot(run: AutomationRun, snapshotId: string): AutomationSnapshot | undefined {
  return run.snapshots.find((snapshot) => snapshot.id === snapshotId);
}

export async function loadLlmConfig() {
  await ensureAutomationDirs();
  const settingsPath = getRuntimePaths().appData.secretsPath;
  try {
    const content = stripUtf8Bom(await fs.readFile(settingsPath, "utf8"));
    return mergeLlmConfigPatch(defaultLlmConfig, JSON.parse(content) as Partial<LlmConfig>);
  } catch {
    return defaultLlmConfig;
  }
}

export async function saveLlmConfig(config: LlmConfig) {
  await ensureAutomationDirs();
  const settingsPath = getRuntimePaths().appData.secretsPath;
  await fs.writeFile(settingsPath, `${JSON.stringify(config, null, 2)}\n`, "utf8");
  return config;
}

export async function loadRecordingRetrievalConfig() {
  await ensureAutomationDirs();
  const recordingRetrievalSettingsPath = getRuntimePaths().appData.settingsPath;
  try {
    const content = stripUtf8Bom(await fs.readFile(recordingRetrievalSettingsPath, "utf8"));
    const parsed = JSON.parse(content) as Partial<RecordingRetrievalConfig>;
    return {
      ...defaultRecordingRetrievalConfig,
      ...parsed,
      expectedProtocolVersion: "v1",
    } satisfies RecordingRetrievalConfig;
  } catch {
    return defaultRecordingRetrievalConfig;
  }
}

export async function saveRecordingRetrievalConfig(config: RecordingRetrievalConfig) {
  await ensureAutomationDirs();
  const recordingRetrievalSettingsPath = getRuntimePaths().appData.settingsPath;
  const nextConfig = {
    ...defaultRecordingRetrievalConfig,
    ...config,
    expectedProtocolVersion: "v1",
  } satisfies RecordingRetrievalConfig;
  await fs.writeFile(recordingRetrievalSettingsPath, `${JSON.stringify(nextConfig, null, 2)}\n`, "utf8");
  return nextConfig;
}
