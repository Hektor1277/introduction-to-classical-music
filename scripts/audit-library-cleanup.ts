import { spawnSync } from "node:child_process";
import { existsSync, promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { loadLibraryFromDisk, loadReviewQueue } from "../packages/data-core/src/library-store.js";
import {
  auditLibraryData,
  buildManualBackfillReference,
  buildManualBackfillQueue,
  groupManualBackfillQueue,
  type RecordingIssueHint,
  summarizeLibraryAuditIssues,
} from "../packages/data-core/src/library-audit.js";
import { parseLegacyRecordingHtml } from "../packages/data-core/src/legacy-parser.js";
import type { ManualRecordingBackfillEntry } from "../packages/data-core/src/manual-recording-backfill.js";
import { classifyRecordingLegacyRepairHint } from "../packages/data-core/src/recording-repair.js";

const SOURCE_ROOT_NAME = "an incomplete guide to classical music";
const defaultSources = [path.join(process.cwd(), "materials", "archive", "an incomplete guide to classical music.rar")];
const unresolvedManualBackfillPath = path.join(
  process.cwd(),
  "materials",
  "references",
  "manual-recording-backfills.unresolved.json",
);
const manualBackfillFilePath = path.join(process.cwd(), "materials", "references", "manual-recording-backfills.json");

async function exists(targetPath: string) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function extractArchive(sourcePath: string) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "classical-guide-audit-"));
  const result = spawnSync("tar", ["-xf", sourcePath, "-C", tempDir], {
    stdio: "pipe",
    encoding: "utf8",
  });

  if (result.error) {
    throw result.error;
  }

  return tempDir;
}

async function findLegacyRoot(startDir: string): Promise<string | null> {
  const queue = [startDir];
  while (queue.length > 0) {
    const currentDir = queue.shift();
    if (!currentDir) {
      continue;
    }
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    if (entries.some((entry) => entry.isDirectory() && entry.name === "作曲家")) {
      return currentDir;
    }
    for (const entry of entries) {
      if (entry.isDirectory()) {
        queue.push(path.join(currentDir, entry.name));
      }
    }
  }
  return null;
}

async function resolveArchiveRoot() {
  const sourcePath = defaultSources.find((candidate) => existsSync(candidate));
  if (!sourcePath) {
    return null;
  }
  const tempDir = await extractArchive(sourcePath);
  const rootDir = await findLegacyRoot(tempDir);
  if (!rootDir) {
    await fs.rm(tempDir, { recursive: true, force: true });
    throw new Error(`在解包目录中未找到 ${SOURCE_ROOT_NAME} 根结构。`);
  }
  return { tempDir, rootDir };
}

async function loadManualBackfillHints() {
  if (!(await exists(manualBackfillFilePath))) {
    return {} as Record<string, RecordingIssueHint>;
  }

  const parsed = JSON.parse(await fs.readFile(manualBackfillFilePath, "utf8")) as ManualRecordingBackfillEntry[];
  const hints: Record<string, RecordingIssueHint> = {};
  for (const entry of parsed || []) {
    if (!entry?.recordingId) {
      continue;
    }
    const waivedMissingRoles = (entry.waivedMissingRoles || []).map((role) => String(role ?? "").trim()).filter(Boolean);
    if (waivedMissingRoles.length === 0) {
      continue;
    }
    hints[entry.recordingId] = {
      resolutionHint: "manual-backfill",
      waivedMissingRoles,
      details: [`人工确认允许缺失署名：${waivedMissingRoles.join(", ")}`],
    };
  }
  return hints;
}

async function buildArchiveIssueHints(library: Awaited<ReturnType<typeof loadLibraryFromDisk>>) {
  const baseIssues = auditLibraryData(library);
  const missingCreditIssues = baseIssues.filter(
    (issue) => issue.code === "recording-missing-credit-role" && issue.entityType === "recording",
  );
  if (missingCreditIssues.length === 0) {
    return {};
  }

  const archiveRoot = await resolveArchiveRoot();
  if (!archiveRoot) {
    return {};
  }

  const hints: Record<string, RecordingIssueHint> = {};
  try {
    for (const issue of missingCreditIssues) {
      const recording = library.recordings.find((entry) => entry.id === issue.entityId);
      if (!recording?.legacyPath) {
        continue;
      }
      const sourcePath = path.join(archiveRoot.rootDir, recording.legacyPath.replace(/\//g, path.sep));
      if (!(await exists(sourcePath))) {
        hints[recording.id] = {
          resolutionHint: "manual-backfill",
          details: ["archive 源文件不存在，无法自动回读。"],
        };
        continue;
      }
      const html = await fs.readFile(sourcePath, "utf8");
      const parsed = parseLegacyRecordingHtml(html);
      hints[recording.id] = classifyRecordingLegacyRepairHint(library, recording, parsed);
    }
    return hints;
  } finally {
    await fs.rm(archiveRoot.tempDir, { recursive: true, force: true });
  }
}

async function main() {
  const [library, reviewQueue] = await Promise.all([loadLibraryFromDisk(), loadReviewQueue()]);
  const [archiveIssueHints, manualBackfillHints] = await Promise.all([
    buildArchiveIssueHints(library),
    loadManualBackfillHints(),
  ]);
  const recordingIssueHints: Record<string, RecordingIssueHint> = { ...archiveIssueHints };
  for (const [recordingId, hint] of Object.entries(manualBackfillHints)) {
    recordingIssueHints[recordingId] = {
      ...recordingIssueHints[recordingId],
      ...hint,
      details: hint.details?.length ? hint.details : recordingIssueHints[recordingId]?.details,
      waivedMissingRoles: hint.waivedMissingRoles?.length
        ? hint.waivedMissingRoles
        : recordingIssueHints[recordingId]?.waivedMissingRoles,
    };
  }
  const issues = auditLibraryData(library, { reviewQueue, recordingIssueHints });
  const summary = summarizeLibraryAuditIssues(issues);
  const manualBackfillQueue = buildManualBackfillQueue(library, issues);
  const manualBackfillGroups = groupManualBackfillQueue(manualBackfillQueue);
  const manualBackfillReference = buildManualBackfillReference(manualBackfillQueue);

  if (process.argv.includes("--write-manual-backfill-reference")) {
    await fs.mkdir(path.dirname(unresolvedManualBackfillPath), { recursive: true });
    await fs.writeFile(unresolvedManualBackfillPath, `${JSON.stringify(manualBackfillReference, null, 2)}\n`, "utf8");
  }

  console.log(
    JSON.stringify(
      {
        summary,
        sample: issues.slice(0, 20),
        manualBackfillQueue,
        manualBackfillGroups,
        manualBackfillReference,
      },
      null,
      2,
    ),
  );
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
