import { spawnSync } from "node:child_process";
import { existsSync, promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { loadLibraryFromDisk, saveLibraryToDisk, writeGeneratedArtifacts } from "../packages/data-core/src/library-store.js";
import { parseLegacyRecordingHtml } from "../packages/data-core/src/legacy-parser.js";
import {
  applyManualRecordingBackfills,
  type ManualRecordingBackfillEntry,
} from "../packages/data-core/src/manual-recording-backfill.js";
import { cleanupLibraryPeople, ensurePeopleForCredits } from "../packages/data-core/src/person-cleanup.js";
import {
  backfillRecordingWorkTypeHints,
  recordingNeedsLegacyRepair,
  repairRecordingFromLegacyParse,
} from "../packages/data-core/src/recording-repair.js";

const SOURCE_ROOT_NAME = "an incomplete guide to classical music";
const defaultSources = [
  path.join(process.cwd(), "materials", "archive", "an incomplete guide to classical music.rar"),
];
const manualBackfillFilePath = path.join(process.cwd(), "materials", "references", "manual-recording-backfills.json");

function compact(value: unknown) {
  return String(value ?? "").trim();
}

async function exists(targetPath: string) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function loadManualRecordingBackfills() {
  if (!(await exists(manualBackfillFilePath))) {
    return [] as ManualRecordingBackfillEntry[];
  }
  const raw = await fs.readFile(manualBackfillFilePath, "utf8");
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? (parsed as ManualRecordingBackfillEntry[]) : [];
}

async function extractArchive(sourcePath: string) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "classical-guide-repair-"));
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
  const sourcePath = defaultSources.find((candidate) => requirePath(candidate));
  if (!sourcePath) {
    throw new Error("找不到原始 RAR 档案，无法执行录音修复。");
  }
  const tempDir = await extractArchive(sourcePath);
  const rootDir = await findLegacyRoot(tempDir);
  if (!rootDir) {
    throw new Error(`在解包目录中未找到 ${SOURCE_ROOT_NAME} 根结构。`);
  }
  return { tempDir, rootDir };
}

function requirePath(candidate: string) {
  return existsSync(candidate);
}

function normalizeNameKey(value: unknown) {
  return compact(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\s'"`"“”‘’.,;:!?()[\]{}\-_/\\|&]+/g, "");
}

function stripUnusedPlaceholderPeople(library: Awaited<ReturnType<typeof loadLibraryFromDisk>>) {
  void normalizeNameKey;
  const referencedPersonIds = new Set(
    (library.recordings || []).flatMap((recording) => (recording.credits || []).map((credit) => compact(credit.personId)).filter(Boolean)),
  );
  return {
    ...library,
    people: (library.people || []).filter((person) => person.id !== "person-item" || referencedPersonIds.has(person.id)),
  };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const originalLibrary = await loadLibraryFromDisk();
  let library = backfillRecordingWorkTypeHints(originalLibrary);

  const recordingsToRepair = library.recordings.filter((recording) => recordingNeedsLegacyRepair(library, recording));
  let repairedCount = 0;
  let manualBackfillAppliedCount = 0;
  let backfilledCount = library.recordings.filter((recording, index) => recording.workTypeHint !== originalLibrary.recordings[index]?.workTypeHint).length;
  let normalizedTitleCount = library.recordings.filter((recording, index) => compact(recording.title) !== compact(originalLibrary.recordings[index]?.title)).length;
  let normalizedMetadataCount = library.recordings.filter(
    (recording, index) =>
      compact(recording.performanceDateText) !== compact(originalLibrary.recordings[index]?.performanceDateText) ||
      compact(recording.venueText) !== compact(originalLibrary.recordings[index]?.venueText),
  ).length;

  if (recordingsToRepair.length > 0) {
    const { tempDir, rootDir } = await resolveArchiveRoot();
    try {
      const repairedRecordings = [];
      for (const recording of library.recordings) {
        if (!recordingNeedsLegacyRepair(library, recording)) {
          repairedRecordings.push(recording);
          continue;
        }
        const legacyRelativePath = compact(recording.legacyPath).replace(/\//g, path.sep);
        const legacyFilePath = path.join(rootDir, legacyRelativePath);
        if (!(await exists(legacyFilePath))) {
          repairedRecordings.push(recording);
          continue;
        }
        const html = await fs.readFile(legacyFilePath, "utf8");
        const parsed = parseLegacyRecordingHtml(html);
        library = ensurePeopleForCredits(library, parsed.credits || []);
        const repairedRecording = repairRecordingFromLegacyParse(library, recording, parsed);
        if (compact(repairedRecording.title) !== compact(recording.title)) {
          normalizedTitleCount += 1;
        }
        if (
          compact(repairedRecording.performanceDateText) !== compact(recording.performanceDateText) ||
          compact(repairedRecording.venueText) !== compact(recording.venueText)
        ) {
          normalizedMetadataCount += 1;
        }
        repairedRecordings.push(repairedRecording);
        repairedCount += 1;
      }
      library = {
        ...library,
        recordings: repairedRecordings,
      };
    } finally {
      await fs.rm(tempDir, { recursive: true, force: true });
    }
  }

  library = cleanupLibraryPeople(library);
  const manualBackfills = await loadManualRecordingBackfills();
  if (manualBackfills.length > 0) {
    const beforeKey = JSON.stringify(library.recordings.map((recording) => ({ id: recording.id, credits: recording.credits, title: recording.title })));
    library = applyManualRecordingBackfills(library, manualBackfills);
    const afterMap = new Map(library.recordings.map((recording) => [recording.id, recording]));
    for (const entry of manualBackfills) {
      const nextRecording = afterMap.get(entry.recordingId);
      if (!nextRecording) {
        continue;
      }
      const hadAllCredits = (entry.credits || []).every((credit) =>
        (nextRecording.credits || []).some(
          (candidate) => compact(candidate.role) === compact(credit.role) && compact(candidate.displayName) === compact(credit.displayName),
        ),
      );
      if (hadAllCredits) {
        manualBackfillAppliedCount += 1;
      }
    }
    const afterKey = JSON.stringify(library.recordings.map((recording) => ({ id: recording.id, credits: recording.credits, title: recording.title })));
    if (beforeKey !== afterKey) {
      normalizedTitleCount = library.recordings.filter((recording, index) => compact(recording.title) !== compact(originalLibrary.recordings[index]?.title)).length;
      normalizedMetadataCount = library.recordings.filter(
        (recording, index) =>
          compact(recording.performanceDateText) !== compact(originalLibrary.recordings[index]?.performanceDateText) ||
          compact(recording.venueText) !== compact(originalLibrary.recordings[index]?.venueText),
      ).length;
    }
  }
  library = cleanupLibraryPeople(library);
  library = backfillRecordingWorkTypeHints(library);
  library = stripUnusedPlaceholderPeople(library);
  if (!dryRun) {
    await saveLibraryToDisk(library);
    await writeGeneratedArtifacts();
  }

  const remainingUnknown = library.recordings.filter((recording) => compact(recording.workTypeHint) === "unknown").length;
  const remainingPlaceholderCredits = library.recordings.filter((recording) =>
    (recording.credits || []).some((credit) => compact(credit.personId) === "person-item" || compact(credit.displayName) === "-"),
  ).length;

  console.log(
    JSON.stringify(
      {
        recordingsTotal: library.recordings.length,
        dryRun,
        workTypeBackfilled: backfilledCount,
        recordingsRepairedFromLegacy: repairedCount,
        manualRecordingBackfillsApplied: manualBackfillAppliedCount,
        normalizedTitles: normalizedTitleCount,
        normalizedMetadata: normalizedMetadataCount,
        remainingUnknownWorkTypeHints: remainingUnknown,
        remainingPlaceholderCredits,
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
