import { spawnSync } from "node:child_process";
import { existsSync, promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { loadLibraryFromDisk } from "../packages/data-core/src/library-store.js";
import { parseLegacyRecordingHtml } from "../packages/data-core/src/legacy-parser.js";
import { normalizeRecordingMetadata } from "../packages/data-core/src/recording-repair.js";

const SOURCE_ROOT_NAME = "an incomplete guide to classical music";
const defaultSources = [path.join(process.cwd(), "materials", "archive", "an incomplete guide to classical music.rar")];

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function normalizeNameKey(value: unknown) {
  return compact(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\s'"`"(),.;:!?/\\|&_-]+/g, "");
}

async function exists(targetPath: string) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function extractArchive(sourcePath: string) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "classical-guide-legacy-audit-"));
  const result = spawnSync("tar", ["-xf", sourcePath, "-C", tempDir], {
    stdio: "pipe",
    encoding: "utf8",
  });

  if (result.error) {
    throw result.error;
  }

  return tempDir;
}

function buildStructuredEnsembleNameSet(
  library: Awaited<ReturnType<typeof loadLibraryFromDisk>>,
  recording: Awaited<ReturnType<typeof loadLibraryFromDisk>>["recordings"][number],
) {
  const names = new Set<string>();
  for (const credit of recording.credits || []) {
    if (!["orchestra", "ensemble", "chorus"].includes(credit.role)) {
      continue;
    }
    const person = library.people.find((entry) => entry.id === compact(credit.personId));
    for (const candidate of [
      credit.displayName,
      person?.name,
      person?.nameLatin,
      ...(person?.aliases || []),
    ]) {
      const normalized = normalizeNameKey(candidate);
      if (normalized) {
        names.add(normalized);
      }
    }
  }
  return names;
}

async function resolveArchiveRoot() {
  const sourcePath = defaultSources.find((candidate) => existsSync(candidate));
  if (!sourcePath) {
    return null;
  }
  const tempDir = await extractArchive(sourcePath);
  const rootDir = path.join(tempDir, SOURCE_ROOT_NAME);
  if (!(await exists(rootDir))) {
    await fs.rm(tempDir, { recursive: true, force: true });
    throw new Error(`Unable to locate legacy root after extracting ${SOURCE_ROOT_NAME}.`);
  }
  return { tempDir, rootDir };
}

async function main() {
  const library = await loadLibraryFromDisk();
  const archiveRoot = await resolveArchiveRoot();
  if (!archiveRoot) {
    throw new Error("Legacy source archive is missing, cannot audit recording alignment.");
  }

  const summary = {
    total: library.recordings.length,
    withLegacyPath: 0,
    aligned: 0,
    mismatched: 0,
    parsedFailed: 0,
    ignoredLegacyEnsembleVenue: 0,
    mismatchedByField: {
      performanceDateText: 0,
      venueText: 0,
    },
  };
  const sample: Array<Record<string, unknown>> = [];

  try {
    for (const recording of library.recordings) {
      const legacyPath = compact(recording.legacyPath);
      if (!legacyPath) {
        continue;
      }

      summary.withLegacyPath += 1;
      const sourcePath = path.join(archiveRoot.rootDir, legacyPath.replace(/\//g, path.sep));
      if (!(await exists(sourcePath))) {
        summary.parsedFailed += 1;
        continue;
      }

      try {
        const parsed = parseLegacyRecordingHtml(await fs.readFile(sourcePath, "utf8"));
        const current = normalizeRecordingMetadata(recording);
        const expected = normalizeRecordingMetadata({
          performanceDateText: parsed.performanceDateText,
          venueText: parsed.venueText,
        });
        const ensembleNames = buildStructuredEnsembleNameSet(library, recording);

        if (expected.venueText && ensembleNames.has(normalizeNameKey(expected.venueText))) {
          expected.venueText = "";
          summary.ignoredLegacyEnsembleVenue += 1;
        }

        const changedFields: string[] = [];
        if (current.performanceDateText !== expected.performanceDateText) {
          summary.mismatchedByField.performanceDateText += 1;
          changedFields.push("performanceDateText");
        }
        if (current.venueText !== expected.venueText) {
          summary.mismatchedByField.venueText += 1;
          changedFields.push("venueText");
        }

        if (changedFields.length === 0) {
          summary.aligned += 1;
          continue;
        }

        summary.mismatched += 1;
        if (sample.length < 20) {
          sample.push({
            id: recording.id,
            title: recording.title,
            changedFields,
            current,
            expected,
            legacyPath,
          });
        }
      } catch {
        summary.parsedFailed += 1;
      }
    }
  } finally {
    await fs.rm(archiveRoot.tempDir, { recursive: true, force: true });
  }

  console.log(
    JSON.stringify(
      {
        summary,
        sample,
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
