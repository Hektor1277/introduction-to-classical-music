import { promises as fs } from "node:fs";
import path from "node:path";

import type { AutomationRun } from "./automation.js";
import type { AnalyzeBatchImportResult, BatchDraftEntities } from "./batch-import.js";
import { validateLibrary, type LibraryData } from "../../shared/src/schema.js";

export type BatchImportSessionStatus = "analyzed" | "created" | "checked" | "applied" | "abandoned";

export type BatchImportSession = {
  id: string;
  createdAt: string;
  updatedAt: string;
  sourceText: string;
  sourceFileName: string;
  status: BatchImportSessionStatus;
  selectedComposerId: string;
  selectedWorkId: string;
  workTypeHint: string;
  composerId: string;
  workId: string;
  baseLibrary: LibraryData;
  draftLibrary: LibraryData;
  draftEntities: BatchDraftEntities;
  createdEntityRefs: AnalyzeBatchImportResult["createdEntityRefs"];
  warnings: string[];
  parseNotes: string[];
  llmUsed: boolean;
  recordingEnrichment?: {
    providerName: string;
    providerJobId?: string;
    requestId?: string;
    submittedAt?: string;
    lastSyncedAt?: string;
    status: string;
    itemProgress?: {
      total: number;
      completed: number;
      succeeded: number;
      partial: number;
      failed: number;
      notFound: number;
    };
    itemMap?: Record<string, string>;
    error?: string;
  };
  runId: string;
  run?: AutomationRun;
};

const rootDir = process.cwd();
const batchesDir = path.join(rootDir, "data", "automation", "batches");

async function ensureBatchDir() {
  await fs.mkdir(batchesDir, { recursive: true });
}

function sessionPath(sessionId: string) {
  return path.join(batchesDir, `${sessionId}.json`);
}

function validateSession(session: BatchImportSession) {
  const baseLibrary = session.baseLibrary || session.draftLibrary;
  return {
    ...session,
    selectedComposerId: session.selectedComposerId || session.composerId || "",
    selectedWorkId: session.selectedWorkId || session.workId || "",
    workTypeHint: session.workTypeHint || "unknown",
    baseLibrary: validateLibrary(baseLibrary),
    draftLibrary: validateLibrary(session.draftLibrary),
  };
}

export async function saveBatchImportSession(session: BatchImportSession) {
  await ensureBatchDir();
  const validated = validateSession(session);
  await fs.writeFile(sessionPath(session.id), `${JSON.stringify(validated, null, 2)}\n`, "utf8");
  return validated;
}

export async function loadBatchImportSession(sessionId: string) {
  await ensureBatchDir();
  const raw = await fs.readFile(sessionPath(sessionId), "utf8");
  return validateSession(JSON.parse(raw) as BatchImportSession);
}

export async function deleteBatchImportSession(sessionId: string) {
  await ensureBatchDir();
  await fs.rm(sessionPath(sessionId), { force: true });
}

export async function listBatchImportSessions() {
  await ensureBatchDir();
  const files = await fs.readdir(batchesDir);
  const sessions = await Promise.all(
    files
      .filter((file) => file.endsWith(".json"))
      .map(async (file) => {
        const raw = await fs.readFile(path.join(batchesDir, file), "utf8");
        return validateSession(JSON.parse(raw) as BatchImportSession);
      }),
  );

  return sessions.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
}

