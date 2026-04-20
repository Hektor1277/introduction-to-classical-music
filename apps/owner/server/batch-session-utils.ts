import { buildConfirmedBatchSelection, type BatchDraftEntities } from "../../../packages/automation/src/batch-import.js";
import type { BatchImportSession } from "../../../packages/automation/src/batch-import-store.js";
import { validateLibrary, type LibraryData } from "../../../packages/shared/src/schema.js";

function upsertCollection<T extends { id: string }>(collection: T[], nextItem: T) {
  const index = collection.findIndex((item) => item.id === nextItem.id);
  if (index >= 0) {
    collection[index] = nextItem;
    return;
  }
  collection.push(nextItem);
}

function rebuildBatchDraftLibrary(
  baseLibrary: LibraryData,
  existingDraftLibrary: LibraryData,
  draftEntities: BatchDraftEntities,
) {
  const nextLibrary = structuredClone(baseLibrary);

  for (const collectionName of ["composers", "people", "workGroups", "works", "recordings"] as const) {
    for (const item of existingDraftLibrary[collectionName] || []) {
      upsertCollection(nextLibrary[collectionName], item);
    }
  }

  for (const entry of draftEntities.composers || []) {
    upsertCollection(nextLibrary.composers, entry.entity);
  }
  for (const entry of draftEntities.people || []) {
    upsertCollection(nextLibrary.people, entry.entity);
  }
  for (const entry of draftEntities.works || []) {
    upsertCollection(nextLibrary.works, entry.entity);
  }
  for (const entry of draftEntities.recordings || []) {
    upsertCollection(nextLibrary.recordings, entry.entity);
  }

  return validateLibrary(nextLibrary);
}

export function replaceBatchDraftEntities(session: BatchImportSession, nextDraftEntities: BatchDraftEntities): BatchImportSession {
  const draftLibrary = rebuildBatchDraftLibrary(session.baseLibrary, session.draftLibrary, nextDraftEntities);
  const selection = buildConfirmedBatchSelection(session.baseLibrary, draftLibrary, nextDraftEntities);
  return {
    ...session,
    draftEntities: structuredClone(nextDraftEntities),
    draftLibrary: draftLibrary,
    createdEntityRefs: selection.createdEntityRefs,
    updatedAt: new Date().toISOString(),
  };
}

export function resolveConfirmedBatchSelection(session: BatchImportSession) {
  const draftLibrary = rebuildBatchDraftLibrary(session.baseLibrary, session.draftLibrary, session.draftEntities);
  return buildConfirmedBatchSelection(session.baseLibrary, draftLibrary, session.draftEntities);
}

export function mergeBatchSessionIntoLibrary(
  library: LibraryData,
  sessionLike: Pick<BatchImportSession, "draftLibrary" | "createdEntityRefs">,
) {
  const nextLibrary = structuredClone(library);

  for (const groupId of sessionLike.createdEntityRefs.workGroups) {
    const group = sessionLike.draftLibrary.workGroups.find((item) => item.id === groupId);
    if (group) {
      upsertCollection(nextLibrary.workGroups, group);
    }
  }
  for (const composerId of sessionLike.createdEntityRefs.composers) {
    const composer = sessionLike.draftLibrary.composers.find((item) => item.id === composerId);
    if (composer) {
      upsertCollection(nextLibrary.composers, composer);
    }
  }
  for (const personId of sessionLike.createdEntityRefs.people) {
    const person = sessionLike.draftLibrary.people.find((item) => item.id === personId);
    if (person) {
      upsertCollection(nextLibrary.people, person);
    }
  }
  for (const workId of sessionLike.createdEntityRefs.works) {
    const work = sessionLike.draftLibrary.works.find((item) => item.id === workId);
    if (work) {
      upsertCollection(nextLibrary.works, work);
    }
  }
  for (const recordingId of sessionLike.createdEntityRefs.recordings) {
    const recording = sessionLike.draftLibrary.recordings.find((item) => item.id === recordingId);
    if (recording) {
      upsertCollection(nextLibrary.recordings, recording);
    }
  }

  return validateLibrary(nextLibrary);
}
