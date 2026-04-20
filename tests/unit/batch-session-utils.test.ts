import { describe, expect, it } from "vitest";

import type { BatchDraftEntities } from "../../packages/automation/src/batch-import.js";
import type { BatchImportSession } from "../../packages/automation/src/batch-import-store.js";
import { validateLibrary, type LibraryData } from "../../packages/shared/src/schema.js";
import { replaceBatchDraftEntities, resolveConfirmedBatchSelection } from "../../apps/owner/server/batch-session-utils.js";

function buildBaseLibrary(): LibraryData {
  return validateLibrary({
    composers: [
      {
        id: "composer-tchaikovsky",
        slug: "tchaikovsky",
        name: "柴可夫斯基",
        fullName: "",
        nameLatin: "Peter Ilyich Tchaikovsky",
        displayName: "柴可夫斯基",
        displayFullName: "",
        displayLatinName: "Peter Ilyich Tchaikovsky",
        country: "Russia",
        countries: ["Russia"],
        avatarSrc: "",
        aliases: [],
        abbreviations: [],
        sortKey: "0010",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
        roles: ["composer"],
      },
    ],
    people: [
      {
        id: "person-rudolf-kempe",
        slug: "rudolf-kempe",
        name: "鲁道夫·肯佩",
        fullName: "",
        nameLatin: "Rudolf Kempe",
        displayName: "鲁道夫·肯佩",
        displayFullName: "",
        displayLatinName: "Rudolf Kempe",
        country: "Germany",
        countries: ["Germany"],
        avatarSrc: "",
        aliases: [],
        abbreviations: [],
        sortKey: "0010",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
        roles: ["conductor"],
      },
    ],
    workGroups: [
      {
        id: "work-group-symphony",
        composerId: "composer-tchaikovsky",
        title: "交响曲",
        slug: "symphony",
        path: ["交响曲"],
        sortKey: "0010",
      },
    ],
    works: [
      {
        id: "work-tchaikovsky-5",
        composerId: "composer-tchaikovsky",
        groupIds: ["work-group-symphony"],
        slug: "tchaikovsky-5",
        title: "第五交响曲",
        titleLatin: "Symphony No. 5",
        aliases: [],
        catalogue: "Op. 64",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        sortKey: "0010",
        updatedAt: "2026-04-14T00:00:00.000Z",
      },
    ],
    recordings: [],
  });
}

function buildDraftEntities(): BatchDraftEntities {
  return {
    composers: [],
    works: [],
    people: [
      {
        draftId: "person:person-ensemble-codexlso",
        entityType: "person",
        sourceLine: "CodexKempe | CodexLSO | 2099 | -",
        notes: ["auto-linked-from=ensemble"],
        reviewState: "unconfirmed",
        entity: {
          id: "person-ensemble-codexlso",
          slug: "codexlso",
          name: "CodexLSO",
          fullName: "",
          nameLatin: "CodexLSO",
          displayName: "CodexLSO",
          displayFullName: "",
          displayLatinName: "CodexLSO",
          country: "",
          countries: [],
          avatarSrc: "",
          aliases: [],
          abbreviations: [],
          sortKey: "0020",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["ensemble"],
        },
      },
    ],
    recordings: [
      {
        draftId: "recording:recording-codex",
        entityType: "recording",
        sourceLine: "CodexKempe | CodexLSO | 2099 | -",
        notes: ["workTypeHint=orchestral"],
        reviewState: "confirmed",
        entity: {
          id: "recording-codex",
          workId: "work-tchaikovsky-5",
          slug: "codex",
          title: "CodexKempe - CodexLSO - 2099",
          workTypeHint: "orchestral",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-04-14T00:00:00.000Z",
          images: [],
          credits: [
            {
              role: "conductor",
              personId: "person-rudolf-kempe",
              displayName: "CodexKempe",
              label: "",
            },
            {
              role: "ensemble",
              personId: "person-ensemble-codexlso",
              displayName: "CodexLSO",
              label: "",
            },
          ],
          links: [],
          notes: "",
          performanceDateText: "2099",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
        },
      },
    ],
  };
}

function buildSession(): BatchImportSession {
  const baseLibrary = buildBaseLibrary();
  return {
    id: "batch-test",
    createdAt: "2026-04-14T00:00:00.000Z",
    updatedAt: "2026-04-14T00:00:00.000Z",
    sourceText: "CodexKempe | CodexLSO | 2099 | -",
    sourceFileName: "",
    status: "created",
    selectedComposerId: "composer-tchaikovsky",
    selectedWorkId: "work-tchaikovsky-5",
    workTypeHint: "orchestral",
    composerId: "composer-tchaikovsky",
    workId: "work-tchaikovsky-5",
    baseLibrary,
    draftLibrary: baseLibrary,
    draftEntities: buildDraftEntities(),
    createdEntityRefs: {
      composers: [],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    },
    warnings: [],
    parseNotes: [],
    llmUsed: false,
    runId: "",
  };
}

describe("batch session utils", () => {
  it("rehydrates draft people referenced by confirmed recordings before resolving the selected batch subset", () => {
    const session = buildSession();

    const nextSession = replaceBatchDraftEntities(session, buildDraftEntities());
    const selection = resolveConfirmedBatchSelection(nextSession);

    expect(nextSession.draftLibrary.people.map((item) => item.id)).toContain("person-ensemble-codexlso");
    expect(selection.createdEntityRefs.people).toContain("person-ensemble-codexlso");
    expect(selection.draftLibrary.people.map((item) => item.id)).toContain("person-ensemble-codexlso");
    expect(selection.draftLibrary.recordings.map((item) => item.id)).toContain("recording-codex");
  });
});
