import { describe, expect, it } from "vitest";

import {
  analyzeBatchImport,
  buildConfirmedBatchSelection,
  cloneBatchDraftEntities,
  loadOrchestraAbbreviationMap,
  normalizeBatchImportSource,
  parseOrchestraAbbreviationText,
} from "@/lib/batch-import";
import { buildReferenceRegistry } from "@/lib/reference-registry";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { validateLibrary, type LibraryData } from "@/lib/schema";

function baseLibrary(): LibraryData {
  return validateLibrary({
    composers: [
      {
        id: "composer-beethoven",
        slug: "beethoven",
        name: "贝多芬",
        fullName: "",
        nameLatin: "Ludwig van Beethoven",
        displayName: "贝多芬",
        displayFullName: "",
        displayLatinName: "Ludwig van Beethoven",
        country: "Germany",
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
      },
    ],
    people: [],
    workGroups: [
      {
        id: "group-symphony",
        composerId: "composer-beethoven",
        title: "交响曲",
        slug: "symphony",
        path: ["交响曲"],
        sortKey: "0010",
      },
    ],
    works: [
      {
        id: "work-beethoven-5",
        composerId: "composer-beethoven",
        groupIds: ["group-symphony"],
        slug: "beethoven-5",
        title: "第五交响曲",
        titleLatin: "Symphony No. 5",
        aliases: [],
        catalogue: "Op. 67",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        sortKey: "0010",
        updatedAt: "2026-03-15T00:00:00.000Z",
      },
    ],
    recordings: [],
  });
}

describe("batch import", () => {
  it("parses orchestra abbreviation text", () => {
    const map = parseOrchestraAbbreviationText("VPO = Wiener Philharmoniker\nLPO = London Philharmonic Orchestra");

    expect(map.VPO).toBe("Wiener Philharmoniker");
    expect(map.LPO).toBe("London Philharmonic Orchestra");
  });

  it("loads orchestra abbreviations from a text file", async () => {
    const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "batch-import-"));
    const filePath = path.join(tempDir, "abbr.txt");
    await fs.writeFile(filePath, "LSO = London Symphony Orchestra\nRCO = Royal Concertgebouw Orchestra\n", "utf8");

    await expect(loadOrchestraAbbreviationMap(filePath)).resolves.toEqual({
      LSO: "London Symphony Orchestra",
      RCO: "Royal Concertgebouw Orchestra",
    });
  });

  it("falls back to materials references orchestra abbreviation file when the default file is missing", async () => {
    const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "batch-import-cwd-"));
    const referencesDir = path.join(tempDir, "materials", "references");
    await fs.mkdir(referencesDir, { recursive: true });
    await fs.writeFile(
      path.join(referencesDir, "Orchestra Abbreviation Comparison.txt"),
      "LSO = London Symphony Orchestra\n",
      "utf8",
    );

    const previousCwd = process.cwd();
    process.chdir(tempDir);
    try {
      await expect(loadOrchestraAbbreviationMap()).resolves.toEqual({
        LSO: "London Symphony Orchestra",
      });
    } finally {
      process.chdir(previousCwd);
    }
  });

  it("clones batch draft entities without mutating the source", () => {
    const source = {
      composers: [],
      people: [],
      works: [],
      recordings: [
        {
          draftId: "recording:1",
          entityType: "recording" as const,
          sourceLine: "Kleiber | Wiener Philharmoniker | 1975 | -",
          notes: ["workTypeHint=orchestral"],
          reviewState: "unconfirmed" as const,
          entity: {
            id: "recording-1",
            workId: "work-beethoven-5",
            slug: "recording-1",
            title: "Kleiber - Wiener Philharmoniker - 1975",
            sortKey: "0001",
            isPrimaryRecommendation: false,
            updatedAt: "2026-03-13T00:00:00.000Z",
            images: [],
            credits: [],
            links: [
              {
                platform: "youtube" as const,
                url: "https://www.youtube.com/watch?v=abc",
                localPath: "",
                title: "",
                linkType: "external" as const,
                visibility: "public" as const,
              },
            ],
            notes: "",
            performanceDateText: "1975",
            venueText: "",
            albumTitle: "",
            label: "",
            releaseDate: "",
            infoPanel: { text: "", articleId: "", collectionLinks: [] },
          },
        },
      ],
    };

    const cloned = cloneBatchDraftEntities(source);
    cloned.recordings[0].entity.links[0].title = "Edited";
    cloned.recordings[0].notes.push("links=1");

    expect(source.recordings[0].entity.links[0].title).toBe("");
    expect(source.recordings[0].notes).toEqual(["workTypeHint=orchestral"]);
  });

  it("requires selecting composer and work before analyzing", async () => {
    await expect(
      analyzeBatchImport({
        sourceText: "Kleiber | Wiener Philharmoniker | 1975 | -",
        library: baseLibrary(),
        workTypeHint: "orchestral",
      }),
    ).rejects.toThrow("批量导入前必须先选定作曲家和作品");
  });

  it("normalizes loose text input into the strict template before analysis", () => {
    const normalized = normalizeBatchImportSource(
      " Kleiber｜ Wiener Philharmoniker ｜ 1975 \r\n\n Karajan|Berlin Philharmonic Orchestra|1963 ",
      "orchestral",
    );

    expect(normalized).toBe(
      "Kleiber | Wiener Philharmoniker | 1975 | -\nKarajan | Berlin Philharmonic Orchestra | 1963 | -",
    );
  });

  it("creates recording drafts and linked coarse people for the orchestral template", async () => {
    const result = await analyzeBatchImport({
      sourceText: "Kleiber | Wiener Philharmoniker | 1975\nKarajan | Berliner Philharmoniker | 1963 | https://example.com",
      library: baseLibrary(),
      composerId: "composer-beethoven",
      workId: "work-beethoven-5",
      workTypeHint: "orchestral",
    });

    expect(result.selectedComposerId).toBe("composer-beethoven");
    expect(result.selectedWorkId).toBe("work-beethoven-5");
    expect(result.workTypeHint).toBe("orchestral");
    expect(result.draftEntities.composers).toHaveLength(0);
    expect(result.draftEntities.people).toHaveLength(4);
    expect(result.draftEntities.works).toHaveLength(0);
    expect(result.draftEntities.recordings).toHaveLength(2);
    expect(result.draftEntities.recordings[0]?.entity.credits.map((item) => item.role)).toEqual(["conductor", "orchestra"]);
    expect(result.draftEntities.recordings[0]?.entity.credits.every((item) => item.personId)).toBe(true);
    expect(result.draftEntities.people.map((entry) => entry.entity.name)).toEqual([
      "Kleiber",
      "Wiener Philharmoniker",
      "Karajan",
      "Berliner Philharmoniker",
    ]);
    expect(result.createdEntityRefs.people).toHaveLength(4);
    expect(result.draftEntities.recordings[1]?.entity.links[0]?.url).toBe("https://example.com");
  });

  it("normalizes conductor and orchestra slots through the reference registry before drafting", async () => {
    const result = await analyzeBatchImport({
      sourceText: "Kletzki | VPO | 1975 | -",
      library: baseLibrary(),
      composerId: "composer-beethoven",
      workId: "work-beethoven-5",
      workTypeHint: "orchestral",
      referenceRegistry: buildReferenceRegistry({
        orchestraSourceText: "VPO = 维也纳爱乐乐团 = 维也纳爱乐 = Wiener Philharmoniker",
        personSourceText: `
#conductor
克莱茨基 = Kletzki = Paul Kletzki
`,
      }),
    });

    expect(result.draftEntities.recordings[0]?.entity.title).toBe("克莱茨基 - 维也纳爱乐乐团 - 1975");
    expect(result.draftEntities.recordings[0]?.entity.credits).toEqual([
      { role: "conductor", displayName: "克莱茨基", personId: "person-conductor-克莱茨基", label: "" },
      { role: "orchestra", displayName: "维也纳爱乐乐团", personId: "person-orchestra-维也纳爱乐乐团", label: "" },
    ]);
  });

  it("supports concerto template slots and explicit missing values", async () => {
    const result = await analyzeBatchImport({
      sourceText: "Pollini | Kleiber | Wiener Philharmoniker | 1975 | -",
      library: baseLibrary(),
      composerId: "composer-beethoven",
      workId: "work-beethoven-5",
      workTypeHint: "concerto",
    });

    const recording = result.draftEntities.recordings[0]?.entity;
    expect(recording?.credits.map((item) => item.role)).toEqual(["soloist", "conductor", "orchestra"]);
    expect(recording?.performanceDateText).toBe("1975");
    expect(recording?.links).toEqual([]);
  });

  it("reuses existing linked people instead of creating duplicate coarse entries", async () => {
    const library = validateLibrary({
      ...baseLibrary(),
      people: [
        {
          id: "person-kleiber",
          slug: "kleiber",
          name: "Kleiber",
          fullName: "",
          nameLatin: "Carlos Kleiber",
          displayName: "Kleiber",
          displayFullName: "",
          displayLatinName: "Carlos Kleiber",
          country: "Austria",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: ["Carlos Kleiber"],
          abbreviations: [],
          sortKey: "0010",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
      ],
    });

    const result = await analyzeBatchImport({
      sourceText: "Kleiber | Wiener Philharmoniker | 1975 | -",
      library,
      composerId: "composer-beethoven",
      workId: "work-beethoven-5",
      workTypeHint: "orchestral",
    });

    expect(result.draftEntities.people).toHaveLength(1);
    expect(result.draftEntities.people[0]?.entity.name).toBe("Wiener Philharmoniker");
    expect(result.draftEntities.recordings[0]?.entity.credits).toEqual([
      { role: "conductor", displayName: "Kleiber", personId: "person-kleiber", label: "" },
      expect.objectContaining({
        role: "orchestra",
        displayName: "Wiener Philharmoniker",
      }),
    ]);
  });

  it("rejects malformed template rows", async () => {
    await expect(
      analyzeBatchImport({
        sourceText: "Kleiber | 1975",
        library: baseLibrary(),
        composerId: "composer-beethoven",
        workId: "work-beethoven-5",
        workTypeHint: "orchestral",
      }),
    ).rejects.toThrow("模板不合法");
  });

  it("builds a confirmed-only selection including linked people referenced by confirmed recordings", async () => {
    const result = await analyzeBatchImport({
      sourceText: "Kleiber | Wiener Philharmoniker | 1975 | -\nKarajan | Berliner Philharmoniker | 1963 | -",
      library: baseLibrary(),
      composerId: "composer-beethoven",
      workId: "work-beethoven-5",
      workTypeHint: "orchestral",
    });

    const draftEntities = structuredClone(result.draftEntities);
    draftEntities.recordings[0].reviewState = "confirmed";
    draftEntities.recordings[1].reviewState = "discarded";

    const selection = buildConfirmedBatchSelection(baseLibrary(), result.draftLibrary, draftEntities);

    expect(selection.createdEntityRefs.composers).toHaveLength(0);
    expect(selection.createdEntityRefs.works).toHaveLength(0);
    expect(selection.createdEntityRefs.people).toHaveLength(2);
    expect(selection.createdEntityRefs.recordings).toHaveLength(1);
    expect(selection.draftLibrary.people).toHaveLength(2);
    expect(selection.draftLibrary.recordings).toHaveLength(1);
    expect(selection.draftLibrary.recordings[0]?.workId).toBe("work-beethoven-5");
  });
});
