import { describe, expect, it } from "vitest";

import { applyManualRecordingBackfills } from "../../packages/data-core/src/manual-recording-backfill.js";
import { validateLibrary, type LibraryData } from "../../packages/shared/src/schema.js";

function createLibrary(overrides?: Partial<LibraryData>): LibraryData {
  return validateLibrary({
    composers: [
      {
        id: "composer-beethoven",
        slug: "beethoven",
        name: "贝多芬",
        fullName: "路德维希·凡·贝多芬",
        nameLatin: "Ludwig van Beethoven",
        country: "Germany",
        avatarSrc: "",
        aliases: [],
        sortKey: "0010",
        summary: "",
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        roles: ["composer"],
      },
    ],
    people: [
      {
        id: "person-bernstein",
        slug: "bernstein",
        name: "伯恩斯坦",
        fullName: "伦纳德·伯恩斯坦",
        nameLatin: "Leonard Bernstein",
        country: "United States",
        avatarSrc: "",
        aliases: [],
        sortKey: "0010",
        summary: "",
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        roles: ["conductor"],
      },
    ],
    workGroups: [
      {
        id: "work-group-symphony",
        composerId: "composer-beethoven",
        title: "交响曲",
        slug: "symphony",
        path: ["交响曲"],
        sortKey: "0010",
      },
    ],
    works: [
      {
        id: "work-beethoven-9",
        composerId: "composer-beethoven",
        groupIds: ["work-group-symphony"],
        slug: "symphony-9",
        title: "第九交响曲“合唱”",
        titleLatin: "Symphony No. 9 in D minor, Op. 125",
        aliases: [],
        catalogue: "Op. 125",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        sortKey: "0010",
        updatedAt: "2026-03-23T00:00:00.000Z",
      },
    ],
    recordings: [
      {
        id: "recording-bernstein-1989",
        workId: "work-beethoven-9",
        slug: "bernstein-1989",
        title: "伯恩斯坦",
        workTypeHint: "orchestral",
        sortKey: "0010",
        isPrimaryRecommendation: false,
        updatedAt: "2026-03-23T00:00:00.000Z",
        images: [],
        credits: [{ role: "conductor", personId: "person-bernstein", displayName: "伯恩斯坦", label: "指挥" }],
        links: [],
        notes: "",
        performanceDateText: "1989",
        venueText: "Berlin",
        albumTitle: "",
        label: "",
        releaseDate: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        legacyPath: "legacy/bernstein-1989.htm",
      },
    ],
    ...(overrides || {}),
  });
}

describe("manual recording backfill", () => {
  it("applies curated ensemble and chorus credits to unresolved recordings and rebuilds derived title fields", () => {
    const library = createLibrary();

    const repaired = applyManualRecordingBackfills(library, [
      {
        recordingId: "recording-bernstein-1989",
        credits: [
          { role: "ensemble", displayName: "柏林自由音乐会联合乐团", label: "联合乐团" },
          { role: "chorus", displayName: "柏林自由音乐会联合合唱团", label: "联合合唱团" },
        ],
      },
    ]);

    const recording = repaired.recordings[0];
    expect(repaired.people).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "柏林自由音乐会联合乐团", roles: ["ensemble"] }),
        expect.objectContaining({ name: "柏林自由音乐会联合合唱团", roles: ["chorus"] }),
      ]),
    );
    expect(recording.credits).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ role: "ensemble", displayName: "柏林自由音乐会联合乐团" }),
        expect.objectContaining({ role: "chorus", displayName: "柏林自由音乐会联合合唱团" }),
      ]),
    );
    expect(recording.title).toContain("伯恩斯坦");
    expect(recording.title).toContain("柏林自由音乐会联合乐团");
    expect(recording.title).toContain("1989");
  });

  it("ignores entries whose recording id does not exist", () => {
    const library = createLibrary();

    const repaired = applyManualRecordingBackfills(library, [
      {
        recordingId: "recording-missing",
        credits: [{ role: "orchestra", displayName: "巴塞罗那交响乐团", label: "乐团" }],
      },
    ]);

    expect(repaired).toEqual(library);
  });

  it("can replace a legacy compound credit with structured multiple credits", () => {
    const library = createLibrary({
      people: [
        {
          id: "person-bernstein",
          slug: "bernstein",
          name: "伯恩斯坦",
          fullName: "伦纳德·伯恩斯坦",
          nameLatin: "Leonard Bernstein",
          country: "United States",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          roles: ["conductor"],
        },
        {
          id: "person-hko-ro",
          slug: "hko-and-ro",
          name: "HKO & RO",
          fullName: "HKO & RO",
          nameLatin: "HKO & RO",
          country: "",
          avatarSrc: "",
          aliases: [],
          sortKey: "0020",
          summary: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          roles: ["orchestra"],
        },
      ],
      recordings: [
        {
          id: "recording-bernstein-1989",
          workId: "work-beethoven-9",
          slug: "bernstein-1989",
          title: "伯恩斯坦",
          workTypeHint: "orchestral",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-23T00:00:00.000Z",
          images: [],
          credits: [
            { role: "conductor", personId: "person-bernstein", displayName: "伯恩斯坦", label: "指挥" },
            { role: "orchestra", personId: "person-hko-ro", displayName: "HKO & RO", label: "乐团" },
          ],
          links: [],
          notes: "",
          performanceDateText: "1945.12.8",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "legacy/bernstein-1989.htm",
        },
      ],
    });

    const repaired = applyManualRecordingBackfills(library, [
      {
        recordingId: "recording-bernstein-1989",
        removeCredits: [{ role: "orchestra", displayName: "HKO & RO" }],
        credits: [
          { role: "orchestra", displayName: "赫尔辛基爱乐乐团", label: "乐团" },
          { role: "orchestra", displayName: "芬兰广播乐团", label: "乐团" },
        ],
      },
    ]);

    const recording = repaired.recordings[0];
    expect(recording.credits).not.toEqual(expect.arrayContaining([expect.objectContaining({ displayName: "HKO & RO" })]));
    expect(recording.credits).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ role: "orchestra", displayName: "赫尔辛基爱乐乐团" }),
        expect.objectContaining({ role: "orchestra", displayName: "芬兰广播乐团" }),
      ]),
    );
  });
  it("can remove wrong derived credits and move venue-only legacy text out of performance date", () => {
    const base = createLibrary();
    const library = validateLibrary({
      ...base,
      people: [
        ...base.people,
        {
          id: "person-rudin",
          slug: "rudin",
          name: "亚历山大·鲁丁",
          fullName: "亚历山大·鲁丁",
          nameLatin: "Alexander Rudin",
          country: "Russia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0020",
          summary: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          roles: ["conductor"],
        },
        {
          id: "person-virsaladze",
          slug: "virsaladze",
          name: "埃莉索·维尔萨拉泽",
          fullName: "埃莉索·维尔萨拉泽",
          nameLatin: "Eliso Virsaladze",
          country: "Georgia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0021",
          summary: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          roles: ["soloist"],
        },
        {
          id: "person-rudin-short",
          slug: "rudin-short",
          name: "鲁丁",
          fullName: "鲁丁",
          nameLatin: "Rudin",
          country: "Russia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0022",
          summary: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          roles: ["soloist"],
        },
      ],
      recordings: [
        {
          id: "recording-rudin-virsaladze",
          workId: "work-beethoven-9",
          slug: "rudin-virsaladze",
          title: "亚历山大·鲁丁 - 埃莉索·维尔萨拉泽 - 鲁丁 - 莫斯科音乐学院大音乐厅",
          workTypeHint: "concerto",
          sortKey: "0011",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-23T00:00:00.000Z",
          images: [],
          credits: [
            { role: "conductor", personId: "person-rudin", displayName: "亚历山大·鲁丁", label: "指挥" },
            { role: "soloist", personId: "person-virsaladze", displayName: "埃莉索·维尔萨拉泽", label: "文件名补全" },
            { role: "soloist", personId: "person-rudin-short", displayName: "鲁丁", label: "文件名补全" },
          ],
          links: [],
          notes: "",
          performanceDateText: "莫斯科音乐学院大音乐厅",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "legacy/rudin-virsaladze.htm",
        },
      ],
    });

    const repaired = applyManualRecordingBackfills(library, [
      {
        recordingId: "recording-rudin-virsaladze",
        removeCredits: [{ role: "soloist", personId: "person-rudin-short" }],
        metadata: {
          performanceDateText: "",
          venueText: "莫斯科音乐学院大音乐厅",
        },
      },
    ]);

    const recording = repaired.recordings[0];
    expect(recording.performanceDateText).toBe("");
    expect(recording.venueText).toBe("莫斯科音乐学院大音乐厅");
    expect(recording.credits).toHaveLength(2);
    expect(recording.credits).not.toEqual(expect.arrayContaining([expect.objectContaining({ personId: "person-rudin-short" })]));
    expect(recording.title).toBe("亚历山大·鲁丁 - 埃莉索·维尔萨拉泽 - 莫斯科音乐学院大音乐厅");
  });
});
