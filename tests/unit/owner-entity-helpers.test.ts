import { describe, expect, it } from "vitest";

import { validateLibrary, type LibraryData } from "@/lib/schema";
import {
  assertOwnerEntityCanDelete,
  buildOwnerEntity,
  collectOwnerEntityRelations,
  normalizeOwnerManagedLibrary,
  removeOwnerEntity,
} from "../../packages/data-core/src/owner-entity-helpers.js";

describe("owner entity helpers", () => {
  it("prunes orphaned and duplicate work groups while rewiring works to the canonical group", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-mahler",
          slug: "gustav-mahler",
          name: "古斯塔夫·马勒",
          nameLatin: "Gustav Mahler",
          roles: ["composer", "conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "作曲家与指挥。",
        },
      ],
      workGroups: [
        {
          id: "group-symphony-primary",
          composerId: "person-mahler",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0010",
        },
        {
          id: "group-symphony-duplicate",
          composerId: "person-mahler",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0011",
        },
        {
          id: "group-orphan",
          composerId: "person-mahler",
          title: "Song Cycle",
          slug: "song-cycle",
          path: ["Song Cycle"],
          sortKey: "0012",
        },
      ],
      works: [
        {
          id: "work-mahler-5",
          composerId: "person-mahler",
          groupIds: ["group-symphony-duplicate"],
          slug: "symphony-no-5",
          title: "第五交响曲",
          titleLatin: "Symphony No. 5",
          aliases: [],
          catalogue: "",
          summary: "测试作品。",
          sortKey: "0100",
          updatedAt: "2026-04-20T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const normalized = normalizeOwnerManagedLibrary(library);

    expect(normalized.works[0]?.groupIds).toEqual(["group-symphony-primary"]);
    expect(normalized.workGroups.map((group) => group.id)).toEqual(["group-symphony-primary"]);
  });

  it("builds recordings without requiring a global compact helper and auto-generates slug/sortKey", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-mahler",
          slug: "gustav-mahler",
          name: "古斯塔夫·马勒",
          nameLatin: "Gustav Mahler",
          roles: ["composer", "conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "作曲家与指挥。",
        },
        {
          id: "orchestra-vpo",
          slug: "wiener-philharmoniker",
          name: "维也纳爱乐乐团",
          nameLatin: "Wiener Philharmoniker",
          roles: ["orchestra"],
          aliases: [],
          sortKey: "0011",
          summary: "测试乐团。",
        },
      ],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "person-mahler",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-mahler-5",
          composerId: "person-mahler",
          groupIds: ["group-symphony"],
          slug: "symphony-no-5",
          title: "第五交响曲",
          titleLatin: "Symphony No. 5",
          aliases: [],
          catalogue: "",
          summary: "测试作品。",
          sortKey: "0100",
          updatedAt: "2026-04-20T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const recording = buildOwnerEntity(library, "recording", {
      workId: "work-mahler-5",
      workTypeHint: "orchestral",
      conductorPersonId: "person-mahler",
      orchestraPersonId: "orchestra-vpo",
      title: "",
      slug: "",
      sortKey: "",
      performanceDateText: "1987",
      venueText: "Vienna",
      albumTitle: "",
      label: "",
      releaseDate: "",
      notes: "",
      images: [],
      credits: [],
      links: [],
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["recordings"][number];

    expect(recording.slug).not.toBe("");
    expect(recording.sortKey).toBe("0010");
    expect(recording.credits).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ role: "conductor", personId: "person-mahler" }),
        expect.objectContaining({ role: "orchestra", personId: "orchestra-vpo" }),
      ]),
    );
  });

  it("assigns distinct recording ids for multiple versions under the same work", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-mahler",
          slug: "gustav-mahler",
          name: "Gustav Mahler",
          nameLatin: "Gustav Mahler",
          roles: ["composer", "conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
        {
          id: "orchestra-vpo",
          slug: "wiener-philharmoniker",
          name: "Wiener Philharmoniker",
          nameLatin: "Wiener Philharmoniker",
          roles: ["orchestra"],
          aliases: [],
          sortKey: "0011",
          summary: "",
        },
      ],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "person-mahler",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-mahler-5",
          composerId: "person-mahler",
          groupIds: ["group-symphony"],
          slug: "symphony-no-5",
          title: "Symphony No. 5",
          titleLatin: "Symphony No. 5",
          aliases: [],
          catalogue: "",
          summary: "",
          sortKey: "0100",
          updatedAt: "2026-04-20T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const first = buildOwnerEntity(library, "recording", {
      workId: "work-mahler-5",
      workTypeHint: "orchestral",
      conductorPersonId: "person-mahler",
      orchestraPersonId: "orchestra-vpo",
      performanceDateText: "1987",
      venueText: "Vienna",
      title: "",
      slug: "",
      sortKey: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
      notes: "",
      images: [],
      credits: [],
      links: [],
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["recordings"][number];
    library.recordings.push(first);

    const second = buildOwnerEntity(library, "recording", {
      workId: "work-mahler-5",
      workTypeHint: "orchestral",
      conductorPersonId: "person-mahler",
      orchestraPersonId: "orchestra-vpo",
      performanceDateText: "1988",
      venueText: "Berlin",
      title: "",
      slug: "",
      sortKey: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
      notes: "",
      images: [],
      credits: [],
      links: [],
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["recordings"][number];

    expect(first.id).not.toBe(second.id);
    expect(second.id).toMatch(/^recording-/);
  });

  it("infers a work group path when the payload omits internal classification fields", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-beethoven",
          slug: "ludwig-van-beethoven",
          name: "路德维希·范·贝多芬",
          nameLatin: "Ludwig van Beethoven",
          roles: ["composer"],
          aliases: [],
          sortKey: "0010",
          summary: "",
          country: "德国",
          countries: ["德国"],
          avatarSrc: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          birthYear: 1770,
          deathYear: 1827,
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const work = buildOwnerEntity(library, "work", {
      composerId: "person-beethoven",
      title: "第五交响曲",
      titleLatin: "Symphony No. 5 in C minor",
      catalogue: "Op. 67",
      groupPath: [],
      slug: "",
      sortKey: "",
      aliases: [],
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["works"][number];

    expect(work.groupIds).toHaveLength(1);
    expect(library.workGroups).toHaveLength(1);
    expect(library.workGroups[0]?.path).toEqual(["交响曲"]);
    expect(work.slug).toBe("第五交响曲-symphony-no-5-in-c-minor-op-67");
  });

  it("tracks multi-role person relations and allows deletion after linked works and recordings are removed", () => {
    const baseLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-mahler",
          slug: "gustav-mahler",
          name: "Gustav Mahler",
          nameLatin: "Gustav Mahler",
          roles: ["composer", "conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "",
          country: "Austria",
          countries: ["Austria"],
          avatarSrc: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          birthYear: 1860,
          deathYear: 1911,
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
        },
        {
          id: "orchestra-vpo",
          slug: "wiener-philharmoniker",
          name: "Wiener Philharmoniker",
          nameLatin: "Wiener Philharmoniker",
          roles: ["orchestra"],
          aliases: [],
          sortKey: "0011",
          summary: "",
          country: "Austria",
          countries: ["Austria"],
          avatarSrc: "",
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const workLibrary = structuredClone(baseLibrary);
    const work = buildOwnerEntity(workLibrary, "work", {
      composerId: "person-mahler",
      title: "Symphony No. 5",
      titleLatin: "Symphony No. 5",
      catalogue: "",
      groupPath: ["Symphony"],
      slug: "",
      sortKey: "",
      aliases: [],
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["works"][number];
    workLibrary.works.push(work);

    const recording = buildOwnerEntity(workLibrary, "recording", {
      workId: work.id,
      workTypeHint: "orchestral",
      conductorPersonId: "person-mahler",
      orchestraPersonId: "orchestra-vpo",
      title: "",
      slug: "",
      sortKey: "",
      performanceDateText: "1987",
      venueText: "Vienna",
      albumTitle: "",
      label: "",
      releaseDate: "",
      notes: "",
      images: [],
      credits: [],
      links: [],
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    }) as LibraryData["recordings"][number];
    workLibrary.recordings.push(recording);

    const normalized = validateLibrary(normalizeOwnerManagedLibrary(workLibrary));
    expect(collectOwnerEntityRelations(normalized, "person", "person-mahler")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ entityType: "work", id: work.id }),
        expect.objectContaining({ entityType: "recording", id: recording.id }),
      ]),
    );
    expect(() => assertOwnerEntityCanDelete(normalized, "person", "person-mahler")).toThrow(/无法删除/);

    const withoutRecording = removeOwnerEntity(normalized, "recording", recording.id);
    const withoutWork = removeOwnerEntity(withoutRecording, "work", work.id);

    expect(withoutWork.workGroups).toEqual([]);
    expect(collectOwnerEntityRelations(withoutWork, "person", "person-mahler")).toEqual([]);
    expect(() => assertOwnerEntityCanDelete(withoutWork, "person", "person-mahler")).not.toThrow();

    const withoutPerson = removeOwnerEntity(withoutWork, "person", "person-mahler");
    expect(withoutPerson.people.find((item) => item.id === "person-mahler")).toBeUndefined();
    expect(withoutPerson.composers.find((item) => item.id === "person-mahler")).toBeUndefined();
  });
});
