import { describe, expect, it } from "vitest";

import { buildIndexes } from "@/lib/indexes";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "beethoven",
      slug: "beethoven",
      name: "Beethoven",
      nameLatin: "Ludwig van Beethoven",
      aliases: ["Ludwig van Beethoven"],
      sortKey: "0010",
      summary: "German composer.",
    },
  ],
  people: [
    {
      id: "carlos-kleiber",
      slug: "carlos-kleiber",
      name: "Carlos Kleiber",
      nameLatin: "Carlos Kleiber",
      roles: ["conductor"],
      aliases: [],
      sortKey: "0010",
      summary: "Argentine-Austrian conductor.",
    },
    {
      id: "kleiber-alias",
      slug: "kleiber-alias",
      name: "Kleiber",
      nameLatin: "",
      roles: ["conductor"],
      aliases: [],
      sortKey: "0020",
      summary: "",
    },
    {
      id: "vienna-philharmonic",
      slug: "vienna-philharmonic",
      name: "Vienna Philharmonic",
      nameLatin: "",
      roles: ["orchestra"],
      aliases: [],
      sortKey: "0030",
      summary: "",
    },
  ],
  workGroups: [
    {
      id: "beethoven-symphony",
      composerId: "beethoven",
      title: "Symphonies",
      slug: "symphonies",
      path: ["Symphonies"],
      sortKey: "0100",
    },
  ],
  works: [
    {
      id: "beethoven-5",
      composerId: "beethoven",
      groupIds: ["beethoven-symphony"],
      slug: "symphony-5",
      title: "Symphony No. 5",
      titleLatin: "Symphony No. 5 in C minor, Op. 67",
      aliases: [],
      catalogue: "Op. 67",
      summary: "A canonical symphony.",
      sortKey: "0200",
      updatedAt: "2026-03-08T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "kleiber-1975",
      workId: "beethoven-5",
      slug: "kleiber-1975",
      title: "Kleiber 1975",
      sortKey: "0010",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-08T00:00:00.000Z",
      images: [],
      credits: [
        { role: "conductor", personId: "kleiber-alias", displayName: "Kleiber" },
        { role: "orchestra", personId: "vienna-philharmonic", displayName: "Vienna Philharmonic" },
      ],
      links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=12345678901" }],
      notes: "",
      performanceDateText: "1975",
      venueText: "Vienna",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
  ],
});

describe("canonical person links", () => {
  it("merges alias conductors into the canonical conductor index and hides the alias duplicate", () => {
    const indexes = buildIndexes(library, {
      canonicalPersonLinks: {
        "kleiber-alias": "carlos-kleiber",
      },
    });

    expect(indexes.canonicalPeople["kleiber-alias"]).toBe("carlos-kleiber");
    expect(indexes.conductorIndex["kleiber-alias"]).toBeUndefined();
    expect(indexes.conductorIndex["carlos-kleiber"]?.groups[0]?.works[0]?.recordings[0]?.id).toBe("kleiber-1975");
    expect(indexes.searchIndex.some((entry) => entry.id === "kleiber-alias")).toBe(false);
  });

  it("surfaces orchestras as a dedicated search kind", () => {
    const indexes = buildIndexes(library, {
      canonicalPersonLinks: {
        "kleiber-alias": "carlos-kleiber",
      },
    });

    expect(indexes.searchIndex.some((entry) => entry.kind === "orchestra" && entry.id === "vienna-philharmonic")).toBe(true);
  });
});
