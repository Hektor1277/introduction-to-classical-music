import { describe, expect, it } from "vitest";

import { inspectRecordingEnhanced } from "@/lib/recording-auto-check";
import { validateLibrary } from "@/lib/schema";

const googleHtml = `
  <html>
    <body>
      <a href="/url?q=https%3A%2F%2Fwww.example-classical.com%2Frecording%2Fkleiber-1963&sa=U">result</a>
    </body>
  </html>
`;

const pageHtml = `
  <html>
    <head>
      <title>Kleiber Beethoven 7 1963</title>
      <meta property="og:title" content="Kleiber Beethoven 7 1963" />
      <meta property="og:description" content="Label: DG | Venue: Vienna | 1963-01-01" />
      <meta property="og:image" content="https://img.example-classical.com/kleiber-1963.jpg" />
    </head>
    <body></body>
  </html>
`;

const wrongVersionPageHtml = `
  <html>
    <head>
      <title>Bernstein Mahler 5 1977</title>
      <meta property="og:title" content="Bernstein Mahler 5 1977" />
      <meta property="og:description" content="Leonard Bernstein, Vienna Philharmonic, 1977 live recording" />
    </head>
    <body></body>
  </html>
`;

function createLibrary(existingLinks: Array<{ platform: string; url: string; title?: string }> = []) {
  return validateLibrary({
    composers: [
      {
        id: "beethoven",
        slug: "beethoven",
        name: "\u8d1d\u591a\u82ac",
        fullName: "",
        nameLatin: "Ludwig van Beethoven",
        displayName: "\u8d1d\u591a\u82ac",
        displayFullName: "",
        displayLatinName: "Ludwig van Beethoven",
        country: "Germany",
        avatarSrc: "",
        aliases: [],
        abbreviations: [],
        sortKey: "0010",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionUrl: "" },
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
      },
    ],
    people: [
      {
        id: "kleiber",
        slug: "kleiber",
        name: "\u514b\u83b1\u4f2f",
        fullName: "",
        nameLatin: "Carlos Kleiber",
        displayName: "\u514b\u83b1\u4f2f",
        displayFullName: "",
        displayLatinName: "Carlos Kleiber",
        country: "Germany",
        avatarSrc: "",
        roles: ["conductor"],
        aliases: [],
        abbreviations: [],
        sortKey: "0010",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionUrl: "" },
        imageSourceUrl: "",
        imageSourceKind: "",
        imageAttribution: "",
        imageUpdatedAt: "",
      },
    ],
    workGroups: [
      {
        id: "group-symphony",
        composerId: "beethoven",
        title: "\u4ea4\u54cd\u66f2",
        slug: "symphony",
        path: ["\u4ea4\u54cd\u66f2"],
        sortKey: "0010",
      },
    ],
    works: [
      {
        id: "beethoven-7",
        composerId: "beethoven",
        groupIds: ["group-symphony"],
        slug: "beethoven-7",
        title: "\u7b2c\u4e03\u4ea4\u54cd\u66f2",
        titleLatin: "",
        aliases: [],
        catalogue: "",
        summary: "",
        infoPanel: { text: "", articleId: "", collectionUrl: "" },
        sortKey: "0010",
        updatedAt: "2026-03-13T00:00:00.000Z",
      },
    ],
    recordings: [
      {
        id: "recording-kleiber-1963",
        workId: "beethoven-7",
        slug: "recording-kleiber-1963",
        title: "\u514b\u83b1\u4f2f 1963",
        sortKey: "0010",
        isPrimaryRecommendation: false,
        updatedAt: "2026-03-13T00:00:00.000Z",
        images: [],
        credits: [{ role: "conductor", personId: "kleiber", displayName: "\u514b\u83b1\u4f2f" }],
        links: existingLinks,
        notes: "",
        performanceDateText: "1963",
        venueText: "",
        albumTitle: "",
        label: "",
        releaseDate: "",
        infoPanel: { text: "", articleId: "", collectionUrl: "" },
      },
    ],
  });
}

describe("recording auto check", () => {
  it("builds one aggregated proposal when no links exist", async () => {
    const library = createLibrary();
    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("google.com/search")) {
        return new Response(googleHtml, { status: 200 });
      }
      if (url.includes("baidu.com/s")) {
        return new Response("<html></html>", { status: 200 });
      }
      if (url.includes("youtube.com/results") || url.includes("bilibili.com/all") || url.includes("music.apple.com")) {
        return new Response("<html></html>", { status: 200 });
      }
      if (url.includes("example-classical.com/recording")) {
        return new Response(pageHtml, { status: 200 });
      }
      return new Response("<html></html>", { status: 200 });
    };

    const proposals = await inspectRecordingEnhanced(library.recordings[0], library, fetchImpl);
    expect(proposals).toHaveLength(1);
    const proposal = proposals[0];

    expect(proposal?.fields.find((field) => field.path === "links")?.after).toEqual([
      expect.objectContaining({
        platform: "other",
        url: "https://www.example-classical.com/recording/kleiber-1963",
      }),
    ]);
    expect(proposal?.fields.some((field) => field.path === "label")).toBe(true);
    expect(proposal?.fields.some((field) => field.path === "releaseDate")).toBe(true);
    expect(proposal?.linkCandidates?.map((item) => item.url)).toContain("https://www.example-classical.com/recording/kleiber-1963");
    expect(proposal?.evidence?.some((item) => item.field === "links")).toBe(true);
  });

  it("keeps existing links and appends new candidates instead of replacing them", async () => {
    const library = createLibrary([{ platform: "youtube", url: "https://www.youtube.com/watch?v=old", title: "Kleiber Beethoven 7 1963" }]);
    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("google.com/search")) {
        return new Response(
          `<html><body><a href="/url?q=${encodeURIComponent("https://www.bilibili.com/video/BV1xx411c7P3")}">bili</a></body></html>`,
          { status: 200 },
        );
      }
      if (url.includes("baidu.com/s") || url.includes("youtube.com/results") || url.includes("music.apple.com")) {
        return new Response("<html></html>", { status: 200 });
      }
      if (url.includes("bilibili.com/all")) {
        return new Response("https://www.bilibili.com/video/BV1xx411c7P3", { status: 200 });
      }
      if (url.includes("bilibili.com/video/BV1xx411c7P3")) {
        return new Response(pageHtml, { status: 200 });
      }
      return new Response("<html></html>", { status: 200 });
    };

    const proposals = await inspectRecordingEnhanced(library.recordings[0], library, fetchImpl);
    expect(proposals).toHaveLength(1);
    const links = proposals[0]?.fields.find((field) => field.path === "links")?.after as Array<{
      platform: string;
      url: string;
      title?: string;
    }>;

    expect(links.map((item) => item.url)).toContain("https://www.youtube.com/watch?v=old");
    expect(links.map((item) => item.url)).toContain("https://www.bilibili.com/video/BV1xx411c7P3");
    expect(proposals[0]?.linkCandidates?.length).toBeGreaterThan(0);
  });

  it("filters out mismatched existing links when fetched metadata points to another version", async () => {
    const library = createLibrary([{ platform: "youtube", url: "https://www.youtube.com/watch?v=wrong", title: "" }]);
    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("google.com/search") || url.includes("baidu.com/s") || url.includes("music.apple.com")) {
        return new Response("<html></html>", { status: 200 });
      }
      if (url.includes("youtube.com/results")) {
        return new Response("<html></html>", { status: 200 });
      }
      if (url.includes("youtube.com/watch?v=wrong")) {
        return new Response(wrongVersionPageHtml, { status: 200 });
      }
      return new Response("<html></html>", { status: 200 });
    };

    const proposals = await inspectRecordingEnhanced(library.recordings[0], library, fetchImpl);
    const links = proposals[0]?.fields.find((field) => field.path === "links")?.after as Array<{
      platform: string;
      url: string;
      title?: string;
    }>;

    expect(links?.map((item) => item.url) || []).not.toContain("https://www.youtube.com/watch?v=wrong");
  });
});
