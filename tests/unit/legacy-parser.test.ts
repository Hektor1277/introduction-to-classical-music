import { describe, expect, it } from "vitest";

import { parseLegacyRecordingHtml } from "../../packages/data-core/src/legacy-parser.js";

describe("legacy parser", () => {
  it("parses Chinese legacy recording labels without collapsing orchestra names to placeholders", () => {
    const parsed = parseLegacyRecordingHtml(`
      <html>
        <body>
          <p>乐团：Bayreuth Festival Orchestra &amp; Chorus</p>
          <p>指挥：威尔海姆·富特文格勒</p>
          <p>时间、地点：29 July 1951, at Festspielhaus, in Bayreuth</p>
          <p>专辑：Bayreuth 1951</p>
          <p>厂牌：Orfeo</p>
          <p>发行日期：1952</p>
        </body>
      </html>
    `);

    expect(parsed.credits).toEqual([
      {
        role: "orchestra",
        personId: "",
        displayName: "Bayreuth Festival Orchestra & Chorus",
        label: "乐团",
      },
      {
        role: "conductor",
        personId: "",
        displayName: "威尔海姆·富特文格勒",
        label: "指挥",
      },
    ]);
    expect(parsed.performanceDateText).toBe("29 July 1951, at Festspielhaus");
    expect(parsed.venueText).toBe("in Bayreuth");
    expect(parsed.albumTitle).toBe("Bayreuth 1951");
    expect(parsed.label).toBe("Orfeo");
    expect(parsed.releaseDate).toBe("1952");
  });

  it("parses English recording labels for orchestra and conductor credits", () => {
    const parsed = parseLegacyRecordingHtml(`
      <html>
        <body>
          <p>Orchestra: Sächsische Staatskapelle Dresden</p>
          <p>Conductor: Giuseppe Sinopoli</p>
          <p>Date: 1999</p>
        </body>
      </html>
    `);

    expect(parsed.credits).toEqual([
      {
        role: "orchestra",
        personId: "",
        displayName: "Sächsische Staatskapelle Dresden",
        label: "Orchestra",
      },
      {
        role: "conductor",
        personId: "",
        displayName: "Giuseppe Sinopoli",
        label: "Conductor",
      },
    ]);
  });

  it("does not treat work titles with orchestra words as credit labels", () => {
    const parsed = parseLegacyRecordingHtml(`
      <html>
        <body>
          <p>Concerto for violin and orchestra in D Major, Op. 61: II. Rondo allegro</p>
        </body>
      </html>
    `);

    expect(parsed.credits).toEqual([]);
  });

  it("skips placeholder ensemble values instead of emitting unusable credits", () => {
    const parsed = parseLegacyRecordingHtml(`
      <html>
        <body>
          <p>乐团：-</p>
          <p>乐团：未知</p>
          <p>指挥：康科迪・杰拉伯特</p>
          <p>时间、地点：1916</p>
        </body>
      </html>
    `);

    expect(parsed.credits).toEqual([
      {
        role: "conductor",
        personId: "",
        displayName: "康科迪・杰拉伯特",
        label: "指挥",
      },
    ]);
  });

  it("parses styled composite ensemble abbreviations without dropping the raw text", () => {
    const parsed = parseLegacyRecordingHtml(`
      <html>
        <body>
          <p><font size=3>乐团：<span><font color=#000000 face=Arial> HPO &amp; RO</font></span></font></p>
          <p><font size=3>指挥：Georg Schnéevoigt（乔治·施内沃伊特）</font></p>
          <p><font size=3>时间、地点：1945.12.8</font></p>
        </body>
      </html>
    `);

    expect(parsed.credits).toEqual([
      {
        role: "orchestra",
        personId: "",
        displayName: "HPO & RO",
        label: "乐团",
      },
      {
        role: "conductor",
        personId: "",
        displayName: "Georg Schnéevoigt（乔治·施内沃伊特）",
        label: "指挥",
      },
    ]);
    expect(parsed.performanceDateText).toBe("1945.12.8");
  });
});
