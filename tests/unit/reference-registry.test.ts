import { describe, expect, it } from "vitest";

import {
  auditReferenceRegistry,
  buildReferenceRegistry,
  consolidateOrchestraReferenceEntries,
  findUniqueOrchestraReferenceMergeTarget,
  lookupOrchestraReference,
  lookupPersonReference,
  parseOrchestraReferenceText,
  parsePersonAliasReferenceText,
} from "@/lib/reference-registry";

describe("reference registry", () => {
  it("parses orchestra entries with abbreviations, aliases, and Chinese translations", () => {
    const entries = parseOrchestraReferenceText(`
# comment
VPO = 维也纳爱乐乐团 = 维也纳爱乐 = Wiener Philharmoniker = Vienna Philharmonic Orchestra
布达佩斯爱乐乐团 = Budapest Philharmonic Orchestra = Budapest Philharmonic
`);

    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({
      preferredValue: "维也纳爱乐乐团",
      canonicalLatin: "Wiener Philharmoniker",
      abbreviations: ["VPO"],
      chineseValues: ["维也纳爱乐乐团", "维也纳爱乐"],
    });
    expect(entries[1]).toMatchObject({
      preferredValue: "布达佩斯爱乐乐团",
      canonicalLatin: "Budapest Philharmonic Orchestra",
      abbreviations: [],
    });
  });

  it("parses person alias sections and keeps role scope", () => {
    const entries = parsePersonAliasReferenceText(`
#global
巴伦博伊姆 = Daniel Barenboim

#conductor
克莱茨基 = Kletzki = Paul Kletzki
`);

    expect(entries).toEqual([
      {
        role: "global",
        preferredValue: "巴伦博伊姆",
        canonicalLatin: "Daniel Barenboim",
        values: ["巴伦博伊姆", "Daniel Barenboim"],
        chineseValues: ["巴伦博伊姆"],
        latinValues: ["Daniel Barenboim"],
      },
      {
        role: "conductor",
        preferredValue: "克莱茨基",
        canonicalLatin: "Paul Kletzki",
        values: ["克莱茨基", "Kletzki", "Paul Kletzki"],
        chineseValues: ["克莱茨基"],
        latinValues: ["Kletzki", "Paul Kletzki"],
      },
    ]);
  });

  it("looks up orchestra aliases across abbreviations, Latin aliases, and Chinese translations", () => {
    const registry = buildReferenceRegistry({
      orchestraSourceText: "VPO = 维也纳爱乐乐团 = 维也纳爱乐 = Wiener Philharmoniker = Vienna Philharmonic Orchestra",
    });

    expect(lookupOrchestraReference(registry, "vpo")?.preferredValue).toBe("维也纳爱乐乐团");
    expect(lookupOrchestraReference(registry, "Vienna Philharmonic Orchestra")?.canonicalLatin).toBe("Wiener Philharmoniker");
    expect(lookupOrchestraReference(registry, "维也纳爱乐")?.preferredValue).toBe("维也纳爱乐乐团");
  });

  it("prefers role-scoped person aliases before global aliases", () => {
    const registry = buildReferenceRegistry({
      personSourceText: `
#global
费舍尔 = Annie Fischer

#soloist
安妮·费舍尔 = Annie Fischer
`,
    });

    expect(lookupPersonReference(registry, "Annie Fischer", "soloist")?.preferredValue).toBe("安妮·费舍尔");
    expect(lookupPersonReference(registry, "Annie Fischer", "conductor")?.preferredValue).toBe("费舍尔");
  });

  it("normalizes harmless punctuation and spacing when querying", () => {
    const registry = buildReferenceRegistry({
      orchestraSourceText: "RCO = 皇家音乐厅管弦乐团 = Concertgebouworkest = Royal Concertgebouw Orchestra",
      personSourceText: `
#conductor
巴伦博伊姆 = Daniel Barenboim
`,
    });

    expect(lookupOrchestraReference(registry, "Royal  Concertgebouw-Orchestra")?.preferredValue).toBe("皇家音乐厅管弦乐团");
    expect(lookupPersonReference(registry, " Daniel  Barenboim ", "conductor")?.preferredValue).toBe("巴伦博伊姆");
  });

  it("does not auto-resolve ambiguous orchestra aliases", () => {
    const registry = buildReferenceRegistry({
      orchestraSourceText: `
LSO = 伦敦交响乐团 = London Symphony Orchestra
LSO = 拉赫蒂交响乐团 = Lahti Symphony Orchestra
`,
    });

    expect(lookupOrchestraReference(registry, "LSO")).toBeNull();
  });

  it("consolidates duplicate orchestra entries when they share a strong identity", () => {
    const entries = parseOrchestraReferenceText(`
RCO = 皇家音乐厅管弦乐团 = Concertgebouworkest = Royal Concertgebouw Orchestra
The Royal Concertgebouw Orchestra = 皇家音乐厅管弦乐团 = RCO
BPO = 柏林爱乐乐团 = Berliner Philharmoniker
BPO = 布达佩斯爱乐乐团 = Budapest Philharmonic Orchestra
`);

    const consolidated = consolidateOrchestraReferenceEntries(entries);

    expect(consolidated).toHaveLength(3);
    expect(consolidated.find((entry) => entry.preferredValue === "皇家音乐厅管弦乐团")?.values).toEqual(
      expect.arrayContaining(["RCO", "Concertgebouworkest", "Royal Concertgebouw Orchestra", "The Royal Concertgebouw Orchestra"]),
    );
  });

  it("refuses to pick a merge target when one dirty legacy orchestra line overlaps multiple canonical entries", () => {
    const [dirtyEntry] = parseOrchestraReferenceText(`
BPO = 柏林爱乐乐团 = Berliner Philharmoniker = 布达佩斯爱乐乐团 = Budapest Philharmonic Orchestra
`);
    const canonicalEntries = parseOrchestraReferenceText(`
BPO = 柏林爱乐乐团 = Berliner Philharmoniker
BpPO = 布达佩斯爱乐乐团 = Budapest Philharmonic Orchestra
`);

    expect(findUniqueOrchestraReferenceMergeTarget(dirtyEntry, canonicalEntries)).toBeNull();
  });

  it("audits ambiguous orchestra abbreviations and duplicate identities", () => {
    const registry = buildReferenceRegistry({
      orchestraSourceText: `
BPO = 柏林爱乐乐团 = Berliner Philharmoniker
BPO = 布达佩斯爱乐乐团 = Budapest Philharmonic Orchestra
RCO = 皇家音乐厅管弦乐团 = Concertgebouworkest
皇家音乐厅管弦乐团 = The Royal Concertgebouw Orchestra = RCO
`,
    });

    expect(auditReferenceRegistry(registry)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "ambiguous_orchestra_abbreviation",
          lookupValue: "BPO",
          preferredValues: expect.arrayContaining(["柏林爱乐乐团", "布达佩斯爱乐乐团"]),
        }),
        expect.objectContaining({
          code: "duplicate_orchestra_identity",
          preferredValues: expect.arrayContaining(["皇家音乐厅管弦乐团"]),
        }),
      ]),
    );
  });
});
