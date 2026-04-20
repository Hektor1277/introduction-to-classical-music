import { describe, expect, it } from "vitest";

import { sanitizeProposalPatchMap } from "../../apps/owner/server/proposal-patch-utils";

describe("proposal patch utils", () => {
  it("coerces life-range numeric fields into numbers before proposal edits are persisted", () => {
    expect(
      sanitizeProposalPatchMap("person", {
        birthYear: "1860",
        deathYear: "1918",
        aliases: ["BPO"],
      }),
    ).toEqual({
      birthYear: 1860,
      deathYear: 1918,
      aliases: ["BPO"],
    });
  });

  it("keeps blank numeric fields undefined instead of invalid strings", () => {
    expect(
      sanitizeProposalPatchMap("composer", {
        birthYear: "",
        deathYear: "  ",
      }),
    ).toEqual({
      birthYear: undefined,
      deathYear: undefined,
    });
  });
});
