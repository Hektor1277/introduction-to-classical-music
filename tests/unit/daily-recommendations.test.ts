import { describe, expect, it } from "vitest";

import { pickDailyRecommendations } from "@/lib/recommendations";

const recordings = [
  { id: "r-1", updatedAt: "2026-03-01T00:00:00.000Z", title: "A" },
  { id: "r-2", updatedAt: "2026-03-02T00:00:00.000Z", title: "B" },
  { id: "r-3", updatedAt: "2026-03-03T00:00:00.000Z", title: "C" },
  { id: "r-4", updatedAt: "2026-03-04T00:00:00.000Z", title: "D" },
  { id: "r-5", updatedAt: "2026-03-05T00:00:00.000Z", title: "E" },
];

describe("pickDailyRecommendations", () => {
  it("returns stable picks for the same day", () => {
    const first = pickDailyRecommendations(recordings, "2026-03-07", 3);
    const second = pickDailyRecommendations(recordings, "2026-03-07", 3);

    expect(first).toEqual(second);
  });

  it("rotates picks when the day changes", () => {
    const first = pickDailyRecommendations(recordings, "2026-03-07", 3);
    const second = pickDailyRecommendations(recordings, "2026-03-08", 3);

    expect(first).not.toEqual(second);
  });

  it("never returns duplicates", () => {
    const picks = pickDailyRecommendations(recordings, "2026-03-07", 5);
    const ids = picks.map((item) => item.id);

    expect(new Set(ids).size).toBe(ids.length);
  });
});
