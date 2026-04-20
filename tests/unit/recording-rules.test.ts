import { describe, expect, it } from "vitest";

import {
  buildBatchRecordingCredits,
  buildBatchRecordingTitle,
  deriveRecordingPresentationFamily,
  getBatchRecordingTemplateSpec,
  getRecordingWorkTypeHintLabel,
  inferRecordingWorkTypeHintFromTexts,
  inferRecordingWorkTypeHintFromWork,
  normalizeRecordingWorkTypeHintValue,
  recordingWorkTypeHintValues,
  resolveRecordingWorkTypeHintValue,
} from "@/lib/recording-rules";

describe("recording rules", () => {
  it("exports a single shared work type source of truth", () => {
    expect(recordingWorkTypeHintValues).toEqual(["orchestral", "concerto", "opera_vocal", "chamber_solo", "unknown"]);
  });

  it("normalizes unknown work type hints back to unknown", () => {
    expect(normalizeRecordingWorkTypeHintValue("concerto")).toBe("concerto");
    expect(normalizeRecordingWorkTypeHintValue("BALLET")).toBe("unknown");
    expect(normalizeRecordingWorkTypeHintValue("")).toBe("unknown");
  });

  it("derives solo and chamber presentation families from chamber_solo recordings", () => {
    expect(
      deriveRecordingPresentationFamily({
        workTypeHint: "chamber_solo",
        soloistCount: 1,
        singerCount: 0,
        ensembleCount: 0,
      }),
    ).toBe("solo");

    expect(
      deriveRecordingPresentationFamily({
        workTypeHint: "chamber_solo",
        soloistCount: 2,
        singerCount: 0,
        ensembleCount: 0,
      }),
    ).toBe("chamber");
  });

  it("derives concerto and opera families from shared credit context", () => {
    expect(
      deriveRecordingPresentationFamily({
        workTypeHint: "concerto",
        conductorCount: 1,
        orchestraCount: 1,
        soloistCount: 1,
        singerCount: 0,
        ensembleCount: 0,
      }),
    ).toBe("concerto");

    expect(
      deriveRecordingPresentationFamily({
        workTypeHint: "opera_vocal",
        conductorCount: 1,
        orchestraCount: 1,
        singerCount: 2,
        ensembleCount: 1,
      }),
    ).toBe("opera");
  });

  it("shares batch template metadata and title builders across work types", () => {
    expect(getBatchRecordingTemplateSpec("concerto").fieldCount).toBe(5);
    expect(getBatchRecordingTemplateSpec("orchestral").fieldCount).toBe(4);
    expect(buildBatchRecordingTitle("concerto", ["奥伊斯特拉赫", "埃林", "斯德哥尔摩节日管弦乐团", "1954"])).toBe(
      "奥伊斯特拉赫 - 埃林 - 斯德哥尔摩节日管弦乐团 - 1954",
    );
    expect(buildBatchRecordingTitle("chamber_solo", ["阿劳", "波恩", "1970"])).toBe("阿劳 - 波恩 - 1970");
  });

  it("builds canonical draft credits from shared batch rules", () => {
    expect(buildBatchRecordingCredits("orchestral", ["卡拉扬", "柏林爱乐乐团", "1971"])).toEqual([
      { role: "conductor", displayName: "卡拉扬", personId: "", label: "" },
      { role: "orchestra", displayName: "柏林爱乐乐团", personId: "", label: "" },
    ]);

    expect(buildBatchRecordingCredits("concerto", ["奥伊斯特拉赫", "埃林", "斯德哥尔摩节日管弦乐团", "1954"])).toEqual([
      { role: "soloist", displayName: "奥伊斯特拉赫", personId: "", label: "" },
      { role: "conductor", displayName: "埃林", personId: "", label: "" },
      { role: "orchestra", displayName: "斯德哥尔摩节日管弦乐团", personId: "", label: "" },
    ]);
  });

  it("splits multi-party batch slots into structured credits without fabricating chamber ensembles", () => {
    expect(
      buildBatchRecordingCredits("orchestral", [
        "Furtwangler",
        "Bayreuth Festival Orchestra + Bayreuth Festival Chorus",
        "1951",
      ]),
    ).toEqual([
      { role: "conductor", displayName: "Furtwangler", personId: "", label: "" },
      { role: "orchestra", displayName: "Bayreuth Festival Orchestra", personId: "", label: "" },
      { role: "chorus", displayName: "Bayreuth Festival Chorus", personId: "", label: "" },
    ]);

    expect(buildBatchRecordingCredits("chamber_solo", ["Zimmermann + Nowak + Budnik", "Poland", "2025"])).toEqual([
      { role: "soloist", displayName: "Zimmermann", personId: "", label: "" },
      { role: "soloist", displayName: "Nowak", personId: "", label: "" },
      { role: "soloist", displayName: "Budnik", personId: "", label: "" },
    ]);
  });

  it("infers recording work type hints from related work and group text", () => {
    expect(inferRecordingWorkTypeHintFromTexts(["第九交响曲“合唱”", "交响曲"])).toBe("orchestral");
    expect(inferRecordingWorkTypeHintFromTexts(["Piano Concerto, Op. 54", "钢琴协奏曲"])).toBe("concerto");
    expect(inferRecordingWorkTypeHintFromTexts(["Piano Sonata No. 23", "钢琴奏鸣曲"])).toBe("chamber_solo");

    expect(
      inferRecordingWorkTypeHintFromWork(
        {
          title: "a小调钢琴协奏曲",
          titleLatin: "Piano Concerto, Op. 54",
          groupIds: ["group-schumann-concerto"],
        },
        [{ id: "group-schumann-concerto", title: "钢琴协奏曲", path: ["协奏曲", "钢琴协奏曲"] }],
      ),
    ).toBe("concerto");
  });

  it("resolves unknown work type hints from work context and exposes stable labels", () => {
    expect(
      resolveRecordingWorkTypeHintValue(
        "unknown",
        {
          title: "第五交响曲“命运”",
          titleLatin: "Symphony No. 5 in C minor, Op. 67",
          groupIds: ["group-beethoven-symphony"],
        },
        [{ id: "group-beethoven-symphony", title: "交响曲", path: ["交响曲"] }],
      ),
    ).toBe("orchestral");

    expect(getRecordingWorkTypeHintLabel("concerto")).toBe("协奏曲");
    expect(getRecordingWorkTypeHintLabel("unknown")).toBe("未分类");
  });
});
