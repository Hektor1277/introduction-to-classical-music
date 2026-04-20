import { describe, expect, it } from "vitest";

import { buildVerifyLiveRuntimeOptions, buildVerifyRecordingRetrievalSettings } from "../../scripts/lib/recording-live-verify.js";

describe("recording live verify config helpers", () => {
  it("uses the owner default retrieval timeout budget by default", () => {
    const settings = buildVerifyRecordingRetrievalSettings({
      serviceBaseUrl: "http://127.0.0.1:4791",
    });

    expect(settings).toMatchObject({
      enabled: true,
      baseUrl: "http://127.0.0.1:4791",
      timeoutMs: 180000,
      pollIntervalMs: 1000,
      expectedProtocolVersion: "v1",
      status: "",
    });
  });

  it("keeps the owner job wait budget above the provider request timeout", () => {
    expect(buildVerifyLiveRuntimeOptions({})).toEqual({
      requestTimeoutMs: 180000,
      jobTimeoutMs: 210000,
    });
    expect(
      buildVerifyLiveRuntimeOptions({
        requestTimeoutMs: 90000,
      }),
    ).toEqual({
      requestTimeoutMs: 90000,
      jobTimeoutMs: 120000,
    });
  });
});
