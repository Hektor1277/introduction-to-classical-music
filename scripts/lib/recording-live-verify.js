/**
 * @param {{
 *   serviceBaseUrl?: string;
 *   requestTimeoutMs?: number;
 *   pollIntervalMs?: number;
 * }} [options]
 */
export function buildVerifyRecordingRetrievalSettings({
  serviceBaseUrl,
  requestTimeoutMs = 180000,
  pollIntervalMs = 1000,
} = {}) {
  return {
    enabled: true,
    baseUrl: serviceBaseUrl || "",
    timeoutMs: requestTimeoutMs,
    pollIntervalMs,
    expectedProtocolVersion: "v1",
    status: "",
  };
}

/**
 * @param {{
 *   requestTimeoutMs?: number;
 *   jobTimeoutMs?: number;
 * }} [options]
 */
export function buildVerifyLiveRuntimeOptions({
  requestTimeoutMs = 180000,
  jobTimeoutMs,
} = {}) {
  return {
    requestTimeoutMs,
    jobTimeoutMs: Math.max(requestTimeoutMs + 30000, jobTimeoutMs ?? requestTimeoutMs + 30000),
  };
}
