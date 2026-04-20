import { spawn } from "node:child_process";
import { once } from "node:events";
import { access, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { buildVerifyLiveRuntimeOptions, buildVerifyRecordingRetrievalSettings } from "./lib/recording-live-verify.js";

const rootDir = process.cwd();
const ownerPort = Number(process.env.OWNER_PORT || 4328);
const ownerBaseUrl = process.env.OWNER_BASE_URL || `http://127.0.0.1:${ownerPort}`;
const servicePort = Number(process.env.LIVE_RECORDING_RETRIEVAL_PORT || 4791);
const serviceBaseUrl = process.env.RECORDING_RETRIEVAL_SERVICE_URL || `http://127.0.0.1:${servicePort}`;
const recordingConfigPath = path.join(rootDir, "data", "automation", "recording-retrieval.local.json");
const ownerEntryPath = path.join(rootDir, "output", "runtime", "apps", "owner", "server", "owner-app.js");
const sharedToolRootCandidates = [rootDir, path.resolve(rootDir, "..", "..")];
const runtimeOptions = buildVerifyLiveRuntimeOptions({
  requestTimeoutMs: Number(process.env.RECORDING_LIVE_VERIFY_REQUEST_TIMEOUT_MS || 180000),
  jobTimeoutMs: Number(process.env.RECORDING_LIVE_VERIFY_JOB_TIMEOUT_MS || 0) || undefined,
});

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function ensureFileExists(filePath) {
  await access(filePath);
}

async function resolveRecordingRetrievalServicePaths() {
  for (const candidateRoot of [...new Set(sharedToolRootCandidates)]) {
    const serviceCwd = path.join(candidateRoot, "tools", "recording-retrieval-service", "app");
    const servicePythonPath = path.join(serviceCwd, ".venv", "Scripts", "python.exe");
    try {
      await ensureFileExists(servicePythonPath);
      return { serviceCwd, servicePythonPath };
    } catch {
      // try next root candidate
    }
  }
  throw new Error("Recording retrieval service Python environment was not found in the current worktree or the primary repository root");
}

async function waitForHealthy(url, timeoutMs = 20000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json();
      }
    } catch {
      // continue polling
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function fetchJson(url, init) {
  const response = await fetch(url, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers || {}),
    },
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`${url} failed: ${response.status} ${body}`.trim());
  }
  return response.json();
}

function captureOutput(child, buffer) {
  child.stdout?.on("data", (chunk) => {
    buffer.push(String(chunk));
  });
  child.stderr?.on("data", (chunk) => {
    buffer.push(String(chunk));
  });
}

async function waitForExit(child, timeoutMs = 5000) {
  if (!child || child.exitCode !== null) {
    return;
  }
  await Promise.race([
    once(child, "exit").catch(() => undefined),
    new Promise((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
}

async function stopChild(child) {
  if (!child || child.exitCode !== null) {
    return;
  }
  child.kill("SIGTERM");
  await waitForExit(child, 1000);
  if (child.exitCode !== null) {
    return;
  }

  if (process.platform === "win32") {
    const killer = spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
      stdio: "ignore",
    });
    await once(killer, "exit").catch(() => undefined);
    await waitForExit(child, 5000);
    return;
  }

  child.kill("SIGKILL");
  await waitForExit(child, 5000);
}

async function main() {
  const servicePaths = await resolveRecordingRetrievalServicePaths();
  await ensureFileExists(ownerEntryPath);
  await mkdir(path.dirname(recordingConfigPath), { recursive: true });

  const [{ loadLibraryFromDisk }, { deleteAutomationRun }, audit] = await Promise.all([
    import("./../output/runtime/packages/data-core/src/library-store.js"),
    import("./../output/runtime/packages/automation/src/automation-store.js"),
    import("./../output/runtime/packages/automation/src/recording-retrieval-audit.js"),
  ]);

  const library = await loadLibraryFromDisk();
  const preferredTargetIds = audit.buildRecordingRetrievalAuditPlan(library, { sampleSizePerGroup: 1 }).targets.map((target) => target.recordingId);
  const targetRecording =
    library.recordings.find((item) => item.id === process.env.RECORDING_ID) ||
    preferredTargetIds.map((recordingId) => library.recordings.find((item) => item.id === recordingId)).find(Boolean) ||
    library.recordings.find((item) => item?.id && item?.title && Array.isArray(item.credits) && item.credits.length > 0);
  assert(targetRecording, "No suitable recording found in library");

  let previousConfig = null;
  try {
    previousConfig = await readFile(recordingConfigPath, "utf8");
  } catch {
    previousConfig = null;
  }

  const serviceLogs = [];
  const ownerLogs = [];
  let ownerChild;
  let serviceChild;
  let runId = "";

  try {
    await writeFile(
      recordingConfigPath,
      `${JSON.stringify(
        buildVerifyRecordingRetrievalSettings({
          serviceBaseUrl,
          requestTimeoutMs: runtimeOptions.requestTimeoutMs,
        }),
        null,
        2,
      )}\n`,
      "utf8",
    );

    serviceChild = spawn(servicePaths.servicePythonPath, ["-m", "app.main", "--mode", "service", "--host", "127.0.0.1", "--port", String(servicePort)], {
      cwd: servicePaths.serviceCwd,
      stdio: ["ignore", "pipe", "pipe"],
    });
    captureOutput(serviceChild, serviceLogs);

    const serviceHealth = await waitForHealthy(`${serviceBaseUrl}/health`);
    assert(serviceHealth.service === "recording-retrieval-service", "Live service health payload did not match expected service name");
    assert(serviceHealth.protocolVersion === "v1", "Live service protocol version mismatch");

    ownerChild = spawn(process.execPath, [ownerEntryPath], {
      cwd: rootDir,
      env: {
        ...process.env,
        OWNER_PORT: String(ownerPort),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    captureOutput(ownerChild, ownerLogs);

    await waitForHealthy(`${ownerBaseUrl}/api/library`);

    const checkResponse = await fetchJson(`${ownerBaseUrl}/api/automation/check`, {
      method: "POST",
      body: JSON.stringify({
        categories: ["recording"],
        recordingIds: [targetRecording.id],
      }),
    });

    const jobId = checkResponse?.job?.id;
    assert(jobId, "Owner did not return a job id");

    let jobPayload = null;
    const startedAt = Date.now();
    while (Date.now() - startedAt < runtimeOptions.jobTimeoutMs) {
      jobPayload = await fetchJson(`${ownerBaseUrl}/api/automation/jobs/${encodeURIComponent(jobId)}`);
      if (jobPayload?.job?.status === "completed") {
        break;
      }
      if (jobPayload?.job?.status === "failed") {
        throw new Error(`Owner automation job failed: ${jobPayload?.job?.error || "unknown error"}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    assert(jobPayload?.job?.status === "completed", "Owner automation job did not complete in time");

    const run = jobPayload?.job?.run;
    runId = String(run?.id || "");
    assert(runId, "Completed owner job did not persist an automation run id");
    assert(run?.provider?.providerName === "recording-retrieval-service", "Run provider name mismatch");
    assert(["partial", "succeeded"].includes(String(run?.provider?.status || "")), "Live provider status was neither partial nor succeeded");
    assert(Array.isArray(run?.proposals), "Run proposals payload is not an array");

    const proposal = run.proposals.find((item) => item.entityType === "recording" && item.entityId === targetRecording.id) || null;

    process.stdout.write(
      `${JSON.stringify(
        {
          ok: true,
          ownerBaseUrl,
          serviceBaseUrl,
          recordingId: targetRecording.id,
          title: targetRecording.title,
          provider: run.provider,
          proposalCount: run.proposals.length,
          proposalSummary: proposal?.summary || "",
          affectedFieldPaths: proposal ? proposal.fields.map((field) => field.path) : [],
        },
        null,
        2,
      )}\n`,
    );
  } catch (error) {
    const payload = {
      ok: false,
      error: error instanceof Error ? error.stack || error.message : String(error),
      ownerBaseUrl,
      serviceBaseUrl,
      serviceLogs: serviceLogs.join(""),
      ownerLogs: ownerLogs.join(""),
    };
    process.stderr.write(`${JSON.stringify(payload, null, 2)}\n`);
    process.exitCode = 1;
  } finally {
    if (runId) {
      await deleteAutomationRun(runId).catch(() => undefined);
    }
    await stopChild(ownerChild);
    await stopChild(serviceChild);
    if (previousConfig === null) {
      await rm(recordingConfigPath, { force: true }).catch(() => undefined);
    } else {
      await writeFile(recordingConfigPath, previousConfig, "utf8");
    }
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exitCode = 1;
});
