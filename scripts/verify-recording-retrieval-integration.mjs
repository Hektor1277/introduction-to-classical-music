import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile, writeFile, rm } from "node:fs/promises";
import path from "node:path";

const rootDir = process.cwd();
const ownerBaseUrl = process.env.OWNER_BASE_URL || "http://127.0.0.1:4322";
const mockPort = Number(process.env.MOCK_RECORDING_RETRIEVAL_PORT || 4789);
const recordingConfigPath = path.join(rootDir, "data", "automation", "recording-retrieval.local.json");

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function waitForHealthy(url, timeoutMs = 10000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // keep polling
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
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

async function main() {
  const libraryPath = path.join(rootDir, "data", "library", "recordings.json");
  const recordings = JSON.parse(await readFile(libraryPath, "utf8"));
  const target = recordings.find((item) => item?.id && item?.title);
  assert(target, "No recording found in data/library/recordings.json");

  let previousConfig = null;
  try {
    previousConfig = await readFile(recordingConfigPath, "utf8");
  } catch {
    previousConfig = null;
  }

  await writeFile(
    recordingConfigPath,
    `${JSON.stringify(
      {
        enabled: true,
        baseUrl: `http://127.0.0.1:${mockPort}`,
        timeoutMs: 30000,
        pollIntervalMs: 300,
        expectedProtocolVersion: "v1",
        status: "",
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  const mockScriptPath = path.join(rootDir, "scripts", "mock-recording-retrieval-service.mjs");
  const child = spawn(process.execPath, [mockScriptPath], {
    cwd: rootDir,
    env: {
      ...process.env,
      MOCK_RECORDING_RETRIEVAL_PORT: String(mockPort),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";
  child.stdout.on("data", (chunk) => {
    output += String(chunk);
  });
  child.stderr.on("data", (chunk) => {
    output += String(chunk);
  });

  try {
    await waitForHealthy(`http://127.0.0.1:${mockPort}/health`);

    const checkResponse = await fetchJson(`${ownerBaseUrl}/api/automation/check`, {
      method: "POST",
      body: JSON.stringify({
        categories: ["recording"],
        recordingIds: [target.id],
      }),
    });
    const jobId = checkResponse?.job?.id;
    assert(jobId, "Owner did not return a job id for recording automation check");

    let jobPayload = null;
    const startedAt = Date.now();
    while (Date.now() - startedAt < 20000) {
      jobPayload = await fetchJson(`${ownerBaseUrl}/api/automation/jobs/${encodeURIComponent(jobId)}`);
      if (jobPayload?.job?.status === "completed") {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    assert(jobPayload?.job?.status === "completed", "Recording automation job did not complete in time");
    const run = jobPayload?.job?.run;
    assert(run?.provider?.providerName === "recording-retrieval-service", "Run did not preserve provider runtime state");
    assert(run?.provider?.status === "succeeded", "External retrieval provider did not succeed");
    assert(Array.isArray(run?.proposals) && run.proposals.length > 0, "Recording automation run did not generate proposals");

    const recordingProposal = run.proposals.find((proposal) => proposal.entityType === "recording");
    assert(recordingProposal, "No recording proposal found in completed run");
    assert(recordingProposal.fields.some((field) => field.path === "label"), "Recording proposal is missing mock label patch");
    assert(
      Array.isArray(recordingProposal.linkCandidates) && recordingProposal.linkCandidates.some((item) => String(item.url).includes("mock.example.com")),
      "Recording proposal is missing mock link candidates",
    );
    assert(
      Array.isArray(recordingProposal.imageCandidates) && recordingProposal.imageCandidates.some((item) => String(item.src).includes("mock.example.com")),
      "Recording proposal is missing mock image candidates",
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          ok: true,
          jobId,
          runId: run.id,
          recordingId: target.id,
          provider: run.provider,
          proposalSummary: recordingProposal.summary,
          affectedFieldPaths: recordingProposal.fields.map((field) => field.path),
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    child.kill("SIGTERM");
    await once(child, "exit").catch(() => undefined);
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
