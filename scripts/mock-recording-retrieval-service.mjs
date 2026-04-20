import http from "node:http";

const port = Number(process.env.MOCK_RECORDING_RETRIEVAL_PORT || 4789);
const jobs = new Map();

function json(response, statusCode, payload) {
  response.statusCode = statusCode;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.end(`${JSON.stringify(payload)}\n`);
}

function now() {
  return new Date().toISOString();
}

function buildLogs(message, itemId) {
  return [{ timestamp: now(), level: "info", message, ...(itemId ? { itemId } : {}) }];
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url || "/", `http://127.0.0.1:${port}`);

  if (request.method === "GET" && url.pathname === "/health") {
    return json(response, 200, {
      service: "recording-retrieval-service",
      version: "mock-1.0.0",
      protocolVersion: "v1",
      status: "ok",
    });
  }

  if (request.method === "POST" && url.pathname === "/v1/jobs") {
    const chunks = [];
    for await (const chunk of request) {
      chunks.push(Buffer.from(chunk));
    }
    const payload = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
    const jobId = `mock-job-${Date.now()}`;
    const requestId = String(payload.requestId || `mock-request-${Date.now()}`);
    const items = Array.isArray(payload.items) ? payload.items : [];
    const acceptedAt = now();

    const results = items.map((item, index) => ({
      itemId: String(item.itemId),
      status: "succeeded",
      confidence: 0.92,
      warnings: [],
      result: {
        label: `Mock Label ${index + 1}`,
        releaseDate: "2099-01-01",
        venueText: "Mock Hall",
        notes: "Mock retrieval notes",
        links: [
          {
            platform: "other",
            url: `https://mock.example.com/recordings/${encodeURIComponent(String(item.itemId))}`,
            title: "Mock Link",
            sourceLabel: "Mock Source",
            confidence: 0.92,
          },
        ],
        images: [
          {
            id: `mock-image-${index + 1}`,
            src: `https://mock.example.com/images/${index + 1}.jpg`,
            sourceUrl: `https://mock.example.com/images/${index + 1}.jpg`,
            sourceKind: "mock-service",
            attribution: "Mock Service",
            title: "Mock Cover",
            width: 1200,
            height: 1200,
          },
        ],
      },
      evidence: [
        {
          field: "label",
          sourceUrl: `https://mock.example.com/recordings/${encodeURIComponent(String(item.itemId))}`,
          sourceLabel: "Mock Source",
          confidence: 0.92,
          note: "Mock evidence",
        },
      ],
      linkCandidates: [
        {
          platform: "other",
          url: `https://mock.example.com/recordings/${encodeURIComponent(String(item.itemId))}`,
          title: "Mock Link",
          sourceLabel: "Mock Source",
          confidence: 0.92,
        },
      ],
      imageCandidates: [
        {
          id: `mock-image-${index + 1}`,
          src: `https://mock.example.com/images/${index + 1}.jpg`,
          sourceUrl: `https://mock.example.com/images/${index + 1}.jpg`,
          sourceKind: "mock-service",
          attribution: "Mock Service",
          title: "Mock Cover",
          width: 1200,
          height: 1200,
        },
      ],
      logs: buildLogs("mock retrieval finished", String(item.itemId)),
    }));

    jobs.set(jobId, {
      requestId,
      items,
      acceptedAt,
      statusChecks: 0,
      canceled: false,
      results,
    });

    return json(response, 200, {
      jobId,
      requestId,
      status: "accepted",
      itemCount: items.length,
      acceptedAt,
    });
  }

  const jobMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)$/);
  if (request.method === "GET" && jobMatch) {
    const job = jobs.get(jobMatch[1]);
    if (!job) {
      return json(response, 404, { error: "job not found" });
    }
    job.statusChecks += 1;
    const running = job.statusChecks < 2 && !job.canceled;
    return json(response, 200, {
      jobId: jobMatch[1],
      requestId: job.requestId,
      status: job.canceled ? "canceled" : running ? "running" : "succeeded",
      progress: {
        total: job.items.length,
        completed: running ? 0 : job.items.length,
        succeeded: running ? 0 : job.items.length,
        partial: 0,
        failed: 0,
        notFound: 0,
      },
      items: job.items.map((item) => ({
        itemId: String(item.itemId),
        status: job.canceled ? "failed" : running ? "running" : "succeeded",
        message: job.canceled ? "canceled" : running ? "running" : "done",
      })),
      logs: buildLogs(job.canceled ? "mock retrieval canceled" : running ? "mock retrieval running" : "mock retrieval done"),
      completedAt: running ? undefined : now(),
    });
  }

  const resultMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/results$/);
  if (request.method === "GET" && resultMatch) {
    const job = jobs.get(resultMatch[1]);
    if (!job) {
      return json(response, 404, { error: "job not found" });
    }
    return json(response, 200, {
      jobId: resultMatch[1],
      requestId: job.requestId,
      status: job.canceled ? "canceled" : "succeeded",
      completedAt: now(),
      items: job.results,
    });
  }

  const cancelMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/cancel$/);
  if (request.method === "POST" && cancelMatch) {
    const job = jobs.get(cancelMatch[1]);
    if (!job) {
      return json(response, 404, { error: "job not found" });
    }
    job.canceled = true;
    return json(response, 200, {
      jobId: cancelMatch[1],
      requestId: job.requestId,
      status: "canceled",
      progress: {
        total: job.items.length,
        completed: job.items.length,
        succeeded: 0,
        partial: 0,
        failed: job.items.length,
        notFound: 0,
      },
      items: job.items.map((item) => ({
        itemId: String(item.itemId),
        status: "failed",
        message: "canceled",
      })),
      logs: buildLogs("mock retrieval canceled"),
      completedAt: now(),
    });
  }

  return json(response, 404, { error: "not found" });
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`Mock recording retrieval service listening on http://127.0.0.1:${port}\n`);
});
