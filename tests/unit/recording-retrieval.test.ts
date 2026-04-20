import { describe, expect, it } from "vitest";

import {
  buildRecordingRetrievalRequest,
  executeRecordingRetrievalJob,
  translateRecordingRetrievalResultsToProposals,
  type RecordingRetrievalProvider,
  type RecordingRetrievalRequest,
} from "@/lib/recording-retrieval";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "composer-beethoven",
      slug: "beethoven",
      name: "贝多芬",
      fullName: "",
      nameLatin: "Ludwig van Beethoven",
      displayName: "贝多芬",
      displayFullName: "",
      displayLatinName: "Ludwig van Beethoven",
      country: "Germany",
      avatarSrc: "",
      aliases: [],
      abbreviations: [],
      sortKey: "0010",
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
      imageSourceUrl: "",
      imageSourceKind: "",
      imageAttribution: "",
      imageUpdatedAt: "",
    },
  ],
  people: [],
  workGroups: [
    {
      id: "group-symphony",
      composerId: "composer-beethoven",
      title: "交响曲",
      slug: "symphony",
      path: ["交响曲"],
      sortKey: "0010",
    },
  ],
  works: [
    {
      id: "work-beethoven-5",
      composerId: "composer-beethoven",
      groupIds: ["group-symphony"],
      slug: "beethoven-5",
      title: "第五交响曲",
      titleLatin: "Symphony No. 5",
      aliases: [],
      catalogue: "Op. 67",
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
      sortKey: "0010",
      updatedAt: "2026-03-15T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "recording-kleiber-1975",
      workId: "work-beethoven-5",
      slug: "kleiber-1975",
      title: "Kleiber 1975",
      sortKey: "0010",
      isPrimaryRecommendation: false,
      updatedAt: "2026-03-15T00:00:00.000Z",
      images: [],
      credits: [{ role: "conductor", displayName: "Kleiber", personId: "" }],
      links: [],
      notes: "",
      performanceDateText: "1975",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
    },
  ],
});

describe("recording retrieval protocol", () => {
  it("builds stable request items from owner recording seeds", () => {
    const request = buildRecordingRetrievalRequest(library, library.recordings, {
      source: {
        kind: "owner-batch-check",
        batchSessionId: "batch-1",
      },
      overrides: {
        "recording-kleiber-1975": {
          sourceLine: "Kleiber | Wiener Philharmoniker | 1975 | -",
          workTypeHint: "orchestral",
        },
      },
    });

    expect(request.items).toHaveLength(1);
    expect(request.items[0]).toMatchObject({
      itemId: "recording-kleiber-1975",
      recordingId: "recording-kleiber-1975",
      workId: "work-beethoven-5",
      composerId: "composer-beethoven",
      workTypeHint: "orchestral",
      sourceLine: "Kleiber | Wiener Philharmoniker | 1975 | -",
    });
  });

  it("rejects duplicate item ids from the provider result payload", async () => {
    const provider: RecordingRetrievalProvider = {
      name: "recording-retrieval-service",
      protocolVersion: "v1",
      checkHealth: async () => ({
        service: "recording-retrieval-service",
        version: "1.0.0",
        protocolVersion: "v1",
        status: "ok",
      }),
      createJob: async (request: RecordingRetrievalRequest) => ({
        jobId: "provider-job-1",
        requestId: request.requestId,
        status: "accepted",
        itemCount: request.items.length,
        acceptedAt: "2026-03-15T00:00:00.000Z",
      }),
      getJob: async () => ({
        jobId: "provider-job-1",
        requestId: "request-1",
        status: "succeeded",
        progress: { total: 1, completed: 1, succeeded: 1, partial: 0, failed: 0, notFound: 0 },
        items: [{ itemId: "recording-kleiber-1975", status: "succeeded" }],
        logs: [{ timestamp: "2026-03-15T00:00:00.000Z", message: "done" }],
      }),
      getResults: async () => ({
        jobId: "provider-job-1",
        requestId: "request-1",
        status: "succeeded",
        completedAt: "2026-03-15T00:00:00.000Z",
        items: [
          {
            itemId: "recording-kleiber-1975",
            status: "succeeded",
            confidence: 0.8,
            warnings: [],
            result: {},
            evidence: [],
            linkCandidates: [],
            imageCandidates: [],
            logs: [],
          },
          {
            itemId: "recording-kleiber-1975",
            status: "succeeded",
            confidence: 0.8,
            warnings: [],
            result: {},
            evidence: [],
            linkCandidates: [],
            imageCandidates: [],
            logs: [],
          },
        ],
      }),
      cancelJob: async () => ({
        jobId: "provider-job-1",
        requestId: "request-1",
        status: "canceled",
        progress: { total: 1, completed: 1, succeeded: 0, partial: 0, failed: 1, notFound: 0 },
        items: [{ itemId: "recording-kleiber-1975", status: "failed" }],
        logs: [],
      }),
    };

    await expect(
      executeRecordingRetrievalJob(provider, buildRecordingRetrievalRequest(library, library.recordings)),
    ).rejects.toThrow("缺失或重复的 itemId");
  });

  it("translates provider results into owner automation proposals", async () => {
    const request = buildRecordingRetrievalRequest(library, library.recordings);
    const execution = {
      accepted: {
        jobId: "provider-job-1",
        requestId: request.requestId,
        status: "accepted" as const,
        itemCount: 1,
        acceptedAt: "2026-03-15T00:00:00.000Z",
      },
      status: {
        jobId: "provider-job-1",
        requestId: request.requestId,
        status: "succeeded" as const,
        progress: { total: 1, completed: 1, succeeded: 1, partial: 0, failed: 0, notFound: 0 },
        items: [{ itemId: "recording-kleiber-1975", status: "succeeded" as const }],
        logs: [],
      },
      results: {
        jobId: "provider-job-1",
        requestId: request.requestId,
        status: "succeeded" as const,
        completedAt: "2026-03-15T00:00:00.000Z",
        items: [
          {
            itemId: "recording-kleiber-1975",
            status: "succeeded" as const,
            confidence: 0.92,
            warnings: [],
            result: {
              label: "DG",
              releaseDate: "1976",
              links: [{ url: "https://example.com/kleiber", platform: "other" }],
            },
            evidence: [{ field: "label", sourceUrl: "https://example.com/kleiber", sourceLabel: "Example", confidence: 0.92 }],
            linkCandidates: [{ url: "https://example.com/kleiber", platform: "other", confidence: 0.92 }],
            imageCandidates: [],
            logs: [],
          },
        ],
      },
      runtimeState: {
        providerName: "recording-retrieval-service" as const,
        providerJobId: "provider-job-1",
        requestId: request.requestId,
        submittedAt: "2026-03-15T00:00:00.000Z",
        lastSyncedAt: "2026-03-15T00:00:00.000Z",
        phase: "completed" as const,
        status: "succeeded" as const,
        progress: { total: 1, completed: 1, succeeded: 1, partial: 0, failed: 0, notFound: 0 },
        logs: [],
      },
    };

    const proposals = translateRecordingRetrievalResultsToProposals(library, execution);

    expect(proposals).toHaveLength(1);
    expect(proposals[0]?.entityType).toBe("recording");
    expect(proposals[0]?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "label", after: "DG" }),
        expect.objectContaining({ path: "releaseDate", after: "1976" }),
        expect.objectContaining({ path: "links" }),
      ]),
    );
  });
});
