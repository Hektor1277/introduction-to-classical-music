from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class StrictModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


JobStatus = Literal["queued", "running", "partial", "succeeded", "failed", "timed_out", "canceled"]
ItemStatus = Literal["queued", "running", "succeeded", "partial", "failed", "not_found"]
WorkTypeHint = Literal["orchestral", "concerto", "opera_vocal", "chamber_solo", "unknown"]
RequestedField = Literal[
    "links",
    "performanceDateText",
    "venueText",
    "albumTitle",
    "label",
    "releaseDate",
    "notes",
    "images",
]


class Credit(StrictModel):
    role: str
    person_id: str = Field(default="", alias="personId")
    display_name: str = Field(default="", alias="displayName")
    label: str = ""


class LinkSeed(StrictModel):
    platform: str
    url: str
    title: str = ""


class Seed(StrictModel):
    title: str
    composer_name: str = Field(alias="composerName")
    composer_name_latin: str = Field(alias="composerNameLatin")
    work_title: str = Field(alias="workTitle")
    work_title_latin: str = Field(alias="workTitleLatin")
    catalogue: str = ""
    performance_date_text: str = Field(default="", alias="performanceDateText")
    venue_text: str = Field(default="", alias="venueText")
    album_title: str = Field(default="", alias="albumTitle")
    label: str = ""
    release_date: str = Field(default="", alias="releaseDate")
    credits: list[Credit] = Field(default_factory=list)
    links: list[LinkSeed] = Field(default_factory=list)
    notes: str = ""


class RetrievalItem(StrictModel):
    item_id: str = Field(alias="itemId")
    recording_id: str = Field(alias="recordingId")
    work_id: str = Field(alias="workId")
    composer_id: str = Field(alias="composerId")
    work_type_hint: WorkTypeHint = Field(alias="workTypeHint")
    source_line: str = Field(default="", alias="sourceLine")
    seed: Seed
    requested_fields: list[RequestedField] = Field(alias="requestedFields")

    @model_validator(mode="after")
    def ensure_ids(self) -> "RetrievalItem":
        if not self.item_id.strip():
            raise ValueError("itemId 不能为空")
        return self


class RequestSource(StrictModel):
    kind: Literal["owner-entity-check", "owner-batch-check"]
    owner_run_id: str | None = Field(default=None, alias="ownerRunId")
    batch_session_id: str | None = Field(default=None, alias="batchSessionId")
    requested_by: Literal["owner-tool"] = Field(alias="requestedBy")


class RequestOptions(StrictModel):
    max_concurrency: int = Field(default=4, alias="maxConcurrency", ge=1, le=32)
    timeout_ms: int = Field(default=180000, alias="timeoutMs", ge=1)
    return_partial_results: bool = Field(default=True, alias="returnPartialResults")


class CreateJobRequest(StrictModel):
    request_id: str = Field(alias="requestId")
    source: RequestSource
    items: list[RetrievalItem]
    options: RequestOptions

    @model_validator(mode="after")
    def ensure_unique_item_ids(self) -> "CreateJobRequest":
        ids = [item.item_id for item in self.items]
        if not ids:
            raise ValueError("items 不能为空")
        if len(ids) != len(set(ids)):
            raise ValueError("itemId 必须唯一")
        return self


class HealthResponse(StrictModel):
    service: Literal["recording-retrieval-service"] = "recording-retrieval-service"
    version: str = "0.1.0"
    protocol_version: Literal["v1"] = Field(default="v1", alias="protocolVersion")
    status: Literal["ok"] = "ok"


class AcceptedJobResponse(StrictModel):
    job_id: str = Field(alias="jobId")
    request_id: str = Field(alias="requestId")
    status: Literal["accepted"] = "accepted"
    item_count: int = Field(alias="itemCount")
    accepted_at: str = Field(default_factory=now_iso, alias="acceptedAt")


class Progress(StrictModel):
    total: int
    completed: int
    succeeded: int
    partial: int
    failed: int
    not_found: int = Field(alias="notFound")


class LogEntry(StrictModel):
    timestamp: str = Field(default_factory=now_iso)
    level: Literal["info", "warning", "error"] = "info"
    message: str
    item_id: str | None = Field(default=None, alias="itemId")


class JobStatusItem(StrictModel):
    item_id: str = Field(alias="itemId")
    status: ItemStatus
    message: str | None = None


class JobStatusResponse(StrictModel):
    job_id: str = Field(alias="jobId")
    request_id: str = Field(alias="requestId")
    status: JobStatus
    progress: Progress
    items: list[JobStatusItem]
    logs: list[LogEntry]
    error: str | None = None
    completed_at: str | None = Field(default=None, alias="completedAt")


class EvidenceItem(StrictModel):
    field: str
    source_url: str = Field(alias="sourceUrl")
    source_label: str = Field(alias="sourceLabel")
    confidence: float
    note: str | None = None


class LinkCandidate(StrictModel):
    platform: str | None = None
    url: str
    title: str | None = None
    source_label: str | None = Field(default=None, alias="sourceLabel")
    confidence: float | None = None
    zone: str | None = None
    note: str | None = None


class ImageCandidate(StrictModel):
    id: str | None = None
    src: str
    source_url: str | None = Field(default=None, alias="sourceUrl")
    source_kind: str | None = Field(default=None, alias="sourceKind")
    attribution: str | None = None
    title: str | None = None
    width: int | None = None
    height: int | None = None


class ResultPayload(StrictModel):
    performance_date_text: str | None = Field(default=None, alias="performanceDateText")
    venue_text: str | None = Field(default=None, alias="venueText")
    album_title: str | None = Field(default=None, alias="albumTitle")
    label: str | None = None
    release_date: str | None = Field(default=None, alias="releaseDate")
    notes: str | None = None
    links: list[LinkCandidate] = Field(default_factory=list)
    images: list[ImageCandidate] = Field(default_factory=list)


class ResultItemResponse(StrictModel):
    item_id: str = Field(alias="itemId")
    status: ItemStatus
    confidence: float
    warnings: list[str] = Field(default_factory=list)
    result: ResultPayload
    evidence: list[EvidenceItem] = Field(default_factory=list)
    link_candidates: list[LinkCandidate] = Field(default_factory=list, alias="linkCandidates")
    image_candidates: list[ImageCandidate] = Field(default_factory=list, alias="imageCandidates")
    logs: list[LogEntry] = Field(default_factory=list)


class ResultsResponse(StrictModel):
    job_id: str = Field(alias="jobId")
    request_id: str = Field(alias="requestId")
    status: Literal["succeeded", "partial", "failed", "canceled", "timed_out"]
    completed_at: str = Field(default_factory=now_iso, alias="completedAt")
    items: list[ResultItemResponse]
