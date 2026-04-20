from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from app.models.protocol import (
    AcceptedJobResponse,
    CreateJobRequest,
    JobStatusItem,
    JobStatusResponse,
    LogEntry,
    Progress,
    ResultItemResponse,
    ResultsResponse,
)
from app.services.retrieval import StubRetriever, build_default_retriever
from app.services.summary_logger import SummaryLogger

TERMINAL_JOB_STATUSES = {"succeeded", "partial", "failed", "timed_out", "canceled"}


@dataclass
class JobRuntime:
    accepted: AcceptedJobResponse
    status: JobStatusResponse
    results: ResultsResponse | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None


class JobOrchestrator:
    def __init__(self, log_dir: Path | None = Path("logs"), retriever: StubRetriever | None = None) -> None:
        self._jobs: dict[str, JobRuntime] = {}
        self._lock = threading.Lock()
        self._logger = SummaryLogger(log_dir)
        self._retriever = retriever or build_default_retriever()

    def create_job(self, request: CreateJobRequest) -> AcceptedJobResponse:
        job_id = f"job-{uuid4().hex}"
        accepted = AcceptedJobResponse(jobId=job_id, requestId=request.request_id, itemCount=len(request.items))
        status = JobStatusResponse(
            jobId=job_id,
            requestId=request.request_id,
            status="queued",
            progress=Progress(total=len(request.items), completed=0, succeeded=0, partial=0, failed=0, notFound=0),
            items=[JobStatusItem(itemId=item.item_id, status="queued") for item in request.items],
            logs=[LogEntry(message="job accepted")],
        )
        runtime = JobRuntime(accepted=accepted, status=status)
        worker = threading.Thread(target=self._run_job, args=(job_id, request), daemon=True)
        runtime.thread = worker
        with self._lock:
            self._jobs[job_id] = runtime
        self._logger.log(job_id, event="accepted", request_id=request.request_id)
        worker.start()
        return accepted

    def get_status(self, job_id: str) -> JobStatusResponse | None:
        with self._lock:
            runtime = self._jobs.get(job_id)
            return runtime.status.model_copy(deep=True) if runtime else None

    def get_results(self, job_id: str) -> ResultsResponse | None:
        with self._lock:
            runtime = self._jobs.get(job_id)
            if runtime is None or runtime.results is None:
                return None
            return runtime.results.model_copy(deep=True)

    def cancel_job(self, job_id: str) -> JobStatusResponse | None:
        with self._lock:
            runtime = self._jobs.get(job_id)
            if runtime is None:
                return None
            runtime.cancel_event.set()
            if runtime.status.status not in TERMINAL_JOB_STATUSES:
                runtime.status.status = "canceled"
                runtime.status.error = "Job canceled by client."
                runtime.status.logs.append(LogEntry(message="job canceled", level="warning"))
                runtime.status.completed_at = runtime.status.completed_at or LogEntry(message="done").timestamp
                for item in runtime.status.items:
                    if item.status in {"queued", "running"}:
                        item.status = "failed"
                        item.message = "Canceled"
                self._update_progress_locked(runtime)
                runtime.results = ResultsResponse(
                    jobId=job_id,
                    requestId=runtime.accepted.request_id,
                    status="canceled",
                    completedAt=runtime.status.completed_at,
                    items=[],
                )
        self._logger.log(job_id, event="canceled")
        return self.get_status(job_id)

    async def wait_for_job(self, job_id: str, timeout: float = 10) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.get_status(job_id)
            if status is None or status.status in TERMINAL_JOB_STATUSES:
                return
            await asyncio.sleep(0.01)
        raise TimeoutError(f"Timed out waiting for job {job_id}")

    def _run_job(self, job_id: str, request: CreateJobRequest) -> None:
        runtime = self._jobs[job_id]
        deadline = time.monotonic() + (request.options.timeout_ms / 1000)
        self._set_status(job_id, "running")
        results: list[ResultItemResponse] = []

        with ThreadPoolExecutor(max_workers=request.options.max_concurrency) as executor:
            future_map = {}
            for item in request.items:
                if runtime.cancel_event.is_set():
                    break
                future = executor.submit(
                    asyncio.run,
                    self._retriever.retrieve(item, cancel_event=runtime.cancel_event, deadline=deadline),
                )
                future_map[future] = item.item_id
                self._set_item_status(job_id, item.item_id, "running")

            try:
                remaining = max(0.01, deadline - time.monotonic())
                for future in as_completed(future_map, timeout=remaining):
                    item_id = future_map[future]
                    if runtime.cancel_event.is_set():
                        break
                    try:
                        result = future.result()
                    except Exception as error:  # pragma: no cover
                        result = ResultItemResponse(
                            itemId=item_id,
                            status="failed",
                            confidence=0.0,
                            warnings=["retrieval failed"],
                            result={},
                            evidence=[],
                            linkCandidates=[],
                            imageCandidates=[],
                            logs=[LogEntry(message=str(error), itemId=item_id, level="error")],
                        )
                    if time.monotonic() > deadline:
                        runtime.cancel_event.set()
                        self._mark_timed_out(job_id)
                        break
                    results.append(result)
                    self._set_item_status(job_id, item_id, result.status)
                    self._append_logs(job_id, result.logs)
                    self._update_progress(job_id)
            except TimeoutError:
                runtime.cancel_event.set()
                self._mark_timed_out(job_id)

        final_status = self._finalize_job(job_id, request, results)
        self._logger.log(job_id, event="completed", status=final_status)

    def _finalize_job(self, job_id: str, request: CreateJobRequest, results: list[ResultItemResponse]) -> str:
        with self._lock:
            runtime = self._jobs[job_id]
            if runtime.status.status == "timed_out":
                final_status = "timed_out"
            elif runtime.cancel_event.is_set() or runtime.status.status == "canceled":
                final_status = "canceled"
            elif not results:
                final_status = "failed"
            elif all(item.status == "succeeded" for item in results):
                final_status = "succeeded"
            elif request.options.return_partial_results and any(item.status in {"succeeded", "partial"} for item in results):
                final_status = "partial"
            else:
                final_status = "failed"

            runtime.status.status = final_status
            runtime.status.completed_at = runtime.status.completed_at or LogEntry(message="done").timestamp
            runtime.status.error = runtime.status.error if final_status in {"timed_out", "canceled"} else None
            self._update_progress_locked(runtime)
            runtime.results = ResultsResponse(
                jobId=job_id,
                requestId=request.request_id,
                status=final_status,
                completedAt=runtime.status.completed_at,
                items=results,
            )
            return final_status

    def _mark_timed_out(self, job_id: str) -> None:
        with self._lock:
            runtime = self._jobs[job_id]
            runtime.status.status = "timed_out"
            runtime.status.error = "Job timed out."
            runtime.status.logs.append(LogEntry(message="job timed out", level="error"))
            runtime.status.completed_at = runtime.status.completed_at or LogEntry(message="done").timestamp
            for item in runtime.status.items:
                if item.status in {"queued", "running"}:
                    item.status = "failed"
                    item.message = "Timed out"
            self._update_progress_locked(runtime)

    def _set_status(self, job_id: str, status: str) -> None:
        with self._lock:
            runtime = self._jobs[job_id]
            runtime.status.status = status
            runtime.status.logs.append(LogEntry(message=f"job {status}"))

    def _set_item_status(self, job_id: str, item_id: str, status: str) -> None:
        with self._lock:
            runtime = self._jobs[job_id]
            for item in runtime.status.items:
                if item.item_id == item_id:
                    item.status = status
                    break
            self._update_progress_locked(runtime)

    def _append_logs(self, job_id: str, entries: list[LogEntry]) -> None:
        with self._lock:
            runtime = self._jobs[job_id]
            runtime.status.logs.extend(entries)

    def _update_progress(self, job_id: str) -> None:
        with self._lock:
            runtime = self._jobs[job_id]
            self._update_progress_locked(runtime)

    def _update_progress_locked(self, runtime: JobRuntime) -> None:
        total = len(runtime.status.items)
        succeeded = sum(1 for item in runtime.status.items if item.status == "succeeded")
        partial = sum(1 for item in runtime.status.items if item.status == "partial")
        failed = sum(1 for item in runtime.status.items if item.status == "failed")
        not_found = sum(1 for item in runtime.status.items if item.status == "not_found")
        completed = sum(1 for item in runtime.status.items if item.status in {"succeeded", "partial", "failed", "not_found"})
        runtime.status.progress = Progress(
            total=total,
            completed=completed,
            succeeded=succeeded,
            partial=partial,
            failed=failed,
            notFound=not_found,
        )
