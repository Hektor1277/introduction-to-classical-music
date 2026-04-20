from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.models.protocol import (
    AcceptedJobResponse,
    CreateJobRequest,
    HealthResponse,
    JobStatusResponse,
    ResultsResponse,
)

TERMINAL_JOB_STATUSES = {"succeeded", "partial", "failed", "timed_out", "canceled"}


class RecordingRetrievalServiceClient:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        self._base_url = str(base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url 不能为空")
        self._client = client or httpx.AsyncClient(base_url=self._base_url, follow_redirects=True, timeout=30.0)
        self._owns_client = client is None
        self._poll_interval_seconds = max(0.01, float(poll_interval_seconds or 0.25))

    async def __aenter__(self) -> "RecordingRetrievalServiceClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def health(self) -> HealthResponse:
        response = await self._client.get(f"{self._base_url}/health")
        response.raise_for_status()
        return HealthResponse.model_validate(response.json())

    async def create_job(self, request: CreateJobRequest | dict[str, Any]) -> AcceptedJobResponse:
        payload = self._request_payload(request)
        response = await self._client.post(f"{self._base_url}/v1/jobs", json=payload)
        response.raise_for_status()
        return AcceptedJobResponse.model_validate(response.json())

    async def get_job(self, job_id: str) -> JobStatusResponse:
        response = await self._client.get(f"{self._base_url}/v1/jobs/{job_id}")
        response.raise_for_status()
        return JobStatusResponse.model_validate(response.json())

    async def get_results(self, job_id: str) -> ResultsResponse:
        response = await self._client.get(f"{self._base_url}/v1/jobs/{job_id}/results")
        response.raise_for_status()
        return ResultsResponse.model_validate(response.json())

    async def cancel_job(self, job_id: str) -> JobStatusResponse:
        response = await self._client.post(f"{self._base_url}/v1/jobs/{job_id}/cancel")
        response.raise_for_status()
        return JobStatusResponse.model_validate(response.json())

    async def wait_for_terminal_status(self, job_id: str, *, timeout_seconds: float = 30.0) -> JobStatusResponse:
        deadline = time.monotonic() + max(0.01, float(timeout_seconds or 30.0))
        while True:
            status = await self.get_job(job_id)
            if status.status in TERMINAL_JOB_STATUSES:
                return status
            if time.monotonic() >= deadline:
                raise TimeoutError(f"等待作业 {job_id} 完成超时")
            await asyncio.sleep(self._poll_interval_seconds)

    async def wait_for_results(
        self,
        job_id: str,
        *,
        timeout_seconds: float = 30.0,
        cancel_on_timeout: bool = False,
    ) -> ResultsResponse:
        try:
            await self.wait_for_terminal_status(job_id, timeout_seconds=timeout_seconds)
        except TimeoutError:
            if cancel_on_timeout:
                await self.cancel_job(job_id)
            raise
        return await self.get_results(job_id)

    async def run_job(
        self,
        request: CreateJobRequest | dict[str, Any],
        *,
        timeout_seconds: float = 30.0,
        cancel_on_timeout: bool = False,
    ) -> ResultsResponse:
        accepted = await self.create_job(request)
        return await self.wait_for_results(
            accepted.job_id,
            timeout_seconds=timeout_seconds,
            cancel_on_timeout=cancel_on_timeout,
        )

    def _request_payload(self, request: CreateJobRequest | dict[str, Any]) -> dict[str, Any]:
        if isinstance(request, CreateJobRequest):
            model = request
        else:
            model = CreateJobRequest.model_validate(request)
        return model.model_dump(by_alias=True)
