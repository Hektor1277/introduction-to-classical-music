from __future__ import annotations

import asyncio

import httpx

from app.main import create_app
from app.services.retrieval import StubRetriever
from app.services.service_client import RecordingRetrievalServiceClient
from tests.fixtures import sample_request


def test_service_client_runs_job_until_results_are_ready() -> None:
    async def run_test() -> None:
        transport = httpx.ASGITransport(app=create_app(retriever=StubRetriever()))
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client:
            client = RecordingRetrievalServiceClient(
                "http://testserver",
                client=http_client,
                poll_interval_seconds=0.001,
            )
            results = await client.run_job(sample_request(item_count=2), timeout_seconds=2.0)

        assert results.status in {"succeeded", "partial"}
        assert len(results.items) == 2
        assert {item.item_id for item in results.items} == {"recording-1", "recording-2"}

    asyncio.run(run_test())
