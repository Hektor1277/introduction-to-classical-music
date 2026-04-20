from __future__ import annotations

import asyncio
import time

from app.models.protocol import CreateJobRequest
from app.models.protocol import LogEntry, ResultItemResponse, ResultPayload
from app.services.orchestrator import JobOrchestrator
from app.services.retrieval import StubRetriever
from tests.fixtures import sample_request


class HungRetriever:
    async def retrieve(self, item, *, cancel_event=None, deadline=None):
        del item, deadline
        started = time.monotonic()
        while True:
            if cancel_event is not None and cancel_event.is_set():
                break
            await asyncio.sleep(0.01)
            if time.monotonic() - started > 1:
                break


class PartialRetriever:
    async def retrieve(self, item, *, cancel_event=None, deadline=None):
        del cancel_event, deadline
        return ResultItemResponse(
            itemId=item.item_id,
            status="partial",
            confidence=0.61,
            warnings=["candidate only"],
            result=ResultPayload(),
            evidence=[],
            linkCandidates=[],
            imageCandidates=[],
            logs=[LogEntry(message="partial result", itemId=item.item_id)],
        )


def test_stub_retriever_returns_contract_safe_payload() -> None:
    retriever = StubRetriever()
    request = CreateJobRequest.model_validate(sample_request())

    result = asyncio.run(retriever.retrieve(request.items[0]))

    assert result.status in {"succeeded", "partial", "not_found"}
    assert result.item_id == "recording-1"
    assert 0 <= result.confidence <= 1
    assert isinstance(result.result.model_dump(by_alias=True), dict)


def test_orchestrator_completes_job_and_isolates_item_results() -> None:
    orchestrator = JobOrchestrator(log_dir=None, retriever=StubRetriever())
    request = CreateJobRequest.model_validate(sample_request(item_count=2))

    accepted = orchestrator.create_job(request)

    asyncio.run(orchestrator.wait_for_job(accepted.job_id, timeout=5))
    results = orchestrator.get_results(accepted.job_id)

    assert results is not None
    assert len(results.items) == 2
    assert {item.item_id for item in results.items} == {"recording-1", "recording-2"}


def test_orchestrator_marks_timeout() -> None:
    orchestrator = JobOrchestrator(log_dir=None, retriever=StubRetriever(delay_seconds=0.05))
    payload = sample_request(item_count=1)
    payload["options"]["timeoutMs"] = 1
    request = CreateJobRequest.model_validate(payload)

    accepted = orchestrator.create_job(request)
    asyncio.run(orchestrator.wait_for_job(accepted.job_id, timeout=5))
    status = orchestrator.get_status(accepted.job_id)

    assert status is not None
    assert status.status == "timed_out"


def test_orchestrator_times_out_even_if_retriever_ignores_deadline() -> None:
    orchestrator = JobOrchestrator(log_dir=None, retriever=HungRetriever())
    payload = sample_request(item_count=1)
    payload["options"]["timeoutMs"] = 50
    request = CreateJobRequest.model_validate(payload)

    accepted = orchestrator.create_job(request)
    asyncio.run(orchestrator.wait_for_job(accepted.job_id, timeout=5))
    status = orchestrator.get_status(accepted.job_id)

    assert status is not None
    assert status.status == "timed_out"


def test_orchestrator_promotes_job_to_partial_when_partial_results_are_returned() -> None:
    orchestrator = JobOrchestrator(log_dir=None, retriever=PartialRetriever())
    request = CreateJobRequest.model_validate(sample_request(item_count=1))

    accepted = orchestrator.create_job(request)
    asyncio.run(orchestrator.wait_for_job(accepted.job_id, timeout=5))
    status = orchestrator.get_status(accepted.job_id)
    results = orchestrator.get_results(accepted.job_id)

    assert status is not None
    assert status.status == "partial"
    assert results is not None
    assert results.status == "partial"
    assert results.items[0].status == "partial"
