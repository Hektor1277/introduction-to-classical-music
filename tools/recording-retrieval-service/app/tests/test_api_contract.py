from __future__ import annotations

import time

from fastapi.testclient import TestClient

from app.main import create_app
from app.services.retrieval import StubRetriever
from tests.fixtures import sample_request


def test_health_matches_owner_contract() -> None:
    client = TestClient(create_app(retriever=StubRetriever()))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": "recording-retrieval-service",
        "version": "0.1.0",
        "protocolVersion": "v1",
        "status": "ok",
    }


def test_create_job_then_poll_and_fetch_results() -> None:
    client = TestClient(create_app(retriever=StubRetriever()))

    accepted = client.post("/v1/jobs", json=sample_request(item_count=2))
    assert accepted.status_code == 202
    accepted_payload = accepted.json()
    assert accepted_payload["status"] == "accepted"
    job_id = accepted_payload["jobId"]

    status_response = client.get(f"/v1/jobs/{job_id}")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["status"] in {"running", "partial", "succeeded"}
    assert status_payload["progress"]["total"] == 2

    final_status = status_payload["status"]
    for _ in range(20):
        if final_status not in {"queued", "running"}:
            break
        time.sleep(0.05)
        final_status = client.get(f"/v1/jobs/{job_id}").json()["status"]

    results_response = client.get(f"/v1/jobs/{job_id}/results")
    assert results_response.status_code == 200
    results_payload = results_response.json()
    assert results_payload["jobId"] == job_id
    assert results_payload["status"] in {"succeeded", "partial"}
    assert len(results_payload["items"]) == 2
    assert {item["itemId"] for item in results_payload["items"]} == {"recording-1", "recording-2"}


def test_duplicate_item_id_is_rejected() -> None:
    client = TestClient(create_app(retriever=StubRetriever()))
    payload = sample_request(item_count=2)
    payload["items"][1]["itemId"] = payload["items"][0]["itemId"]

    response = client.post("/v1/jobs", json=payload)

    assert response.status_code == 422
    assert "itemId" in response.text


def test_cancel_updates_job_status() -> None:
    client = TestClient(create_app(retriever=StubRetriever(delay_seconds=0.2)))

    accepted = client.post("/v1/jobs", json=sample_request(item_count=1))
    job_id = accepted.json()["jobId"]

    canceled = client.post(f"/v1/jobs/{job_id}/cancel")

    assert canceled.status_code == 200
    payload = canceled.json()
    assert payload["jobId"] == job_id
    assert payload["status"] == "canceled"
