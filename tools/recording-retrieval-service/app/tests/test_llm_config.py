from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from app.services.llm_client import (
    DualModelLlmClient,
    ModelConfig,
    OpenAiCompatibleLlmClient,
    ensure_runtime_llm_config,
    load_llm_config,
    migrate_legacy_llm_config,
)


def test_load_llm_config_reads_dual_model_shape(tmp_path: Path) -> None:
    config_path = tmp_path / "llm.local.json"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "fastModel": {
                    "enabled": True,
                    "baseUrl": "https://fast.example/v1",
                    "apiKey": "fast-secret",
                    "model": "deepseek-chat",
                    "timeoutMs": 12000,
                },
                "reasoningModel": {
                    "enabled": True,
                    "baseUrl": "https://reason.example/v1",
                    "apiKey": "reason-secret",
                    "model": "deepseek-reasoner",
                    "timeoutMs": 30000,
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_llm_config(config_path)

    assert config.fast.model == "deepseek-chat"
    assert config.fast.base_url == "https://fast.example/v1"
    assert config.reasoning.model == "deepseek-reasoner"
    assert config.reasoning.base_url == "https://reason.example/v1"


def test_migrate_legacy_llm_config_reads_two_entries_and_persists_dual_models(tmp_path: Path) -> None:
    legacy_path = tmp_path / "LLM config.txt"
    legacy_path.write_text(
        "\n".join(
            [
                "key1:",
                "https://api.deepseek.com/v1",
                "sk-reason",
                "deepseek-reasoner",
                "key2:",
                "https://api.deepseek.com/v1",
                "sk-fast",
                "deepseek-chat",
            ]
        ),
        encoding="utf-8",
    )
    runtime_path = tmp_path / "llm.local.json"

    migrated = migrate_legacy_llm_config(legacy_path=legacy_path, runtime_path=runtime_path)
    assert migrated is True

    config = load_llm_config(runtime_path)
    assert config.reasoning.model == "deepseek-reasoner"
    assert config.reasoning.api_key == "sk-reason"
    assert config.fast.model == "deepseek-chat"
    assert config.fast.api_key == "sk-fast"


def test_ensure_runtime_llm_config_bootstraps_from_example_file(tmp_path: Path) -> None:
    example_path = tmp_path / "llm.example.json"
    runtime_path = tmp_path / "llm.local.json"
    example_payload = {
        "enabled": False,
        "fastModel": {
            "enabled": False,
            "baseUrl": "",
            "apiKey": "",
            "model": "",
            "timeoutMs": 12000,
        },
        "reasoningModel": {
            "enabled": False,
            "baseUrl": "",
            "apiKey": "",
            "model": "",
            "timeoutMs": 30000,
        },
    }
    example_path.write_text(json.dumps(example_payload), encoding="utf-8")

    result_path = ensure_runtime_llm_config(runtime_path=runtime_path, example_path=example_path)

    assert result_path == runtime_path
    assert json.loads(runtime_path.read_text(encoding="utf-8")) == example_payload

def test_dual_model_client_flags_realtime_capabilities_based_on_fast_model(tmp_path: Path) -> None:
    payload = {
        "enabled": True,
        "fastModel": {
            "enabled": True,
            "baseUrl": "https://fast.example/v1",
            "apiKey": "fast-secret",
            "model": "deepseek-chat",
            "timeoutMs": 12000,
        },
        "reasoningModel": {
            "enabled": True,
            "baseUrl": "https://reason.example/v1",
            "apiKey": "reason-secret",
            "model": "deepseek-reasoner",
            "timeoutMs": 30000,
        },
    }

    client = DualModelLlmClient(load_llm_config_from_payload(payload, tmp_path))

    assert client.allow_realtime_analysis is True
    assert client.allow_realtime_synthesis is True
    assert client.minimum_synthesis_timeout_seconds == 4.0


def test_openai_compatible_client_recreates_underlying_client_when_event_loop_changes() -> None:
    created_clients: list[LoopBoundFakeClient] = []

    def factory() -> LoopBoundFakeClient:
        client = LoopBoundFakeClient()
        created_clients.append(client)
        return client

    client = OpenAiCompatibleLlmClient(
        ModelConfig(
            enabled=True,
            base_url="https://llm.example/v1",
            api_key="secret",
            model="deepseek-chat",
            timeout_ms=12000,
        ),
        client_factory=factory,
    )

    async def run_once() -> dict:
        return await client._chat_json(
            [
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": "{}"},
            ]
        )

    first = asyncio.run(run_once())
    second = asyncio.run(run_once())

    assert first == {}
    assert second == {}
    assert len(created_clients) == 2


def test_openai_compatible_client_retries_once_after_transport_timeout() -> None:
    attempts = 0

    class TimeoutOnceClient:
        async def post(self, *args, **kwargs) -> "_FakeResponse":
            nonlocal attempts
            del args, kwargs
            attempts += 1
            if attempts == 1:
                raise httpx.ReadTimeout("timed out")
            return _FakeResponse()

        async def aclose(self) -> None:
            return None

    client = OpenAiCompatibleLlmClient(
        ModelConfig(
            enabled=True,
            base_url="https://llm.example/v1",
            api_key="secret",
            model="deepseek-chat",
            timeout_ms=12000,
        ),
        client_factory=TimeoutOnceClient,
    )

    async def run_once() -> dict:
        return await client._chat_json(
            [
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": "{}"},
            ]
        )

    payload = asyncio.run(run_once())

    assert payload == {}
    assert attempts == 2


def test_dual_model_client_prefers_fast_model_for_realtime_synthesis(tmp_path: Path) -> None:
    class FakeFastClient:
        minimum_synthesis_timeout_seconds = 4.0

        async def analyze_input(self, raw_text: str, work_type_hint: str) -> dict[str, str]:
            del raw_text, work_type_hint
            return {}

        async def synthesize(self, draft, profile, records) -> dict[str, object]:
            del draft, profile, records
            return {"summary": "fast", "notes": "", "warnings": [], "acceptedUrls": ["fast-url"]}

    class FakeReasoningClient:
        minimum_synthesis_timeout_seconds = 18.0

        async def analyze_input(self, raw_text: str, work_type_hint: str) -> dict[str, str]:
            del raw_text, work_type_hint
            return {}

        async def synthesize(self, draft, profile, records) -> dict[str, object]:
            del draft, profile, records
            raise AssertionError("reasoning client should not be called when fast model is available")

    client = DualModelLlmClient(load_llm_config_from_payload({"enabled": False}, tmp_path))
    client._fast = FakeFastClient()
    client._reasoning = FakeReasoningClient()

    payload = asyncio.run(client.synthesize(None, None, []))

    assert payload["acceptedUrls"] == ["fast-url"]


class LoopBoundFakeClient:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None

    async def post(self, *args, **kwargs) -> "_FakeResponse":
        del args, kwargs
        current_loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = current_loop
        elif self._loop is not current_loop:
            raise RuntimeError("Event loop is closed")
        return _FakeResponse()


class _FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": "{}"}}]}


def load_llm_config_from_payload(payload: dict, tmp_path: Path) -> object:
    path = tmp_path / ".llm-config-test.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return load_llm_config(path)
