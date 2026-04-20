from __future__ import annotations

import asyncio
import json
import os
import threading
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import httpx

from app.services.pipeline import DraftRecordingEntry, RetrievalProfile, SourceRecord
from app.services.source_profiles import materials_root


@dataclass(slots=True)
class ModelConfig:
    enabled: bool = False
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    timeout_ms: int = 30000


@dataclass(slots=True)
class LlmConfigBundle:
    fast: ModelConfig
    reasoning: ModelConfig


def runtime_config_root() -> Path:
    app_data_dir = str(os.getenv("ICM_APP_DATA_DIR", "")).strip()
    if app_data_dir:
        return Path(app_data_dir).resolve() / "retrieval-service" / "config"
    return Path(__file__).resolve().parents[2] / "config"


def default_llm_config_path() -> Path:
    return runtime_config_root() / "llm.local.json"


def example_llm_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "llm.example.json"


def legacy_llm_config_path() -> Path:
    return materials_root() / "LLM config.txt"


def default_llm_config_payload() -> dict[str, Any]:
    return {
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


def _normalize_model_config(payload: dict[str, Any]) -> ModelConfig:
    return ModelConfig(
        enabled=bool(payload.get("enabled", False)),
        base_url=str(payload.get("baseUrl", "")).strip().rstrip("/"),
        api_key=str(payload.get("apiKey", "")).strip(),
        model=str(payload.get("model", "")).strip(),
        timeout_ms=int(payload.get("timeoutMs", 30000) or 30000),
    )


def _legacy_entries(source_path: Path) -> list[ModelConfig]:
    entries: list[ModelConfig] = []
    current: list[str] = []
    for raw_line in source_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith(":"):
            if len(current) >= 3:
                entries.append(
                    ModelConfig(
                        enabled=True,
                        base_url=current[0].rstrip("/"),
                        api_key=current[1],
                        model=current[2],
                        timeout_ms=30000,
                    )
                )
            current = []
            continue
        current.append(line)
    if len(current) >= 3:
        entries.append(
            ModelConfig(
                enabled=True,
                base_url=current[0].rstrip("/"),
                api_key=current[1],
                model=current[2],
                timeout_ms=30000,
            )
        )
    return entries


def migrate_legacy_llm_config(legacy_path: Path | None = None, runtime_path: Path | None = None) -> bool:
    source_path = legacy_path or legacy_llm_config_path()
    target_path = runtime_path or default_llm_config_path()
    if not source_path.is_file():
        return False

    target_path.parent.mkdir(parents=True, exist_ok=True)
    entries = _legacy_entries(source_path)
    if not entries:
        return False

    reasoning = next((entry for entry in entries if "reasoner" in entry.model.lower()), entries[0])
    fast = next((entry for entry in entries if "reasoner" not in entry.model.lower()), entries[0])

    payload = {
        "enabled": True,
        "fastModel": {
            "enabled": fast.enabled,
            "baseUrl": fast.base_url,
            "apiKey": fast.api_key,
            "model": fast.model,
            "timeoutMs": fast.timeout_ms,
        },
        "reasoningModel": {
            "enabled": reasoning.enabled,
            "baseUrl": reasoning.base_url,
            "apiKey": reasoning.api_key,
            "model": reasoning.model,
            "timeoutMs": reasoning.timeout_ms,
        },
    }
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def ensure_runtime_llm_config(runtime_path: Path | None = None, example_path: Path | None = None) -> Path:
    target_path = runtime_path or default_llm_config_path()
    if target_path.is_file():
        return target_path

    target_path.parent.mkdir(parents=True, exist_ok=True)
    source_path = example_path or example_llm_config_path()
    if source_path.is_file():
        target_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        target_path.write_text(json.dumps(default_llm_config_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


def load_llm_config(path: Path | None = None) -> LlmConfigBundle:
    config_path = path or default_llm_config_path()
    if path is None:
        migrate_legacy_llm_config()
        ensure_runtime_llm_config(config_path)

    payload: dict[str, Any] = {}
    if config_path.is_file():
        payload = json.loads(config_path.read_text(encoding="utf-8"))

    if "fastModel" in payload or "reasoningModel" in payload:
        fast_payload = dict(payload.get("fastModel") or {})
        reasoning_payload = dict(payload.get("reasoningModel") or {})
    else:
        single_payload = {
            "enabled": bool(payload.get("enabled", False)),
            "baseUrl": payload.get("baseUrl", os.getenv("RECORDING_RETRIEVAL_LLM_BASE_URL", "")),
            "apiKey": payload.get("apiKey", os.getenv("RECORDING_RETRIEVAL_LLM_API_KEY", "")),
            "model": payload.get("model", os.getenv("RECORDING_RETRIEVAL_LLM_MODEL", "")),
            "timeoutMs": payload.get("timeoutMs", os.getenv("RECORDING_RETRIEVAL_LLM_TIMEOUT_MS", "30000")),
        }
        if "reasoner" in str(single_payload.get("model", "")).lower():
            fast_payload = {"enabled": False}
            reasoning_payload = single_payload
        else:
            fast_payload = single_payload
            reasoning_payload = {"enabled": False}

    return LlmConfigBundle(
        fast=_normalize_model_config(fast_payload),
        reasoning=_normalize_model_config(reasoning_payload),
    )


def is_model_configured(config: ModelConfig | None) -> bool:
    return bool(config and config.enabled and config.base_url and config.api_key and config.model)


def is_llm_configured(config: LlmConfigBundle | None) -> bool:
    return bool(config and (is_model_configured(config.fast) or is_model_configured(config.reasoning)))


class OpenAiCompatibleLlmClient:
    def __init__(
        self,
        config: ModelConfig,
        client: httpx.AsyncClient | None = None,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        self._config = config
        self._client_factory = client_factory or (lambda: client or httpx.AsyncClient(timeout=config.timeout_ms / 1000))
        self._loop_clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, httpx.AsyncClient] = weakref.WeakKeyDictionary()
        self._loop_clients_lock = threading.Lock()

    @property
    def minimum_synthesis_timeout_seconds(self) -> float:
        return 18.0 if "reasoner" in self._config.model.lower() else 4.0

    async def analyze_input(self, raw_text: str, work_type_hint: str) -> dict[str, Any]:
        if not is_model_configured(self._config) or not raw_text.strip():
            return {}

        messages = [
            {
                "role": "system",
                "content": (
                    "You extract structured classical-recording search inputs from user text. "
                    "Return strict JSON only. Allowed keys: "
                    "composerName, composerNameLatin, workTitle, workTitleLatin, catalogue, "
                    "primaryPerson, primaryPersonLatin, secondaryPerson, secondaryPersonLatin, "
                    "groupName, groupNameLatin, performanceDateText, title. "
                    "Use empty strings for missing values. Do not invent facts. "
                    "For concerto prefer primaryPerson=soloist and secondaryPerson=conductor. "
                    "For orchestral prefer primaryPerson=conductor. "
                    "For opera_vocal prefer primaryPerson=conductor and secondaryPerson=important singers. "
                    "For chamber_solo prefer primaryPerson=main performer and secondaryPerson=collaborator or ensemble lead. "
                    "Keep Chinese names in non-Latin fields and Latin/original names in Latin fields."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "workTypeHint": work_type_hint,
                        "rawText": raw_text,
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        return await self._chat_json(messages)

    async def synthesize(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        records: list[SourceRecord],
    ) -> dict[str, Any]:
        if not is_model_configured(self._config):
            return {}
        ranked_records = sorted(records, key=lambda record: record.same_recording_score, reverse=True)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a strict classical recording matcher. "
                    "Use only the provided evidence. Do not invent facts. "
                    "Return strict JSON with exactly these keys: summary, notes, warnings, acceptedUrls. "
                    "acceptedUrls must include only URLs that clearly point to the same recording version as the draft. "
                    "Be conservative. summary and notes must be concise Chinese text. warnings must be short Chinese strings."
                ),
            },
            {
                "role": "user",
                "content": build_synthesis_prompt(draft, profile, ranked_records[:10]),
            },
        ]
        return await self._chat_json(messages)

    async def _chat_json(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        request_payload = {
            "model": self._config.model,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 1024 if "reasoner" in self._config.model.lower() else 384,
            "response_format": {"type": "json_object"},
        }
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                response = await self._get_client().post(
                    f"{self._config.base_url}/chat/completions",
                    headers={
                        "content-type": "application/json",
                        "authorization": f"Bearer {self._config.api_key}",
                    },
                    json=request_payload,
                )
                response.raise_for_status()
                payload = response.json()
                content = str(payload.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
                return normalize_llm_payload(parse_json_object(content))
            except (httpx.TimeoutException, httpx.TransportError) as error:
                last_error = error
                self._drop_client_for_current_loop()
                if attempt == 0:
                    continue
                raise
        if last_error is not None:
            raise last_error
        return {}

    def _get_client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        with self._loop_clients_lock:
            client = self._loop_clients.get(loop)
            if client is None:
                client = self._client_factory()
                self._loop_clients[loop] = client
            return client

    def _drop_client_for_current_loop(self) -> None:
        loop = asyncio.get_running_loop()
        with self._loop_clients_lock:
            client = self._loop_clients.pop(loop, None)
        if client is not None:
            try:
                asyncio.create_task(client.aclose())
            except Exception:
                pass


class DualModelLlmClient:
    def __init__(self, config: LlmConfigBundle) -> None:
        self._fast = OpenAiCompatibleLlmClient(config.fast) if is_model_configured(config.fast) else None
        self._reasoning = OpenAiCompatibleLlmClient(config.reasoning) if is_model_configured(config.reasoning) else None

    @property
    def minimum_synthesis_timeout_seconds(self) -> float:
        if self._fast is not None:
            return self._fast.minimum_synthesis_timeout_seconds
        if self._reasoning is not None:
            return self._reasoning.minimum_synthesis_timeout_seconds
        return 0.0

    @property
    def allow_realtime_analysis(self) -> bool:
        return self._fast is not None

    @property
    def allow_realtime_synthesis(self) -> bool:
        return self._fast is not None or self._reasoning is not None

    async def analyze_input(self, raw_text: str, work_type_hint: str) -> dict[str, Any]:
        if self._fast is not None:
            return await self._fast.analyze_input(raw_text, work_type_hint)
        if self._reasoning is not None:
            return await self._reasoning.analyze_input(raw_text, work_type_hint)
        return {}

    async def synthesize(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        records: list[SourceRecord],
    ) -> dict[str, Any]:
        if self._fast is not None:
            try:
                return await self._fast.synthesize(draft, profile, records)
            except Exception:
                if self._reasoning is not None:
                    return await self._reasoning.synthesize(draft, profile, records)
                raise
        if self._reasoning is not None:
            return await self._reasoning.synthesize(draft, profile, records)
        return {}


def should_use_reasoning(
    records: list[SourceRecord],
    *,
    draft: DraftRecordingEntry | None = None,
    profile: RetrievalProfile | None = None,
) -> bool:
    if len(records) < 5:
        if not (profile and profile.category == "chamber_solo" and draft and len(draft.lead_names_latin or draft.lead_names) <= 1):
            return False
    scores = sorted((record.same_recording_score for record in records), reverse=True)
    if not scores or scores[0] < 0.55:
        return False
    if len(scores) == 1:
        return False
    if profile and profile.category == "chamber_solo" and draft and len(draft.lead_names_latin or draft.lead_names) <= 1:
        return abs(scores[0] - scores[1]) <= 0.14
    return abs(scores[0] - scores[1]) <= 0.06


def parse_json_object(content: str) -> dict[str, Any]:
    normalized = content.strip()
    if normalized.startswith("```"):
        normalized = normalized.split("```", 2)[1].strip()
        if normalized.lower().startswith("json"):
            normalized = normalized[4:].strip()
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_synthesis_prompt(draft: DraftRecordingEntry, profile: RetrievalProfile, records: list[SourceRecord]) -> str:
    lines = [
        "Decide whether any candidate URLs clearly match the same recording version as the draft.",
        "Return JSON only with keys: summary, notes, warnings, acceptedUrls.",
        "summary and notes must be concise Chinese text.",
        "warnings must be an array of concise Chinese strings.",
        "acceptedUrls must be an array of confirmed URLs.",
        "",
        "Draft:",
        f"title={draft.title}",
        f"composer={draft.composer_name} | {draft.composer_name_latin}",
        f"work={draft.work_title} | {draft.work_title_latin}",
        f"catalogue={draft.catalogue}",
        f"person={', '.join(draft.lead_names_latin or draft.lead_names)}",
        f"ensemble={', '.join(draft.ensemble_names_latin or draft.ensemble_names)}",
        f"year={draft.performance_date_text}",
        f"category={profile.category}",
        f"titleHint={draft.title}",
        "",
        "Records:",
    ]
    for index, record in enumerate(records, start=1):
        lines.append(
            f"{index}. score={record.same_recording_score:.2f} url={record.url} platform={record.platform} "
            f"title={record.title} desc={record.description} fields={json.dumps(record.fields, ensure_ascii=False)}"
        )
    lines.append("")
    lines.append("Reject movement-only, aria-only, wrong-year, wrong-ensemble or biography pages.")
    lines.append("If titleHint implies a collaboration pair but credits are sparse, use that hint conservatively.")
    return "\n".join(lines)


def normalize_llm_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    warnings = payload.get("warnings", [])
    accepted_urls = payload.get("acceptedUrls", [])
    if isinstance(warnings, str):
        warnings = [warnings] if warnings.strip() else []
    if isinstance(accepted_urls, str):
        accepted_urls = [accepted_urls] if accepted_urls.strip() else []
    payload["warnings"] = [str(item).strip() for item in warnings if str(item).strip()]
    payload["acceptedUrls"] = [str(item).strip() for item in accepted_urls if str(item).strip()]
    if "summary" in payload:
        payload["summary"] = str(payload.get("summary", "")).strip()
    if "notes" in payload:
        payload["notes"] = str(payload.get("notes", "")).strip()
    return payload
