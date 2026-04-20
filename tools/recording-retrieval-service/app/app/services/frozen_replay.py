from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from app.services.pipeline import DraftRecordingEntry, RetrievalProfile, SourceRecord


def clone_jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


class RecordingSourceProvider:
    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self._snapshots: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)

    async def inspect_existing_links(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        payload = await self._provider.inspect_existing_links(draft, profile)
        self._snapshots[draft.item_id]["inspect_existing_links"] = clone_jsonable(payload)
        return payload

    async def search_high_quality(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        payload = await self._provider.search_high_quality(draft, profile)
        self._snapshots[draft.item_id]["search_high_quality"] = clone_jsonable(payload)
        return payload

    async def search_streaming(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        payload = await self._provider.search_streaming(draft, profile)
        self._snapshots[draft.item_id]["search_streaming"] = clone_jsonable(payload)
        return payload

    async def search_fallback(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        payload = await self._provider.search_fallback(draft, profile)
        self._snapshots[draft.item_id]["search_fallback"] = clone_jsonable(payload)
        return payload

    async def aclose(self) -> None:
        close = getattr(self._provider, "aclose", None)
        if callable(close):
            await close()

    def consume_stage_snapshots(self) -> dict[str, dict[str, list[dict[str, Any]]]]:
        payload = clone_jsonable(self._snapshots)
        self._snapshots.clear()
        return payload


class FrozenSourceProvider:
    def __init__(self, stage_snapshots: dict[str, dict[str, list[dict[str, Any]]]]) -> None:
        self._stage_snapshots = clone_jsonable(stage_snapshots)

    async def inspect_existing_links(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        del profile
        return clone_jsonable(self._stage_snapshots.get(draft.item_id, {}).get("inspect_existing_links", []))

    async def search_high_quality(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        del profile
        return clone_jsonable(self._stage_snapshots.get(draft.item_id, {}).get("search_high_quality", []))

    async def search_streaming(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        del profile
        return clone_jsonable(self._stage_snapshots.get(draft.item_id, {}).get("search_streaming", []))

    async def search_fallback(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]:
        del profile
        return clone_jsonable(self._stage_snapshots.get(draft.item_id, {}).get("search_fallback", []))

    async def aclose(self) -> None:
        return None


class RecordingLlmClient:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._snapshots: dict[str, dict[str, Any]] = {}

    @property
    def minimum_synthesis_timeout_seconds(self) -> float:
        return float(getattr(self._client, "minimum_synthesis_timeout_seconds", 0.0) or 0.0)

    @property
    def allow_realtime_synthesis(self) -> bool:
        return bool(getattr(self._client, "allow_realtime_synthesis", True))

    async def synthesize(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        records: list[SourceRecord],
    ) -> dict[str, Any]:
        payload = await self._client.synthesize(draft, profile, records)
        self._snapshots[draft.item_id] = clone_jsonable(payload)
        return payload

    def consume_synthesis_snapshots(self) -> dict[str, dict[str, Any]]:
        payload = clone_jsonable(self._snapshots)
        self._snapshots.clear()
        return payload


class FrozenLlmClient:
    def __init__(self, synthesis_snapshots: dict[str, dict[str, Any]]) -> None:
        self._synthesis_snapshots = clone_jsonable(synthesis_snapshots)
        self.minimum_synthesis_timeout_seconds = 0.0
        self.allow_realtime_synthesis = True

    async def synthesize(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        records: list[SourceRecord],
    ) -> dict[str, Any]:
        del profile, records
        payload = self._synthesis_snapshots.get(draft.item_id)
        if payload is None:
            return {"summary": "", "notes": "", "warnings": [], "acceptedUrls": []}
        return clone_jsonable(payload)
