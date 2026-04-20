from __future__ import annotations

import asyncio

from app.services.pipeline import DraftRecordingEntry, RetrievalProfile
from app.services.frozen_replay import (
    FrozenLlmClient,
    FrozenSourceProvider,
    RecordingLlmClient,
    RecordingSourceProvider,
)


def sample_draft(*, item_id: str = "item-1") -> DraftRecordingEntry:
    return DraftRecordingEntry(
        item_id=item_id,
        title="Schumann Piano Concerto",
        composer_name="Robert Schumann",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="March 8, 1980",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto, Op.54 | Radu Lupu | Giulini | 1980",
        raw_text="Robert Schumann Piano Concerto, Op.54 Radu Lupu Giulini 1980",
        existing_links=[],
        primary_names=["拉度·鲁普"],
        primary_names_latin=["Radu Lupu"],
        secondary_names=["卡罗·马里亚·朱里尼"],
        secondary_names_latin=["Carlo Maria Giulini"],
        query_lead_names=["拉度·鲁普", "卡罗·马里亚·朱里尼"],
        query_lead_names_latin=["Radu Lupu", "Carlo Maria Giulini"],
        lead_names=["拉度·鲁普", "卡罗·马里亚·朱里尼"],
        lead_names_latin=["Radu Lupu", "Carlo Maria Giulini"],
        ensemble_names=["洛杉矶爱乐乐团"],
        ensemble_names_latin=["Los Angeles Philharmonic"],
    )


def sample_profile() -> RetrievalProfile:
    return RetrievalProfile(
        category="concerto",
        tags=["piano"],
        queries=["Schumann Piano Concerto Radu Lupu Giulini 1980"],
        latin_queries=["Schumann Piano Concerto Op.54 Radu Lupu Giulini 1980"],
        zh_queries=["舒曼 钢琴协奏曲 鲁普 朱里尼 1980"],
        mixed_queries=[],
    )


class FakeSourceProvider:
    async def inspect_existing_links(self, draft, profile):
        del profile
        return [{"url": f"https://existing.example/{draft.item_id}", "title": "existing"}]

    async def search_high_quality(self, draft, profile):
        del profile
        return [{"url": f"https://hq.example/{draft.item_id}", "title": "hq"}]

    async def search_streaming(self, draft, profile):
        del profile
        return [{"url": f"https://stream.example/{draft.item_id}", "title": "stream"}]

    async def search_fallback(self, draft, profile):
        del profile
        return [{"url": f"https://fallback.example/{draft.item_id}", "title": "fallback"}]

    async def aclose(self) -> None:
        return None


class FakeLlmClient:
    async def synthesize(self, draft, profile, records):
        del profile, records
        return {
            "summary": f"summary-{draft.item_id}",
            "notes": "",
            "warnings": [f"warning-{draft.item_id}"],
            "acceptedUrls": [f"https://accepted.example/{draft.item_id}"],
        }


def test_recording_source_provider_captures_stage_payloads_per_item() -> None:
    provider = RecordingSourceProvider(FakeSourceProvider())
    draft = sample_draft(item_id="item-a")
    profile = sample_profile()

    assert asyncio.run(provider.inspect_existing_links(draft, profile))[0]["url"].endswith("/item-a")
    assert asyncio.run(provider.search_streaming(draft, profile))[0]["url"].endswith("/item-a")

    snapshots = provider.consume_stage_snapshots()

    assert snapshots == {
        "item-a": {
            "inspect_existing_links": [{"url": "https://existing.example/item-a", "title": "existing"}],
            "search_streaming": [{"url": "https://stream.example/item-a", "title": "stream"}],
        }
    }


def test_frozen_source_provider_replays_item_scoped_stage_payloads() -> None:
    provider = FrozenSourceProvider(
        {
            "item-a": {
                "search_high_quality": [{"url": "https://hq.example/item-a", "title": "hq"}],
            }
        }
    )

    first = asyncio.run(provider.search_high_quality(sample_draft(item_id="item-a"), sample_profile()))
    second = asyncio.run(provider.search_high_quality(sample_draft(item_id="item-a"), sample_profile()))
    second[0]["title"] = "mutated"

    assert first == [{"url": "https://hq.example/item-a", "title": "hq"}]
    assert asyncio.run(provider.search_high_quality(sample_draft(item_id="item-a"), sample_profile())) == [
        {"url": "https://hq.example/item-a", "title": "hq"}
    ]
    assert asyncio.run(provider.search_fallback(sample_draft(item_id="missing"), sample_profile())) == []


def test_recording_llm_client_captures_payloads_per_item() -> None:
    llm = RecordingLlmClient(FakeLlmClient())

    payload = asyncio.run(llm.synthesize(sample_draft(item_id="item-a"), sample_profile(), []))

    assert payload["acceptedUrls"] == ["https://accepted.example/item-a"]
    assert llm.consume_synthesis_snapshots() == {
        "item-a": {
            "summary": "summary-item-a",
            "notes": "",
            "warnings": ["warning-item-a"],
            "acceptedUrls": ["https://accepted.example/item-a"],
        }
    }


def test_frozen_llm_client_replays_payloads_and_defaults_to_empty_response() -> None:
    llm = FrozenLlmClient(
        {
            "item-a": {
                "summary": "frozen-summary",
                "notes": "frozen-notes",
                "warnings": ["frozen-warning"],
                "acceptedUrls": ["https://accepted.example/item-a"],
            }
        }
    )

    assert asyncio.run(llm.synthesize(sample_draft(item_id="item-a"), sample_profile(), [])) == {
        "summary": "frozen-summary",
        "notes": "frozen-notes",
        "warnings": ["frozen-warning"],
        "acceptedUrls": ["https://accepted.example/item-a"],
    }
    assert asyncio.run(llm.synthesize(sample_draft(item_id="missing"), sample_profile(), [])) == {
        "summary": "",
        "notes": "",
        "warnings": [],
        "acceptedUrls": [],
    }
