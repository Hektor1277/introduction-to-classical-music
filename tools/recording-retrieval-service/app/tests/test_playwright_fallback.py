from __future__ import annotations

import asyncio

import httpx

from app.services.http_sources import HttpSourceProvider
from app.services.pipeline import DraftRecordingEntry, RetrievalProfile
from app.services.source_profiles import SourceProfileLoader


class EmptyHttpTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, text="<html><head><title></title></head><body></body></html>")


class FakeBrowserFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str]:
        del timeout_seconds
        self.calls.append(url)
        return {
            "title": "Mahler Symphony No. 5 | Live in Berlin 1993",
            "description": "Berliner Philharmoniker. Label: DG. Release 1994.",
        }


class FailingBrowserFetcher:
    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str]:
        del url, timeout_seconds
        raise RuntimeError("browser unavailable")


def build_provider(tmp_path, browser_fetcher):
    root = tmp_path / "source-profiles"
    (root / "high-quality").mkdir(parents=True)
    (root / "streaming").mkdir(parents=True)
    (root / "high-quality" / "global.txt").write_text("https://catalog.example\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=EmptyHttpTransport(), follow_redirects=True)
    return HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=browser_fetcher,
    )


def sample_draft() -> DraftRecordingEntry:
    return DraftRecordingEntry(
        item_id="recording-1",
        title="Abbado Mahler 5",
        composer_name="马勒",
        composer_name_latin="Gustav Mahler",
        work_title="第五交响曲",
        work_title_latin="Symphony No. 5",
        catalogue="",
        performance_date_text="1993",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Abbado | Berlin | 1993",
        raw_text="Abbado | Berlin | 1993",
        existing_links=[{"platform": "other", "url": "https://catalog.example/recording", "title": ""}],
        lead_names=["Claudio Abbado"],
        ensemble_names=["Berliner Philharmoniker"],
    )


def sample_profile() -> RetrievalProfile:
    return RetrievalProfile(category="orchestral", tags=["live"], queries=["Mahler Symphony No. 5 Abbado Berlin 1993"])


def test_existing_link_uses_browser_fallback_when_http_metadata_is_empty(tmp_path) -> None:
    browser_fetcher = FakeBrowserFetcher()
    provider = build_provider(tmp_path, browser_fetcher)

    records = asyncio.run(provider.inspect_existing_links(sample_draft(), sample_profile()))

    assert browser_fetcher.calls == ["https://catalog.example/recording"]
    assert records[0]["title"] == "Mahler Symphony No. 5 | Live in Berlin 1993"
    assert records[0]["fields"]["label"] == "DG"


def test_browser_fallback_failure_degrades_without_crashing(tmp_path) -> None:
    provider = build_provider(tmp_path, FailingBrowserFetcher())

    records = asyncio.run(provider.inspect_existing_links(sample_draft(), sample_profile()))

    assert len(records) == 1
    assert records[0]["title"] == ""
    assert records[0]["description"] == ""
