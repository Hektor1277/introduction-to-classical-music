from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from urllib.parse import quote_plus

import httpx
import pytest

from app.models.protocol import Credit, LinkCandidate, RetrievalItem, Seed
from app.services.browser_fetcher import (
    normalize_search_result_payload,
    prune_browser_diagnostic_files,
)
from app.services.http_sources import (
    HttpSourceProvider,
    bilibili_chamber_solo_person_tokens,
    build_bilibili_chamber_solo_opus_probe_queries,
    build_bilibili_chamber_duo_trailer_probe_queries,
    build_bilibili_metadata_from_detail,
    build_chinese_host_bundle_context_queries,
    build_chinese_host_primary_work_rescue_queries,
    build_work_aliases,
    contains_cjk,
    dedupe_streaming_hosts_for_execution,
    extract_bilibili_result_links,
    extract_bing_result_links,
    extract_cjk_person_query_keyword,
    extract_person_query_keyword,
    looks_like_single_movement,
    merge_bilibili_browser_query_rows,
    merge_bilibili_search_rows,
    merge_streaming_host_rows,
    name_matches,
    normalize_host,
    normalize_text,
    prepare_bilibili_browser_queries,
    score_recording_match,
    select_bilibili_browser_queries,
    should_force_chamber_solo_second_pass_for_bilibili,
    should_probe_apple_auxiliary_hosts,
    should_expand_initial_streaming_window,
    streaming_host_priority,
)
from app.services.parent_work_eval import build_work_dataset, find_work_id as resolve_work_id, load_library_indices
from app.services.pipeline import (
    DraftRecordingEntry,
    InputNormalizer,
    ProfileResolver,
    RetrievalProfile,
    candidate_title_quality_score,
    candidate_conflicting_credit_tokens,
    classify_link_candidate_zone,
)
from app.services.platform_clients import BilibiliVideoDetail
from app.services.platform_search_config import (
    AppleMusicSearchConfig,
    BilibiliSearchConfig,
    PlatformSearchConfig,
    YouTubeSearchConfig,
)
from app.services.source_profiles import OrchestraAliasLoader, SourceProfileEntry, SourceProfileLoader
from app.services.source_profiles import PersonAliasLoader


def find_work_id(*, works: dict[str, dict], work_id: str = "", title_latin: str = "", title: str = "") -> str:
    try:
        return resolve_work_id(works=works, work_id=work_id, title_latin=title_latin, title=title)
    except KeyError:
        selector = title_latin or title or work_id or "<unknown>"
        pytest.skip(f"public repository regression dataset does not include required work selector: {selector}")


class SearchTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "bing.com" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><body>
                  <li class="b_algo"><h2><a href="https://catalog.example/releases/recording-2">hit</a></h2></li>
                </body></html>
                """,
            )
        if "youtube.com/results" in url:
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"abc123xyz00","title":{"runs":[{"text":"Klemperer LSO Tchaikovsky 5"}]}}}',
            )
        if "catalog.example" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head>
                  <title>Tchaikovsky Symphony No. 5 - Klemperer - LSO - 1964</title>
                  <meta property="og:description" content="Recorded live in London. Label: EMI. Release 1965." />
                  <meta property="og:image" content="https://catalog.example/cover.jpg" />
                </head><body>Otto Klemperer London Symphony Orchestra 1964</body></html>
                """,
            )
        if "youtube.com/watch" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head>
                  <title>Klemperer LSO Tchaikovsky 5</title>
                  <meta property="og:description" content="London Symphony Orchestra, 1964." />
                </head><body>Otto Klemperer London Symphony Orchestra 1964</body></html>
                """,
            )
        return httpx.Response(404, request=request, text="not found")


class CountingSearchTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.requests.append(url)
        if "youtube.com/results" in url:
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"abc123xyz00","title":{"runs":[{"text":"Klemperer LSO Tchaikovsky 5"}]}}}',
            )
        if "youtube.com/watch" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head>
                  <title>Klemperer LSO Tchaikovsky 5</title>
                  <meta property="og:description" content="London Symphony Orchestra, 1964." />
                </head><body>Otto Klemperer London Symphony Orchestra 1964</body></html>
                """,
            )
        if "bilibili.com" in url:
            return httpx.Response(500, request=request, text="should not be called")
        return httpx.Response(404, request=request, text="not found")


class MultiQueryYouTubeTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/results" in url:
            if "Otto+Klemperer+1960" in url:
                return httpx.Response(
                    200,
                    request=request,
                    text='{"videoRenderer":{"videoId":"later000001","title":{"runs":[{"text":"Better Klemperer result"}]}}}',
                )
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"first000001","title":{"runs":[{"text":"Movement only"}]}}}',
            )
        if "youtube.com/watch?v=first000001" in url:
            return httpx.Response(
                200,
                request=request,
                text="""<html><head><title>Symphony No. 5 in C Minor, Op. 67: I. Allegro con brio</title></head><body></body></html>""",
            )
        if "youtube.com/watch?v=later000001" in url:
            return httpx.Response(
                200,
                request=request,
                text="""<html><head><title>Beethoven - Symphony No 5 in C minor, Op 67 - Klemperer</title><meta property="og:description" content="Philharmonia Orchestra 1960." /></head><body>Otto Klemperer Philharmonia Orchestra 1960</body></html>""",
            )
        return httpx.Response(404, request=request, text="not found")


class QueryRecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        if "youtube.com/results" in url or "bilibili.com" in url:
            return httpx.Response(200, request=request, text="")
        return httpx.Response(404, request=request, text="not found")


class LaterQueryYouTubeTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/results" not in url:
            return httpx.Response(404, request=request, text="not found")
        if "query-one" in url:
            text = "".join(
                f'{{"videoRenderer":{{"videoId":"q1{i:02d}","title":{{"runs":[{{"text":"query one {i}"}}]}}}}}}'
                for i in range(4)
            )
            return httpx.Response(200, request=request, text=text)
        if "query-two" in url:
            text = "".join(
                f'{{"videoRenderer":{{"videoId":"q2{i:02d}","title":{{"runs":[{{"text":"query two {i}"}}]}}}}}}'
                for i in range(4)
            )
            return httpx.Response(200, request=request, text=text)
        text = '{"videoRenderer":{"videoId":"later-hit-01","title":{"runs":[{"text":"later hit"}]}}}'
        return httpx.Response(200, request=request, text=text)


class DeepResultYouTubeTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/results" not in url:
            return httpx.Response(404, request=request, text="not found")
        text = "".join(
            f'{{"videoRenderer":{{"videoId":"junk{i:02d}","title":{{"runs":[{{"text":"junk {i}"}}]}}}}}}'
            for i in range(4)
        )
        text += "".join(
            f'{{"videoRenderer":{{"videoId":"deep{i:02d}","title":{{"runs":[{{"text":"deep {i}"}}]}}}}}}'
            for i in range(1, 5)
        )
        return httpx.Response(200, request=request, text=text)


class QueryCoverageYouTubeTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/results" not in url:
            return httpx.Response(404, request=request, text="not found")
        if "generic-one" in url:
            text = "".join(
                f'{{"videoRenderer":{{"videoId":"generica{i:02d}","title":{{"runs":[{{"text":"generic a {i}"}}]}}}}}}'
                for i in range(1, 13)
            )
            return httpx.Response(200, request=request, text=text)
        if "generic-two" in url:
            text = "".join(
                f'{{"videoRenderer":{{"videoId":"genericb{i:02d}","title":{{"runs":[{{"text":"generic b {i}"}}]}}}}}}'
                for i in range(1, 13)
            )
            return httpx.Response(200, request=request, text=text)
        text = '{"videoRenderer":{"videoId":"alias-hit-01","title":{"runs":[{"text":"alias hit"}]}}}'
        return httpx.Response(200, request=request, text=text)


class RankedLaterQueryYouTubeTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/results" not in url:
            return httpx.Response(404, request=request, text="not found")
        if "generic+query" in url:
            text = "".join(
                f'{{"videoRenderer":{{"videoId":"generic{i:02d}","title":{{"runs":[{{"text":"generic {i}"}}]}}}}}}'
                for i in range(1, 9)
            )
            return httpx.Response(200, request=request, text=text)
        if "exact+query" in url:
            text = '{"videoRenderer":{"videoId":"exact-hit-01","title":{"runs":[{"text":"exact hit"}]}}}'
            text += "".join(
                f'{{"videoRenderer":{{"videoId":"exactf{i:02d}","title":{{"runs":[{{"text":"exact filler {i}"}}]}}}}}}'
                for i in range(1, 8)
            )
            return httpx.Response(200, request=request, text=text)
        text = '{"videoRenderer":{"videoId":"fallback-late-01","title":{"runs":[{"text":"fallback late"}]}}}'
        return httpx.Response(200, request=request, text=text)


class HostSliceAwareProvider(HttpSourceProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hydrated_urls: list[str] = []

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile, host
        return [
            {"url": f"https://stream.example/{index}", "source_label": "Streaming Search", "source_kind": "streaming"}
            for index in range(1, 13)
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        self.hydrated_urls = [row["url"] for row in rows]
        return rows


class ParallelHostProvider(HttpSourceProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.host_start_times: dict[str, float] = {}

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile
        self.host_start_times[host.url] = time.perf_counter()
        await asyncio.sleep(0.12)
        return [
            {
                "url": f"{host.url.rstrip('/')}/video/result",
                "source_label": normalize_host(host.url),
                "source_kind": "streaming",
            }
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        return rows


class PriorityCoverageProvider(HttpSourceProvider):
    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile
        normalized = normalize_host(host.url)
        if "youtube.com" in normalized:
            return [
                {
                    "url": f"https://www.youtube.com/watch?v=yt{index:02d}",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                }
                for index in range(12)
            ]
        return [
            {
                "url": "https://www.bilibili.com/video/BV1priorityhit1",
                "source_label": "Bilibili Search",
                "source_kind": "streaming",
            }
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        return rows


class MultiHostDeepSliceAwareProvider(HttpSourceProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hydrated_urls: list[str] = []

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile
        normalized = normalize_host(host.url)
        if "youtube.com" in normalized:
            return [
                {
                    "url": f"https://www.youtube.com/watch?v=yt{index:02d}",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                }
                for index in range(1, 7)
            ] + [
                {
                    "url": "https://www.youtube.com/watch?v=annie-deep-hit",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
                {
                    "url": "https://www.youtube.com/watch?v=yt08",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
            ]
        return [
            {
                "url": "https://www.bilibili.com/video/BV1coverage01/",
                "source_label": "Bilibili Search",
                "source_kind": "streaming",
            },
            {
                "url": "https://www.bilibili.com/video/BV1coverage02/",
                "source_label": "Bilibili Search",
                "source_kind": "streaming",
            },
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        self.hydrated_urls = [row["url"] for row in rows]
        return rows


class AdaptiveHydrationProvider(HttpSourceProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hydration_windows: list[list[str]] = []

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile
        normalized = normalize_host(host.url)
        if "youtube.com" in normalized:
            return [
                {
                    "url": f"https://www.youtube.com/watch?v=yt{index:02d}",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                }
                for index in range(1, 15)
            ]
        return [
            {
                "url": f"https://www.bilibili.com/video/BV1adaptive{index:02d}/",
                "source_label": "Bilibili Search",
                "source_kind": "streaming",
            }
            for index in range(1, 5)
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        urls = [row["url"] for row in rows]
        self.hydration_windows.append(urls)
        hydrated: list[dict[str, str]] = []
        for row in rows:
            score = 0.1
            if row["url"] == "https://www.youtube.com/watch?v=yt09":
                score = 0.72
            hydrated.append(
                {
                    **row,
                    "title": row["url"].rsplit("=", 1)[-1],
                    "platform": "youtube" if "youtube.com" in row["url"] else "bilibili",
                    "weight": 0.6,
                    "same_recording_score": score,
                    "fields": {},
                    "images": [],
                }
            )
        return hydrated


class DeepBilibiliMultiHostProvider(HttpSourceProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hydration_windows: list[list[str]] = []

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host,
    ) -> list[dict[str, str]]:
        del draft, profile
        normalized = normalize_host(host.url)
        if "youtube.com" in normalized:
            return [
                {
                    "url": "https://www.youtube.com/watch?v=yt01",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
                {
                    "url": "https://www.youtube.com/watch?v=yt02",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
                {
                    "url": "https://www.youtube.com/watch?v=bohm-target-hit",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
                {
                    "url": "https://www.youtube.com/watch?v=yt04",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                },
            ]
        return [
            {
                "url": f"https://www.bilibili.com/video/BV1deep{i:02d}/",
                "source_label": "Bilibili Search",
                "source_kind": "streaming",
            }
            for i in range(1, 11)
        ]

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, str]]:
        del draft, source_kind
        self.hydration_windows.append([row["url"] for row in rows])
        return [
            {
                **row,
                "title": row["url"],
                "platform": "youtube" if "youtube.com" in row["url"] else "bilibili",
                "weight": 0.6,
                "same_recording_score": 0.7,
                "fields": {},
                "images": [],
            }
            for row in rows
        ]


class FlakyYouTubeTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.call_count = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.call_count += 1
        url = str(request.url)
        if "youtube.com/results" in url:
            if self.call_count == 1:
                raise httpx.ReadTimeout("timed out", request=request)
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"abc123xyz00","title":{"runs":[{"text":"Klemperer LSO Tchaikovsky 5"}]}}}',
            )
        if "youtube.com/watch" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head><title>Klemperer LSO Tchaikovsky 5</title></head>
                <body>Otto Klemperer London Symphony Orchestra 1964</body></html>
                """,
            )
        return httpx.Response(404, request=request, text="not found")


class EngineRecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        if "bing.com" in url:
            return httpx.Response(
                200,
                request=request,
                text='<li class="b_algo"><h2><a href="https://catalog.example/releases/recording-2">hit</a></h2></li>',
            )
        if "duckduckgo.com" in url:
            return httpx.Response(403, request=request, text="forbidden")
        if "catalog.example" in url:
            return httpx.Response(
                200,
                request=request,
                text="<html><head><title>hit</title></head><body>content</body></html>",
            )
        return httpx.Response(404, request=request, text="not found")


class ApiFirstTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []
        self.headers: dict[str, dict[str, str]] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        self.headers[url] = {key.decode().lower(): value.decode() for key, value in request.headers.raw}
        if "googleapis.com/youtube/v3/search" in url:
            return httpx.Response(
                200,
                request=request,
                json={
                    "items": [
                        {
                            "id": {"videoId": "apiyoutube01"},
                            "snippet": {"title": "Klemperer API result"},
                        }
                    ]
                },
            )
        if "api.music.apple.com/v1/catalog" in url:
            return httpx.Response(
                200,
                request=request,
                json={
                    "results": {
                        "songs": {
                            "data": [
                                {
                                    "attributes": {
                                        "url": "https://music.apple.com/us/album/demo/1?i=1",
                                        "name": "Apple API Result",
                                        "artistName": "Otto Klemperer",
                                        "albumName": "Beethoven: Symphony No. 7",
                                        "composerName": "Ludwig van Beethoven",
                                        "durationInMillis": 233000,
                                    }
                                }
                            ]
                        }
                    }
                },
            )
        if url.rstrip("/") == "https://www.bilibili.com":
            return httpx.Response(200, request=request, text="home")
        if "api.bilibili.com/x/web-interface/nav" in url:
            return httpx.Response(
                200,
                request=request,
                json={
                    "code": 0,
                    "data": {
                        "wbi_img": {
                            "img_url": "https://i0.hdslb.com/bfs/wbi/abcdefghijklmnopqrstuvwxyz123456.png",
                            "sub_url": "https://i0.hdslb.com/bfs/wbi/uvwxyzabcdefghijklmnopqrstuvwxyz123456.jpg",
                        }
                    },
                },
            )
        if "api.bilibili.com/x/web-interface/wbi/search/type" in url:
            return httpx.Response(
                200,
                request=request,
                json={
                    "code": 0,
                    "data": {
                        "result": [
                            {"arcurl": "https://www.bilibili.com/video/BV1apiresult1"}
                        ]
                    }
                },
            )
        if "youtube.com/results" in url:
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"fallback001","title":{"runs":[{"text":"fallback"}]}}}',
            )
        if "music.apple.com/search" in url:
            return httpx.Response(
                200,
                request=request,
                text='"url":"https:\\/\\/music.apple.com\\/us\\/album\\/fallback\\/1?i=1"',
            )
        if "search.bilibili.com/all" in url:
            return httpx.Response(
                200,
                request=request,
                text='"arcurl":"https:\\/\\/www.bilibili.com\\/video\\/BV1fallback1"',
            )
        return httpx.Response(404, request=request, text="not found")


class FallbackApiTransport(ApiFirstTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "googleapis.com/youtube/v3/search" in url:
            self.urls.append(url)
            return httpx.Response(403, request=request, json={"error": {"message": "quota exceeded"}})
        return await super().handle_async_request(request)


class HtmlEndpointFallbackTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []
        self.headers: dict[str, dict[str, str]] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        self.headers[url] = {key.decode().lower(): value.decode() for key, value in request.headers.raw}
        if "classical.music.apple.com/search" in url:
            return httpx.Response(200, request=request, text="")
        if "music.apple.com/search" in url:
            return httpx.Response(
                200,
                request=request,
                text='"url":"https:\\/\\/music.apple.com\\/us\\/album\\/fallback\\/1?i=1"',
            )
        if "search.bilibili.com/all" in url:
            return httpx.Response(200, request=request, text="")
        if "search.bilibili.com/video" in url:
            return httpx.Response(
                200,
                request=request,
                text='"arcurl":"https:\\/\\/www.bilibili.com\\/video\\/BV1fallbackvideo1"',
            )
        return httpx.Response(404, request=request, text="not found")


class PlatformEngineFallbackTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        if "search.bilibili.com" in url or "classical.music.apple.com/search" in url or "music.apple.com/search" in url:
            return httpx.Response(200, request=request, text="")
        if "bing.com" in url and "site%3Awww.bilibili.com" in url:
            return httpx.Response(
                200,
                request=request,
                text='<li class="b_algo"><h2><a href="https://www.bilibili.com/video/BV1enginefallback1">hit</a></h2></li>',
            )
        if "bing.com" in url and "site%3Amusic.apple.com" in url:
            return httpx.Response(
                200,
                request=request,
                text='<li class="b_algo"><h2><a href="https://music.apple.com/us/album/engine-fallback/1?i=1">hit</a></h2></li>',
            )
        return httpx.Response(404, request=request, text="not found")


def test_extract_bilibili_result_links_supports_unquoted_arcurl_and_direct_anchor_hrefs() -> None:
    html_text = """
    <script>
      var item = {arcurl:"http:\\u002F\\u002Fwww.bilibili.com\\u002Fvideo\\u002FBV1arcurl001",bvid:"BV1arcurl001"};
    </script>
    <div class="bili-video-card__wrap">
      <a href="//www.bilibili.com/video/BV1anchor002/" target="_blank">video</a>
    </div>
    """

    links = extract_bilibili_result_links(html_text)

    assert "http://www.bilibili.com/video/BV1arcurl001" in links
    assert "https://www.bilibili.com/video/BV1anchor002/" in links


class ApplePublicApiTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        if "itunes.apple.com/search" in url:
            return httpx.Response(
                200,
                request=request,
                json={
                    "results": [
                        {
                            "trackViewUrl": "https://music.apple.com/us/album/demo-track/123?i=456",
                            "trackName": "Symphony No. 7 in A major, Op. 92: II. Allegretto",
                            "collectionName": "Beethoven: Symphony No. 7",
                            "artistName": "Otto Klemperer, Philharmonia Orchestra",
                            "releaseDate": "2011-01-01T08:00:00Z",
                            "trackTimeMillis": 512000,
                        },
                        {
                            "artistViewUrl": "https://music.apple.com/us/artist/noise-artist/999",
                            "artistName": "Noise Artist",
                        },
                    ]
                },
            )
        return httpx.Response(404, request=request, text="not found")


class YouTubeEngineMergeTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        if "youtube.com/results" in url:
            return httpx.Response(
                200,
                request=request,
                text='{"videoRenderer":{"videoId":"native001","title":{"runs":[{"text":"native"}]}}}',
            )
        if "bing.com" in url and "site%3Awww.youtube.com" in url:
            return httpx.Response(
                200,
                request=request,
                text='<li class="b_algo"><h2><a href="https://www.youtube.com/watch?v=engine002">hit</a></h2></li>',
            )
        return httpx.Response(404, request=request, text="not found")


class BrowserResultFetcher:
    def __init__(
        self,
        links_by_url: dict[str, list[str]],
        search_evidence_by_url: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.links_by_url = links_by_url
        self.search_evidence_by_url = search_evidence_by_url or {}
        self.page_calls: list[str] = []
        self.link_calls: list[str] = []
        self.evidence_calls: list[str] = []

    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str]:
        del timeout_seconds
        self.page_calls.append(url)
        return {}

    async def fetch_links(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        del url_patterns, timeout_seconds
        self.link_calls.append(url)
        return list(self.links_by_url.get(url, []))

    async def fetch_search_evidence(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
        capture_screenshot: bool = False,
    ) -> dict[str, Any]:
        del url_patterns, timeout_seconds, capture_screenshot
        self.evidence_calls.append(url)
        return dict(self.search_evidence_by_url.get(url, {}))


class StructuredBrowserFetcher(BrowserResultFetcher):
    def __init__(
        self,
        links_by_url: dict[str, list[str]],
        page_payloads: dict[str, dict[str, str | int]],
    ) -> None:
        super().__init__(links_by_url)
        self.page_payloads = page_payloads

    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str | int]:
        del timeout_seconds
        self.page_calls.append(url)
        return dict(self.page_payloads.get(url, {}))


class TimeoutRecordingBrowserFetcher(BrowserResultFetcher):
    def __init__(self, links_by_url: dict[str, list[str]]) -> None:
        super().__init__(links_by_url)
        self.link_timeout_calls: list[tuple[str, float | None]] = []

    async def fetch_links(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        del url_patterns
        self.link_timeout_calls.append((url, timeout_seconds))
        return await super().fetch_links(url, timeout_seconds=timeout_seconds)


def build_draft() -> DraftRecordingEntry:
    return DraftRecordingEntry(
        item_id="recording-1",
        title="Klemperer LSO 1964",
        composer_name="柴可夫斯基",
        composer_name_latin="Pyotr Ilyich Tchaikovsky",
        work_title="第五交响曲",
        work_title_latin="Symphony No. 5 in E Minor",
        catalogue="Op.64",
        performance_date_text="1964",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Otto Klemperer | London Symphony Orchestra | 1964",
        raw_text="Otto Klemperer | London Symphony Orchestra | 1964",
        existing_links=[],
        lead_names=["Otto Klemperer"],
        ensemble_names=["London Symphony Orchestra"],
    )


def build_profile() -> RetrievalProfile:
    return RetrievalProfile(
        category="orchestral",
        tags=[],
        queries=["Symphony No. 5 in E Minor Op.64 Otto Klemperer London Symphony Orchestra 1964"],
    )


def test_extract_bing_result_links_reads_direct_result_url() -> None:
    html = '<li class="b_algo"><h2><a href="https://catalog.example/releases/recording-2">hit</a></h2></li>'

    assert extract_bing_result_links(html) == ["https://catalog.example/releases/recording-2"]


def test_extract_bing_result_links_decodes_redirect_url() -> None:
    html = (
        '<li class="b_algo"><h2><a href="https://www.bing.com/ck/a?u=a1'
        'aHR0cHM6Ly9jYXRhbG9nLmV4YW1wbGUvcmVsZWFzZXMvcmVjb3JkaW5nLTM&ntb=1">hit</a></h2></li>'
    )

    assert extract_bing_result_links(html) == ["https://catalog.example/releases/recording-3"]


def test_provider_uses_bing_for_high_quality_search_engine(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = EngineRecordingTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    results = asyncio.run(provider.search_high_quality(build_draft(), build_profile()))

    assert results
    assert results[0]["url"] == "https://catalog.example/releases/recording-2"
    assert any("bing.com" in url for url in transport.urls)
    assert not any("duckduckgo.com" in url for url in transport.urls)


def test_provider_can_search_youtube_directly_without_external_search_engine(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=SearchTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    results = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    assert results


def test_high_quality_search_caps_engine_query_fanout(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text(
        "#global\nhttps://alpha.example\nhttps://beta.example\nhttps://gamma.example\nhttps://delta.example\nhttps://epsilon.example\n",
        encoding="utf-8",
    )
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class CountingHighQualityProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(profile_loader=SourceProfileLoader(root), browser_fetcher=BrowserResultFetcher({}))
            self.engine_queries: list[str] = []

        def _queries_for_host(self, draft: DraftRecordingEntry, profile: RetrievalProfile, host: SourceProfileEntry) -> list[str]:
            del draft, profile, host
            return [f"query-{index}" for index in range(6)]

        async def _search_query_via_engines(self, *, query: str, source_label: str, source_kind: str) -> list[dict[str, str]]:
            del source_label, source_kind
            self.engine_queries.append(query)
            return []

        async def _hydrate_results(self, draft: DraftRecordingEntry, rows: list[dict[str, str]], source_kind: str) -> list[dict[str, Any]]:
            del draft, source_kind
            return list(rows)

    provider = CountingHighQualityProvider()

    results = asyncio.run(provider.search_high_quality(build_draft(), build_profile()))

    assert results == []
    assert len(provider.engine_queries) == 8
    assert provider.engine_queries[:4] == [
        "site:alpha.example query-0",
        "site:alpha.example query-1",
        "site:beta.example query-0",
        "site:beta.example query-1",
    ]


def test_provider_merges_youtube_native_results_with_search_engine_recall(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = YouTubeEngineMergeTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            youtube=YouTubeSearchConfig(enabled=False),
        ),
    )

    rows = asyncio.run(provider._search_youtube(["schumann annie fischer"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=native001" for row in rows)
    assert any(row["url"] == "https://www.youtube.com/watch?v=engine002" for row in rows)
    assert any("bing.com" in url and "site%3Awww.youtube.com" in url for url in transport.urls)


def test_provider_keeps_streaming_host_scan_for_deeper_candidates(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\nhttps://www.bilibili.com\n", encoding="utf-8")
    transport = CountingSearchTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
    )

    results = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    assert results
    assert any("youtube.com/results" in url for url in transport.requests)
    assert any("bilibili.com" in url for url in transport.requests)


def test_provider_can_be_reused_across_multiple_asyncio_runs(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=SearchTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    first = asyncio.run(provider.search_streaming(build_draft(), build_profile()))
    second = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    assert first
    assert second


def test_provider_aggregates_multiple_queries_within_same_streaming_host(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=MultiQueryYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)
    profile = RetrievalProfile(
        category="orchestral",
        tags=[],
        queries=["first query", "Otto Klemperer 1960"],
    )

    results = asyncio.run(provider.search_streaming(build_draft(), profile))

    assert any(row["url"] == "https://www.youtube.com/watch?v=first000001" for row in results)
    assert any(row["url"] == "https://www.youtube.com/watch?v=later000001" for row in results)


def test_youtube_search_keeps_later_queries_even_after_two_queries_fill_initial_budget(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=LaterQueryYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    rows = asyncio.run(provider._search_youtube(["query one", "query two", "query three"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=later-hit-01" for row in rows)


def test_youtube_search_reads_deeper_results_beyond_first_four_links_per_query(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=DeepResultYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    rows = asyncio.run(provider._search_youtube(["deep query"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=deep04" for row in rows)


def test_youtube_search_keeps_top_result_from_later_alias_query_even_when_earlier_queries_fill_budget(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=QueryCoverageYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    rows = asyncio.run(provider._search_youtube(["generic one", "generic two", "alias query"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=alias-hit-01" for row in rows)


def test_youtube_search_promotes_exact_later_query_hit_ahead_of_early_generic_fill(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=RankedLaterQueryYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    rows = asyncio.run(provider._search_youtube(["generic query", "exact query"]))

    urls = [row["url"] for row in rows]
    assert "https://www.youtube.com/watch?v=exact-hit-01" in urls
    assert urls.index("https://www.youtube.com/watch?v=exact-hit-01") < urls.index(
        "https://www.youtube.com/watch?v=generic05"
    )


def test_search_streaming_hydrates_more_than_first_four_rows_from_successful_host(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    provider = HostSliceAwareProvider(profile_loader=SourceProfileLoader(root))

    results = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    assert any(row["url"] == "https://stream.example/12" for row in results)
    assert "https://stream.example/12" in provider.hydrated_urls


def test_provider_collects_access_telemetry_and_host_summary(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=SearchTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    results = asyncio.run(provider.search_streaming(build_draft(), build_profile()))
    events = provider.consume_access_events()
    summary = provider.get_access_summary()

    assert results
    assert any(event["host"] == "www.youtube.com" for event in events)
    assert any(event["operation"] == "streaming-search" for event in events)
    assert summary["hosts"]["www.youtube.com"]["requests"] >= 2
    assert summary["hosts"]["www.youtube.com"]["avgLatencyMs"] >= 0


def test_search_bilibili_records_strategy_event_with_selected_browser_queries() -> None:
    class BilibiliStrategyProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliStrategyProvider(browser_fetcher=BrowserResultFetcher({}))

    asyncio.run(
        provider._search_bilibili(
            [
                "q1 generic",
                "q2 generic",
                "q3 focused",
                "q4 exact primary",
                "q5 collaboration",
                "q6 tail",
            ]
        )
    )
    events = provider.consume_access_events()

    strategy_events = [event for event in events if event["operation"] == "search-strategy"]
    assert strategy_events
    assert strategy_events[0]["host"] == "search.bilibili.com"
    assert strategy_events[0]["strategy"] == "bilibili-mixed"
    assert strategy_events[0]["selectedBrowserQueries"]
    assert strategy_events[-1]["strategy"] in {"bilibili-mixed", "bilibili-second-pass"}


def test_search_youtube_records_api_first_strategy_event() -> None:
    class YouTubeStrategyProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.youtube.com/watch?v=apiyoutube01", "source_label": "YouTube API Search", "source_kind": "streaming"}]

    provider = YouTubeStrategyProvider(
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(api_key="yt-key")),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))
    events = provider.consume_access_events()

    assert rows
    strategy_events = [event for event in events if event["operation"] == "search-strategy"]
    assert strategy_events
    assert strategy_events[-1]["host"] == "www.youtube.com"
    assert strategy_events[-1]["strategy"] == "youtube-api-first"


def test_search_bilibili_browser_search_uses_relaxed_timeout_budget() -> None:
    fetcher = TimeoutRecordingBrowserFetcher(
        {
            "https://search.bilibili.com/video?keyword=focused+bilibili+query": [
                "https://www.bilibili.com/video/BV1browsertimeout/"
            ]
        }
    )
    provider = HttpSourceProvider(browser_fetcher=fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["focused bilibili query"],
            url_builders=[
                lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
                lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
            ],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
        )
    )

    assert rows
    assert fetcher.link_timeout_calls
    assert fetcher.link_timeout_calls[0][1] is not None
    assert fetcher.link_timeout_calls[0][1] >= 10.0


def test_search_bilibili_browser_search_tries_all_page_before_video_page() -> None:
    fetcher = TimeoutRecordingBrowserFetcher(
        {
            "https://search.bilibili.com/video?keyword=focused+bilibili+query": [
                "https://www.bilibili.com/video/BV1videoresult/"
            ]
        }
    )
    provider = HttpSourceProvider(browser_fetcher=fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["focused bilibili query"],
            url_builders=[
                lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
                lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
            ],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
        )
    )

    assert rows == [
        {
            "url": "https://www.bilibili.com/video/BV1videoresult/",
            "source_label": "Bilibili Search Browser Search",
            "source_kind": "streaming",
        }
    ]
    assert fetcher.link_timeout_calls
    assert fetcher.link_timeout_calls[0][0].startswith("https://search.bilibili.com/all?")
    assert any(call[0].startswith("https://search.bilibili.com/video?") for call in fetcher.link_timeout_calls)


def test_search_bilibili_records_layer_summary_event() -> None:
    class BilibiliLayerProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1api/"}]

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browser/"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return [{"url": "https://www.bilibili.com/video/BV1engine/"}]

    provider = BilibiliLayerProvider(browser_fetcher=BrowserResultFetcher({}))

    rows = asyncio.run(provider._search_bilibili(["focused query"]))
    events = provider.consume_access_events()

    assert rows
    summary_events = [event for event in events if event["operation"] == "search-layer-summary"]
    assert summary_events
    assert summary_events[-1]["host"] == "search.bilibili.com"
    assert summary_events[-1]["apiResultCount"] == 1
    assert summary_events[-1]["browserResultCount"] == 1
    assert summary_events[-1]["engineResultCount"] == 0


def test_search_bilibili_runs_second_pass_queries_when_first_pass_is_empty() -> None:
    class BilibiliSecondPassProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []
            self.primary_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.browser_query_batches.append(queries)
            if len(self.browser_query_batches) == 1:
                return []
            return [{"url": "https://www.bilibili.com/video/BV1secondpass/"}]

        async def _search_streaming_platform(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.primary_query_batches.append(queries)
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliSecondPassProvider()

    rows = asyncio.run(
        provider._search_bilibili(
            [
                "first query",
                "second query",
                "third query",
                "fourth recovery query",
                "fifth recovery query",
            ]
        )
    )

    assert rows == [{"url": "https://www.bilibili.com/video/BV1secondpass/"}]
    assert len(provider.browser_query_batches) == 2
    assert len(provider.primary_query_batches) == 2
    assert provider.primary_query_batches[0] == ["first query", "second query", "third query"]
    assert provider.primary_query_batches[1] == ["fourth recovery query"]


def test_search_bilibili_uses_focused_browser_probe_for_primary_when_browser_hits() -> None:
    class BilibiliProbeProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.primary_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browserprobe/"}]

        async def _search_streaming_platform(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.primary_query_batches.append(queries)
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliProbeProvider()

    rows = asyncio.run(
        provider._search_bilibili(
            [
                "a小调钢琴协奏曲 埃莉索·维尔萨拉泽 亚历山大·鲁丁",
                "a小调钢琴协奏曲 埃莉索·维尔萨拉泽 / 亚历山大·鲁丁",
                "Virsaladze rudin Schumann concerto",
                "钢协 alexander rudin",
                "a小调钢琴协奏曲 Eliso Virsaladze",
                "Eliso Virsaladze Schumann concerto",
            ]
        )
    )

    assert rows == [{"url": "https://www.bilibili.com/video/BV1browserprobe/"}]
    assert provider.primary_query_batches == [["钢协 alexander rudin"]]


def test_search_bilibili_second_pass_only_uses_remaining_queries() -> None:
    class BilibiliSecondPassProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.browser_query_batches.append(queries)
            return []

        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliSecondPassProvider()

    asyncio.run(
        provider._search_bilibili(
            [
                "alpha query",
                "beta query",
                "gamma query",
                "delta query",
                "epsilon query",
            ]
        )
    )

    assert len(provider.browser_query_batches) == 2
    assert set(provider.browser_query_batches[0]).isdisjoint(set(provider.browser_query_batches[1]))


def test_search_bilibili_forces_second_pass_for_chamber_duo_when_first_pass_has_no_person_match() -> None:
    class BilibiliForcedSecondPassProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.browser_query_batches.append(queries)
            if len(self.browser_query_batches) == 1:
                return [
                    {
                        "url": "https://www.bilibili.com/video/BV1noisefirst/",
                        "title": "Other violin performance",
                        "description": "unrelated artists",
                        "source_label": "Bilibili Search Browser Search",
                        "source_kind": "streaming",
                    }
                ]
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1forcedhit/",
                    "title": "Schneiderhan Seemann Beethoven violin sonatas",
                    "description": "",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                }
            ]

        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliForcedSecondPassProvider()

    rows = asyncio.run(
        provider._search_bilibili(
            [
                "spring Schneiderhan Seemann",
                "spring violin sonata no 5 Schneiderhan Seemann 1959",
                "Ludwig van Beethoven spring Schneiderhan Seemann",
                "Schneiderhan Seemann violin sonatas",
                "backup query one",
                "backup query two",
            ]
        )
    )

    assert any(row.get("url") == "https://www.bilibili.com/video/BV1forcedhit/" for row in rows)
    assert len(provider.browser_query_batches) == 2


def test_search_bilibili_does_not_force_second_pass_for_non_chamber_queries() -> None:
    class BilibiliNoForcedSecondPassProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.browser_query_batches.append(queries)
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1noisefirst/",
                    "title": "Bruckner Symphony",
                    "description": "Bohm recording",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                }
            ]

        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliNoForcedSecondPassProvider()

    asyncio.run(
        provider._search_bilibili(
            [
                "布鲁克纳 伯姆",
                "布鲁克纳 交响曲 伯姆",
                "伯姆 指挥",
                "维也纳 爱乐",
                "1977",
            ]
        )
    )

    assert len(provider.browser_query_batches) == 1


def test_build_bilibili_chamber_duo_trailer_probe_queries_extracts_complete_bundle_focus() -> None:
    probes = build_bilibili_chamber_duo_trailer_probe_queries(
        [
            "Ludwig van Beethoven complete violin sonatas Schneiderhan Seemann 1959",
            "spring violin sonata no 5 Schneiderhan Seemann 1959",
        ]
    )

    assert probes == ["complete violin sonatas Schneiderhan Seemann trailer"]


def test_build_bilibili_chamber_solo_opus_probe_queries_prefers_latin_opus_anchor() -> None:
    probes = build_bilibili_chamber_solo_opus_probe_queries(
        [
            "第二十三号奏鸣曲，热情 埃贡·佩特里 1956",
            "热情op57 埃贡·佩特里",
            "Egon Petri op 57",
            "Ludwig van Beethoven Egon Petri op 57",
            "Petri appassionata Beethoven",
        ]
    )

    assert probes[0] == "Egon Petri op 57"


def test_should_force_chamber_solo_second_pass_for_bilibili_when_rows_miss_named_work() -> None:
    should_force = should_force_chamber_solo_second_pass_for_bilibili(
        queries=[
            "Egon Petri op 57",
            "Petri appassionata Beethoven",
            "appassionata Egon Petri 1956",
        ],
        api_rows=[
            {
                "title": "Egon Petri Beethoven Op.106",
                "description": "historic upload",
            }
        ],
        browser_rows=[],
    )

    assert should_force is True


def test_should_not_force_chamber_solo_second_pass_for_bilibili_when_rows_have_named_work_match() -> None:
    should_force = should_force_chamber_solo_second_pass_for_bilibili(
        queries=[
            "Egon Petri op 57",
            "Petri appassionata Beethoven",
            "appassionata Egon Petri 1956",
        ],
        api_rows=[
            {
                "title": 'Egon Petri plays Beethoven Sonata No. 23 in F minor Op. 57 "Appassionata"',
                "description": "historic upload",
            }
        ],
        browser_rows=[],
    )

    assert should_force is False


def test_should_force_chamber_solo_second_pass_for_bilibili_with_cjk_person_queries() -> None:
    should_force = should_force_chamber_solo_second_pass_for_bilibili(
        queries=[
            "第二十三号奏鸣曲，热情 園田高弘 2023",
            "熱情 op57 園田高弘",
            "beethoven appassionata op 57 sonoda",
        ],
        api_rows=[
            {
                "title": "贝多芬热情奏鸣曲 Emil Gilels",
                "description": "historic upload",
            }
        ],
        browser_rows=[],
    )

    assert should_force is True


def test_bilibili_chamber_solo_person_tokens_extracts_cjk_name_chunks() -> None:
    tokens = bilibili_chamber_solo_person_tokens(
        [
            "第二十三号奏鸣曲，热情 園田高弘 2023",
            "热情op57 園田高弘",
            "beethoven appassionata op 57 sonoda",
        ]
    )

    assert "園田高弘" in tokens


def test_search_bilibili_forced_second_pass_prefers_chamber_trailer_probe_query() -> None:
    class BilibiliChamberTrailerProbeProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []
            self.primary_query_batches: list[list[str]] = []

        async def _search_platform_via_browser_pages(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.browser_query_batches.append(queries)
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1noisefirst/",
                    "title": "Other violin performance",
                    "description": "unrelated artists",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                }
            ]

        async def _search_streaming_platform(self, **kwargs):
            queries = list(kwargs.get("queries") or [])
            self.primary_query_batches.append(queries)
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliChamberTrailerProbeProvider()

    asyncio.run(
        provider._search_bilibili(
            [
                "spring Schneiderhan Seemann",
                "spring violin sonata no 5 Schneiderhan Seemann 1959",
                "Ludwig van Beethoven spring Schneiderhan Seemann",
                "Ludwig van Beethoven complete violin sonatas Schneiderhan Seemann 1959",
                "backup query one",
                "backup query two",
            ]
        )
    )

    assert len(provider.primary_query_batches) >= 2
    assert provider.primary_query_batches[1][0] == "complete violin sonatas Schneiderhan Seemann trailer"


def test_search_bilibili_forced_second_pass_promotes_chamber_bundle_api_hit_into_final_rows() -> None:
    class BilibiliPromoteChamberBundleHitProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.streaming_call_count = 0

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [
                {
                    "url": f"https://www.bilibili.com/video/BV1browser{i:02d}/",
                    "title": f"noise result {i}",
                    "description": "unrelated artists",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                }
                for i in range(16)
            ]

        async def _search_streaming_platform(self, **kwargs):
            self.streaming_call_count += 1
            if self.streaming_call_count == 1:
                return []
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1targethit/",
                    "title": "Schneiderhan Seemann Complete Violin Sonatas",
                    "description": "live recording",
                    "source_label": "Bilibili API Search",
                    "source_kind": "streaming",
                }
            ]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

        async def _capture_rendered_search_evidence(self, **kwargs):
            del kwargs
            return []

    provider = BilibiliPromoteChamberBundleHitProvider()

    rows = asyncio.run(
        provider._search_bilibili(
            [
                "spring Schneiderhan Seemann",
                "spring violin sonata no 5 Schneiderhan Seemann 1959",
                "Ludwig van Beethoven complete violin sonatas Schneiderhan Seemann 1959",
                "backup query one",
                "backup query two",
            ]
        )
    )

    assert any(row.get("url") == "https://www.bilibili.com/video/BV1targethit/" for row in rows)


def test_search_bilibili_host_stats_do_not_shrink_depth_too_early() -> None:
    provider = HttpSourceProvider(browser_fetcher=BrowserResultFetcher({}))
    provider._host_stats["search.bilibili.com"] = {
        "requests": 4.0,
        "successes": 2.0,
        "failures": 2.0,
        "totalLatencyMs": 14000.0,
        "totalResults": 4.0,
        "cacheHits": 0.0,
    }

    assert provider._recommended_query_depth("search.bilibili.com", 6) == 6
    assert provider._should_skip_host("search.bilibili.com", min_requests=3) is False


def test_search_bilibili_streaming_host_does_not_skip_too_early() -> None:
    provider = HttpSourceProvider(browser_fetcher=BrowserResultFetcher({}))
    provider._host_stats["www.bilibili.com"] = {
        "requests": 4.0,
        "successes": 0.0,
        "failures": 4.0,
        "totalLatencyMs": 24000.0,
        "totalResults": 0.0,
        "cacheHits": 0.0,
    }

    assert provider._should_skip_host("www.bilibili.com", min_requests=2) is False


def test_search_youtube_records_layer_summary_event() -> None:
    class YouTubeLayerProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.youtube.com/watch?v=apiyoutube01"}]

    provider = YouTubeLayerProvider(
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(api_key="yt-key")),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))
    events = provider.consume_access_events()

    assert rows
    summary_events = [event for event in events if event["operation"] == "search-layer-summary"]
    assert summary_events
    assert summary_events[-1]["host"] == "www.youtube.com"
    assert summary_events[-1]["primaryResultCount"] == 1
    assert summary_events[-1]["engineResultCount"] == 0


def test_search_bilibili_records_anomaly_event_when_browser_outperforms_primary() -> None:
    class BilibiliAnomalyProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browserhit/"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliAnomalyProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://search.bilibili.com/all?keyword=focused+bilibili+query": {
                    "title": "focused bilibili query - Bilibili Search",
                    "matchedLinks": ["https://www.bilibili.com/video/BV1browserhit/"],
                    "matchedLinkCount": 1,
                    "anchorCount": 12,
                    "resultCardCount": 1,
                    "extractionMode": "result-card-priority",
                    "htmlLength": 2048,
                    "bodyTextSample": "rendered bilibili result",
                    "screenshotPath": "output/browser-diagnostics/bilibili-focused.png",
                }
            },
        )
    )

    rows = asyncio.run(provider._search_bilibili(["focused bilibili query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert anomaly_events
    assert anomaly_events[-1]["host"] == "search.bilibili.com"
    assert anomaly_events[-1]["anomalyType"] == "browser_outperformed_primary"
    assert anomaly_events[-1]["browserResultCount"] == 1
    assert anomaly_events[-1]["apiResultCount"] == 0
    assert anomaly_events[-1]["browserTopUrls"] == ["https://www.bilibili.com/video/BV1browserhit/"]
    assert anomaly_events[-1]["renderedEvidence"][0]["query"] == "focused bilibili query"
    assert anomaly_events[-1]["renderedEvidence"][0]["matchedLinks"] == ["https://www.bilibili.com/video/BV1browserhit/"]
    assert anomaly_events[-1]["renderedEvidence"][0]["resultCardCount"] == 1
    assert anomaly_events[-1]["renderedEvidence"][0]["extractionMode"] == "result-card-priority"
    assert anomaly_events[-1]["renderedEvidence"][0]["screenshotPath"].endswith("bilibili-focused.png")


def test_search_youtube_records_anomaly_event_when_engine_only_recovers_html_gap() -> None:
    class YouTubeAnomalyProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return [{"url": "https://www.youtube.com/watch?v=engineyoutube01"}]

    provider = YouTubeAnomalyProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://www.youtube.com/results?search_query=klemperer+query": {
                    "title": "klemperer query - YouTube",
                    "matchedLinks": [],
                    "matchedLinkCount": 0,
                    "anchorCount": 8,
                    "htmlLength": 1024,
                    "bodyTextSample": "empty rendered youtube results",
                    "screenshotPath": "output/browser-diagnostics/youtube-klemperer.png",
                }
            },
        ),
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(enabled=False, api_key="")),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert anomaly_events
    assert anomaly_events[-1]["host"] == "www.youtube.com"
    assert anomaly_events[-1]["anomalyType"] == "engine_only_recovery"
    assert anomaly_events[-1]["primaryResultCount"] == 0
    assert anomaly_events[-1]["engineResultCount"] == 1
    assert anomaly_events[-1]["engineTopUrls"] == ["https://www.youtube.com/watch?v=engineyoutube01"]
    assert anomaly_events[-1]["renderedEvidence"][0]["query"] == "klemperer query"
    assert anomaly_events[-1]["renderedEvidence"][0]["title"] == "klemperer query - YouTube"


def test_search_youtube_records_parser_mismatch_when_rendered_results_do_not_overlap_html_parser() -> None:
    class YouTubeParserMismatchProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.youtube.com/watch?v=htmlparser01"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return [{"url": "https://www.youtube.com/watch?v=engineyoutube01"}]

    provider = YouTubeParserMismatchProvider(
        browser_fetcher=BrowserResultFetcher(
                {},
                search_evidence_by_url={
                    "https://www.youtube.com/results?search_query=klemperer+query": {
                        "title": "klemperer query - YouTube",
                        "matchedLinks": ["https://www.youtube.com/watch?v=engineyoutube01"],
                        "matchedLinkCount": 1,
                        "anchorCount": 12,
                        "htmlLength": 1536,
                        "bodyTextSample": "rendered result differs from parsed html",
                    }
            },
        ),
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(enabled=False, api_key="")),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert anomaly_events
    assert anomaly_events[-1]["host"] == "www.youtube.com"
    assert anomaly_events[-1]["anomalyType"] == "parser_mismatch"
    assert anomaly_events[-1]["primaryTopUrls"] == ["https://www.youtube.com/watch?v=htmlparser01"]
    assert anomaly_events[-1]["renderedEvidence"][0]["matchedLinks"] == ["https://www.youtube.com/watch?v=engineyoutube01"]
    assert anomaly_events[-1]["overlapCount"] == 0
    assert anomaly_events[-1]["alternateOverlapCount"] == 1


def test_search_youtube_does_not_record_parser_mismatch_without_rendered_support_for_engine_truth() -> None:
    class YouTubeParserMismatchProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.youtube.com/watch?v=htmlparser01"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return [{"url": "https://www.youtube.com/watch?v=engineyoutube01"}]

    provider = YouTubeParserMismatchProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://www.youtube.com/results?search_query=klemperer+query": {
                    "title": "klemperer query - YouTube",
                    "matchedLinks": ["https://www.youtube.com/watch?v=unrelated999"],
                    "matchedLinkCount": 1,
                    "anchorCount": 12,
                    "htmlLength": 1536,
                    "bodyTextSample": "rendered result is unrelated",
                }
            },
        ),
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(enabled=False, api_key="")),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert not anomaly_events


def test_search_bilibili_records_parser_mismatch_when_browser_and_primary_do_not_overlap() -> None:
    class BilibiliParserMismatchProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1apiresult1/"}]

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browsertruth/"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliParserMismatchProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://search.bilibili.com/all?keyword=focused+bilibili+query": {
                    "title": "focused bilibili query - Bilibili Search",
                    "matchedLinks": ["https://www.bilibili.com/video/BV1browsertruth/"],
                    "matchedLinkCount": 1,
                    "anchorCount": 10,
                    "htmlLength": 2024,
                    "bodyTextSample": "rendered bilibili truth",
                }
            },
        )
    )

    rows = asyncio.run(provider._search_bilibili(["focused bilibili query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert anomaly_events
    assert anomaly_events[-1]["host"] == "search.bilibili.com"
    assert anomaly_events[-1]["anomalyType"] == "parser_mismatch"
    assert anomaly_events[-1]["apiTopUrls"] == ["https://www.bilibili.com/video/BV1apiresult1/"]
    assert anomaly_events[-1]["browserTopUrls"] == ["https://www.bilibili.com/video/BV1browsertruth/"]
    assert anomaly_events[-1]["renderedEvidence"][0]["matchedLinks"] == ["https://www.bilibili.com/video/BV1browsertruth/"]
    assert anomaly_events[-1]["overlapCount"] == 0
    assert anomaly_events[-1]["alternateOverlapCount"] == 1


def test_search_bilibili_does_not_record_parser_mismatch_without_rendered_support_for_browser_truth() -> None:
    class BilibiliParserMismatchProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1apiresult1/"}]

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browsertruth/"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliParserMismatchProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://search.bilibili.com/all?keyword=focused+bilibili+query": {
                    "title": "focused bilibili query - Bilibili Search",
                    "matchedLinks": ["https://www.bilibili.com/video/BV1othernoise/"],
                    "matchedLinkCount": 1,
                    "anchorCount": 10,
                    "htmlLength": 2024,
                    "bodyTextSample": "rendered bilibili unrelated result",
                }
            },
        )
    )

    rows = asyncio.run(provider._search_bilibili(["focused bilibili query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert not anomaly_events


def test_search_bilibili_does_not_record_parser_mismatch_for_same_video_when_api_uses_av_url_and_bvid() -> None:
    class BilibiliParserMismatchProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [
                {
                    "url": "http://www.bilibili.com/video/av123456789",
                    "bvid": "BV1browsertruth",
                }
            ]

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [{"url": "https://www.bilibili.com/video/BV1browsertruth/"}]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliParserMismatchProvider(
        browser_fetcher=BrowserResultFetcher(
            {},
            search_evidence_by_url={
                "https://search.bilibili.com/all?keyword=focused+bilibili+query": {
                    "title": "focused bilibili query - Bilibili Search",
                    "matchedLinks": ["https://www.bilibili.com/video/BV1browsertruth/"],
                    "matchedLinkCount": 1,
                    "anchorCount": 10,
                    "htmlLength": 2024,
                    "bodyTextSample": "rendered bilibili truth",
                }
            },
        )
    )

    rows = asyncio.run(provider._search_bilibili(["focused bilibili query"]))
    events = provider.consume_access_events()

    assert rows
    anomaly_events = [event for event in events if event["operation"] == "search-anomaly"]
    assert not anomaly_events


def test_normalize_search_result_payload_prefers_bilibili_result_cards_over_noisy_anchor_scan() -> None:
    payload = normalize_search_result_payload(
        "https://search.bilibili.com/all?keyword=richter",
        {
            "allLinks": [
                "https://www.bilibili.com",
                "https://space.bilibili.com/123",
                "https://www.bilibili.com/video/BV1noise111/",
                "https://www.bilibili.com/video/BV1truth222/",
            ],
            "resultCardLinks": [
                "https://www.bilibili.com/video/BV1truth222/",
                "https://www.bilibili.com/video/BV1truth333/",
            ],
        },
    )

    assert payload["matchedLinks"][:2] == [
        "https://www.bilibili.com/video/BV1truth222/",
        "https://www.bilibili.com/video/BV1truth333/",
    ]
    assert payload["matchedLinks"][2] == "https://www.bilibili.com/video/BV1noise111/"
    assert payload["resultCardCount"] == 2
    assert payload["extractionMode"] == "result-card-priority"


def test_normalize_search_result_payload_keeps_generic_anchor_scan_for_youtube() -> None:
    payload = normalize_search_result_payload(
        "https://www.youtube.com/results?search_query=richter",
        {
            "allLinks": [
                "https://www.youtube.com/watch?v=truth111",
                "https://www.youtube.com/watch?v=truth222",
            ],
            "resultCardLinks": [
                "https://www.youtube.com/watch?v=unused333",
            ],
        },
    )

    assert payload["matchedLinks"] == [
        "https://www.youtube.com/watch?v=truth111",
        "https://www.youtube.com/watch?v=truth222",
    ]
    assert payload["resultCardCount"] == 1
    assert payload["extractionMode"] == "anchor-scan"


def test_merge_bilibili_search_rows_suppresses_non_overlapping_api_rows_after_parser_mismatch() -> None:
    browser_rows = [
        {"url": "https://www.bilibili.com/video/BV1browsertruth/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        {"url": "https://www.bilibili.com/video/BV1browsernext/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
    ]
    api_rows = [
        {"url": "https://www.bilibili.com/video/BV1apinoise1/", "source_label": "Bilibili API Search", "source_kind": "streaming"},
        {"url": "https://www.bilibili.com/video/BV1apinoise2/", "source_label": "Bilibili API Search", "source_kind": "streaming"},
    ]
    engine_rows = [
        {"url": "https://www.bilibili.com/video/BV1browsertruth/", "source_label": "Bilibili Search via Bing", "source_kind": "streaming"},
    ]

    merged = merge_bilibili_search_rows(
        api_rows,
        browser_rows,
        engine_rows,
        parser_mismatch=True,
    )

    urls = [row["url"] for row in merged]
    assert urls[:2] == [
        "https://www.bilibili.com/video/BV1browsertruth/",
        "https://www.bilibili.com/video/BV1browsernext/",
    ]
    assert "https://www.bilibili.com/video/BV1apinoise1/" not in urls
    assert "https://www.bilibili.com/video/BV1apinoise2/" not in urls


def test_merge_bilibili_search_rows_keeps_overlapping_api_rows_after_parser_mismatch() -> None:
    browser_rows = [
        {"url": "https://www.bilibili.com/video/BV1browsertruth/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
    ]
    api_rows = [
        {"url": "https://www.bilibili.com/video/BV1browsertruth/", "source_label": "Bilibili API Search", "source_kind": "streaming"},
        {"url": "https://www.bilibili.com/video/BV1apinoise2/", "source_label": "Bilibili API Search", "source_kind": "streaming"},
    ]
    engine_rows = []

    merged = merge_bilibili_search_rows(
        api_rows,
        browser_rows,
        engine_rows,
        parser_mismatch=True,
    )

    urls = [row["url"] for row in merged]
    assert urls == ["https://www.bilibili.com/video/BV1browsertruth/"]


def test_prune_browser_diagnostic_files_keeps_newest_files_within_count(tmp_path: Path) -> None:
    output_dir = tmp_path / "browser-diagnostics"
    output_dir.mkdir(parents=True)
    files = []
    for index in range(4):
        target = output_dir / f"capture-{index}.png"
        target.write_bytes(f"file-{index}".encode("utf-8"))
        timestamp = time.time() - (40 - index)
        target.touch()
        Path(target).stat()

        os.utime(target, (timestamp, timestamp))
        files.append(target)

    prune_browser_diagnostic_files(output_dir, max_files=2, max_total_bytes=10_000)

    remaining = sorted(path.name for path in output_dir.glob("*.png"))
    assert remaining == ["capture-2.png", "capture-3.png"]


def test_prune_browser_diagnostic_files_keeps_total_size_within_budget(tmp_path: Path) -> None:
    output_dir = tmp_path / "browser-diagnostics"
    output_dir.mkdir(parents=True)
    for index in range(3):
        target = output_dir / f"capture-{index}.png"
        target.write_bytes(b"x" * 12)
        timestamp = time.time() - (30 - index)

        os.utime(target, (timestamp, timestamp))

    prune_browser_diagnostic_files(output_dir, max_files=5, max_total_bytes=24)

    remaining = sorted(path.name for path in output_dir.glob("*.png"))
    assert remaining == ["capture-1.png", "capture-2.png"]


def test_provider_access_summary_marks_unstable_host_and_recommends_higher_timeout(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=FlakyYouTubeTransport(), follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(youtube=YouTubeSearchConfig(enabled=False, api_key="")),
    )

    asyncio.run(provider.search_streaming(build_draft(), build_profile()))
    asyncio.run(provider.search_streaming(build_draft(), build_profile()))
    summary = provider.get_access_summary()

    assert summary["hosts"]["www.youtube.com"]["failures"] >= 1
    assert summary["hosts"]["www.youtube.com"]["recommendedTimeoutSeconds"] > 6.0
    assert summary["hosts"]["www.youtube.com"]["status"] == "degraded"


def test_provider_skips_duckduckgo_after_repeated_failures(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = EngineRecordingTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    for _ in range(3):
        provider._record_access_event(
            url="https://html.duckduckgo.com/html/?q=test",
            operation="search-engine",
            ok=False,
            duration_ms=7000,
            source_kind="search",
            source_label="Web Search",
            error="403",
        )

    rows = asyncio.run(provider._search_query_via_engines(query="test query", source_label="Web Search", source_kind="search"))

    assert rows
    assert any("bing.com" in url for url in transport.urls)
    assert not any("duckduckgo.com" in url for url in transport.urls)


class ExactLinkTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "youtube.com/watch?v=shortclip001" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head>
                  <title>Annie Fischer - Schumann / Piano Concerto in A Minor / A-moll zongoraverseny</title>
                  <meta property="og:description" content="Annie Fischer (1914 -- 1995) Schumann - Piano Concerto in A Minor&#10;vezényel: Paul Kletzki" />
                  <meta property="og:image" content="https://img.youtube.com/vi/shortclip001/hqdefault.jpg" />
                  <script>var ytInitialPlayerResponse = {"videoDetails":{"lengthSeconds":"225","author":"MaldororArt","shortDescription":"Annie Fischer (1914 -- 1995) Schumann - Piano Concerto in A Minor\\nvezényel: Paul Kletzki","title":"Annie Fischer - Schumann / Piano Concerto in A Minor / A-moll zongoraverseny","viewCount":"5506"}};</script>
                </head><body>Annie Fischer Paul Kletzki</body></html>
                """,
            )
        if "youtube.com/watch?v=fullclip001" in url:
            return httpx.Response(
                200,
                request=request,
                text="""
                <html><head>
                  <title>Annie Fischer plays Schumann: Klavierkonzert a-minor  video! full!</title>
                  <meta property="og:description" content="Full performance" />
                  <meta property="og:image" content="https://img.youtube.com/vi/fullclip001/hqdefault.jpg" />
                  <script>var ytInitialPlayerResponse = {"videoDetails":{"lengthSeconds":"2016","author":"Katalin Sin","shortDescription":"Full performance","title":"Annie Fischer plays Schumann: Klavierkonzert a-minor  video! full!","viewCount":"88772"}};</script>
                </head><body>Annie Fischer Klavierkonzert a-minor full performance</body></html>
                """,
            )
        return httpx.Response(404, request=request, text="not found")


def build_annie_draft() -> DraftRecordingEntry:
    return DraftRecordingEntry(
        item_id="recording-annie",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto in A Minor, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        query_lead_names=["Annie Fischer", "Kletzki"],
        query_lead_names_latin=["Annie Fischer", "Kletzki"],
    )


def test_provider_penalizes_short_excerpt_against_full_length_upload(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    client = httpx.AsyncClient(transport=ExactLinkTransport(), follow_redirects=True)
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root), client=client)

    rows = asyncio.run(
        provider._hydrate_results(
            build_annie_draft(),
            [
                {"url": "https://www.youtube.com/watch?v=shortclip001", "source_label": "YouTube Search", "source_kind": "streaming"},
                {"url": "https://www.youtube.com/watch?v=fullclip001", "source_label": "YouTube Search", "source_kind": "streaming"},
            ],
            "streaming",
        )
    )
    score_by_url = {row["url"]: row["same_recording_score"] for row in rows}

    assert score_by_url["https://www.youtube.com/watch?v=fullclip001"] > score_by_url["https://www.youtube.com/watch?v=shortclip001"]
    assert score_by_url["https://www.youtube.com/watch?v=shortclip001"] < 0.75


def test_build_work_aliases_infers_violin_concerto_from_chinese_title() -> None:
    aliases = build_work_aliases("D大调小提琴协奏曲")

    assert "violin concerto d major" in aliases
    assert "violin concerto in d major" in aliases


def test_build_work_aliases_adds_chinese_piano_concerto_shorthand() -> None:
    aliases = build_work_aliases("a小调钢琴协奏曲")

    assert "钢协" in aliases
    assert "a小调钢协" in aliases


def test_provider_prefers_youtube_api_when_configured(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = ApiFirstTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            youtube=YouTubeSearchConfig(api_key="yt-key"),
        ),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=apiyoutube01" for row in rows)
    assert any("googleapis.com/youtube/v3/search" in url for url in transport.urls)
    assert not any("youtube.com/results" in url for url in transport.urls)


def test_provider_falls_back_to_youtube_html_when_api_fails(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = FallbackApiTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            youtube=YouTubeSearchConfig(api_key="yt-key"),
        ),
    )

    rows = asyncio.run(provider._search_youtube(["klemperer query"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=fallback001" for row in rows)
    assert any("googleapis.com/youtube/v3/search" in url for url in transport.urls)
    assert any("youtube.com/results" in url for url in transport.urls)


def test_provider_disables_youtube_api_after_quota_error_in_same_run(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = FallbackApiTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            youtube=YouTubeSearchConfig(api_key="yt-key"),
        ),
    )

    rows = asyncio.run(provider._search_youtube(["query-one", "query-two"]))

    assert any(row["url"] == "https://www.youtube.com/watch?v=fallback001" for row in rows)
    assert sum("googleapis.com/youtube/v3/search" in url for url in transport.urls) == 1
    assert sum("youtube.com/results" in url for url in transport.urls) >= 2


def test_streaming_platform_serializes_api_failure_before_fallback_queries() -> None:
    class ApiBudgetProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.api_calls = 0

        async def _fetch_text(self, url: str, **kwargs) -> str:
            del url, kwargs
            return "<html></html>"

    provider = ApiBudgetProvider()

    async def failing_api_search(query: str, result_depth: int):
        del query, result_depth
        provider.api_calls += 1
        await asyncio.sleep(0.01)
        request = httpx.Request("GET", "https://www.googleapis.com/youtube/v3/search")
        response = httpx.Response(403, request=request, text="quota exceeded")
        raise httpx.HTTPStatusError("quota exceeded", request=request, response=response)

    rows = asyncio.run(
        provider._search_streaming_platform(
            queries=["query-one", "query-two", "query-three", "query-four"],
            url_builder=lambda query: f"https://www.youtube.com/results?search_query={quote_plus(query)}",
            parser=lambda html_text: ["https://www.youtube.com/watch?v=fallback001"],
            source_label="YouTube Search",
            api_search=failing_api_search,
            api_source_label="YouTube API Search",
        )
    )

    assert rows
    assert provider.api_calls == 1


def test_provider_expands_title_inferred_chinese_collaborator_into_latin_youtube_queries(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    person_alias_path = tmp_path / "person-name-aliases.txt"
    person_alias_path.write_text(
        "#global\n托斯卡尼尼 = Arturo Toscanini\n亚莎·海菲兹 = Jascha Heifetz\n",
        encoding="utf-8",
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        person_alias_loader=PersonAliasLoader(person_alias_path),
    )
    draft = DraftRecordingEntry(
        item_id="recording-yt-query-1",
        title="托斯卡尼尼 - 海菲兹 - NBC Symphony Orchestra - March 11, 1940",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="D大调小提琴协奏曲",
        work_title_latin="Violin Concerto in D major, Op.61",
        catalogue="Op.61",
        performance_date_text="March 11, 1940",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        query_lead_names=["亚莎·海菲兹", "托斯卡尼尼"],
        query_lead_names_latin=["Jascha Heifetz"],
        ensemble_names=["NBC Symphony Orchestra"],
        ensemble_names_latin=["NBC Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = provider._profile_loader.load(category="concerto", tags=[]).streaming[0]

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Arturo Toscanini" in query for query in queries)


def test_queries_for_host_keep_soloist_only_work_query_for_concerto_full_draft() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-annie-query-1",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        query_lead_names=["Annie Fischer", "Kletzki"],
        query_lead_names_latin=["Annie Fischer", "Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Piano Concerto, Op.54 Annie Fischer" in queries
    assert any("Annie Fischer" in query and "klavierkonzert" in query.lower() for query in queries)


def test_queries_for_host_add_bilibili_chinese_shorthand_alias_for_concerto() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-annie-query-zh-1",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        query_lead_names=["安妮·费舍尔", "Annie Fischer", "凯莱茨基", "Kletzki"],
        query_lead_names_latin=["Annie Fischer", "Kletzki"],
        lead_names=["安妮·费舍尔", "Annie Fischer", "凯莱茨基", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["布达佩斯爱乐乐团", "Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("钢协" in query and ("Annie Fischer" in query or "安妮" in query) for query in queries)


def test_queries_for_bilibili_host_keep_exact_latin_collaboration_query_for_annie_kletzki() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-annie-query-zh-2",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Paul Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Paul Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["安妮·费舍尔", "Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["保罗·克列茨基", "Paul Kletzki"],
        secondary_names_latin=["Paul Kletzki"],
        query_lead_names=["安妮·费舍尔", "Annie Fischer", "保罗·克列茨基", "Paul Kletzki"],
        query_lead_names_latin=[
            "Annie Fischer Paul Kletzki",
            "Annie Fischer / Paul Kletzki",
            "Annie Fischer",
            "Paul Kletzki",
        ],
        lead_names=["安妮·费舍尔", "Annie Fischer", "保罗·克列茨基", "Paul Kletzki"],
        lead_names_latin=["Annie Fischer", "Paul Kletzki"],
        ensemble_names=["布达佩斯爱乐乐团", "Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Orchestra", "Budapest Philharmonic Orchestra", "BpPO"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Piano Concerto, Op.54 Annie Fischer Paul Kletzki" in query for query in queries[:10])
    assert any("Paul Kletzki" in query for query in queries[:10])


def test_queries_for_bilibili_host_add_exact_primary_year_work_query_for_moiseiwitsch_partial() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-moiseiwitsch-query-zh-1",
        title="Benno Moiseiwitsch Schumann concerto",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1954",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Benno Moiseiwitsch | Otto Ackermann | - | 1954",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Benno Moiseiwitsch | Otto Ackermann | - | 1954",
        existing_links=[],
        primary_names=["本诺·莫伊谢维奇", "Benno Moiseiwitsch"],
        primary_names_latin=["Benno Moiseiwitsch"],
        secondary_names=["奥托·阿克曼", "Otto Ackermann"],
        secondary_names_latin=["Otto Ackermann"],
        query_lead_names=["本诺·莫伊谢维奇", "Benno Moiseiwitsch", "奥托·阿克曼", "Otto Ackermann"],
        query_lead_names_latin=[
            "Benno Moiseiwitsch Otto Ackermann",
            "Benno Moiseiwitsch / Otto Ackermann",
            "Benno Moiseiwitsch",
            "Otto Ackermann",
        ],
        lead_names=["本诺·莫伊谢维奇", "Benno Moiseiwitsch", "奥托·阿克曼", "Otto Ackermann"],
        lead_names_latin=["Benno Moiseiwitsch", "Otto Ackermann"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Moiseiwitsch Schumann Piano Concerto 1954" in query for query in queries[:10])


def test_queries_for_bilibili_host_add_exact_primary_year_work_query_for_actual_moiseiwitsch_partial_scenario() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Concerto, Op.54")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "bilibili:BV1Gx4y1U7kW" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Moiseiwitsch Schumann Piano Concerto 1954" in query for query in queries[:10])


def test_queries_for_bilibili_host_does_not_add_overbroad_exact_primary_year_work_query_for_actual_annie_partial_scenario() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Concerto, Op.54")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "bilibili:BV1yqYEeKErH" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Fischer Schumann Piano Concerto 1985" not in queries[:10]


def test_queries_for_chinese_host_include_bilingual_primary_alias_with_chinese_work_shorthand() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-de-lara-query-zh-1",
        title="Adelina de Lara Schumann concerto",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        existing_links=[],
        primary_names=["阿德莉娜·德·拉腊"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        query_lead_names=["阿德莉娜·德·拉腊", "伊恩·怀特"],
        query_lead_names_latin=["Adelina de Lara Ian Whyte", "Adelina de Lara / Ian Whyte", "Adelina de Lara", "Ian Whyte"],
        lead_names=["阿德莉娜·德·拉腊", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["BBC 苏格兰交响乐团"],
        ensemble_names_latin=["BBC Scottish Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Adelina de Lara" in query and draft.work_title in query for query in queries)


def test_queries_for_chinese_host_include_bilingual_short_primary_alias_for_richter_style_titles() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-richter-query-zh-1",
        title="Sviatoslav Richter Schumann concerto",
        composer_name="\u8212\u66fc",
        composer_name_latin="Robert Schumann",
        work_title="a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1954",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | Ferencsik Janos | Hungarian National Philharmonic Orchestra | 1954",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | Ferencsik Janos | Hungarian National Philharmonic Orchestra | 1954",
        existing_links=[],
        primary_names=["\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279"],
        primary_names_latin=["Sviatoslav Richter", "Sviatoslav Teofilovich Richter"],
        secondary_names=["\u8d39\u4f26\u5947\u514b"],
        secondary_names_latin=["Ferencsik Janos", "Janos Ferencsik"],
        query_lead_names=[
            "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279",
            "\u8d39\u4f26\u5947\u514b",
        ],
        query_lead_names_latin=[
            "Sviatoslav Richter Ferencsik Janos",
            "Sviatoslav Richter / Ferencsik Janos",
            "Sviatoslav Richter",
            "Ferencsik Janos",
            "Janos Ferencsik",
        ],
        lead_names=[
            "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279",
            "\u8d39\u4f26\u5947\u514b",
        ],
        lead_names_latin=["Sviatoslav Richter", "Ferencsik Janos", "Janos Ferencsik"],
        ensemble_names=["\u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2"],
        ensemble_names_latin=["Hungarian National Philharmonic Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("舒曼" in query and draft.work_title in query and "斯维亚托斯拉夫" in query for query in queries)
    assert any(
        "Sviatoslav Richter" in query
        and "1954" in query
        and "Hungarian" not in query
        and "匈牙利" not in query
        for query in queries
    )
    assert any(query.startswith("里赫特 匈牙利 1954 舒曼钢协") for query in queries)


def test_queries_for_chinese_host_include_decade_bucket_rescue_query_for_compilation_style_titles() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-kempff-query-zh-1",
        title="Wilhelm Kempff Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["肯普夫"],
        primary_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff"],
        secondary_names=["多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        query_lead_names=["肯普夫", "多拉蒂"],
        query_lead_names_latin=[
            "Wilhelm Kempff Antal Dorati",
            "Wilhelm Kempff / Antal Dorati",
            "Wilhelm Kempff",
            "Wilhelm Walter Friedrich Kempff",
            "Antal Dorati",
        ],
        lead_names=["肯普夫", "多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Kempff Schumann Piano Concerto 1950s Op.54" in queries
    assert "Kempff Schumann Piano Concertos 1950s Op.54" in queries


def test_queries_for_chinese_host_append_catalogue_to_work_rescue_queries() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-kempff-query-zh-opus",
        title="Wilhelm Kempff Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["肯普夫"],
        primary_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff"],
        secondary_names=["多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        query_lead_names=["肯普夫", "多拉蒂"],
        query_lead_names_latin=[
            "Wilhelm Kempff Antal Dorati",
            "Wilhelm Kempff / Antal Dorati",
            "Wilhelm Kempff",
            "Wilhelm Walter Friedrich Kempff",
            "Antal Dorati",
        ],
        lead_names=["肯普夫", "多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Kempff Schumann Piano Concerto 1950s Op.54" in queries
    assert "Kempff Dorati Schumann concerto 1959 Op.54" in queries


def test_queries_for_chinese_host_include_generic_plural_bundle_rescue_without_catalogue_hint() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-kempff-query-zh-generic-bundle",
        title="Wilhelm Kempff Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["肯普夫"],
        primary_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff"],
        secondary_names=["多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        query_lead_names=["肯普夫", "多拉蒂"],
        query_lead_names_latin=[
            "Wilhelm Kempff Antal Dorati",
            "Wilhelm Kempff / Antal Dorati",
            "Wilhelm Kempff",
            "Wilhelm Walter Friedrich Kempff",
            "Antal Dorati",
        ],
        lead_names=["肯普夫", "多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Kempff Piano Concertos 1950s" in queries[:6]
    assert "Kempff Piano Concertos 1950s Op.54" not in queries


def test_queries_for_chinese_host_include_primary_composer_work_rescue_queries() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-de-lara-query-1",
        title="Adelina de Lara Schumann concerto",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        existing_links=[],
        primary_names=["阿德利纳·德·劳拉"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        query_lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        query_lead_names_latin=[
            "Adelina de Lara Ian Whyte",
            "Adelina de Lara / Ian Whyte",
            "Adelina de Lara",
            "Ian Whyte",
        ],
        lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["英国广播公司苏格兰交响乐团"],
        ensemble_names_latin=["BBC Scottish Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Adelina de Lara" in query
        and "Schumann" in query
        and ("concerto" in query.lower() or "Piano Concerto" in query)
        for query in queries
    )
    assert "de Lara Whyte Schumann concerto 1951 Op.54" in queries


def test_queries_for_non_chinese_host_append_catalogue_to_collaboration_rescue_queries() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-larrocha-query-opus",
        title="Alicia de Larrocha Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="January 12, 1977",
        venue_text="Victoria Hall",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977 - Victoria Hall",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977 - Victoria Hall",
        existing_links=[],
        primary_names=["阿利西亚·德·拉罗查"],
        primary_names_latin=["Alicia de Larrocha"],
        secondary_names=["沃尔夫冈·萨瓦利施"],
        secondary_names_latin=["Wolfgang Sawallisch"],
        query_lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        query_lead_names_latin=[
            "Alicia de Larrocha Wolfgang Sawallisch",
            "Alicia de Larrocha / Wolfgang Sawallisch",
            "Alicia de Larrocha",
            "Wolfgang Sawallisch",
        ],
        lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        lead_names_latin=["Alicia de Larrocha", "Wolfgang Sawallisch"],
        ensemble_names=["瑞士罗曼德管弦乐团"],
        ensemble_names_latin=["Orchestre de la Suisse Romande", "OSR"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "de Larrocha Sawallisch Schumann concerto 1977 Op.54" in queries
    assert "de Larrocha Sawallisch Schumann concerto Op.54" in queries


def test_extract_person_query_keyword_preserves_surname_particles() -> None:
    assert extract_person_query_keyword("Adelina de Lara") == "de Lara"
    assert extract_person_query_keyword("Wilhelm Kempff") == "Kempff"


def test_build_chinese_host_primary_work_rescue_queries_include_secondary_collaboration_hint() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-kempff-query-zh-rescue",
        title="Wilhelm Kempff & Antal Dorati",
        composer_name="Schumann",
        composer_name_latin="Robert Schumann",
        work_title="Piano Concerto in A minor",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["Kempff"],
        primary_names_latin=["Wilhelm Kempff"],
        secondary_names=["Dorati"],
        secondary_names_latin=["Antal Dorati"],
        query_lead_names=["Kempff", "Dorati"],
        query_lead_names_latin=["Wilhelm Kempff Antal Dorati", "Wilhelm Kempff / Antal Dorati", "Wilhelm Kempff", "Antal Dorati"],
        lead_names=["Kempff", "Dorati"],
        lead_names_latin=["Wilhelm Kempff", "Antal Dorati"],
        ensemble_names=["Concertgebouw Orchestra Amsterdam"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )

    queries = build_chinese_host_primary_work_rescue_queries(draft)

    assert queries[0] == "Kempff Dorati Schumann concerto 1959"
    assert "Kempff Dorati concerto 1959" in queries
    assert "Wilhelm Kempff Schumann concerto" in queries
    assert any("Wilhelm Kempff Dorati" in query for query in queries)


def test_queries_for_chinese_host_include_ensemble_bundle_rescue_query_for_nhk_style_uploads() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-annie-query-zh-bundle",
        title="Annie Fischer NHK live",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="October 18, 1985",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | Christof Prick | NHK Symphony Orchestra | October 18, 1985",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | Christof Prick | NHK Symphony Orchestra | October 18, 1985",
        existing_links=[],
        primary_names=["安妮·费舍尔"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["克里斯托夫·佩里克"],
        secondary_names_latin=["Christof Prick"],
        query_lead_names=["安妮·费舍尔", "克里斯托夫·佩里克"],
        query_lead_names_latin=[
            "Annie Fischer Christof Prick",
            "Annie Fischer / Christof Prick",
            "Annie Fischer",
            "Christof Prick",
        ],
        lead_names=["安妮·费舍尔", "克里斯托夫·佩里克"],
        lead_names_latin=["Annie Fischer", "Christof Prick"],
        ensemble_names=["日本放送协会交响乐团"],
        ensemble_names_latin=["NHK Symphony Orchestra"],
    )
    queries = build_chinese_host_bundle_context_queries(
        draft,
        ensemble_terms=["NHK Symphony Orchestra", "日本放送协会交响乐团"],
    )

    assert any(
        "Fischer" in query and "Prick" in query and "NHK" in query and "Schumann" in query and "1985" in query
        for query in queries
    )
    assert any(
        "Annie Fischer" in query and "NHK" in query and "Schumann" in query and "1985" in query
        for query in queries
    )


def test_queries_for_chinese_host_prioritize_bundle_context_query_into_primary_execution_window() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-annie-query-zh-bundle-priority",
        title="Annie Fischer NHK live",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="October 18, 1985",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | Christof Prick | NHK Symphony Orchestra | October 18, 1985",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | Christof Prick | NHK Symphony Orchestra | October 18, 1985",
        existing_links=[],
        primary_names=["安妮·费舍尔"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["克里斯托夫·佩里克"],
        secondary_names_latin=["Christof Prick"],
        query_lead_names=["安妮·费舍尔", "克里斯托夫·佩里克"],
        query_lead_names_latin=[
            "Annie Fischer Christof Prick",
            "Annie Fischer / Christof Prick",
            "Annie Fischer",
            "Christof Prick",
        ],
        lead_names=["安妮·费舍尔", "克里斯托夫·佩里克"],
        lead_names_latin=["Annie Fischer", "Christof Prick"],
        ensemble_names=["日本放送协会交响乐团"],
        ensemble_names_latin=["NHK Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("NHK" in query and "1985" in query for query in queries[:5])


def test_queries_for_chinese_host_keep_primary_work_rescue_query_for_kempff_within_execution_window() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-kempff-query-zh-window",
        title="安塔尔·多拉蒂 - 肯普夫 - 阿姆斯特丹皇家音乐厅管弦乐团 - Amsterdam",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="罗伯特·舒曼 | a小调钢琴协奏曲 | 威廉·沃尔特·弗里德里希·肯普夫 | 安塔尔·多拉蒂 | 阿姆斯特丹皇家音乐厅管弦乐团 | 1959",
        raw_text="罗伯特·舒曼 | a小调钢琴协奏曲 | 威廉·沃尔特·弗里德里希·肯普夫 | 安塔尔·多拉蒂 | 阿姆斯特丹皇家音乐厅管弦乐团 | 1959",
        existing_links=[],
        primary_names=["威廉·沃尔特·弗里德里希·肯普夫"],
        primary_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff"],
        secondary_names=["安塔尔·多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        query_lead_names=["威廉·沃尔特·弗里德里希·肯普夫", "安塔尔·多拉蒂"],
        query_lead_names_latin=[
            "Wilhelm Kempff Antal Dorati",
            "Wilhelm Kempff / Antal Dorati",
            "Wilhelm Kempff",
            "Wilhelm Walter Friedrich Kempff",
            "Antal Dorati",
        ],
        lead_names=["威廉·沃尔特·弗里德里希·肯普夫", "安塔尔·多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Wilhelm Walter Friedrich Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Royal Orchestra", "Royal Concertgebouw Orchestra", "RCO"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "Kempff Dorati Schumann concerto 1959" in queries[:6]


def test_queries_for_chinese_host_keep_primary_work_rescue_query_for_de_lara_within_execution_window() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-de-lara-query-zh-window",
        title="怀特 - 劳拉 - 英国广播公司苏格兰交响乐团 - May 29, 1951",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="罗伯特·舒曼 | a小调钢琴协奏曲 | 阿德利纳·德·劳拉 | 伊恩·怀特 | 英国广播公司苏格兰交响乐团 | May 29, 1951",
        raw_text="罗伯特·舒曼 | a小调钢琴协奏曲 | 阿德利纳·德·劳拉 | 伊恩·怀特 | 英国广播公司苏格兰交响乐团 | May 29, 1951",
        existing_links=[],
        primary_names=["阿德利纳·德·劳拉"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        query_lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        query_lead_names_latin=[
            "Adelina de Lara Ian Whyte",
            "Adelina de Lara / Ian Whyte",
            "Adelina de Lara",
            "Ian Whyte",
        ],
        lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["英国广播公司苏格兰交响乐团"],
        ensemble_names_latin=["BBC Orchestra", "BBC Scottish Symphony Orchestra", "BSSO"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "de Lara Whyte Schumann concerto 1951" in queries[:6]


def test_queries_for_chinese_host_keep_short_primary_year_query_for_richter_partial_after_rescue_insertion() -> None:
    provider = HttpSourceProvider()
    item = RetrievalItem(
        itemId="recording-a小调钢琴协奏曲-里赫特-and-费伦奇克1954-partial",
        recordingId="recording-a小调钢琴协奏曲-里赫特-and-费伦奇克1954",
        workId="work-1",
        composerId="composer-1",
        workTypeHint="concerto",
        sourceLine="罗伯特·舒曼 | a小调钢琴协奏曲 | 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 | 匈牙利国家爱乐乐团 | -",
        seed=Seed(
            title="费伦奇克 - 里赫特 - 匈牙利国家爱乐乐团 - 布达佩斯音乐学院",
            composerName="罗伯特·舒曼",
            composerNameLatin="Robert Schumann",
            workTitle="a小调钢琴协奏曲",
            workTitleLatin="Piano Concerto, Op.54",
            catalogue="",
            performanceDateText="",
            venueText="",
            albumTitle="",
            label="",
            releaseDate="",
            credits=[
                Credit(
                    role="soloist",
                    personId="person-斯维亚托斯拉夫特奥菲洛维奇里赫特",
                    displayName="斯维亚托斯拉夫·特奥菲洛维奇·里赫特",
                    label="文件名补录",
                )
            ],
            links=[],
            notes="",
        ),
        requestedFields=["links"],
    )
    draft = InputNormalizer().normalize(item)
    profile = ProfileResolver().resolve(item)
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954" in queries


def test_prepare_bilibili_browser_queries_prefers_exact_year_bundle_query_over_generic_decade_queries() -> None:
    queries = [
        "钢协 Wilhelm Kempff 1959",
        "Kempff Schumann Piano Concerto 1950s",
        "Kempff Schumann Piano Concertos 1950s",
        "Kempff Dorati Concertgebouw Schumann concerto 1959",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert "Kempff Dorati Concertgebouw Schumann concerto 1959" in selected


def test_bilibili_browser_search_tries_all_page_before_video_page_when_extracting_results() -> None:
    class AllPageOnlyFetcher:
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def fetch_links(self, url: str, *, url_patterns=None, timeout_seconds=None):
            del url_patterns, timeout_seconds
            self.urls.append(url)
            if "/all?" in url:
                return ["https://www.bilibili.com/video/BV1yqYEeKErH/"]
            return []

    fetcher = AllPageOnlyFetcher()
    provider = HttpSourceProvider(browser_fetcher=fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["Fischer Prick NHK Schumann concerto 1985"],
            url_builders=[
                lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
                lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
            ],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\\.bilibili\\.com/video/(?:BV[0-9A-Za-z]+|av\\d+)/?"],
        )
    )

    assert rows == [
        {
            "url": "https://www.bilibili.com/video/BV1yqYEeKErH/",
            "source_label": "Bilibili Search Browser Search",
            "source_kind": "streaming",
        }
    ]
    assert any("/all?keyword=" in url for url in fetcher.urls)


def test_bilibili_browser_search_skips_all_page_after_timeout_within_same_run() -> None:
    class TimeoutThenVideoFetcher:
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def fetch_links(self, url: str, *, url_patterns=None, timeout_seconds=None):
            del url_patterns, timeout_seconds
            self.urls.append(url)
            if "/all?" in url:
                raise TimeoutError("browser fetch timed out")
            if "second+query" in url:
                return ["https://www.bilibili.com/video/BV1second0001/"]
            return []

    fetcher = TimeoutThenVideoFetcher()
    provider = HttpSourceProvider(browser_fetcher=fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["first query", "second query"],
            url_builders=[
                lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
                lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
            ],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\\.bilibili\\.com/video/(?:BV[0-9A-Za-z]+|av\\d+)/?"],
        )
    )

    assert rows == [
        {
            "url": "https://www.bilibili.com/video/BV1second0001/",
            "source_label": "Bilibili Search Browser Search",
            "source_kind": "streaming",
        }
    ]
    assert "https://search.bilibili.com/all?keyword=first+query" in fetcher.urls
    assert "https://search.bilibili.com/video?keyword=first+query" in fetcher.urls
    assert "https://search.bilibili.com/video?keyword=second+query" in fetcher.urls
    assert "https://search.bilibili.com/all?keyword=second+query" not in fetcher.urls


def test_bilibili_browser_search_stops_after_full_query_timeout() -> None:
    class AlwaysTimeoutFetcher:
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def fetch_links(self, url: str, *, url_patterns=None, timeout_seconds=None):
            del url_patterns, timeout_seconds
            self.urls.append(url)
            raise TimeoutError("browser fetch timed out")

    fetcher = AlwaysTimeoutFetcher()
    provider = HttpSourceProvider(browser_fetcher=fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["first query", "second query"],
            url_builders=[
                lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
                lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
            ],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\\.bilibili\\.com/video/(?:BV[0-9A-Za-z]+|av\\d+)/?"],
        )
    )

    assert rows == []
    assert fetcher.urls == [
        "https://search.bilibili.com/all?keyword=first+query",
        "https://search.bilibili.com/video?keyword=first+query",
    ]


def test_streaming_host_timeout_seconds_caps_primary_platform_hosts(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class TimeoutCapProvider(HttpSourceProvider):
        def _queries_for_host(self, draft: DraftRecordingEntry, profile: RetrievalProfile, host: SourceProfileEntry) -> list[str]:
            del draft, profile, host
            return [f"query-{index}" for index in range(6)]

    provider = TimeoutCapProvider(profile_loader=SourceProfileLoader(root), browser_fetcher=BrowserResultFetcher({}))
    host = next(host for host in provider._profile_loader.load(category="orchestral", tags=[]).streaming if "youtube.com" in host.url)

    timeout_seconds = provider._streaming_host_timeout_seconds(build_draft(), build_profile(), host)

    assert timeout_seconds <= 18.0


def test_queries_for_host_include_compact_collaboration_surname_rescue_query_for_non_chinese_hosts() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-larrocha-query-1",
        title="Alicia de Larrocha Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="January 12, 1977",
        venue_text="Victoria Hall",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977 - Victoria Hall",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977 - Victoria Hall",
        existing_links=[],
        primary_names=["阿利西亚·德·拉罗查"],
        primary_names_latin=["Alicia de Larrocha"],
        secondary_names=["沃尔夫冈·萨瓦利施"],
        secondary_names_latin=["Wolfgang Sawallisch"],
        query_lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        query_lead_names_latin=[
            "Alicia de Larrocha Wolfgang Sawallisch",
            "Alicia de Larrocha / Wolfgang Sawallisch",
            "Alicia de Larrocha",
            "Wolfgang Sawallisch",
        ],
        lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        lead_names_latin=["Alicia de Larrocha", "Wolfgang Sawallisch"],
        ensemble_names=["瑞士罗曼德管弦乐团"],
        ensemble_names_latin=["Orchestre de la Suisse Romande", "OSR"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert "de Larrocha Sawallisch Schumann concerto 1977 Op.54" in queries


def test_queries_for_host_include_condensed_primary_alias_for_long_parent_person_name() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-grinberg-query-1",
        title="Maria Grinberg Schumann concerto",
        composer_name="Schumann",
        composer_name_latin="Robert Schumann",
        work_title="Piano Concerto in A minor",
        work_title_latin="Piano Concerto in A minor, Op.54",
        catalogue="Op.54",
        performance_date_text="1958",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        primary_names=["Maria Grinberg"],
        primary_names_latin=["Maria Israilevna Grinberg", "Mariya Israilevna Grinberg"],
        secondary_names=["Carl Eliasberg"],
        secondary_names_latin=["Carl Eliasberg"],
        query_lead_names=["Maria Grinberg", "Carl Eliasberg"],
        query_lead_names_latin=[
            "Maria Israilevna Grinberg Carl Eliasberg",
            "Maria Israilevna Grinberg / Carl Eliasberg",
            "Mariya Israilevna Grinberg Carl Eliasberg",
            "Mariya Israilevna Grinberg / Carl Eliasberg",
            "Maria Israilevna Grinberg",
            "Mariya Israilevna Grinberg",
            "Carl Eliasberg",
        ],
        lead_names=["Maria Grinberg", "Carl Eliasberg"],
        lead_names_latin=["Maria Israilevna Grinberg", "Mariya Israilevna Grinberg", "Carl Eliasberg"],
        ensemble_names=["USSR State Symphony Orchestra"],
        ensemble_names_latin=["USSR State Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Maria Grinberg" in query for query in queries)


def test_queries_for_chinese_host_include_cjk_context_rescue_for_grinberg_full_case() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-grinberg-query-zh-1",
        title="Maria Grinberg Schumann concerto",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1958",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="罗伯特·舒曼 | a小调钢琴协奏曲 | 玛丽亚·伊斯拉列夫娜·格林伯格 | 卡尔·埃利亚斯伯格 | 苏联国家交响乐团 | 1958",
        raw_text="罗伯特·舒曼 | a小调钢琴协奏曲 | 玛丽亚·伊斯拉列夫娜·格林伯格 | 卡尔·埃利亚斯伯格 | 苏联国家交响乐团 | 1958",
        existing_links=[],
        primary_names=["玛丽亚·伊斯拉列夫娜·格林伯格"],
        primary_names_latin=["Maria Grinberg", "Maria Israilevna Grinberg"],
        secondary_names=["卡尔·埃利亚斯伯格"],
        secondary_names_latin=["Carl Eliasberg"],
        query_lead_names=["玛丽亚·伊斯拉列夫娜·格林伯格", "卡尔·埃利亚斯伯格"],
        query_lead_names_latin=[
            "Maria Grinberg Carl Eliasberg",
            "Maria Grinberg / Carl Eliasberg",
            "Maria Grinberg",
            "Carl Eliasberg",
        ],
        lead_names=["玛丽亚·伊斯拉列夫娜·格林伯格", "卡尔·埃利亚斯伯格"],
        lead_names_latin=["Maria Grinberg", "Carl Eliasberg"],
        ensemble_names=["苏联国家交响乐团"],
        ensemble_names_latin=["USSR State Symphony Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.startswith("格林伯格 苏联 1958 舒曼钢协") for query in queries)


def test_extract_cjk_person_query_keyword_handles_single_and_multi_segment_names() -> None:
    assert extract_cjk_person_query_keyword("舒曼") == "舒曼"
    assert extract_cjk_person_query_keyword("费伦奇克") == "费伦奇克"
    assert extract_cjk_person_query_keyword("斯维亚托斯拉夫·特奥菲洛维奇·里赫特") == "里赫特"


def test_queries_for_host_include_hungarian_given_name_order_alias_from_bundled_person_aliases() -> None:
    provider = HttpSourceProvider()
    draft = DraftRecordingEntry(
        item_id="recording-richter-query-1",
        title="Sviatoslav Richter Schumann concerto",
        composer_name="Schumann",
        composer_name_latin="Robert Schumann",
        work_title="Piano Concerto in A minor",
        work_title_latin="Piano Concerto in A minor, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        primary_names=["Sviatoslav Richter"],
        primary_names_latin=["Sviatoslav Teofilovich Richter"],
        secondary_names=["Ferencsik Janos"],
        secondary_names_latin=["Ferencsik Janos", "Ferencsik Janos"],
        query_lead_names=["Sviatoslav Richter", "Ferencsik Janos"],
        query_lead_names_latin=[
            "Sviatoslav Teofilovich Richter Ferencsik Janos",
            "Sviatoslav Teofilovich Richter / Ferencsik Janos",
            "Sviatoslav Teofilovich Richter Ferencsik Janos",
            "Sviatoslav Teofilovich Richter / Ferencsik Janos",
            "Sviatoslav Teofilovich Richter",
            "Ferencsik Janos",
            "Ferencsik Janos",
        ],
        lead_names=["Sviatoslav Richter", "Ferencsik Janos"],
        lead_names_latin=["Sviatoslav Teofilovich Richter", "Ferencsik Janos", "Ferencsik Janos"],
        ensemble_names=["Hungarian National Philharmonic Orchestra"],
        ensemble_names_latin=["Hungarian National Philharmonic Orchestra"],
    )
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any("Janos Ferencsik" in query for query in queries)


def test_provider_prefers_apple_music_api_when_configured(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://music.apple.com\n", encoding="utf-8")
    transport = ApiFirstTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            apple_music=AppleMusicSearchConfig(developer_token="apple-token", storefront="us"),
        ),
    )

    rows = asyncio.run(provider._search_apple_music(["schumann query"]))

    assert any("music.apple.com/us/album/demo/1" in row["url"] for row in rows)
    apple_row = next(row for row in rows if "music.apple.com/us/album/demo/1" in row["url"])
    assert apple_row["title"] == "Apple API Result"
    assert "Otto Klemperer" in apple_row["description"]
    assert apple_row["duration_seconds"] == 233
    assert any("api.music.apple.com/v1/catalog/us/search" in url for url in transport.urls)
    assert not any("music.apple.com/search" in url for url in transport.urls)


def test_provider_shapes_apple_public_api_rows_and_filters_artist_noise(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://music.apple.com\n", encoding="utf-8")
    transport = ApplePublicApiTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            apple_music=AppleMusicSearchConfig(enabled=True, developer_token="", use_itunes_fallback=True),
        ),
    )

    rows = asyncio.run(provider._search_apple_music(["beethoven 7 klemperer"]))

    urls = [row["url"] for row in rows]
    assert "https://music.apple.com/us/album/demo-track/123?i=456" in urls
    assert not any("/artist/" in url for url in urls)
    shaped = next(row for row in rows if row["url"] == "https://music.apple.com/us/album/demo-track/123?i=456")
    assert shaped["title"] == "Symphony No. 7 in A major, Op. 92: II. Allegretto"
    assert "Otto Klemperer" in shaped["description"]
    assert shaped["duration_seconds"] == 512


def test_fetch_page_record_keeps_seeded_apple_title_when_browser_returns_generic_player_title(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://music.apple.com\n", encoding="utf-8")

    class AppleGenericPageTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text="<html><head><title>Apple Music 网页播放器</title></head><body></body></html>",
            )

    browser_fetcher = StructuredBrowserFetcher(
        {},
        {
            "https://music.apple.com/us/album/demo-track/123?i=456": {
                "title": "Apple Music 网页播放器",
                "description": "",
            }
        },
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=AppleGenericPageTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://music.apple.com/us/album/demo-track/123?i=456",
            "Apple Music Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
            seed_data={
                "title": "Symphony 7",
                "description": "",
                "uploader": "Otto Klemperer",
            },
        )
    )

    assert row is not None
    assert row["title"] == "Symphony 7"


def test_fetch_page_record_keeps_seeded_apple_metadata_when_html_page_is_generic_player(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://music.apple.com\n", encoding="utf-8")

    class AppleGenericHtmlTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<title>Apple Music 网页播放器</title>"
                    '<meta property="og:title" content="Apple Music 网页播放器" />'
                    '<meta property="og:description" content="在 Apple Music 上畅听数千万首歌曲，全无广告干扰。" />'
                    "</head><body></body></html>"
                ),
            )

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=AppleGenericHtmlTransport(), follow_redirects=True),
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://music.apple.com/us/album/demo-track/123?i=456",
            "Apple Music Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
            seed_data={
                "title": "Symphony No. 7 in A Major, Op. 92: II. Allegretto",
                "description": "Philharmonia Orchestra & Otto Klemperer | Beethoven: Symphony No. 7",
                "uploader": "Otto Klemperer",
                "duration_seconds": 512,
            },
        )
    )

    assert row is not None
    assert row["title"] == "Symphony No. 7 in A Major, Op. 92: II. Allegretto"
    assert "Otto Klemperer" in row["description"]


def test_fetch_page_record_uses_browser_metadata_when_youtube_html_is_generic_watch_shell(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class YoutubeGenericHtmlTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<title>- YouTube</title>"
                    '<meta property="og:title" content="- YouTube" />'
                    '<meta property="og:description" content="YouTube でお気に入りの動画や音楽を楽しみ、オリジナルのコンテンツをアップロードして友だちや家族、世界中の人たちと共有しましょう。" />'
                    "</head><body>"
                    "YouTube でお気に入りの動画や音楽を楽しみ、オリジナルのコンテンツをアップロードして友だちや家族、世界中の人たちと共有しましょう。"
                    "</body></html>"
                ),
            )

    watch_url = "https://www.youtube.com/watch?v=vCC5o4A3HMY"
    browser_fetcher = StructuredBrowserFetcher(
        {},
        {
            watch_url: {
                "title": 'Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)',
                "description": "Historic upload with rendered watch metadata",
                "uploader": "Archive C",
                "durationSeconds": 1449,
            }
        },
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=YoutubeGenericHtmlTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
    )

    row = asyncio.run(
        provider._fetch_page_record(
            watch_url,
            "YouTube Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == 'Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)'
    assert browser_fetcher.page_calls == [watch_url]


def test_fetch_page_record_strips_generic_youtube_watch_suffix_and_description(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class YoutubeWatchTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    '<meta property="og:title" content="Mogilevsky & Kreutzer - Beethoven : Spring Sonata in F Op.24 (1931) 再復刻 - YouTube" />'
                    '<meta property="og:description" content="在 YouTube 上畅享你喜爱的视频和音乐，上传原创内容并与亲朋好友和全世界观众分享你的视频。" />'
                    "</head><body></body></html>"
                ),
            )

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=YoutubeWatchTransport(), follow_redirects=True),
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.youtube.com/watch?v=2kH5DsmdubM",
            "YouTube Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == "Mogilevsky & Kreutzer - Beethoven : Spring Sonata in F Op.24 (1931) 再復刻"
    assert row["description"] == ""


def test_fetch_page_record_reads_youtube_structured_metadata_before_browser_fallback(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class YoutubeStructuredMetadataTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<title>- YouTube</title>"
                    "<script>"
                    'var ytInitialPlayerResponse = {"videoDetails":{"title":"Alexandre Moguilewsky & Leonid Kreutzer: '
                    'Beethoven: Violin Sonata No. 5 (R. ca 1931)","author":"Archive C","shortDescription":"Historic '
                    'upload with rendered watch metadata","lengthSeconds":"1449","viewCount":"12345"}};'
                    "</script>"
                    "</head><body></body></html>"
                ),
            )

    watch_url = "https://www.youtube.com/watch?v=vCC5o4A3HMY"
    browser_fetcher = StructuredBrowserFetcher({}, {})
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=YoutubeStructuredMetadataTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
    )

    row = asyncio.run(
        provider._fetch_page_record(
            watch_url,
            "YouTube Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == "Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)"
    assert row["description"] == "Historic upload with rendered watch metadata"
    assert row["uploader"] == "Archive C"
    assert row["duration_seconds"] == 1449
    assert row["view_count"] == 12345
    assert browser_fetcher.page_calls == []


def test_fetch_page_record_reads_youtube_overlay_metadata_when_player_response_lacks_video_details(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class YoutubeOverlayMetadataTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<title>- YouTube</title>"
                    "<script>var ytInitialPlayerResponse = {\"responseContext\":{\"serviceTrackingParams\":[]}};</script>"
                    "<script>"
                    '{"videoDetails":{"playerOverlayVideoDetailsRenderer":{"title":{"simpleText":"Alexandre Moguilewsky '
                    '& Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)"},"subtitle":{"runs":[{"text":'
                    '"Archive C"},{"text":"   "},{"text":"154 views"},{"text":"   "},{"text":"4 years ago"}]}}},'
                    '"lengthText":{"accessibility":{"accessibilityData":{"label":"24 minutes, 9 seconds"}}}}'
                    "</script>"
                    "</head><body></body></html>"
                ),
            )

    watch_url = "https://www.youtube.com/watch?v=vCC5o4A3HMY"
    browser_fetcher = StructuredBrowserFetcher({}, {})
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=YoutubeOverlayMetadataTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
    )

    row = asyncio.run(
        provider._fetch_page_record(
            watch_url,
            "YouTube Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == "Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)"
    assert row["uploader"] == "Archive C"
    assert row["duration_seconds"] == 1449
    assert browser_fetcher.page_calls == []


def test_score_recording_match_treats_apple_track_resource_as_viable_version_evidence() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-beethoven7-klemperer",
        title="Otto Klemperer 1957",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in A major,Op.92",
        catalogue="Op.92",
        performance_date_text="1957",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Otto Klemperer | Philharmonia Orchestra | 1957",
        raw_text="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Otto Klemperer | Philharmonia Orchestra | 1957",
        existing_links=[],
        primary_names=["克伦佩勒"],
        primary_names_latin=["Otto Klemperer"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["克伦佩勒"],
        query_lead_names_latin=["Otto Klemperer"],
        lead_names=["克伦佩勒"],
        lead_names_latin=["Otto Klemperer"],
        ensemble_names=["爱乐乐团"],
        ensemble_names_latin=["Philharmonia Orchestra"],
    )

    apple_track_score = score_recording_match(
        "Symphony No. 7 in A major, Op. 92: II. Allegretto Otto Klemperer Philharmonia Orchestra Beethoven",
        "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-ii-allegretto/616361485?i=616361604",
        draft,
        duration_seconds=512,
        uploader="Otto Klemperer",
    )
    wrong_artist_score = score_recording_match(
        "Symphony No. 7 in A major, Op. 92: II. Allegretto Another Conductor Another Orchestra Beethoven",
        "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-ii-allegretto/999999999?i=999999999",
        draft,
        duration_seconds=512,
        uploader="Another Conductor",
    )

    assert apple_track_score >= 0.45
    assert apple_track_score > wrong_artist_score


def test_score_recording_match_reduces_single_movement_penalty_for_apple_track_resources() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-beethoven7-klemperer-apple-boost",
        title="Otto Klemperer 1957",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in A major,Op.92",
        catalogue="Op.92",
        performance_date_text="1957",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Otto Klemperer | Philharmonia Orchestra | 1957",
        raw_text="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Otto Klemperer | Philharmonia Orchestra | 1957",
        existing_links=[],
        primary_names=["克伦佩勒"],
        primary_names_latin=["Otto Klemperer"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["克伦佩勒"],
        query_lead_names_latin=["Otto Klemperer"],
        lead_names=["克伦佩勒"],
        lead_names_latin=["Otto Klemperer"],
        ensemble_names=["爱乐乐团"],
        ensemble_names_latin=["Philharmonia Orchestra"],
    )

    apple_track_score = score_recording_match(
        "Symphony No. 7 in A Major, Op. 92: I. Poco sostenuto - Vivace Philharmonia Orchestra Otto Klemperer Beethoven Symphony No. 7",
        "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-i-poco-sostenuto-vivace/930843917?i=930843927",
        draft,
        duration_seconds=768,
        uploader="Philharmonia Orchestra & Otto Klemperer",
    )

    assert apple_track_score >= 0.6


def test_score_recording_match_does_not_over_penalize_apple_track_for_multi_work_album_context() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-kleiber-apple-track",
        title="Carlos Kleiber 1976",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in A major,Op.92",
        catalogue="Op.92",
        performance_date_text="1976",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Carlos Kleiber | Vienna Philharmonic | 1976",
        raw_text="Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Carlos Kleiber | Vienna Philharmonic | 1976",
        existing_links=[],
        primary_names=["Carlos Kleiber"],
        primary_names_latin=["Carlos Kleiber"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["Carlos Kleiber"],
        query_lead_names_latin=["Carlos Kleiber"],
        lead_names=["Carlos Kleiber"],
        lead_names_latin=["Carlos Kleiber"],
        ensemble_names=["Vienna Philharmonic"],
        ensemble_names_latin=["Vienna Philharmonic", "Wiener Philharmoniker"],
    )

    score = score_recording_match(
        "Symphony No. 7 in A Major, Op. 92: I. Poco sostenuto - Vivace Vienna Philharmonic & Carlos Kleiber Beethoven: Symphonies Nos. 5 & 7 Classical 1995-02-20T12:00:00Z",
        "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-i-poco-sostenuto-vivace/1644892939?i=1644892962",
        draft,
        duration_seconds=814,
        uploader="Vienna Philharmonic & Carlos Kleiber",
    )

    assert score >= 0.67


def test_provider_prefers_bilibili_api_when_cookie_configured(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = ApiFirstTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(cookie="SESSDATA=abc", user_agent="UA/1.0"),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["布鲁克纳 伯姆"]))

    assert any(row["url"] == "https://www.bilibili.com/video/BV1apiresult1" for row in rows)
    bilibili_api_url = next(url for url in transport.urls if "api.bilibili.com/x/web-interface/wbi/search/type" in url)
    assert transport.headers[bilibili_api_url]["cookie"] == "SESSDATA=abc"
    assert transport.headers[bilibili_api_url]["referer"] == "https://www.bilibili.com"
    assert "w_rid=" in bilibili_api_url
    assert "wts=" in bilibili_api_url
    assert any(url.rstrip("/") == "https://www.bilibili.com" for url in transport.urls)
    assert any("api.bilibili.com/x/web-interface/nav" in url for url in transport.urls)


def test_provider_uses_bilibili_public_wbi_search_without_cookie(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = ApiFirstTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=True, user_agent="UA/1.0"),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["海菲兹 托斯卡尼尼 1940"]))

    assert any(row["url"] == "https://www.bilibili.com/video/BV1apiresult1" for row in rows)
    bilibili_api_url = next(url for url in transport.urls if "api.bilibili.com/x/web-interface/wbi/search/type" in url)
    assert transport.headers[bilibili_api_url]["user-agent"] == "UA/1.0"
    assert transport.headers[bilibili_api_url]["referer"] == "https://www.bilibili.com"
    assert not any("search.bilibili.com/all" in url for url in transport.urls)


def test_provider_keeps_bilibili_api_results_when_duration_is_mmss_string(tmp_path: Path) -> None:
    class DurationStringApiTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            self.urls.append(url)
            if url.rstrip("/") == "https://www.bilibili.com":
                return httpx.Response(200, request=request, text="home")
            if "api.bilibili.com/x/web-interface/nav" in url:
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "code": 0,
                        "data": {
                            "wbi_img": {
                                "img_url": "https://i0.hdslb.com/bfs/wbi/abcdefghijklmnopqrstuvwxyz123456.png",
                                "sub_url": "https://i0.hdslb.com/bfs/wbi/uvwxyzabcdefghijklmnopqrstuvwxyz123456.jpg",
                            }
                        },
                    },
                )
            if "api.bilibili.com/x/web-interface/wbi/search/type" in url:
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "code": 0,
                        "data": {
                            "result": [
                                {
                                    "arcurl": "https://www.bilibili.com/video/BV1duration95m56s/",
                                    "bvid": "BV1duration95m56s",
                                    "title": "Schumann Piano Concerto live",
                                    "author": "Archive",
                                    "duration": "95:56",
                                    "play": 12345,
                                }
                            ]
                        },
                    },
                )
            return httpx.Response(404, request=request, text="not found")

    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = DurationStringApiTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=True, user_agent="UA/1.0"),
        ),
    )
    provider.start_request_scope()

    rows = asyncio.run(provider._search_bilibili(["布鲁克纳 伯姆"]))
    warnings = provider.consume_warnings()

    assert rows
    assert rows[0]["url"] == "https://www.bilibili.com/video/BV1duration95m56s/"
    assert rows[0]["bvid"] == "BV1duration95m56s"
    assert rows[0]["duration_seconds"] == 5756
    assert not warnings
    assert not any("search.bilibili.com/all" in url for url in transport.urls)


def test_provider_tries_multiple_apple_music_html_endpoints_without_api(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://classical.music.apple.com\nhttps://music.apple.com\n", encoding="utf-8")
    transport = HtmlEndpointFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            apple_music=AppleMusicSearchConfig(enabled=False, use_itunes_fallback=False),
        ),
    )

    rows = asyncio.run(provider._search_apple_music(["schumann query"]))

    assert any("music.apple.com/us/album/fallback/1" in row["url"] for row in rows)
    assert any("classical.music.apple.com/search" in url for url in transport.urls)
    assert any("music.apple.com/search" in url for url in transport.urls)


def test_provider_tries_multiple_bilibili_html_endpoints_without_api(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = HtmlEndpointFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["布鲁克纳 伯姆"]))

    assert any(row["url"] == "https://www.bilibili.com/video/BV1fallbackvideo1" for row in rows)
    assert any("search.bilibili.com/all" in url for url in transport.urls)
    assert any("search.bilibili.com/video" in url for url in transport.urls)


def test_provider_sends_bilibili_headers_for_html_search_when_configured(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = HtmlEndpointFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(
                enabled=False,
                cookie="SESSDATA=abc; buvid3=def",
                user_agent="TestAgent/1.0",
                referer="https://www.bilibili.com",
            ),
        ),
    )

    asyncio.run(provider._search_bilibili(["布鲁克纳 伯姆"]))

    bilibili_search_url = next(url for url in transport.urls if "search.bilibili.com/all" in url)
    assert transport.headers[bilibili_search_url]["cookie"] == "SESSDATA=abc; buvid3=def"
    assert transport.headers[bilibili_search_url]["referer"] == "https://www.bilibili.com"
    assert transport.headers[bilibili_search_url]["user-agent"] == "TestAgent/1.0"


def test_provider_falls_back_to_search_engine_when_bilibili_html_is_empty(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = PlatformEngineFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["布鲁克纳 伯姆"]))

    assert any(row["url"] == "https://www.bilibili.com/video/BV1enginefallback1" for row in rows)
    assert any("search.bilibili.com/all" in url for url in transport.urls)
    assert any("bing.com" in url and "site%3Awww.bilibili.com" in url for url in transport.urls)


def test_provider_uses_browser_rendered_bilibili_results_before_search_engine_fallback(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = PlatformEngineFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    browser_fetcher = BrowserResultFetcher(
        {
            "https://search.bilibili.com/video?keyword=%E6%B5%B7%E8%8F%B2%E5%85%B9+%E6%89%98%E6%96%AF%E5%8D%A1%E5%B0%BC%E5%B0%BC+1940": [
                "https://www.bilibili.com/video/BV1browserhit1/",
                "https://www.bilibili.com/video/BV1browserhit2/",
            ]
        }
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=browser_fetcher,
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["海菲兹 托斯卡尼尼 1940"]))

    assert [row["url"] for row in rows[:2]] == [
        "https://www.bilibili.com/video/BV1browserhit1/",
        "https://www.bilibili.com/video/BV1browserhit2/",
    ]
    assert any("search.bilibili.com/video" in url for url in browser_fetcher.link_calls)
    assert not any("bing.com" in url and "site%3Awww.bilibili.com" in url for url in transport.urls)
    assert not any(row["url"] == "https://www.bilibili.com/video/BV1enginefallback1" for row in rows)


def test_provider_accepts_browser_rendered_bilibili_av_links(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    transport = PlatformEngineFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    browser_fetcher = BrowserResultFetcher(
        {
            "https://search.bilibili.com/video?keyword=%E4%BC%AF%E6%81%A9%E6%96%AF%E5%9D%A6+1977": [
                "https://www.bilibili.com/video/av317938669",
            ]
        }
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=browser_fetcher,
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )

    rows = asyncio.run(provider._search_bilibili(["伯恩斯坦 1977"]))

    assert any(row["url"] == "https://www.bilibili.com/video/av317938669" for row in rows)


def test_provider_uses_browser_metadata_to_enrich_bilibili_video_pages(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class MinimalBilibiliTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "www.bilibili.com/video/BV1TE411f7uh" in str(request.url):
                return httpx.Response(
                    200,
                    request=request,
                    text=(
                        "<html><head><title>"
                        "【安妮·费舍尔】舒曼钢协现场视频 Annie Fischer plays Schumann Piano Concerto Op. 54_哔哩哔哩_bilibili"
                        "</title></head><body></body></html>"
                    ),
                )
            return httpx.Response(404, request=request, text="not found")

    browser_fetcher = StructuredBrowserFetcher(
        {},
        {
            "https://www.bilibili.com/video/BV1TE411f7uh/": {
                "title": "【安妮·费舍尔】舒曼钢协现场视频 Annie Fischer plays Schumann Piano Concerto Op. 54",
                "description": "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
                "bodyText": "Annie Fischer Schumann Piano Concerto Op.54",
                "imageUrl": "",
                "uploader": "艾斯跳票",
                "durationSeconds": 2017,
                "viewCount": 1748,
            }
        },
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=MinimalBilibiliTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
    )
    draft = DraftRecordingEntry(
        item_id="annie-metadata",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV1TE411f7uh/",
            "Bilibili Search Browser Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["uploader"] == "艾斯跳票"
    assert row["duration_seconds"] == 2017
    assert row["view_count"] == 1748
    assert row["same_recording_score"] >= 0.6


def test_fetch_page_record_uses_browser_fallback_for_suspicious_bilibili_metadata(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class SuspiciousBilibiliTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "www.bilibili.com/video/BV14GZhYEE2D" not in str(request.url):
                return httpx.Response(404, request=request, text="not found")
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<script>"
                    'window.__INITIAL_STATE__={"videoData":{"title":"Egon Petri","bvid":"BV14GZhYEE2D",'
                    '"owner":{"name":"tANcL-4"},"stat":{"view":2603},"duration":86990,"desc":"保全录"}};'
                    "</script>"
                    "</head><body></body></html>"
                ),
            )

    browser_fetcher = StructuredBrowserFetcher(
        {},
        {
            "https://www.bilibili.com/video/BV14GZhYEE2D/": {
                "title": 'Egon Petri plays Beethoven Sonata No. 23 in F minor Op. 57 "Appassionata"',
                "description": "Historic upload. Egon Petri live 1956.",
                "bodyText": 'Egon Petri Beethoven Sonata No. 23 in F minor Op. 57 "Appassionata" 1956',
                "imageUrl": "",
                "uploader": "Franz-van-Chopin",
                "durationSeconds": 1214,
                "viewCount": 314,
            }
        },
    )
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=SuspiciousBilibiliTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )
    draft = DraftRecordingEntry(
        item_id="egon-petri-appassionata",
        title="Egon Petri 1956",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="1956",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        raw_text="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        existing_links=[],
        primary_names=["埃贡·佩特里"],
        primary_names_latin=["Egon Petri"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["埃贡·佩特里"],
        query_lead_names_latin=["Egon Petri"],
        lead_names=["埃贡·佩特里"],
        lead_names_latin=["Egon Petri"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV14GZhYEE2D/",
            "Bilibili Search Browser Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == 'Egon Petri plays Beethoven Sonata No. 23 in F minor Op. 57 "Appassionata"'
    assert row["duration_seconds"] == 1214
    assert row["fields"]["performanceDateText"] == "1956"
    assert row["same_recording_score"] >= 0.6
    assert browser_fetcher.page_calls == ["https://www.bilibili.com/video/BV14GZhYEE2D/"]


def test_fetch_page_record_does_not_extract_misleading_year_from_suspicious_bilibili_metadata_when_browser_fallback_stays_empty(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class SuspiciousBilibiliYearLeakTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "www.bilibili.com/video/BV14GZhYEE2D" not in str(request.url):
                return httpx.Response(404, request=request, text="not found")
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    "<script>"
                    'window.__INITIAL_STATE__={"videoData":{"title":"Egon Petri","bvid":"BV14GZhYEE2D",'
                    '"owner":{"name":"tANcL-4"},"stat":{"view":2603},"duration":86990,'
                    '"desc":"淇濆叏褰?","ctime":1959}};'
                    "</script>"
                    "</head><body></body></html>"
                ),
            )

    browser_fetcher = StructuredBrowserFetcher({}, {})
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=SuspiciousBilibiliYearLeakTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )
    draft = DraftRecordingEntry(
        item_id="egon-petri-appassionata-year-guard",
        title="Egon Petri 1956",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="1956",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        raw_text="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        existing_links=[],
        primary_names=["埃贡·佩特里"],
        primary_names_latin=["Egon Petri"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["埃贡·佩特里"],
        query_lead_names_latin=["Egon Petri"],
        lead_names=["埃贡·佩特里"],
        lead_names_latin=["Egon Petri"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV14GZhYEE2D/",
            "Bilibili Search Browser Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == "Egon Petri"
    assert row["fields"]["performanceDateText"] == "1956"
    assert browser_fetcher.page_calls == ["https://www.bilibili.com/video/BV14GZhYEE2D/"]


def test_fetch_page_record_does_not_extract_misleading_year_from_suspicious_bilibili_detail_api_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class SuspiciousBilibiliDetailApiTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.rstrip("/") == "https://www.bilibili.com":
                return httpx.Response(200, request=request, text="home")
            if "api.bilibili.com/x/web-interface/nav" in url:
                return httpx.Response(
                    200,
                    request=request,
                    json={"data": {"wbi_img": {"img_url": "https://i0.hdslb.com/bfs/wbi/abc.png", "sub_url": "https://i0.hdslb.com/bfs/wbi/def.png"}}},
                )
            if "api.bilibili.com/x/web-interface/view" in url and "BV14GZhYEE2D" in url:
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "code": 0,
                        "data": {
                            "bvid": "BV14GZhYEE2D",
                            "title": "Egon Petri",
                            "desc": "괏홍쩌",
                            "pic": "https://i0.hdslb.com/demo.jpg",
                            "duration": 86990,
                            "owner": {"name": "tANcL-4"},
                            "stat": {"view": 2605},
                            "pages": [{"part": "Historic source 1959 archive part"}],
                        },
                    },
                )
            if "www.bilibili.com/video/BV14GZhYEE2D" in url:
                return httpx.Response(200, request=request, text="<html><head><title>Egon Petri</title></head><body></body></html>")
            return httpx.Response(404, request=request, text="not found")

    browser_fetcher = StructuredBrowserFetcher({}, {})
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=SuspiciousBilibiliDetailApiTransport(), follow_redirects=True),
        browser_fetcher=browser_fetcher,
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=True, user_agent="UA/1.0"),
        ),
    )
    draft = DraftRecordingEntry(
        item_id="egon-petri-appassionata-detail-api-year-guard",
        title="Egon Petri 1956",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="1956",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        raw_text="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        existing_links=[],
        primary_names=["埃贡·佩特里"],
        primary_names_latin=["Egon Petri"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["埃贡·佩特里"],
        query_lead_names_latin=["Egon Petri"],
        lead_names=["埃贡·佩特里"],
        lead_names_latin=["Egon Petri"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV14GZhYEE2D/",
            "Bilibili Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["title"] == "Egon Petri"
    assert row["fields"]["performanceDateText"] == "1956"
    assert browser_fetcher.page_calls == ["https://www.bilibili.com/video/BV14GZhYEE2D/"]


def test_fetch_page_record_extracts_bilibili_month_year_from_description_context(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class BilibiliMonthYearTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                request=request,
                text=(
                    "<html><head>"
                    '<meta property="og:title" content="Szymon Goldberg, Miyoko Yamane - Beethoven Spring Sonata" />'
                    '<meta property="og:description" content="录制于1991年6月，Szymon Goldberg, Miyoko Yamane" />'
                    "</head><body></body></html>"
                ),
            )

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=BilibiliMonthYearTransport(), follow_redirects=True),
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV1tS411P7xf/",
            "Bilibili Search Browser Search",
            "streaming",
            build_draft(),
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["fields"]["performanceDateText"] == "June, 1991"


def test_provider_canonicalizes_bilibili_av_url_to_bv_when_metadata_exposes_bvid(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class CanonicalBilibiliTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "www.bilibili.com/video/av317938669" in str(request.url):
                return httpx.Response(
                    200,
                    request=request,
                    text=(
                        '<html><script>window.__INITIAL_STATE__={"videoData":{"title":"Bernstein Fantastique",'
                        '"bvid":"BV16P411Y7J1","owner":{"name":"uploader"},"stat":{"view":1024},"duration":3010}};</script></html>'
                    ),
                )
            return httpx.Response(404, request=request, text="not found")

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=CanonicalBilibiliTransport(), follow_redirects=True),
        browser_fetcher=BrowserResultFetcher({}),
    )
    draft = DraftRecordingEntry(
        item_id="bernstein-bvid",
        title="Bernstein Fantastique",
        composer_name="柏辽兹",
        composer_name_latin="Hector Berlioz",
        work_title="幻想交响曲",
        work_title_latin="Symphonie Fantastique",
        catalogue="",
        performance_date_text="1977",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Berlioz | Symphonie Fantastique | Leonard Bernstein | 1977",
        raw_text="Berlioz | Symphonie Fantastique | Leonard Bernstein | 1977",
        existing_links=[],
        primary_names=["Leonard Bernstein"],
        primary_names_latin=["Leonard Bernstein"],
        lead_names=["Leonard Bernstein"],
        lead_names_latin=["Leonard Bernstein"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/av317938669",
            "Bilibili Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["url"] == "https://www.bilibili.com/video/BV16P411Y7J1/"


def test_provider_ignores_related_video_year_noise_in_bilibili_metadata(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class NoisyBilibiliTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "www.bilibili.com/video/BV16P411Y7J1" in str(request.url):
                return httpx.Response(
                    200,
                    request=request,
                    text=(
                        '<html><script>window.__INITIAL_STATE__={"videoData":{'
                        '"title":"伯恩斯坦《柏辽兹：幻想交响曲》法国国家管弦乐团「BD」",'
                        '"desc":"Blu-ray Disc（蓝光碟） - 1080i片源'
                        ' Hector Louis Berlioz (1803—1869)'
                        ' Symphonie fantastique, Op. 14'
                        ' Orchestre National de France'
                        ' Leonard Bernstein, conductor'
                        ' 相关视频：卡拉扬《贝多芬：第五交响曲“命运”》柏林爱乐1982「欧盟版」",'
                        '"bvid":"BV16P411Y7J1",'
                        '"owner":{"name":"Rigel口袋音乐"},'
                        '"stat":{"view":6655},'
                        '"duration":3340'
                        '}};</script></html>'
                    ),
                )
            return httpx.Response(404, request=request, text="not found")

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=NoisyBilibiliTransport(), follow_redirects=True),
        browser_fetcher=BrowserResultFetcher({}),
    )
    draft = DraftRecordingEntry(
        item_id="bernstein-noisy-bvid",
        title="Bernstein Fantastique 1977",
        composer_name="柏辽兹",
        composer_name_latin="Hector Berlioz",
        work_title="幻想交响曲",
        work_title_latin="Symphonie Fantastique",
        catalogue="Op. 14",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Berlioz | Symphonie Fantastique | Leonard Bernstein | -",
        raw_text="Berlioz | Symphonie Fantastique | Leonard Bernstein | -",
        existing_links=[],
        primary_names=["Leonard Bernstein"],
        primary_names_latin=["Leonard Bernstein"],
        lead_names=["Leonard Bernstein"],
        lead_names_latin=["Leonard Bernstein"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV16P411Y7J1/",
            "Bilibili Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["same_recording_score"] >= 0.9
    assert row["fields"]["releaseDate"] == ""


def test_provider_prefers_bilibili_detail_api_before_html_page_fetch(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class BilibiliDetailApiTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            self.urls.append(url)
            if url.rstrip("/") == "https://www.bilibili.com":
                return httpx.Response(200, request=request, text="home")
            if "api.bilibili.com/x/web-interface/view" in url and "BV1TE411f7uh" in url:
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "code": 0,
                        "data": {
                            "bvid": "BV1TE411f7uh",
                            "title": "【安妮·费舍尔】舒曼钢协现场视频 Annie Fischer plays Schumann Piano Concerto Op. 54",
                            "desc": "Annie Fischer Budapest Philharmonic Orchestra Kletzki",
                            "pic": "https://i0.hdslb.com/demo.jpg",
                            "duration": 2017,
                            "owner": {"name": "艾斯路票"},
                            "stat": {"view": 1748},
                            "pages": [
                                {"part": "I. Allegro affettuoso"},
                                {"part": "II. Intermezzo"},
                                {"part": "III. Allegro vivace"},
                            ],
                        },
                    },
                )
            if "www.bilibili.com/video/BV1TE411f7uh" in url:
                return httpx.Response(500, request=request, text="html should not be required")
            return httpx.Response(404, request=request, text="not found")

    transport = BilibiliDetailApiTransport()
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=transport, follow_redirects=True),
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=True, user_agent="UA/1.0"),
        ),
    )
    draft = DraftRecordingEntry(
        item_id="annie-detail-api",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV1TE411f7uh/",
            "Bilibili API Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["uploader"] == "艾斯路票"
    assert row["duration_seconds"] == 2017
    assert row["view_count"] == 1748
    assert row["same_recording_score"] >= 0.6
    assert any("api.bilibili.com/x/web-interface/view" in url for url in transport.urls)
    assert not any("www.bilibili.com/video/BV1TE411f7uh" in url for url in transport.urls)


def test_provider_uses_seeded_bilibili_search_metadata_without_fetching_video_page(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\n", encoding="utf-8")

    class LoggingTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            self.urls.append(str(request.url))
            return httpx.Response(500, request=request, text="should not fetch")

    transport = LoggingTransport()
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=transport, follow_redirects=True),
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            bilibili=BilibiliSearchConfig(enabled=False),
        ),
    )
    draft = DraftRecordingEntry(
        item_id="annie-seeded-bilibili",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.bilibili.com/video/BV1TE411f7uh/",
            "Bilibili API Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
            seed_data={
                "title": "Annie Fischer plays Schumann Piano Concerto Op. 54",
                "description": "Budapest Philharmonic Orchestra Kletzki",
                "uploader": "鑹炬柉璺エ",
                "duration_seconds": 2017,
                "view_count": 1748,
                "bvid": "BV1TE411f7uh",
            },
        )
    )

    assert row is not None
    assert row["url"] == "https://www.bilibili.com/video/BV1TE411f7uh/"
    assert row["uploader"] == "鑹炬柉璺エ"
    assert row["duration_seconds"] == 2017
    assert row["view_count"] == 1748
    assert row["same_recording_score"] >= 0.6
    assert transport.urls == []


def test_provider_uses_page_body_text_to_score_sparse_collaboration_upload(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")

    class SparseYouTubeTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "www.youtube.com/watch?v=9YWr1UcbZE8" in url:
                return httpx.Response(
                    200,
                    request=request,
                    text=(
                        "<html>"
                        '<meta property="og:title" content="Beethoven: Violin Concerto (1940) Heifetz">'
                        '<meta property="og:description" content="Historic upload.">'
                        "<body>"
                        "Ludwig van Beethoven Violin Concerto in D, Op. 61 "
                        "1. Allegro ma non troppo 2. Larghetto 3. Rondo "
                        "Jascha Heifetz violin Arturo Toscanini conductor"
                        "</body>"
                        "</html>"
                    ),
                )
            return httpx.Response(404, request=request, text="not found")

    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=httpx.AsyncClient(transport=SparseYouTubeTransport(), follow_redirects=True),
        browser_fetcher=BrowserResultFetcher({}),
    )
    draft = DraftRecordingEntry(
        item_id="heifetz-body-score",
        title="托斯卡尼尼 - 海菲兹 - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="D大调小提琴协奏曲",
        work_title_latin="Violin Concerto in D major, Op. 61",
        catalogue="Op.61",
        performance_date_text="March 11, 1940",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | -",
        raw_text="Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | -",
        existing_links=[],
        primary_names=["亚莎·海菲兹"],
        primary_names_latin=["Jascha Heifetz"],
        secondary_names=["托斯卡尼尼"],
        secondary_names_latin=[],
        lead_names=["亚莎·海菲兹", "托斯卡尼尼"],
        lead_names_latin=["Jascha Heifetz"],
        ensemble_names=["NBC Symphony Orchestra"],
        ensemble_names_latin=["NBC Symphony Orchestra"],
    )

    row = asyncio.run(
        provider._fetch_page_record(
            "https://www.youtube.com/watch?v=9YWr1UcbZE8",
            "YouTube Search",
            "streaming",
            draft,
            asyncio.Semaphore(1),
        )
    )

    assert row is not None
    assert row["same_recording_score"] >= 0.75


def test_build_bilibili_metadata_from_detail_keeps_late_page_parts_for_multi_p_target() -> None:
    detail = BilibiliVideoDetail(
        endpoint_url="https://api.bilibili.com/x/web-interface/view?bvid=BV1lateparts",
        title="Larrocha拉罗查现场录音③勃拉姆斯、舒曼 Brahms Schumann",
        description="Alicia de Larrocha live collection",
        image_url="https://img.example/cover.jpg",
        uploader="天霁通明",
        bvid="BV1lateparts",
        duration_seconds=5400,
        view_count=1200,
        page_parts=[
            "Brahms Concerto",
            "Interview",
            "Encore",
            "Credits",
            "Schumann Piano Concerto Op.54",
        ],
    )

    metadata = build_bilibili_metadata_from_detail(detail)

    assert "Schumann Piano Concerto Op.54" in metadata["body_text"]


def test_provider_falls_back_to_search_engine_when_apple_html_is_empty(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://classical.music.apple.com\nhttps://music.apple.com\n", encoding="utf-8")
    transport = PlatformEngineFallbackTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        platform_search_config=PlatformSearchConfig(
            apple_music=AppleMusicSearchConfig(enabled=False, use_itunes_fallback=False),
        ),
    )

    rows = asyncio.run(provider._search_apple_music(["schumann query"]))

    assert any("music.apple.com/us/album/engine-fallback/1" in row["url"] for row in rows)
    assert any("classical.music.apple.com/search" in url for url in transport.urls)
    assert any("bing.com" in url and "site%3Amusic.apple.com" in url for url in transport.urls)


def test_search_streaming_does_not_let_first_host_starve_youtube_follow_up(tmp_path: Path) -> None:
    class NoHydrateProvider(HttpSourceProvider):
        async def _hydrate_results(self, draft, rows, source_kind):
            del draft, source_kind
            return rows

    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\nhttps://www.youtube.com\n", encoding="utf-8")
    transport = ApiFirstTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = NoHydrateProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        browser_fetcher=BrowserResultFetcher({}),
        platform_search_config=PlatformSearchConfig(
            youtube=YouTubeSearchConfig(api_key="yt-key"),
            bilibili=BilibiliSearchConfig(cookie="SESSDATA=abc", user_agent="UA/1.0"),
        ),
    )

    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    assert rows
    assert any("googleapis.com/youtube/v3/search" in url for url in transport.urls)
    assert any("api.bilibili.com/x/web-interface/wbi/search/type" in url for url in transport.urls)


def test_search_streaming_queries_priority_hosts_in_parallel(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    provider = ParallelHostProvider(profile_loader=SourceProfileLoader(root))

    started = time.perf_counter()
    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))
    elapsed = time.perf_counter() - started

    assert len(rows) == 2
    assert elapsed < 0.2
    assert len(provider.host_start_times) == 2
    start_times = sorted(provider.host_start_times.values())
    assert start_times[-1] - start_times[0] < 0.08


def test_search_streaming_keeps_bilibili_coverage_even_when_youtube_fills_budget(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    provider = PriorityCoverageProvider(profile_loader=SourceProfileLoader(root))

    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    urls = [row["url"] for row in rows]
    assert any("youtube.com/watch" in url for url in urls)
    assert any("bilibili.com/video/BV1priorityhit1" in url for url in urls)


def test_search_streaming_keeps_deeper_youtube_hit_when_multiple_priority_hosts_share_budget(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    provider = MultiHostDeepSliceAwareProvider(profile_loader=SourceProfileLoader(root))

    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    urls = [row["url"] for row in rows]
    assert "https://www.youtube.com/watch?v=annie-deep-hit" in urls
    assert "https://www.youtube.com/watch?v=annie-deep-hit" in provider.hydrated_urls
    assert any("bilibili.com/video/BV1coverage01/" in url for url in urls)


def test_search_streaming_expands_hydration_window_when_initial_slice_has_no_promising_hits(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n[zh] https://www.bilibili.com\n", encoding="utf-8")
    provider = AdaptiveHydrationProvider(profile_loader=SourceProfileLoader(root))

    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    urls = [row["url"] for row in rows]
    assert "https://www.youtube.com/watch?v=yt09" in urls
    assert len(provider.hydration_windows) >= 2
    assert len(provider.hydration_windows[0]) == 12
    assert len(provider.hydration_windows[-1]) > 12


def test_search_streaming_broadens_initial_window_for_deep_bilibili_multi_host_mix(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\n[zh] https://www.bilibili.com\nhttps://www.youtube.com\n", encoding="utf-8")
    provider = DeepBilibiliMultiHostProvider(profile_loader=SourceProfileLoader(root))

    rows = asyncio.run(provider.search_streaming(build_draft(), build_profile()))

    urls = [row["url"] for row in rows]
    assert "https://www.youtube.com/watch?v=bohm-target-hit" in urls
    assert len(provider.hydration_windows) >= 1
    assert len(provider.hydration_windows[0]) > 12


def test_bilibili_browser_search_keeps_later_query_hit_even_when_first_query_fills_budget() -> None:
    browser_fetcher = BrowserResultFetcher(
        {
            "https://search.bilibili.com/all?keyword=generic+one": [
                f"https://www.bilibili.com/video/BV1generic{i:02d}/" for i in range(1, 11)
            ],
            "https://search.bilibili.com/all?keyword=target+two": [
                "https://www.bilibili.com/video/BV1targethit1/",
            ],
        }
    )
    provider = HttpSourceProvider(browser_fetcher=browser_fetcher)

    rows = asyncio.run(
        provider._search_platform_via_browser_pages(
            queries=["generic one", "target two"],
            url_builders=[lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}"],
            source_label="Bilibili Search",
            url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
        )
    )

    assert any(row["url"] == "https://www.bilibili.com/video/BV1targethit1/" for row in rows)


def test_search_bilibili_keeps_browser_coverage_when_api_rows_fill_budget() -> None:
    class BilibiliMergeCoverageProvider(HttpSourceProvider):
        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return [
                {
                    "url": f"https://www.bilibili.com/video/BV1api{i:02d}/",
                    "source_label": "Bilibili API Search",
                    "source_kind": "streaming",
                }
                for i in range(1, 15)
            ]

        async def _search_platform_via_browser_pages(self, **kwargs):
            del kwargs
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1browserhit1/",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                },
                {
                    "url": "https://www.bilibili.com/video/BV1browserhit2/",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                },
            ]

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliMergeCoverageProvider(browser_fetcher=BrowserResultFetcher({}))

    rows = asyncio.run(provider._search_bilibili(["generic one", "target two"]))

    urls = [row["url"] for row in rows]
    assert "https://www.bilibili.com/video/BV1browserhit1/" in urls


def test_search_bilibili_samples_precise_browser_queries_beyond_first_three() -> None:
    class BilibiliBrowserQuerySelectionProvider(HttpSourceProvider):
        def __init__(self) -> None:
            super().__init__(browser_fetcher=BrowserResultFetcher({}))
            self.browser_query_batches: list[list[str]] = []

        async def _search_streaming_platform(self, **kwargs):
            del kwargs
            return []

        async def _search_platform_via_browser_pages(self, **kwargs):
            self.browser_query_batches.append(list(kwargs["queries"]))
            return []

        async def _search_platform_via_site_engines(self, *args, **kwargs):
            del args, kwargs
            return []

    provider = BilibiliBrowserQuerySelectionProvider()
    queries = [
        "q1 generic",
        "q2 generic",
        "q3 generic",
        "q4 medium specificity",
        "q5 ensemble date exact",
        "q6 exact latin query",
        "q7 longer exact latin query",
        "q8 final exact latin query",
    ]

    asyncio.run(provider._search_bilibili(queries))

    assert provider.browser_query_batches
    first_batch = provider.browser_query_batches[0]
    assert len(first_batch) == 4
    assert "q4 medium specificity" in first_batch
    assert "q6 exact latin query" in first_batch
    assert "q7 longer exact latin query" in first_batch
    assert "q8 final exact latin query" in first_batch


def test_merge_bilibili_browser_query_rows_keeps_deeper_hit_from_first_focused_query() -> None:
    query_rows_list = [
        [
            {"url": f"https://www.bilibili.com/video/BV1focus{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 7)
        ]
        + [
            {
                "url": "https://www.bilibili.com/video/BV1target7hit/",
                "source_label": "Bilibili Search Browser Search",
                "source_kind": "streaming",
            }
        ]
        + [
            {"url": f"https://www.bilibili.com/video/BV1focus{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(8, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1backup{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1tail{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
    ]

    merged = merge_bilibili_browser_query_rows(query_rows_list, result_depth=10)

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1target7hit/" in urls


def _obsolete_merge_bilibili_browser_query_rows_keeps_deeper_hit_from_best_focused_middle_query() -> None:
    query_rows_list = [
        [
            {"url": f"https://www.bilibili.com/video/BV1head{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1cover{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1focus{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 7)
        ]
        + [
            {"url": "https://www.bilibili.com/video/BV1HP411m7JC/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
        ]
        + [
            {"url": f"https://www.bilibili.com/video/BV1focus{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(8, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1tail{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "Richter Schumann Piano Concerto 1950s",
            "Richter Piano Concerto 1950s",
            "钢协 Sviatoslav Richter 1954",
            "a小调钢琴协奏曲 Sviatoslav Richter Hungarian Orchestra 1954",
        ],
        result_depth=10,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1HP411m7JC/" in urls


def test_merge_bilibili_browser_query_rows_keeps_top_hit_from_later_primary_rescue_query() -> None:
    query_rows_list = [
        [
            {"url": f"https://www.bilibili.com/video/BV1head{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1cover{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1date{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 11)
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1CWb7eHENQ/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
        ]
        + [
            {"url": f"https://www.bilibili.com/video/BV1late{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(2, 11)
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "Lara Schumann Piano Concerto 1950s",
            "Lara Piano Concerto 1950s",
            "阿德利纳·德·劳拉 May 29, 1951",
            "Adelina de Lara Schumann concerto",
        ],
        result_depth=10,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1CWb7eHENQ/" in urls


def test_merge_bilibili_browser_query_rows_promotes_consensus_mid_rank_hit() -> None:
    target = {
        "url": "https://www.bilibili.com/video/BV1consensus/",
        "source_label": "Bilibili Search Browser Search",
        "source_kind": "streaming",
    }
    query_rows_list = [
        [
            {"url": f"https://www.bilibili.com/video/BV1heada{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 5)
        ]
        + [target]
        + [
            {"url": f"https://www.bilibili.com/video/BV1taila{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(6, 9)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1headb{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 4)
        ]
        + [target]
        + [
            {"url": f"https://www.bilibili.com/video/BV1tailb{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(5, 9)
        ],
        [
            {"url": f"https://www.bilibili.com/video/BV1headc{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(1, 6)
        ]
        + [target]
        + [
            {"url": f"https://www.bilibili.com/video/BV1tailc{i:02d}/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"}
            for i in range(7, 9)
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "Kempff Schumann Piano Concerto 1950s",
            "Kempff Dorati concerto 1959",
            "Wilhelm Kempff Schumann concerto",
        ],
        result_depth=3,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1consensus/" in urls


def test_merge_bilibili_browser_query_rows_keeps_first_unique_hit_from_later_exact_query() -> None:
    target = {
        "url": "https://www.bilibili.com/video/BV1TE411f7uh/",
        "source_label": "Bilibili Search Browser Search",
        "source_kind": "streaming",
    }
    query_rows_list = [
        [
            {"url": "https://www.bilibili.com/video/BV1ypqBBrExq/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1KEqBBLE9G/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1hN3xzNEkG/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV18mxqzKENG/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1PR4y1j7Qm/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DtE6zcEbX/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1x94y1C7Tc/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1sx4y1j7Y9/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1yqYEeKErH/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1V7411q7Do/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1dVapz9EZE/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1z2UKY1E4X/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1E84y1e7kH/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV169XwBzEUF/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV17o4y1m7U4/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1CqntzWEU5/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1E84y1e7kH/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1yqYEeKErH/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            target,
            {"url": "https://www.bilibili.com/video/BV1dVapz9EZE/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV17o4y1m7U4/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1z2UKY1E4X/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1YP4y147Ms/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1bNNHemEZV/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "钢协 Paul Kletzki 布达佩斯爱乐乐团",
            "a小调钢琴协奏曲 Annie Fischer 布达佩斯爱乐乐团",
            "Annie Fischer Schumann concerto",
        ],
        result_depth=10,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1TE411f7uh/" in urls


def test_merge_bilibili_browser_query_rows_keeps_first_unseen_second_result_from_later_medium_query() -> None:
    target = {
        "url": "https://www.bilibili.com/video/BV1HP411m7JC/",
        "source_label": "Bilibili Search Browser Search",
        "source_kind": "streaming",
    }
    query_rows_list = [
        [
            {"url": "https://www.bilibili.com/video/BV1ED4y1g7Gm/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1P5ATetE1D/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1eW411k7mL/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DZ4y187HS/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1g4411q7NP/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1PpZeBUEPS/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV14g411d7J3/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV153zFBWEqo/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1eW411k7mL/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1Ay4y1i74b/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV14x4y1Y7Le/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1fW411B7m9/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1LEHzeSESt/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1764y1d7fc/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV11F411s7a5/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            target,
            {"url": "https://www.bilibili.com/video/BV1Pp4y1V7Wa/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1LaPWzWE7h/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1P5ATetE1D/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1cAUjBKEJ8/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "Sviatoslav Richter HO Schumann concerto 1954",
            "Richter Ferencsik János Schumann concerto 1954",
            "钢协 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        ],
        result_depth=12,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1HP411m7JC/" in urls


def test_merge_bilibili_browser_query_rows_keeps_deeper_hit_from_chamber_bundle_query() -> None:
    target = {
        "url": "https://www.bilibili.com/video/BV1TARGET1111/",
        "source_label": "Bilibili Search Browser Search",
        "source_kind": "streaming",
    }
    query_rows_list = [
        [
            {"url": "https://www.bilibili.com/video/BV1AA4111111/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1AA4111112/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1AA4111113/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1AA4111114/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1AA4111115/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1BB4111111/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1BB4111112/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1BB4111113/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1BB4111114/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1BB4111115/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1CC4111111/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1CC4111112/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1CC4111113/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1CC4111114/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            target,
        ],
        [
            {"url": "https://www.bilibili.com/video/BV1DD4111111/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DD4111112/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DD4111113/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DD4111114/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
            {"url": "https://www.bilibili.com/video/BV1DD4111115/", "source_label": "Bilibili Search Browser Search", "source_kind": "streaming"},
        ],
    ]

    merged = merge_bilibili_browser_query_rows(
        query_rows_list,
        queries=[
            "春天奏鸣曲 Szigeti Schnabel",
            "spring Szigeti Schnabel",
            "Szigeti Schnabel violin sonatas",
            "spring Joseph Szigeti Artur Schnabel",
        ],
        result_depth=12,
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1TARGET1111/" in urls


def test_merge_streaming_host_rows_preserves_deeper_bilibili_slice_when_multiple_hosts() -> None:
    bilibili_rows = [
        {
            "url": f"https://www.bilibili.com/video/BV1row{i:02d}/",
            "source_label": "Bilibili Search",
            "source_kind": "streaming",
        }
        for i in range(1, 11)
    ]
    youtube_rows = [
        {
            "url": f"https://www.youtube.com/watch?v=yt{i:02d}",
            "source_label": "YouTube Search",
            "source_kind": "streaming",
        }
        for i in range(1, 3)
    ]

    merged = merge_streaming_host_rows(
        [
            (SourceProfileEntry(url="https://www.bilibili.com", is_chinese=True), bilibili_rows),
            (SourceProfileEntry(url="https://www.youtube.com", is_chinese=False), youtube_rows),
        ]
    )

    urls = [row["url"] for row in merged]
    assert "https://www.bilibili.com/video/BV1row09/" in urls


def test_dedupe_streaming_hosts_for_execution_collapses_duplicate_apple_hosts() -> None:
    hosts = dedupe_streaming_hosts_for_execution(
        [
            SourceProfileEntry(url="https://classical.music.apple.com", is_chinese=False),
            SourceProfileEntry(url="https://music.apple.com", is_chinese=False),
            SourceProfileEntry(url="https://www.youtube.com", is_chinese=False),
        ]
    )

    assert [host.url for host in hosts] == [
        "https://classical.music.apple.com",
        "https://www.youtube.com",
    ]


def test_streaming_host_priority_treats_apple_music_as_primary_platform() -> None:
    assert streaming_host_priority("https://music.apple.com")[0] == 0
    assert streaming_host_priority("https://classical.music.apple.com")[0] == 0


def test_streaming_host_priority_orders_apple_after_bilibili_and_youtube_within_primary_hosts() -> None:
    ordered = sorted(
        [
            "https://music.apple.com",
            "https://www.youtube.com",
            "https://www.bilibili.com",
        ],
        key=streaming_host_priority,
    )

    assert ordered == [
        "https://www.bilibili.com",
        "https://www.youtube.com",
        "https://music.apple.com",
    ]


def test_should_probe_apple_auxiliary_hosts_when_primary_results_lack_platform_diversity() -> None:
    assert (
        should_probe_apple_auxiliary_hosts(
            [
                {
                    "url": "https://www.youtube.com/watch?v=strong001",
                    "platform": "youtube",
                    "same_recording_score": 0.91,
                }
            ]
        )
        is True
    )


def test_should_probe_apple_auxiliary_hosts_skips_when_primary_results_are_already_strong_and_diverse() -> None:
    assert (
        should_probe_apple_auxiliary_hosts(
            [
                {
                    "url": "https://www.youtube.com/watch?v=strong001",
                    "platform": "youtube",
                    "same_recording_score": 0.92,
                },
                {
                    "url": "https://www.bilibili.com/video/BV1strong001/",
                    "platform": "bilibili",
                    "same_recording_score": 0.89,
                },
            ]
        )
        is False
    )


def test_should_expand_initial_streaming_window_when_apple_primary_results_are_present() -> None:
    host_results = [
        (
            SourceProfileEntry(url="https://www.youtube.com", is_chinese=False),
            [{"url": "https://www.youtube.com/watch?v=yt001"}],
        ),
        (
            SourceProfileEntry(url="https://music.apple.com", is_chinese=False),
            [{"url": "https://music.apple.com/us/album/demo/123?i=456"}],
        ),
    ]

    assert should_expand_initial_streaming_window(host_results) is True


def test_select_bilibili_browser_queries_keeps_precise_middle_conductor_query() -> None:
    queries = [
        "吉泽金",
        "罗伯特·舒曼 a小调钢琴协奏曲 吉泽金",
        "吉泽金 March 3, 1942 Berlin",
        "a小调钢琴协奏曲 吉泽金 March 3, 1942 Berlin",
        "a小调钢琴协奏曲 吉泽金 富特文格勒 柏林爱乐乐团 March 3, 1942 Berlin",
        "a小调钢琴协奏曲 吉泽金 / 富特文格勒 柏林爱乐乐团 March 3, 1942 Berlin",
        "Piano Concerto, Op.54 Walter Gieseking Wilhelm Furtwangler Berlin Philharmonic Orchestra March 3, 1942 Berlin",
        "Piano Concerto, Op.54 Walter Gieseking / Wilhelm Furtwangler Berlin Philharmonic Orchestra March 3, 1942 Berlin",
    ]

    selected = select_bilibili_browser_queries(queries)

    assert len(selected) == 6
    assert "a小调钢琴协奏曲 吉泽金 富特文格勒 柏林爱乐乐团 March 3, 1942 Berlin" in selected
    assert "Piano Concerto, Op.54 Walter Gieseking / Wilhelm Furtwangler Berlin Philharmonic Orchestra March 3, 1942 Berlin" in selected


def test_select_bilibili_browser_queries_keeps_focused_middle_primary_query() -> None:
    queries = [
        "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279",
        "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954",
        "\u94a2\u534f \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954",
        "\u94a2\u534f Sviatoslav Richter 1954",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Sviatoslav Richter \u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2 1954",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Sviatoslav Richter Hungarian Orchestra 1954",
        "\u94a2\u534f Ferencsik J\u00e1nos \u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2 1954",
        "\u94a2\u534f Ferencsik J\u00e1nos Hungarian Orchestra 1954",
    ]

    selected = select_bilibili_browser_queries(queries)

    assert len(selected) == 6
    assert "\u94a2\u534f Sviatoslav Richter 1954" in selected
    assert "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Sviatoslav Richter Hungarian Orchestra 1954" in selected


def test_select_bilibili_browser_queries_keeps_primary_work_rescue_query_for_de_lara() -> None:
    queries = [
        "Lara Schumann Piano Concerto 1950s",
        "Lara Piano Concerto 1950s",
        "Adelina de Lara Schumann concerto",
        "Adelina de Lara 舒曼钢协",
        "阿德利纳·德·劳拉",
        "阿德利纳·德·劳拉 May 29, 1951",
        "罗伯特·舒曼 a小调钢琴协奏曲 阿德利纳·德·劳拉",
    ]

    selected = select_bilibili_browser_queries(queries)

    assert "Adelina de Lara Schumann concerto" in selected


def test_select_bilibili_browser_queries_keeps_primary_work_rescue_query_for_richter_after_rescue_insertions() -> None:
    queries = [
        "Richter Schumann Piano Concerto 1950s",
        "Richter Piano Concerto 1950s",
        "Sviatoslav Richter Schumann concerto",
        "Sviatoslav Richter 舒曼钢协",
        "斯维亚托斯拉夫·特奥菲洛维奇·里赫特",
        "斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        "钢协 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        "钢协 Sviatoslav Richter 1954",
    ]

    selected = select_bilibili_browser_queries(queries)

    assert "钢协 Sviatoslav Richter 1954" in selected


def test_select_bilibili_browser_queries_keeps_collaboration_middle_query() -> None:
    queries = [
        "\u5a01\u5ec9\u00b7\u6c83\u5c14\u7279\u00b7\u5f17\u91cc\u5fb7\u91cc\u5e0c\u00b7\u80af\u666e\u592b",
        "\u5a01\u5ec9\u00b7\u6c83\u5c14\u7279\u00b7\u5f17\u91cc\u5fb7\u91cc\u5e0c\u00b7\u80af\u666e\u592b 1959",
        "\u94a2\u534f Wilhelm Kempff 1959",
        "\u94a2\u534f \u5a01\u5ec9\u00b7\u6c83\u5c14\u7279\u00b7\u5f17\u91cc\u5fb7\u91cc\u5e0c\u00b7\u80af\u666e\u592b 1959",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Wilhelm Kempff Dorati 1959",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Wilhelm Kempff Royal Orchestra 1959",
        "\u94a2\u534f Antal Dorati \u963f\u59c6\u65af\u7279\u4e39\u7687\u5bb6\u97f3\u4e50\u5385\u7ba1\u5f26\u4e50\u56e2 1959",
        "\u94a2\u534f Antal Dorati Royal Orchestra 1959",
    ]

    selected = select_bilibili_browser_queries(queries)

    assert len(selected) == 6
    assert "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 Wilhelm Kempff Dorati 1959" in selected


def test_prepare_bilibili_browser_queries_keeps_collaboration_rescue_for_kempff() -> None:
    queries = [
        "\u94a2\u534f Wilhelm Kempff 1959",
        "Kempff Schumann Piano Concerto 1950s",
        "Kempff Schumann Piano Concertos 1950s",
        "Wilhelm Kempff Schumann concerto",
        "Kempff Dorati concerto 1959",
        "Wilhelm Kempff Dorati Schumann concerto",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert len(selected) == 3
    assert "Kempff Dorati concerto 1959" in selected
    assert any("Dorati" in query for query in selected)


def test_prepare_bilibili_browser_queries_keeps_exact_collaboration_query_for_annie_kletzki() -> None:
    queries = [
        "Piano Concerto, Op.54 Annie Fischer Paul Kletzki Budapest Orchestra",
        "Piano Concerto, Op.54 Annie Fischer Paul Kletzki Budapest Philharmonic Orchestra",
        "Piano Concerto, Op.54 Annie Fischer Paul Kletzki",
        "Piano Concerto, Op.54 Annie Fischer",
        "Piano Concerto, Op.54 Annie Fischer BpPO",
        "Piano Concerto, Op.54 BpPO",
        "Piano Concerto, Op.54 Budapest Orchestra",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert len(selected) == 4
    assert any("Piano Concerto, Op.54 Annie Fischer Paul Kletzki" in query for query in selected)
    assert any("Paul Kletzki" in query for query in selected)


def test_prepare_bilibili_browser_queries_keeps_generic_plural_bundle_rescue_for_kempff_full_case() -> None:
    queries = [
        "a小调钢琴协奏曲 威廉·沃尔特·弗里德里希·肯普夫 安塔尔·多拉蒂 阿姆斯特丹皇家音乐厅管弦乐团 1959",
        "a小调钢琴协奏曲 威廉·沃尔特·弗里德里希·肯普夫 / 安塔尔·多拉蒂 阿姆斯特丹皇家音乐厅管弦乐团 1959",
        "Kempff Schumann Piano Concerto 1950s Op.54",
        "Kempff Schumann Piano Concertos 1950s Op.54",
        "Kempff Piano Concertos 1950s",
        "Wilhelm Kempff RO Schumann concerto 1959",
        "Kempff Dorati Schumann concerto 1959 Op.54",
        "钢协 Wilhelm Kempff 1959",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "Kempff Piano Concertos 1950s" in selected


def test_prepare_bilibili_browser_queries_preserves_short_rescue_and_bundle_for_kempff_full_case_when_budget_allows() -> None:
    queries = [
        "a小调钢琴协奏曲 威廉·沃尔特·弗里德里希·肯普夫 安塔尔·多拉蒂 阿姆斯特丹皇家音乐厅管弦乐团 1959",
        "a小调钢琴协奏曲 威廉·沃尔特·弗里德里希·肯普夫 / 安塔尔·多拉蒂 阿姆斯特丹皇家音乐厅管弦乐团 1959",
        "Kempff Schumann Piano Concerto 1950s Op.54",
        "Kempff Schumann Piano Concertos 1950s Op.54",
        "Kempff Piano Concertos 1950s",
        "Wilhelm Kempff RO Schumann concerto 1959",
        "Kempff Dorati Schumann concerto 1959 Op.54",
        "钢协 Wilhelm Kempff 1959",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "Kempff Piano Concertos 1950s" in selected
    assert "钢协 Wilhelm Kempff 1959" in selected


def test_classify_link_candidate_zone_keeps_kempff_bundle_upload_out_of_conflicting_credit_red_zone() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Concerto, Op.54")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1NY411y7Wc" in scenario.target_urls
    )
    draft = InputNormalizer().normalize(scenario.item)
    candidate = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1NY411y7Wc/",
        title="肯普夫 蒙特勒现场 贝一、舒曼钢协 | Kempff - Beethoven, Schumann Piano Concertos（1950s）",
        confidence=0.81,
    )

    conflicts = candidate_conflicting_credit_tokens(draft, candidate.title or "")
    zone, note = classify_link_candidate_zone(draft, candidate)

    assert conflicts == set()
    assert zone == "yellow"
    assert note == "review-needed"


def test_prepare_bilibili_browser_queries_keeps_decade_primary_work_rescue_for_de_lara_partial_case() -> None:
    queries = [
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u963f\u5fb7\u5229\u7eb3\u00b7\u5fb7\u00b7\u52b3\u62c9 \u6000\u7279 \u82f1\u56fd\u5e7f\u64ad\u516c\u53f8\u82cf\u683c\u5170\u4ea4\u54cd\u4e50\u56e2 May 29, 1951",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u963f\u5fb7\u5229\u7eb3\u00b7\u5fb7\u00b7\u52b3\u62c9 / \u6000\u7279 \u82f1\u56fd\u5e7f\u64ad\u516c\u53f8\u82cf\u683c\u5170\u4ea4\u54cd\u4e50\u56e2 May 29, 1951",
        "de Lara Schumann Piano Concerto 1950s",
        "de Lara Schumann Piano Concertos 1950s",
        "Adelina de Lara \u82f1\u56fd\u5e7f\u64ad\u516c\u53f8\u82cf\u683c\u5170 Schumann concerto 1951",
        "Adelina de Lara Schumann concerto",
        "Adelina de Lara \u8212\u66fc\u94a2\u534f",
        "\u6000\u7279 - \u52b3\u62c9 - \u82f1\u56fd\u5e7f\u64ad\u516c\u53f8\u82cf\u683c\u5170\u4ea4\u54cd\u4e50\u56e2 - May 29, 1951 \u963f\u5fb7\u5229\u7eb3\u00b7\u5fb7\u00b7\u52b3\u62c9",
        "\u963f\u5fb7\u5229\u7eb3\u00b7\u5fb7\u00b7\u52b3\u62c9",
        "\u963f\u5fb7\u5229\u7eb3\u00b7\u5fb7\u00b7\u52b3\u62c9 May 29, 1951",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert "de Lara Schumann Piano Concerto 1950s" in selected


def test_prepare_bilibili_browser_queries_keeps_decade_primary_work_rescue_for_gieseking_partial_case() -> None:
    queries = [
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u74e6\u5c14\u7279\u00b7\u5409\u6cfd\u91d1 \u5bcc\u7279\u6587\u683c\u52d2 \u67cf\u6797\u7231\u4e50\u4e50\u56e2 March 3, 1942",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u74e6\u5c14\u7279\u00b7\u5409\u6cfd\u91d1 / \u5bcc\u7279\u6587\u683c\u52d2 \u67cf\u6797\u7231\u4e50\u4e50\u56e2 March 3, 1942",
        "Gieseking Schumann Piano Concerto 1940s",
        "Gieseking Schumann Piano Concertos 1940s",
        "Walter Gieseking \u67cf\u6797 Schumann concerto 1942",
        "Walter Gieseking Schumann concerto",
        "Walter Gieseking \u8212\u66fc\u94a2\u534f",
        "\u5bcc\u7279\u6587\u683c\u52d2 - \u5409\u6cfd\u91d1 - \u67cf\u6797\u7231\u4e50\u4e50\u56e2 - March 3, 1942 Berlin \u74e6\u5c14\u7279\u00b7\u5409\u6cfd\u91d1",
        "\u74e6\u5c14\u7279\u00b7\u5409\u6cfd\u91d1",
        "\u74e6\u5c14\u7279\u00b7\u5409\u6cfd\u91d1 March 3, 1942",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert "Gieseking Schumann Piano Concerto 1940s" in selected


def test_prepare_bilibili_browser_queries_keeps_short_primary_year_rescue_for_richter_full_case() -> None:
    queries = [
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 \u8d39\u4f26\u5947\u514b\u00b7\u4e9a\u8bfa\u4ec0 \u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2 1954",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 / \u8d39\u4f26\u5947\u514b\u00b7\u4e9a\u8bfa\u4ec0 \u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2 1954",
        "Richter Schumann Piano Concerto 1950s",
        "Richter Schumann Piano Concertos 1950s",
        "Sviatoslav Richter HO Schumann concerto 1954",
        "Richter Ferencsik J\u00e1nos Schumann concerto 1954",
        "Sviatoslav Richter Schumann concerto",
        "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279",
        "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954",
        "\u94a2\u534f \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert "\u94a2\u534f \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954" in selected


def test_prepare_bilibili_browser_queries_preserves_short_anchors_when_adding_bundle_for_richter_full_case() -> None:
    queries = [
        "a小调钢琴协奏曲 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 费伦奇克·亚诺什 匈牙利国家爱乐乐团 1954",
        "a小调钢琴协奏曲 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 / 费伦奇克·亚诺什 匈牙利国家爱乐乐团 1954",
        "Richter Schumann Piano Concerto 1950s",
        "Richter Schumann Piano Concertos 1950s",
        "Richter Piano Concertos 1950s",
        "Sviatoslav Richter HO Schumann concerto 1954",
        "Richter Ferencsik János Schumann concerto 1954",
        "Sviatoslav Richter Schumann concerto",
        "斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        "钢协 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        "a小调钢琴协奏曲 Sviatoslav Richter 匈牙利国家爱乐乐团 1954",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "Richter Piano Concertos 1950s" in selected
    assert "钢协 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954" in selected
    assert "Richter Ferencsik János Schumann concerto 1954" in selected


def test_prepare_bilibili_browser_queries_keeps_cjk_context_rescue_for_richter_full_case() -> None:
    queries = [
        "里赫特 匈牙利 1954 舒曼钢协 Op.54",
        "费伦奇克 里赫特 匈牙利 1954 舒曼钢协 Op.54",
        "Richter Schumann Piano Concerto 1950s Op.54",
        "Richter Schumann Piano Concertos 1950s Op.54",
        "Richter Piano Concertos 1950s",
        "Sviatoslav Richter HNPO Schumann concerto 1954 Op.54",
        "Richter Janos Schumann concerto 1954 Op.54",
        "Sviatoslav Richter Schumann concerto Op.54",
        "斯维亚托斯拉夫·特奥菲洛维奇·里赫特 1954",
        "舒曼 a小调钢琴协奏曲 Op.54 斯维亚托斯拉夫·特奥菲洛维奇·里赫特",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "里赫特 匈牙利 1954 舒曼钢协 Op.54" in selected


def test_prepare_bilibili_browser_queries_preserves_primary_year_anchor_over_generic_decade_rescue_for_richter_partial_case() -> None:
    queries = [
        "Sviatoslav Richter \u5308\u7259\u5229\u56fd\u5bb6 Schumann concerto 1954",
        "a\u5c0f\u8c03\u94a2\u7434\u534f\u594f\u66f2 \u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 / \u8d39\u4f26\u5947\u514b \u5308\u7259\u5229\u56fd\u5bb6\u7231\u4e50\u4e50\u56e2 1954",
        "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954",
        "Richter Schumann Piano Concerto 1950s",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=3)

    assert "\u65af\u7ef4\u4e9a\u6258\u65af\u62c9\u592b\u00b7\u7279\u5965\u83f2\u6d1b\u7ef4\u5947\u00b7\u91cc\u8d6b\u7279 1954" in selected


def test_prepare_bilibili_browser_queries_keeps_decade_rescue_alongside_primary_year_anchor_for_moiseiwitsch_partial_case() -> None:
    queries = [
        "a小调钢琴协奏曲 班诺·莫伊塞维奇 奥托·阿克曼 爱乐乐团 1954",
        "a小调钢琴协奏曲 班诺·莫伊塞维奇 / 奥托·阿克曼 爱乐乐团 1954",
        "班诺·莫伊塞维奇 1954",
        "Moiseiwitsch Schumann Piano Concerto 1950s",
        "Moiseiwitsch Schumann Piano Concertos 1950s",
        "Benno Moiseiwitsch 爱乐 Schumann concerto 1954",
        "Benno Moiseiwitsch Schumann concerto",
        "Benno Moiseiwitsch 舒曼钢协",
        "奥托·阿克曼 - 莫伊塞维奇 - 爱乐乐团 - 1954 班诺·莫伊塞维奇",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "班诺·莫伊塞维奇 1954" in selected
    assert "Moiseiwitsch Schumann Piano Concerto 1950s" in selected


def test_prepare_bilibili_browser_queries_preserves_short_rescue_and_bundle_for_grinberg_full_case_when_budget_allows() -> None:
    queries = [
        "a小调钢琴协奏曲 玛丽亚·伊斯拉列夫娜·格林伯格 卡尔·埃利亚斯伯格 苏联国家交响乐团 1958",
        "a小调钢琴协奏曲 玛丽亚·伊斯拉列夫娜·格林伯格 / 卡尔·埃利亚斯伯格 苏联国家交响乐团 1958",
        "Grinberg Schumann Piano Concerto 1950s",
        "Grinberg Schumann Piano Concertos 1950s",
        "Grinberg Piano Concertos 1950s",
        "Maria Grinberg USSR Schumann concerto 1958",
        "Grinberg Eliasberg Schumann concerto 1958",
        "Maria Grinberg Schumann concerto",
        "玛丽亚·伊斯拉列夫娜·格林伯格 1958",
        "钢协 Maria Grinberg 1958",
        "a小调钢琴协奏曲 Maria Grinberg 苏联国家交响乐团 1958",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "Grinberg Piano Concertos 1950s" in selected
    assert "钢协 Maria Grinberg 1958" in selected


def test_prepare_bilibili_browser_queries_keeps_cjk_context_rescue_for_grinberg_full_case() -> None:
    queries = [
        "格林伯格 苏联 1958 舒曼钢协 Op.54",
        "埃利亚斯伯格 格林伯格 苏联 1958 舒曼钢协 Op.54",
        "Grinberg Schumann Piano Concerto 1950s Op.54",
        "Grinberg Schumann Piano Concertos 1950s Op.54",
        "Grinberg Piano Concertos 1950s",
        "Maria Grinberg USSR Schumann concerto 1958 Op.54",
        "Grinberg Eliasberg Schumann concerto 1958 Op.54",
        "Maria Grinberg Schumann concerto Op.54",
        "格林伯格 1958",
        "舒曼 a小调钢琴协奏曲 Op.54 玛丽亚·伊斯拉列夫娜·格林伯格",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "格林伯格 苏联 1958 舒曼钢协 Op.54" in selected


def test_prepare_bilibili_browser_queries_preserves_year_anchor_and_bundle_for_gieseking_partial_case_when_budget_allows() -> None:
    queries = [
        "a小调钢琴协奏曲 瓦尔特·吉泽金 富特文格勒 柏林爱乐乐团 March 3, 1942",
        "a小调钢琴协奏曲 瓦尔特·吉泽金 / 富特文格勒 柏林爱乐乐团 March 3, 1942",
        "Gieseking Schumann Piano Concerto 1940s",
        "Gieseking Schumann Piano Concertos 1940s",
        "Gieseking Piano Concertos 1940s",
        "Walter Gieseking 柏林 Schumann concerto 1942",
        "Walter Gieseking Schumann concerto",
        "Walter Gieseking 舒曼钢协",
        "瓦尔特·吉泽金 1942",
        "富特文格勒 - 吉泽金 - 柏林爱乐乐团 - March 3, 1942 Berlin 瓦尔特·吉泽金",
        "瓦尔特·吉泽金 March 3, 1942",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "Gieseking Piano Concertos 1940s" in selected
    assert "瓦尔特·吉泽金 1942" in selected


def test_looks_like_single_movement_ignores_complete_tracklist_descriptions() -> None:
    text = (
        'Jean Fournier & Ginette Doyen play Beethoven "Spring" Sonata '
        'Violin Sonata No. 5 in F major Opus 24, "Frühlingssonate"'
        '1. Allegro2. Adagio molto espressivo (7:27)3. Scherzo: Allegro molto '
        '(14:22)4. Rondo: Allegro ma non...'
    )

    assert looks_like_single_movement(text) is False


def test_looks_like_single_movement_detects_hyphen_numbered_heading() -> None:
    text = "Schumann: Piano Concerto in A Minor, Op.54 - 1. Allegro affettuoso"

    assert looks_like_single_movement(text) is True


def test_name_matches_does_not_treat_movement_word_as_person_initials() -> None:
    haystack = normalize_text(
        "Robert Schumann - Piano concerto in A minor, Op.54 I: Allegro affettuoso "
        "II: Intermezzo (Andantino grazioso) III: Allegro Vivace"
    )

    assert name_matches(haystack, "Annie Fischer") is False


def test_provider_uses_chinese_queries_only_for_chinese_platforms_and_expands_abbreviations(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text(
        "#global\n[zh] https://www.bilibili.com\nhttps://www.youtube.com\n",
        encoding="utf-8",
    )
    alias_path = tmp_path / "orchestra-abbreviations.txt"
    alias_path.write_text("BSO = Boston Symphony Orchestra\n", encoding="utf-8")
    transport = QueryRecordingTransport()
    client = httpx.AsyncClient(transport=transport, follow_redirects=True)
    provider = HttpSourceProvider(
        profile_loader=SourceProfileLoader(root),
        client=client,
        orchestra_alias_loader=OrchestraAliasLoader(alias_path),
        browser_fetcher=BrowserResultFetcher({}),
    )
    draft = DraftRecordingEntry(
        item_id="recording-8",
        title="蒙都 - BSO - 第五交响曲 op.64",
        composer_name="柴可夫斯基",
        composer_name_latin="Tchaikovsky",
        work_title="第五交响曲",
        work_title_latin="Symphony No. 5 in E minor, Op. 64",
        catalogue="Op.64",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="柴可夫斯基 | 第五交响曲 | 蒙都 | BSO | -",
        raw_text="柴可夫斯基 | 第五交响曲 | 蒙都 | BSO | -",
        existing_links=[],
        lead_names=["蒙都"],
        lead_names_latin=["Monteux"],
        ensemble_names=["BSO"],
        ensemble_names_latin=["BSO"],
    )
    profile = RetrievalProfile(
        category="orchestral",
        tags=[],
        queries=["placeholder"],
        latin_queries=["Tchaikovsky Symphony No. 5 in E minor, Op. 64 Monteux BSO", "Tchaikovsky Symphony No. 5 in E minor, Op. 64 Monteux Boston Symphony Orchestra"],
        zh_queries=["柴可夫斯基 第五交响曲 蒙都 BSO"],
        mixed_queries=["Tchaikovsky 第五交响曲 Monteux BSO"],
    )

    asyncio.run(provider.search_streaming(draft, profile))

    bilibili_urls = [url for url in transport.urls if "bilibili.com" in url]
    youtube_urls = [url for url in transport.urls if "youtube.com/results" in url]
    assert any("Tchaikovsky" not in url and "search.bilibili.com" in url for url in bilibili_urls)
    assert any("Boston+Symphony+Orchestra" in url for url in youtube_urls)
    assert not any("search_query=%E6%9F%B4" in url for url in youtube_urls)


def test_non_chinese_platform_queries_promote_named_concerto_aliases_into_executed_budget(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root))
    draft = build_annie_draft()
    item_profile = RetrievalProfile(
        category="concerto",
        tags=[],
        queries=[
            "Robert Schumann Piano Concerto, Op.54 Annie Fischer",
            "Piano Concerto, Op.54 Annie Fischer",
            "Robert Schumann piano concerto Annie Fischer",
            "Robert Schumann concerto a minor Annie Fischer",
            "Piano Concerto, Op.54 Annie Fischer Kletzki Budapest Philharmonic Orchestra",
            "Piano Concerto, Op.54 Annie Fischer / Kletzki Budapest Philharmonic Orchestra",
        ],
        latin_queries=[
            "Robert Schumann Piano Concerto, Op.54 Annie Fischer",
            "Piano Concerto, Op.54 Annie Fischer",
            "Robert Schumann piano concerto Annie Fischer",
            "Robert Schumann concerto a minor Annie Fischer",
        ],
    )
    youtube_host = SourceProfileLoader(root).load(category="concerto", tags=[]).streaming[0]

    queries = provider._queries_for_host(draft, item_profile, youtube_host)

    assert any("klavierkonzert" in query.lower() for query in queries[:8])


def test_non_chinese_platform_queries_include_named_work_aliases_for_solo_repertoire(tmp_path: Path) -> None:
    root = tmp_path / "source-profiles"
    root.mkdir(parents=True)
    (root / "high-quality.txt").write_text("#global\nhttps://catalog.example\n", encoding="utf-8")
    (root / "streaming.txt").write_text("#global\nhttps://www.youtube.com\n", encoding="utf-8")
    provider = HttpSourceProvider(profile_loader=SourceProfileLoader(root))
    draft = DraftRecordingEntry(
        item_id="recording-9",
        title="Claudio Arrau 1970",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="Beethovenfest Bonn 1970",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Claudio Arrau"],
        lead_names_latin=["Claudio Arrau"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )
    profile = RetrievalProfile(category="chamber_solo", tags=["piano"], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = provider._profile_loader.load(category="chamber_solo", tags=["piano"]).streaming[0]

    queries = provider._queries_for_host(draft, profile, host)

    assert any("appassionata" in query.lower() for query in queries)


def test_non_chinese_platform_queries_for_actual_appassionata_scenario_promote_yearless_named_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:eMi5KZ4lDCA" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "appassionata walter gieseking" for query in queries[:4])
    assert all("1951、1947" not in query for query in queries[:3])


def test_queries_for_bilibili_host_for_actual_spring_sonata_scenario_promote_yearless_duo_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1tN4y1B7fQ" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "spring" in query.lower()
        and "Joseph Szigeti" in query
        and "Mieczyslaw Horszowski" in query
        and "1953" not in query
        for query in queries[:6]
    )
    assert any("春天奏鸣曲" in query and "西盖蒂" in query for query in queries[:6])
    assert queries[0] != "西盖蒂·约瑟夫 1953"


def test_non_chinese_platform_queries_for_actual_appassionata_scenario_include_combined_named_numbered_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:eMi5KZ4lDCA" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="chamber_solo", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "appassionata" in query.lower()
        and "piano sonata no 23" in query.lower()
        and "Walter Gieseking" in query
        and "1951" not in query
        and "1947" not in query
        for query in queries[:6]
    )


def test_queries_for_bilibili_host_for_actual_szigeti_schnabel_scenario_include_compact_duo_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    compact_duo_index = next(
        (
            index
            for index, query in enumerate(queries[:6])
            if ("Szigeti-Schnabel" in query or "Szigeti Schnabel" in query)
            and ("spring" in query.lower() or "violin sonata no.5" in query.lower() or "violin sonata no 5" in query.lower())
        ),
        None,
    )

    assert compact_duo_index is not None
    if "瑗跨洊钂偮风害鐟熷か 1948" in queries:
        assert compact_duo_index < queries.index("瑗跨洊钂偮风害鐟熷か 1948")


def test_queries_for_youtube_host_for_actual_mogilevsky_partial_scenario_include_transliterated_duo_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "youtube:vCC5o4A3HMY" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "spring" in query.lower()
        and "Alexandre Moguilewsky" in query
        and "Leonid Kreutzer" in query
        for query in queries[:10]
    )


def test_queries_for_bilibili_host_for_actual_schneiderhan_seemann_scenario_include_mixed_named_work_compact_duo_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        ("\u6625\u5929" in query or "\u6625\u5929\u594f\u9e23\u66f2" in query)
        and ("Schneiderhan Seemann" in query or "Schneiderhan-Seemann" in query)
        for query in queries[:8]
    )


def test_prepare_bilibili_browser_queries_for_actual_schneiderhan_seemann_scenario_keep_mixed_named_work_compact_duo_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(
        ("\u6625\u5929" in query or "\u6625\u5929\u594f\u9e23\u66f2" in query)
        and ("Schneiderhan Seemann" in query or "Schneiderhan-Seemann" in query)
        for query in selected
    )


def test_queries_for_bilibili_host_for_actual_schneiderhan_seemann_scenario_include_complete_bundle_year_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Schneiderhan Seemann" in query
        and "complete violin sonatas" in query.lower()
        and "1959" in query
        for query in queries[:12]
    )


def test_queries_for_bilibili_host_for_actual_schneiderhan_seemann_scenario_include_complete_bundle_trailer_probe() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Schneiderhan Seemann" in query
        and "complete violin sonatas" in query.lower()
        and "trailer" in query.lower()
        for query in queries[:12]
    )


def test_prepare_bilibili_browser_queries_for_actual_schneiderhan_seemann_scenario_keep_complete_bundle_year_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(
        "Schneiderhan Seemann" in query
        and "complete violin sonatas" in query.lower()
        and "1959" in query
        for query in selected
    )


def test_queries_for_bilibili_host_for_actual_szigeti_schnabel_partial_scenario_include_duo_bundle_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Szigeti Schnabel" in query
        and ("violin sonatas" in query.lower() or "sonata collection" in query.lower() or "sonatas" in query.lower())
        for query in queries[:12]
    )


def test_queries_for_bilibili_host_for_actual_szigeti_schnabel_full_scenario_include_duo_bundle_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Szigeti Schnabel" in query
        and ("violin sonatas" in query.lower() or "sonata collection" in query.lower() or "sonatas" in query.lower())
        for query in queries[:12]
    )


def test_prepare_bilibili_browser_queries_for_actual_szigeti_schnabel_partial_scenario_keep_duo_bundle_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(
        "Szigeti Schnabel" in query
        and ("violin sonatas" in query.lower() or "sonata collection" in query.lower() or "sonatas" in query.lower())
        for query in selected
    )


def test_prepare_bilibili_browser_queries_for_actual_szigeti_schnabel_full_scenario_keep_duo_bundle_query() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(
        "Szigeti Schnabel" in query
        and ("violin sonatas" in query.lower() or "sonata collection" in query.lower() or "sonatas" in query.lower())
        for query in selected
    )


def test_prepare_bilibili_browser_queries_keeps_cjk_orchestral_context_query() -> None:
    queries = [
        "幻想交响曲 伯恩斯坦 法国国家管弦乐团 1977",
        "赫克托·路易·柏辽兹 幻想交响曲 伯恩斯坦 法国国家管弦乐团 1977",
        "伯恩斯坦 1977",
        "幻想交响曲 Leonard Bernstein 法国国家管弦乐团 1977",
        "Symphonie Fantastique Leonard Bernstein 1977",
        "Hector Louis Berlioz Symphonie Fantastique Leonard Bernstein 1977",
        "Symphonie Fantastique 伯恩斯坦 1977",
    ]

    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert "幻想交响曲 伯恩斯坦 法国国家管弦乐团 1977" in selected


def test_non_chinese_platform_queries_for_actual_appassionata_scenario_split_ambiguous_years_into_single_year_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:eMi5KZ4lDCA" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="chamber_solo", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        "Walter Gieseking" in query
        and "1947" in query
        and "1951、1947" not in query
        and ("appassionata" in query.lower() or "piano sonata no 23" in query.lower())
        for query in queries[:8]
    )


def test_non_chinese_platform_queries_for_actual_ernst_levy_scenario_include_surname_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:4X7woG5vmb4" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="chamber_solo", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "youtube.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "levy appassionata beethoven" for query in queries[:8])


def test_queries_for_bilibili_host_for_actual_egon_petri_scenario_include_surname_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "petri appassionata beethoven" for query in queries[:10])


def test_prepare_bilibili_browser_queries_for_actual_egon_petri_scenario_keep_surname_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(query.lower() == "petri appassionata beethoven" for query in selected)


def test_queries_for_bilibili_host_for_actual_egon_petri_scenario_include_full_name_year_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "appassionata egon petri 1956" for query in queries[:10])


def test_queries_for_bilibili_host_for_actual_egon_petri_scenario_include_full_name_yearless_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "partial" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "appassionata egon petri" for query in queries[:10])


def test_queries_for_bilibili_host_for_actual_egon_petri_scenario_include_opus_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(query.lower() == "egon petri op 57" for query in queries[:3])


def test_prepare_bilibili_browser_queries_for_actual_egon_petri_scenario_keep_full_name_year_named_work_rescue() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV14GZhYEE2D" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(query.lower() == "appassionata egon petri 1956" for query in selected)


def test_queries_for_bilibili_host_for_actual_sonoda_scenario_include_cjk_numbered_piano_sonata_alias() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1ouNbzqEQt" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        query.startswith("第二十三钢琴奏鸣曲 園田高弘") or query.startswith("热情op57 園田高弘")
        for query in queries[:8]
    )


def test_prepare_bilibili_browser_queries_for_actual_sonoda_scenario_keep_cjk_numbered_piano_sonata_alias() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1ouNbzqEQt" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = ProfileResolver().resolve(scenario.item)
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)
    selected = prepare_bilibili_browser_queries(queries, max_queries=4)

    assert any(
        query.startswith("第二十三钢琴奏鸣曲 園田高弘") or query.startswith("热情op57 園田高弘")
        for query in selected
    )


def test_queries_for_bilibili_host_for_actual_szigeti_schnabel_scenario_include_numbered_work_year_anchor() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1nJ411r7n5" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="chamber_solo", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="chamber_solo", tags=[]).streaming if "bilibili.com" in host.url)

    queries = provider._queries_for_host(draft, profile, host)

    assert any(
        ("Szigeti-Schnabel" in query or "Szigeti Schnabel" in query)
        and "1948" in query
        and ("violin sonata no.5" in query.lower() or "violin sonata no 5" in query.lower())
        for query in queries[:8]
    )


def test_non_chamber_profile_does_not_call_chamber_anchor_builders(monkeypatch) -> None:
    import app.services.http_sources as http_sources_module

    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Concerto, Op.54")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "bilibili:BV1RiZJYAEBD" in scenario.target_urls
    )
    provider = HttpSourceProvider()
    draft = InputNormalizer().normalize(scenario.item)
    profile = RetrievalProfile(category="concerto", tags=[], queries=[], latin_queries=[], zh_queries=[], mixed_queries=[])
    host = next(host for host in provider._profile_loader.load(category="concerto", tags=[]).streaming if "youtube.com" in host.url)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("chamber anchor builder should not run for concerto profiles")

    monkeypatch.setattr(http_sources_module, "build_chamber_work_anchor_queries", fail_if_called)

    queries = provider._queries_for_host(draft, profile, host)

    assert queries


def test_score_recording_match_for_actual_appassionata_scenario_accepts_alternate_year_from_multi_year_source() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:eMi5KZ4lDCA" in scenario.target_urls
    )
    draft = InputNormalizer().normalize(scenario.item)

    alternate_year_hit = score_recording_match(
        'Walter Gieseking plays Beethoven Piano Sonata No.23 "Appassionata" (1947)',
        "https://www.youtube.com/watch?v=eMi5KZ4lDCA",
        draft,
        duration_seconds=1114,
    )
    wrong_year = score_recording_match(
        'Walter Gieseking plays Beethoven Piano Sonata No.23 "Appassionata" (1935)',
        "https://www.youtube.com/watch?v=wrong-year",
        draft,
        duration_seconds=1114,
    )

    assert alternate_year_hit >= 0.45
    assert alternate_year_hit > wrong_year


def test_candidate_title_quality_score_for_actual_appassionata_scenario_accepts_alternate_year_from_multi_year_source() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenario = next(
        scenario
        for scenario in build_work_dataset(
            work_id=work_id,
            recordings=recordings,
            works=works,
            composers=composers,
        )
        if scenario.variant == "full" and "youtube:eMi5KZ4lDCA" in scenario.target_urls
    )
    draft = InputNormalizer().normalize(scenario.item)

    alternate_year_title = 'Walter Gieseking plays Beethoven Piano Sonata No.23 "Appassionata" (1947)'
    wrong_year_title = 'Walter Gieseking plays Beethoven Piano Sonata No.23 "Appassionata" (1935)'

    assert candidate_title_quality_score(draft, alternate_year_title) >= 0.08
    assert candidate_title_quality_score(draft, alternate_year_title) > candidate_title_quality_score(draft, wrong_year_title)


def test_score_recording_match_accepts_group_acronym_and_year_when_latin_fields_missing() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-2",
        title="Rudolf Kempe - London Symphony Orchestra - 第五交响曲 - 1964",
        composer_name="柴可夫斯基",
        composer_name_latin="",
        work_title="第五交响曲",
        work_title_latin="",
        catalogue="",
        performance_date_text="1964",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="柴可夫斯基 | 第五交响曲 | Rudolf Kempe | London Symphony Orchestra | 1964",
        raw_text="柴可夫斯基 | 第五交响曲 | Rudolf Kempe | London Symphony Orchestra | 1964",
        existing_links=[],
        lead_names=["Rudolf Kempe"],
        ensemble_names=["London Symphony Orchestra"],
    )

    score = score_recording_match(
        "Tchaikovsky : Symphony No.5 R.Kempe /LSO 1964 Proms live",
        "https://www.youtube.com/watch?v=demo",
        draft,
    )

    assert score >= 0.45


def test_score_recording_match_accepts_sparse_ui_input_with_abbreviation_and_surname_only() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-2b",
        title="monteux - BSO - 第五交响曲",
        composer_name="柴可夫斯基",
        composer_name_latin="",
        work_title="第五交响曲",
        work_title_latin="",
        catalogue="op.64",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="柴可夫斯基 | 第五交响曲 op.64 | monteux | BSO | -",
        raw_text="柴可夫斯基 | 第五交响曲 op.64 | monteux | BSO | -",
        existing_links=[],
        lead_names=["monteux"],
        lead_names_latin=["monteux"],
        ensemble_names=["BSO"],
        ensemble_names_latin=["BSO"],
    )

    score = score_recording_match(
        "Tchaikovsky - Symphony No. 5 in E minor, Op. 64 - Boston Symphony Orchestra - Pierre Monteux (1958)",
        "https://www.youtube.com/watch?v=F70Ofs15dEQ",
        draft,
    )

    assert score >= 0.75


def test_score_recording_match_accepts_exact_chamber_recording_even_when_title_omits_catalogue() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-2c",
        title="亚历山大·莫吉列夫斯基 & 列奥尼德·克鲁策",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第5号小提琴奏鸣曲, “春天”",
        work_title_latin="Violin Sonata No.5, Op.24",
        catalogue="",
        performance_date_text="1931",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Alexander Yakovlevich Mogilevsky", "Leonid Kreutzer"],
        lead_names_latin=["Alexander Yakovlevich Mogilevsky", "Leonid Kreutzer"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    score = score_recording_match(
        "Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)",
        "https://www.youtube.com/watch?v=vCC5o4A3HMY",
        draft,
    )

    assert score >= 0.45


def test_score_recording_match_accepts_concerto_resource_when_title_uses_klavierkonzert_and_only_soloist_is_visible() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-1",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Annie Fischer", "Paul Kletzki"],
        lead_names_latin=["Annie Fischer", "Paul Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    score = score_recording_match(
        "Annie Fischer plays Schumann: Klavierkonzert a-minor video! full!",
        "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
        draft,
    )

    assert score >= 0.58


def test_score_recording_match_penalizes_alternative_collaborator_when_second_required_lead_is_missing() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-2",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Annie Fischer", "Paul Kletzki"],
        lead_names_latin=["Annie Fischer", "Paul Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    exact_like = score_recording_match(
        "Annie Fischer plays Schumann: Klavierkonzert a-minor video! full!",
        "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
        draft,
    )
    wrong_collaborator = score_recording_match(
        "Schumann, Piano Concerto in A Minor, Op.54 / Fischer & Giulini",
        "https://www.youtube.com/watch?v=R4YZRoHbrCw",
        draft,
    )

    assert exact_like > wrong_collaborator
    assert exact_like >= 0.4


def test_score_recording_match_accepts_german_keyed_concerto_alias_from_clean_chinese_work_title() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-2b",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Annie Fischer", "Paul Kletzki"],
        lead_names_latin=["Annie Fischer", "Paul Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    exact_like = score_recording_match(
        "Annie Fischer plays Schumann: Klavierkonzert a-minor video! full!",
        "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
        draft,
    )
    wrong_collaborator = score_recording_match(
        "Schumann, Piano Concerto in A Minor, Op.54 / Fischer & Giulini",
        "https://www.youtube.com/watch?v=R4YZRoHbrCw",
        draft,
    )

    assert exact_like > wrong_collaborator
    assert exact_like >= 0.4


def test_score_recording_match_penalizes_explicit_wrong_collaborator_even_when_title_is_rich() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-2c-klemperer",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Annie Fischer", "Paul Kletzki"],
        lead_names_latin=["Annie Fischer", "Paul Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    exact_like = score_recording_match(
        "Annie Fischer plays Schumann: Klavierkonzert a-minor video! full!",
        "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
        draft,
    )
    wrong_collaborator = score_recording_match(
        "SCHUMANN - Concerto Piano A minor op. 54 - Annie Fischer - PHILHARMONIA Orch. , Otto Klemperer 1963",
        "https://www.youtube.com/watch?v=crIta1ClQeo",
        draft,
    )

    assert exact_like > wrong_collaborator


def test_score_recording_match_accepts_violin_concerto_alias_when_only_soloist_is_visible() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-2c",
        title="Heifetz 1940",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="D大调小提琴协奏曲",
        work_title_latin="",
        catalogue="Op.61",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        primary_names=["亚莎·海菲兹"],
        primary_names_latin=["Jascha Heifetz"],
        secondary_names=["阿图罗·托斯卡尼尼"],
        secondary_names_latin=["Arturo Toscanini"],
        query_lead_names=["亚莎·海菲兹"],
        query_lead_names_latin=["Jascha Heifetz"],
        lead_names=["亚莎·海菲兹", "阿图罗·托斯卡尼尼"],
        lead_names_latin=["Jascha Heifetz", "Arturo Toscanini"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    exact_like = score_recording_match(
        "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
        "https://www.youtube.com/watch?v=9YWr1UcbZE8",
        draft,
        duration_seconds=2315,
        uploader="Private Reserve",
    )
    wrong_work = score_recording_match(
        "Tchaikovsky: Violin Concerto in D Major, Op. 35 (reference rec.: Jascha Heifetz / 2023 Remastered)",
        "https://www.youtube.com/watch?v=qhSxS6UnXBo",
        draft,
        duration_seconds=1773,
        uploader="Classical Music Reference Recording",
    )

    assert exact_like >= 0.45
    assert exact_like > wrong_work


def test_score_recording_match_uses_title_year_hint_to_prefer_target_upload_cluster() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-concerto-2d",
        title="海菲兹&托斯卡尼尼 1940",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="D大调小提琴协奏曲",
        work_title_latin="",
        catalogue="Op.61",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | D大调小提琴协奏曲 | Jascha Heifetz | -",
        raw_text="路德维希·凡·贝多芬 | D大调小提琴协奏曲 | Jascha Heifetz | - | 海菲兹&托斯卡尼尼 1940",
        existing_links=[],
        primary_names=["亚莎·海菲兹"],
        primary_names_latin=["Jascha Heifetz"],
        query_lead_names=["亚莎·海菲兹"],
        query_lead_names_latin=["Jascha Heifetz"],
        lead_names=["亚莎·海菲兹"],
        lead_names_latin=["Jascha Heifetz"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    exact_like = score_recording_match(
        "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
        "https://www.youtube.com/watch?v=9YWr1UcbZE8",
        draft,
        duration_seconds=2315,
        uploader="Private Reserve",
    )
    wrong_upload = score_recording_match(
        "Jascha Heifetz - Beethoven : Violin Concerto Op.61 (1955)",
        "https://www.youtube.com/watch?v=4hEgZXlYOVY",
        draft,
        duration_seconds=2295,
        uploader="uchukyoku1",
    )

    assert exact_like > wrong_upload


def test_score_recording_match_accepts_appassionata_nickname_for_exact_solo_piano_recording() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-solo-1",
        title="Claudio Arrau 1970",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="Beethovenfest Bonn 1970",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Claudio Arrau"],
        lead_names_latin=["Claudio Arrau"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    exact_like = score_recording_match(
        'Claudio Arrau Beethoven "Appassionata" (Full)',
        "https://www.youtube.com/watch?v=Tdg-DT8rTUQ",
        draft,
    )
    wrong_pianist = score_recording_match(
        "Anna Fedorova - Ludwig van Beethoven - Appassionata - Piano Sonata No. 23 in F minor, Op. 57",
        "https://www.youtube.com/watch?v=9uj9g-eH0uw",
        draft,
    )

    assert exact_like > wrong_pianist


def test_score_recording_match_does_not_mistake_complete_multi_movement_listing_for_single_movement() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-solo-1b",
        title="Claudio Arrau 1970",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23 in F minor, Op.57 Appassionata",
        catalogue="Op.57",
        performance_date_text="Beethovenfest Bonn 1970",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Claudio Arrau"],
        lead_names_latin=["Claudio Arrau"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    complete_listing = score_recording_match(
        'Claudio Arrau Beethoven "Appassionata" (Full) Piano Sonata No. 23 in F minor, Op. 57 '
        '"Appassionata" I. Allegro assai II. Andante con moto III. Allegro ma non troppo Beethovenfest Bonn 1970',
        "https://www.youtube.com/watch?v=Tdg-DT8rTUQ",
        draft,
    )
    movement_only = score_recording_match(
        "Piano Sonata No. 23 in F Minor, Op. 57, \"Appassionata\": II. Andante con moto (Live at...)",
        "https://www.youtube.com/watch?v=movement-only",
        draft,
    )

    assert complete_listing > movement_only
    assert complete_listing >= 0.8


def test_score_recording_match_does_not_mistake_arabic_numbered_tracklist_for_single_movement() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-orchestral-1a",
        title="Karl Bohm 1976",
        composer_name="安东·布鲁克纳",
        composer_name_latin="Anton Bruckner",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in E major, WAB 107",
        catalogue="WAB 107",
        performance_date_text="February 2-5 1976",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Karl Böhm", "Karl Bohm"],
        lead_names_latin=["Karl Böhm", "Karl Bohm"],
        ensemble_names=["Wiener Philharmoniker"],
        ensemble_names_latin=["Wiener Philharmoniker"],
    )

    complete_tracklist = score_recording_match(
        "[High quality] Anton Bruckner - Symphony No. 7 in E major / Karl Böhm & Wiener Philharmoniker "
        "Anton Bruckner Symphony No. 7 in E major, WAB 107 (00:00) - 1. Allegro moderato "
        "(19:38) - 2. Adagio: Sehr feierlich und sehr langsam",
        "https://www.youtube.com/watch?v=jCkO-GbPLnk",
        draft,
    )
    movement_only = score_recording_match(
        "Symphony No. 7 in E Major, WAB 107: I. Allegro Moderato (Live)",
        "https://www.youtube.com/watch?v=Mr5VNv6jgSU",
        draft,
    )

    assert complete_tracklist > movement_only
    assert complete_tracklist >= 0.75


def test_score_recording_match_rewards_non_year_performance_context_for_exact_recording() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-solo-1c",
        title="Claudio Arrau 1970",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23 in F minor, Op.57 Appassionata",
        catalogue="Op.57",
        performance_date_text="Beethovenfest Bonn 1970",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Claudio Arrau"],
        lead_names_latin=["Claudio Arrau"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    with_context = score_recording_match(
        'Claudio Arrau Beethoven "Appassionata" (Full) Piano Sonata No. 23 in F minor, Op. 57 '
        '"Appassionata" Beethovenfest Bonn 1970',
        "https://www.youtube.com/watch?v=Tdg-DT8rTUQ",
        draft,
    )
    without_context = score_recording_match(
        '1970 Beethoven Piano Sonata No 23 F minor Op 57 Appassionata Claudio Arrau',
        "https://www.youtube.com/watch?v=dmF2fryWk8A",
        draft,
    )

    assert with_context > without_context


def test_score_recording_match_penalizes_wrong_soloist_even_when_work_and_year_match() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-solo-1d",
        title="Claudio Arrau 1970",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23 in F minor, Op.57 Appassionata",
        catalogue="Op.57",
        performance_date_text="1970",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        lead_names=["Claudio Arrau"],
        lead_names_latin=["Claudio Arrau"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    exact_like = score_recording_match(
        '1970 Beethoven Piano Sonata No 23 F minor Op 57 Appassionata Claudio Arrau',
        "https://www.youtube.com/watch?v=dmF2fryWk8A",
        draft,
    )
    wrong_soloist = score_recording_match(
        "Gould/Beethoven Sonata No.23 in F minor, op.57 'Appassionata'",
        "https://www.youtube.com/watch?v=T1Kljp4_60U",
        draft,
    )

    assert exact_like > wrong_soloist
    assert wrong_soloist < 0.75


def test_score_recording_match_uses_chinese_work_title_aliases_to_separate_true_and_false_hits() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-3",
        title="Albert Coates - 第五交响曲 - 1922",
        composer_name="柴可夫斯基",
        composer_name_latin="",
        work_title="第五交响曲",
        work_title_latin="",
        catalogue="",
        performance_date_text="1922",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="柴可夫斯基 | 第五交响曲 | Albert Coates | - | 1922",
        raw_text="",
        existing_links=[],
        lead_names=["Albert Coates"],
        ensemble_names=[],
    )

    true_hit = score_recording_match(
        "Albert Coates and The Symphony Orchestra - Symphony No. 5 in E minor, Op. 64 (Tchaikovsky) (1922)",
        "https://www.youtube.com/watch?v=true",
        draft,
    )
    false_hit = score_recording_match(
        "Albert Coates (1882-1953): Wagner with Davis, Radford & Whitehill  (London 1922-26)",
        "https://www.youtube.com/watch?v=false",
        draft,
    )

    assert true_hit >= 0.45
    assert false_hit < true_hit


def test_score_recording_match_penalizes_wrong_year_and_multi_work_compilations() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-4",
        title="Otto Klemperer - Philharmonia Orchestra - Symphony No. 5 - 1960",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第五交响曲",
        work_title_latin="Symphony No. 5 in C minor",
        catalogue="Op.67",
        performance_date_text="1960",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="贝多芬 | 第五交响曲 | Otto Klemperer | Philharmonia Orchestra | 1960",
        raw_text="",
        existing_links=[],
        lead_names=["Otto Klemperer"],
        ensemble_names=["Philharmonia Orchestra"],
    )

    exact_hit = score_recording_match(
        "Beethoven - Symphony No 5 in C minor, Op 67 - Klemperer Philharmonia Orchestra 1960",
        "https://www.youtube.com/watch?v=exact",
        draft,
    )
    wrong_year = score_recording_match(
        "Symphonies Nos. 5 and 7 [Philharmonia Orchestra / Otto Klemperer, 1955 Recorded]",
        "https://www.youtube.com/watch?v=wrong-year",
        draft,
    )

    assert exact_hit > wrong_year


def test_score_recording_match_keeps_collection_above_first_movement_when_both_match_version() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-larrocha-priority",
        title="萨瓦利施 - 拉罗查 - 瑞士罗曼德管弦乐团 - January 12, 1977",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="January 12, 1977",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977",
        raw_text="",
        existing_links=[],
        primary_names=["阿利西亚·德·拉罗查"],
        primary_names_latin=["Alicia de Larrocha"],
        secondary_names=["沃尔夫冈·萨瓦利施"],
        secondary_names_latin=["Wolfgang Sawallisch"],
        lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        lead_names_latin=["Alicia de Larrocha", "Wolfgang Sawallisch"],
        ensemble_names=["瑞士罗曼德管弦乐团"],
        ensemble_names_latin=["Orchestre de la Suisse Romande"],
    )

    standalone = score_recording_match(
        "Schumann Piano Concerto in A minor, Op.54 Alicia de Larrocha Wolfgang Sawallisch Orchestre de la Suisse Romande 1977 complete live Full standalone performance",
        "https://www.youtube.com/watch?v=standalone",
        draft,
        duration_seconds=1880,
    )
    collection = score_recording_match(
        "Alicia de Larrocha live 1977 Brahms and Schumann concertos complete collection Sawallisch including Schumann Piano Concerto Op.54 Wolfgang Sawallisch Orchestre de la Suisse Romande",
        "https://www.bilibili.com/video/BV1collection?p=12",
        draft,
        duration_seconds=5400,
    )
    first_movement = score_recording_match(
        "Schumann Piano Concerto in A minor, Op.54 I. Allegro affettuoso Alicia de Larrocha Sawallisch 1977",
        "https://www.youtube.com/watch?v=movement1",
        draft,
        duration_seconds=620,
    )

    assert standalone >= collection
    assert collection > first_movement


def test_score_recording_match_penalizes_chamber_multi_sonata_compilations() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-spring-compile",
        title='Jean Fournier & Ginette Doyen - Spring Sonata',
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title='第5号小提琴奏鸣曲“春天”',
        work_title_latin='Violin Sonata No.5, Op.24 "Spring"',
        catalogue="Op.24",
        performance_date_text="early '50s",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="贝多芬 | 第5号小提琴奏鸣曲“春天” | Jean Fournier | Ginette Doyen | early '50s",
        raw_text="",
        existing_links=[],
        primary_names=["让·富尼埃"],
        primary_names_latin=["Jean Fournier"],
        secondary_names=["吉内特·多延"],
        secondary_names_latin=["Ginette Doyen"],
        lead_names=["让·富尼埃", "吉内特·多延"],
        lead_names_latin=["Jean Fournier", "Ginette Doyen"],
    )

    exact_hit = score_recording_match(
        'Jean Fournier & Ginette Doyen play Beethoven "Spring" Sonata',
        "https://www.youtube.com/watch?v=exact-spring",
        draft,
    )
    compilation_hit = score_recording_match(
        "Beethoven Sonatas Violin & Piano Jean Fournier & Ginette Doyen Westminster WL-5176",
        "https://www.youtube.com/watch?v=compilation",
        draft,
    )
    multi_number_hit = score_recording_match(
        "Beethoven, Violin Sonata No 3,5, Fournier,Doyen",
        "https://www.youtube.com/watch?v=multi-number",
        draft,
    )

    assert exact_hit > compilation_hit
    assert exact_hit > multi_number_hit


def test_score_recording_match_penalizes_wrong_composer_when_other_signals_match() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-4b",
        title="Karl Bohm - VPO - Symphony No.7",
        composer_name="安东·布鲁克纳",
        composer_name_latin="Anton Bruckner",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in E major, WAB 107",
        catalogue="WAB 107",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="安东·布鲁克纳 | 第七交响曲 | Karl Bohm | Vienna Philharmonic Orchestra | -",
        raw_text="",
        existing_links=[],
        lead_names=["Karl Böhm", "Karl Bohm"],
        lead_names_latin=["Karl Böhm", "Karl Bohm"],
        ensemble_names=["Vienna Philharmonic Orchestra", "Wiener Philharmoniker", "VPO"],
        ensemble_names_latin=["Vienna Philharmonic Orchestra", "Wiener Philharmoniker", "VPO"],
    )

    exact_hit = score_recording_match(
        "[High quality] Anton Bruckner - Symphony No. 7 in E major / Karl Böhm & Wiener Philharmoniker",
        "https://www.youtube.com/watch?v=jCkO-GbPLnk",
        draft,
    )
    wrong_composer = score_recording_match(
        "贝姆排练贝多芬第七交响曲 A rehearsal : Beethoven Symphony No.7",
        "https://www.youtube.com/watch?v=1MmYV8m6ZIQ",
        draft,
    )

    assert exact_hit > wrong_composer
    assert wrong_composer < 0.5


def test_score_recording_match_heavily_penalizes_real_world_wrong_composer_false_positive() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-4c",
        title="Karl Bohm - VPO - Symphony No.7",
        composer_name="安东·布鲁克纳",
        composer_name_latin="Anton Bruckner",
        work_title="第七交响曲",
        work_title_latin="Symphony No.7 in E major, WAB 107",
        catalogue="WAB 107",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="安东·布鲁克纳 | 第七交响曲 | Karl Bohm | Vienna Philharmonic Orchestra | -",
        raw_text="",
        existing_links=[],
        lead_names=["Karl Böhm", "Karl Bohm"],
        lead_names_latin=["Karl Böhm", "Karl Bohm"],
        ensemble_names=["Vienna Philharmonic Orchestra", "Wiener Philharmoniker", "VPO"],
        ensemble_names_latin=["Vienna Philharmonic Orchestra", "Wiener Philharmoniker", "VPO"],
    )

    exact_hit = score_recording_match(
        "[High quality] Anton Bruckner - Symphony No. 7 in E major / Karl Böhm & Wiener Philharmoniker",
        "https://www.youtube.com/watch?v=jCkO-GbPLnk",
        draft,
    )
    wrong_composer = score_recording_match(
        "贝姆排练贝多芬第七交响曲   A rehearsal : Beethoven Symphony No.7",
        "https://www.youtube.com/watch?v=1MmYV8m6ZIQ",
        draft,
    )

    assert exact_hit > wrong_composer
    assert wrong_composer <= 0.55


def test_score_recording_match_penalizes_single_movement_track() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-5",
        title="Otto Klemperer - Philharmonia Orchestra - Symphony No. 5 - 1960",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第五交响曲",
        work_title_latin="Symphony No. 5 in C minor",
        catalogue="Op.67",
        performance_date_text="1960",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="贝多芬 | 第五交响曲 | Otto Klemperer | Philharmonia Orchestra | 1960",
        raw_text="",
        existing_links=[],
        lead_names=["Otto Klemperer"],
        ensemble_names=["Philharmonia Orchestra"],
    )

    full_recording = score_recording_match(
        "Beethoven - Symphony No 5 in C minor, Op 67 - Klemperer",
        "https://www.youtube.com/watch?v=full",
        draft,
    )
    movement_only = score_recording_match(
        "Symphony No. 5 in C Minor, Op. 67: I. Allegro con brio (1960)",
        "https://www.youtube.com/watch?v=movement",
        draft,
    )

    assert full_recording > movement_only


def test_score_recording_match_keeps_first_movement_video_above_low_confidence_when_version_signals_align() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-schumann-first-movement-video",
        title="Eliso Virsaladze 1989",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto in A minor, Op.54",
        catalogue="Op.54",
        performance_date_text="1989",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | Alexander Rudin | 1989",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | Alexander Rudin | 1989",
        existing_links=[],
        primary_names=["Eliso Virsaladze"],
        primary_names_latin=["Eliso Virsaladze"],
        secondary_names=["Alexander Rudin"],
        secondary_names_latin=["Alexander Rudin"],
        lead_names=["Eliso Virsaladze", "Alexander Rudin"],
        lead_names_latin=["Eliso Virsaladze", "Alexander Rudin"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    aligned_first_movement = score_recording_match(
        "Schumann: Piano Concerto in A Minor, Op.54: I. Allegro affettuoso - Eliso Virsaladze 1989 live",
        "https://www.youtube.com/watch?v=movement1",
        draft,
        duration_seconds=914,
        uploader="Archive",
    )
    wrong_first_movement = score_recording_match(
        "Schumann: Piano Concerto in A Minor, Op.54: I. Allegro affettuoso - Martha Argerich 1989 live",
        "https://www.youtube.com/watch?v=movement2",
        draft,
        duration_seconds=914,
        uploader="Archive",
    )

    assert aligned_first_movement >= 0.45
    assert aligned_first_movement > wrong_first_movement


def test_score_recording_match_penalizes_aria_extract_for_goldberg_variations() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-6",
        title="Glenn Gould - Goldberg Variations - 1955",
        composer_name="巴赫",
        composer_name_latin="Bach",
        work_title="哥德堡变奏曲",
        work_title_latin="Goldberg Variations",
        catalogue="BWV988",
        performance_date_text="1955",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Goldberg Variations | Glenn Gould | - | 1955",
        raw_text="",
        existing_links=[],
        lead_names=["Glenn Gould"],
        ensemble_names=[],
    )

    full_recording = score_recording_match(
        "Glenn Gould plays BACH : The Goldberg Variations (1955)",
        "https://www.youtube.com/watch?v=full",
        draft,
    )
    aria_extract = score_recording_match(
        "Goldberg Variations, BWV 988: Aria",
        "https://www.youtube.com/watch?v=aria",
        draft,
    )

    assert full_recording > aria_extract


def test_score_recording_match_treats_bilingual_people_as_same_role_slots() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-role-aware",
        title="Annie Fischer & Kletzki",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="舒曼 | a小调钢琴协奏曲 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="",
        existing_links=[],
        primary_names=["安妮·费舍尔"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["保罗·克列茨基"],
        secondary_names_latin=["Kletzki"],
        lead_names=["安妮·费舍尔", "保罗·克列茨基"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["布达佩斯爱乐乐团"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    exact_like = score_recording_match(
        "Schumann Piano Concerto Op.54 Annie Fischer Kletzki Budapest Philharmonic Orchestra",
        "https://www.youtube.com/watch?v=exact-like",
        draft,
    )
    wrong_collaborator = score_recording_match(
        "Schumann Piano Concerto Op.54 Annie Fischer & Giulini Budapest Philharmonic Orchestra",
        "https://www.youtube.com/watch?v=wrong-collaborator",
        draft,
    )

    assert exact_like > wrong_collaborator


def test_score_recording_match_penalizes_missing_secondary_credit_for_concerto_uploads() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-kempff-missing-secondary",
        title="Wilhelm Kempff & Antal Dorati",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["肯普夫"],
        primary_names_latin=["Wilhelm Kempff"],
        secondary_names=["多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        lead_names=["肯普夫", "多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )

    exact_like = score_recording_match(
        "Wilhelm Kempff Antal Dorati Schumann Piano Concerto Op.54 1959 Concertgebouw Orchestra Amsterdam complete",
        "https://www.bilibili.com/video/BV1exact/",
        draft,
        duration_seconds=1880,
        uploader="Classical Vault",
    )
    missing_secondary = score_recording_match(
        "Wilhelm Kempff Schumann Piano Concerto Op.54 1959 Concertgebouw Orchestra Amsterdam complete",
        "https://www.bilibili.com/video/BV1missing/",
        draft,
        duration_seconds=1880,
        uploader="Classical Vault",
    )

    assert exact_like >= 0.9
    assert missing_secondary <= 0.82
    assert exact_like > missing_secondary


def test_score_recording_match_prefers_exact_date_context_over_remastered_alt_upload() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-delara-date-context",
        title="Adelina de Lara & Ian Whyte",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        existing_links=[],
        primary_names=["阿德利纳·德·劳拉"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["英国广播公司苏格兰交响乐团"],
        ensemble_names_latin=["BBC Scottish Symphony Orchestra"],
    )

    exact_date_context = score_recording_match(
        "Adelina de Lara Schumann Piano Concerto Op.54 BBC Scottish Symphony Orchestra May 29 1951 complete",
        "https://www.bilibili.com/video/BV1target/",
        draft,
        duration_seconds=1880,
        uploader="Archive",
    )
    remastered_alt = score_recording_match(
        "Schumann Piano Concerto Op.54 Adelina de Lara 1951 remaster",
        "https://www.bilibili.com/video/BV1alt/",
        draft,
        duration_seconds=1880,
        uploader="Archive",
    )

    assert exact_date_context >= 0.84
    assert exact_date_context > remastered_alt


def test_score_recording_match_prefers_actual_delara_bbc_broadcast_over_wrong_clara_upload() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-delara-bbc-broadcast",
        title="Adelina de Lara & Ian Whyte",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        existing_links=[],
        primary_names=["阿德利纳·德·劳拉"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["英国广播公司苏格兰交响乐团"],
        ensemble_names_latin=["BBC Scottish Symphony Orchestra"],
    )

    actual_upload = score_recording_match(
        "【Adelina de Lara】克拉拉的爱徒会如何演奏舒曼钢协？ BBC broadcast; 29 May 1951 【Adelina de Lara 弹舒曼钢琴协奏曲】",
        "https://www.bilibili.com/video/BV1CWb7eHENQ/",
        draft,
        duration_seconds=1991,
        uploader="_HideousLight_",
    )
    wrong_clara_upload = score_recording_match(
        "Clara Schumann Piano Concerto in A minor Piano: Michal Tal Conductor: Keren Kagarlitsky Israel Camerata Jerusalem Orchestra",
        "https://www.bilibili.com/video/BV1Za9QYnE9D/",
        draft,
        duration_seconds=1378,
        uploader="Amy-yui",
    )

    assert actual_upload >= 0.62
    assert actual_upload > wrong_clara_upload


def test_score_recording_match_keeps_exact_annie_schumann_above_wrong_concerto_version() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-annie-version-guard",
        title="Annie Fischer & Kletzki",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        raw_text="Robert Schumann | Piano Concerto in A Minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -",
        existing_links=[],
        primary_names=["Annie Fischer"],
        primary_names_latin=["Annie Fischer"],
        secondary_names=["Kletzki"],
        secondary_names_latin=["Kletzki"],
        lead_names=["Annie Fischer", "Kletzki"],
        lead_names_latin=["Annie Fischer", "Kletzki"],
        ensemble_names=["Budapest Philharmonic Orchestra"],
        ensemble_names_latin=["Budapest Philharmonic Orchestra"],
    )

    exact_like = score_recording_match(
        "Annie Fischer plays Schumann Piano Concerto Op.54 full performance",
        "https://www.bilibili.com/video/BV1TE411f7uh/",
        draft,
        duration_seconds=2017,
        uploader="艾斯跳票",
    )
    wrong_work = score_recording_match(
        "Beethoven c minor third piano concerto Annie Fischer Dorati",
        "https://www.bilibili.com/video/BV1ht411X7rC/",
        draft,
        duration_seconds=2140,
        uploader="古典搬运",
    )

    assert exact_like >= 0.6
    assert exact_like > wrong_work


def test_score_recording_match_prefers_complete_chamber_collaboration_over_single_name_hit() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-spring-role-aware",
        title="Jean Fournier & Ginette Doyen",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第5号小提琴奏鸣曲“春天”",
        work_title_latin="Violin Sonata No.5, Op.24",
        catalogue="Op.24",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="贝多芬 | 第5号小提琴奏鸣曲“春天” | Jean Fournier | Ginette Doyen | -",
        raw_text="",
        existing_links=[],
        primary_names=["让·富尼埃"],
        primary_names_latin=["Jean Fournier"],
        secondary_names=["吉内特·多延"],
        secondary_names_latin=["Ginette Doyen"],
        lead_names=["让·富尼埃", "吉内特·多延"],
        lead_names_latin=["Jean Fournier", "Ginette Doyen"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    complete_duo = score_recording_match(
        'Jean Fournier & Ginette Doyen play Beethoven "Spring" Sonata',
        "https://www.youtube.com/watch?v=complete",
        draft,
    )
    single_name = score_recording_match(
        "Beethoven, Violin Sonata No 5, Jean Fournier",
        "https://www.youtube.com/watch?v=single",
        draft,
    )

    assert complete_duo > single_name


def test_score_recording_match_treats_complete_sonata_tracklist_as_full_work() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-spring-tracklist",
        title="让·富尼埃&吉内特·多延",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第5号小提琴奏鸣曲, “春天”",
        work_title_latin='Violin Sonata No.5, Op.24 "Spring"',
        catalogue="Op.24",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="",
        raw_text="",
        existing_links=[],
        primary_names=["让·富尼埃"],
        primary_names_latin=["Jean Fournier"],
        secondary_names=["吉内特·多延"],
        secondary_names_latin=[],
        lead_names=["让·富尼埃", "吉内特·多延"],
        lead_names_latin=["Jean Fournier"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    tracklisted_full = score_recording_match(
        'Jean Fournier & Ginette Doyen play Beethoven "Spring" Sonata '
        'Violin Sonata No. 5 in F major Opus 24, "Frühlingssonate"'
        '1. Allegro2. Adagio molto espressivo (7:27)3. Scherzo: Allegro molto '
        '(14:22)4. Rondo: Allegro ma non...',
        "https://www.youtube.com/watch?v=n0bji6PXYso",
        draft,
    )
    single_name = score_recording_match(
        "Beethoven, Violin Sonata No 5, Jean Fournier YouTube でお気に入りの動画や音楽を楽しみ、"
        "オリジナルのコンテンツをアップロードして友だちや家族、世界中の人たちと共有しましょう。",
        "https://www.youtube.com/watch?v=puC1aT9nRzI",
        draft,
    )

    assert tracklisted_full > single_name


def test_score_recording_match_keeps_sparse_cjk_duo_tracklist_above_final_threshold() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-spring-sparse-duo",
        title="让·富尼埃 - 吉内特·多延 - early '50s",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第5号小提琴奏鸣曲, “春天”",
        work_title_latin='Violin Sonata No.5, Op.24 "Spring"',
        catalogue="Op.24",
        performance_date_text="early '50s",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | 第5号小提琴奏鸣曲, “春天” | Jean Fournier | -",
        raw_text="路德维希·凡·贝多芬 | 第5号小提琴奏鸣曲, “春天” | Jean Fournier | - | 让·富尼埃 - 吉内特·多延 - early '50s",
        existing_links=[],
        primary_names=["让·富尼埃"],
        primary_names_latin=["Jean Fournier"],
        secondary_names=["吉内特·多延"],
        secondary_names_latin=[],
        lead_names=["让·富尼埃", "吉内特·多延"],
        lead_names_latin=["Jean Fournier"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    tracklisted_full = score_recording_match(
        'Jean Fournier & Ginette Doyen play Beethoven "Spring" Sonata '
        'Violin Sonata No. 5 in F major Opus 24, "Frühlingssonate"'
        '1. Allegro2. Adagio molto espressivo (7:27)3. Scherzo: Allegro molto '
        '(14:22)4. Rondo: Allegro ma non...',
        "https://www.youtube.com/watch?v=n0bji6PXYso",
        draft,
    )
    single_name = score_recording_match(
        "Beethoven, Violin Sonata No 5, Jean Fournier",
        "https://www.youtube.com/watch?v=puC1aT9nRzI",
        draft,
    )

    assert tracklisted_full >= 0.65
    assert tracklisted_full > single_name
