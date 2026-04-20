from __future__ import annotations

import asyncio
import base64
import contextvars
import html
import json
import re
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import httpx

from app.services.browser_fetcher import BrowserFetchUnavailable, PlaywrightBrowserFetcher
from app.services.platform_clients import BilibiliVideoDetail, PlatformSearchClients
from app.services.platform_search_config import PlatformSearchConfig, load_platform_search_config, runtime_config_root
from app.services.pipeline import (
    DraftRecordingEntry,
    LOW_CONFIDENCE_THRESHOLD,
    RetrievalProfile,
    build_queries,
    build_work_query,
    contains_cjk,
    has_collaboration_marker,
    looks_latin,
)
from app.services.source_profiles import (
    OrchestraAliasLoader,
    PersonAliasLoader,
    SourceProfileEntry,
    SourceProfileLoader,
    default_orchestra_alias_path,
    default_person_alias_path,
)

DEFAULT_HEADERS = {
    "user-agent": "Mozilla/5.0 (compatible; RecordingRetrievalService/0.1; +https://example.invalid)",
}

HOST_QUERY_DEPTH = 6
ENGINE_RESULT_DEPTH = 6
STREAMING_RESULT_DEPTH = 8
HYDRATE_DEPTH = 12
HOST_SEARCH_DEPTH = 5
HIGH_QUALITY_QUERY_DEPTH = 2
HIGH_QUALITY_TOTAL_ENGINE_QUERY_BUDGET = 8
PRIMARY_STREAMING_HOST_TIMEOUT_CAP = 18.0
BILIBILI_BROWSER_SEARCH_BASE_TIMEOUT = 10.0
BILIBILI_BROWSER_SEARCH_MAX_TIMEOUT = 14.0
BILIBILI_HOST_STABILITY_MIN_REQUESTS = 6
BILIBILI_PRIMARY_QUERY_DEPTH = 3
BILIBILI_BROWSER_QUERY_DEPTH = 4
BILIBILI_ENGINE_QUERY_DEPTH = 2
BILIBILI_SECOND_PASS_QUERY_DEPTH = 2


class BrowserFetcher(Protocol):
    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str]: ...

    async def fetch_links(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> list[str]: ...


def compact(value: Any) -> str:
    return str(value or "").strip()


def sanitize_bilibili_metadata_text(value: str) -> str:
    cleaned = compact(value)
    if not cleaned:
        return ""
    markers = [
        "相关视频",
        "作者简介",
        "视频作者",
        "视频播放量",
        "弹幕量",
        "点赞数",
        "投硬币枚数",
        "收藏人数",
        "转发人数",
    ]
    cut_points = [cleaned.find(marker) for marker in markers if marker in cleaned]
    if cut_points:
        cleaned = cleaned[: min(cut_points)]
    cleaned = re.sub(r"©\s*\d+\s*,?\s*$", "", cleaned, flags=re.I)
    return compact(cleaned.strip(" ,;|/-\n"))


def materials_root() -> Path:
    return Path(__file__).resolve().parents[2] / "materials" / "source-profiles"


def build_bilibili_metadata_from_detail(detail: BilibiliVideoDetail) -> dict[str, Any]:
    parts = " ".join(compact(part) for part in detail.page_parts[:16] if compact(part))
    description = compact(detail.description)
    return {
        "title": compact(detail.title),
        "description": description,
        "body_text": compact(" ".join(part for part in [compact(detail.title), compact(detail.uploader), parts] if part)),
        "image_url": compact(detail.image_url),
        "uploader": compact(detail.uploader),
        "bvid": compact(detail.bvid),
        "duration_seconds": int(detail.duration_seconds or 0),
        "view_count": int(detail.view_count or 0),
    }


class HttpSourceProvider:
    def __init__(
        self,
        profile_loader: SourceProfileLoader | None = None,
        client: httpx.AsyncClient | None = None,
        browser_fetcher: BrowserFetcher | None = None,
        orchestra_alias_loader: OrchestraAliasLoader | None = None,
        person_alias_loader: PersonAliasLoader | None = None,
        platform_search_config: PlatformSearchConfig | None = None,
    ) -> None:
        self._profile_loader = profile_loader or SourceProfileLoader(materials_root())
        self._client = client
        self._orchestra_alias_loader = orchestra_alias_loader or OrchestraAliasLoader(default_orchestra_alias_path())
        self._person_alias_loader = person_alias_loader or PersonAliasLoader(default_person_alias_path())
        self._platform_search_config = platform_search_config or load_platform_search_config()
        bilibili_storage_state_path = self._platform_search_config.bilibili.storage_state_path
        if bilibili_storage_state_path and not Path(bilibili_storage_state_path).is_absolute():
            bilibili_storage_state_path = str((runtime_config_root() / bilibili_storage_state_path).resolve())
        self._browser_fetcher = browser_fetcher or PlaywrightBrowserFetcher(
            bilibili_cookie=self._platform_search_config.bilibili.cookie,
            bilibili_user_agent=self._platform_search_config.bilibili.user_agent,
            bilibili_referer=self._platform_search_config.bilibili.referer,
            bilibili_storage_state_path=bilibili_storage_state_path,
        )
        self._disabled_platform_apis: set[str] = set()
        self._text_cache: dict[str, str] = {}
        self._thread_local = threading.local()
        self._warning_state: contextvars.ContextVar[list[str] | None] = contextvars.ContextVar("http_source_warnings", default=None)
        self._request_access_state: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
            "http_source_access_events",
            default=None,
        )
        self._access_events: list[dict[str, Any]] = []
        self._host_stats: dict[str, dict[str, float]] = {}
        self._state_lock = threading.RLock()

    async def aclose(self) -> None:
        browser_fetcher = getattr(self._browser_fetcher, "aclose", None)
        if callable(browser_fetcher):
            await browser_fetcher()
        client = self._client
        if client is not None:
            with suppress(Exception):
                await client.aclose()
        thread_client = getattr(self._thread_local, "client", None)
        if thread_client is not None and thread_client is not client:
            with suppress(Exception):
                await thread_client.aclose()
        self._thread_local.client = None
        self._thread_local.client_loop = None

    def start_request_scope(self) -> None:
        self._warning_state.set([])
        self._request_access_state.set([])

    def consume_warnings(self) -> list[str]:
        warnings = self._warning_state.get() or []
        self._warning_state.set([])
        return dedupe_text(warnings)

    def consume_access_events(self) -> list[dict[str, Any]]:
        request_events = self._request_access_state.get()
        if request_events is not None:
            events = list(request_events)
            self._request_access_state.set([])
            return events
        with self._state_lock:
            events = list(self._access_events)
            self._access_events.clear()
        return events

    def get_access_summary(self) -> dict[str, Any]:
        hosts: dict[str, Any] = {}
        with self._state_lock:
            host_items = list(self._host_stats.items())
            fallback_event_count = len(self._access_events)
        for host, stats in host_items:
            requests = int(stats.get("requests", 0))
            successes = int(stats.get("successes", 0))
            failures = int(stats.get("failures", 0))
            total_latency = float(stats.get("totalLatencyMs", 0.0))
            total_results = float(stats.get("totalResults", 0.0))
            cache_hits = int(stats.get("cacheHits", 0))
            failure_rate = failures / requests if requests else 0.0
            avg_latency = total_latency / requests if requests else 0.0
            avg_results = total_results / requests if requests else 0.0
            hosts[host] = {
                "requests": requests,
                "successes": successes,
                "failures": failures,
                "cacheHits": cache_hits,
                "avgLatencyMs": round(avg_latency, 2),
                "avgResultCount": round(avg_results, 2),
                "failureRate": round(failure_rate, 3),
                "recommendedTimeoutSeconds": self._recommended_timeout_seconds(host, 6.0),
                "recommendedQueryDepth": self._recommended_query_depth(host, HOST_QUERY_DEPTH),
                "recommendedResultDepth": self._recommended_result_depth(host, STREAMING_RESULT_DEPTH),
                "status": self._host_health_status(host),
            }
        request_event_count = len(self._request_access_state.get() or [])
        return {"hosts": hosts, "eventCount": request_event_count or fallback_event_count}

    def _reset_warnings(self) -> None:
        self._warning_state.set([])

    def _warn(self, message: str) -> None:
        warnings = list(self._warning_state.get() or [])
        warnings.append(message)
        self._warning_state.set(warnings)

    def _record_access_event(
        self,
        *,
        url: str,
        operation: str,
        ok: bool,
        duration_ms: float,
        source_kind: str = "",
        source_label: str = "",
        query: str = "",
        result_count: int = 0,
        status_code: int | None = None,
        error: str = "",
        cache_hit: bool = False,
        timeout_seconds: float | None = None,
        details: dict[str, Any] | None = None,
        track_stats: bool = True,
    ) -> None:
        host = urlparse(url).netloc.lower() or normalize_host(url)
        event = {
            "host": host,
            "url": url,
            "operation": operation,
            "sourceKind": source_kind,
            "sourceLabel": source_label,
            "query": query,
            "ok": ok,
            "durationMs": round(duration_ms, 2),
            "statusCode": status_code,
            "error": error,
            "cacheHit": cache_hit,
            "resultCount": result_count,
            "timeoutSeconds": timeout_seconds,
        }
        if details:
            event.update(details)
        request_events = self._request_access_state.get()
        if request_events is not None:
            request_events.append(event)
        else:
            with self._state_lock:
                self._access_events.append(event)

        if not track_stats:
            return
        with self._state_lock:
            stats = self._host_stats.setdefault(
                host,
                {
                    "requests": 0.0,
                    "successes": 0.0,
                    "failures": 0.0,
                    "totalLatencyMs": 0.0,
                    "totalResults": 0.0,
                    "cacheHits": 0.0,
                },
            )
            stats["requests"] += 1
            stats["successes"] += 1 if ok else 0
            stats["failures"] += 0 if ok else 1
            stats["totalLatencyMs"] += max(0.0, duration_ms)
            stats["totalResults"] += max(0, result_count)
            stats["cacheHits"] += 1 if cache_hit else 0

    def _result_url_sample(self, rows: list[dict[str, Any]], *, limit: int = 3) -> list[str]:
        sample: list[str] = []
        for row in rows:
            if len(sample) >= limit:
                break
            url = compact(str(row.get("url") or ""))
            if url:
                sample.append(url)
        return sample

    def _normalized_result_identity(self, url: str, *, bvid: str = "") -> str:
        normalized_url = compact(url)
        normalized_bvid = compact(bvid)
        if normalized_bvid:
            return f"bilibili:{normalized_bvid.lower()}"
        if not normalized_url:
            return ""
        bilibili_bvid_match = re.search(r"/(BV[0-9A-Za-z]+)/?", normalized_url, re.I)
        if bilibili_bvid_match:
            return f"bilibili:{bilibili_bvid_match.group(1).lower()}"
        bilibili_aid_match = re.search(r"/av(\d+)", normalized_url, re.I)
        if bilibili_aid_match:
            return f"bilibili:av{bilibili_aid_match.group(1)}"
        parsed = urlparse(normalized_url)
        host = parsed.netloc.lower()
        if "youtube.com" in host:
            video_id = parse_qs(parsed.query).get("v", [""])[0]
            if compact(video_id):
                return f"youtube:{compact(video_id).lower()}"
        if "youtu.be" in host:
            video_id = parsed.path.strip("/")
            if compact(video_id):
                return f"youtube:{compact(video_id).lower()}"
        return normalized_url.rstrip("/").lower()

    def _normalized_url_set(self, values: list[str]) -> set[str]:
        normalized: set[str] = set()
        for value in values:
            identity = self._normalized_result_identity(value)
            if identity:
                normalized.add(identity)
        return normalized

    def _row_identity_set(self, rows: list[dict[str, Any]], *, limit: int = 8) -> set[str]:
        normalized: set[str] = set()
        for row in rows[:limit]:
            identity = self._normalized_result_identity(
                str(row.get("url") or ""),
                bvid=str(row.get("bvid") or ""),
            )
            if identity:
                normalized.add(identity)
        return normalized

    def _row_overlap_count(self, left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> int:
        left = self._row_identity_set(left_rows, limit=8)
        right = self._row_identity_set(right_rows, limit=8)
        return len(left & right)

    def _rendered_overlap_count(self, primary_rows: list[dict[str, Any]], rendered_evidence: list[dict[str, Any]]) -> int:
        left = self._row_identity_set(primary_rows, limit=8)
        rendered_links: list[str] = []
        for row in rendered_evidence:
            rendered_links.extend(str(value) for value in row.get("matchedLinks") or [])
        right = self._normalized_url_set(rendered_links[:8])
        return len(left & right)

    def _record_search_anomaly(
        self,
        *,
        url: str,
        source_label: str,
        strategy: str,
        anomaly_type: str,
        primary_rows: list[dict[str, Any]],
        engine_rows: list[dict[str, Any]] | None = None,
        browser_rows: list[dict[str, Any]] | None = None,
        selected_queries: list[str] | None = None,
        selected_browser_queries: list[str] | None = None,
        rendered_evidence: list[dict[str, Any]] | None = None,
        extra_details: dict[str, Any] | None = None,
    ) -> None:
        details = {
            "strategy": strategy,
            "anomalyType": anomaly_type,
            "primaryResultCount": len(primary_rows),
            "primaryTopUrls": self._result_url_sample(primary_rows),
            "engineResultCount": len(engine_rows or []),
            "engineTopUrls": self._result_url_sample(engine_rows or []),
        }
        if browser_rows is not None:
            details["browserResultCount"] = len(browser_rows)
            details["browserTopUrls"] = self._result_url_sample(browser_rows)
            details["apiResultCount"] = len(primary_rows)
            details["apiTopUrls"] = self._result_url_sample(primary_rows)
        if selected_queries:
            details["selectedQueries"] = list(selected_queries)
        if selected_browser_queries:
            details["selectedBrowserQueries"] = list(selected_browser_queries)
        if rendered_evidence:
            details["renderedEvidence"] = rendered_evidence
            details["renderedEvidenceCount"] = len(rendered_evidence)
        if extra_details:
            details.update(extra_details)
        self._record_access_event(
            url=url,
            operation="search-anomaly",
            ok=True,
            duration_ms=0.0,
            source_kind="streaming",
            source_label=source_label,
            query=" || ".join(selected_queries or selected_browser_queries or []),
            details=details,
            track_stats=False,
        )

    async def _capture_rendered_search_evidence(
        self,
        *,
        queries: list[str],
        url_builders: list,
        url_patterns: list[str],
        source_label: str,
        max_queries: int = 2,
    ) -> list[dict[str, Any]]:
        fetch_search_evidence = getattr(self._browser_fetcher, "fetch_search_evidence", None)
        if not callable(fetch_search_evidence):
            return []
        evidence_rows: list[dict[str, Any]] = []
        for query in dedupe_text([compact(value) for value in queries if compact(value)])[:max_queries]:
            for build_url in url_builders[:2]:
                search_url = build_url(query)
                started = time.perf_counter()
                try:
                    payload = await fetch_search_evidence(
                        search_url,
                        url_patterns=url_patterns,
                        timeout_seconds=min(
                            4.0,
                            self._recommended_timeout_seconds(urlparse(search_url).netloc.lower(), 4.0),
                        ),
                        capture_screenshot=True,
                    )
                except (AttributeError, BrowserFetchUnavailable, RuntimeError, TimeoutError) as error:
                    self._warn(f"{source_label} 渲染证据抓取失败: {error}")
                    self._record_access_event(
                        url=search_url,
                        operation="browser-search-evidence",
                        ok=False,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        source_kind="streaming",
                        source_label=f"{source_label} Browser Evidence",
                        query=query,
                        error=str(error),
                        track_stats=False,
                    )
                    continue
                normalized = {
                    "query": query,
                    "url": search_url,
                    "title": compact(str(payload.get("title") or "")),
                    "matchedLinks": [
                        compact(str(value))
                        for value in payload.get("matchedLinks") or []
                        if compact(str(value))
                    ],
                    "matchedLinkCount": int(payload.get("matchedLinkCount", 0) or 0),
                    "anchorCount": int(payload.get("anchorCount", 0) or 0),
                    "resultCardCount": int(payload.get("resultCardCount", 0) or 0),
                    "extractionMode": compact(str(payload.get("extractionMode") or "")),
                    "htmlLength": int(payload.get("htmlLength", 0) or 0),
                    "bodyTextSample": compact(str(payload.get("bodyTextSample") or "")),
                    "screenshotPath": compact(str(payload.get("screenshotPath") or "")),
                }
                self._record_access_event(
                    url=search_url,
                    operation="browser-search-evidence",
                    ok=True,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    source_kind="streaming",
                    source_label=f"{source_label} Browser Evidence",
                    query=query,
                    result_count=normalized["matchedLinkCount"],
                    details={
                        "anchorCount": normalized["anchorCount"],
                        "resultCardCount": normalized["resultCardCount"],
                        "extractionMode": normalized["extractionMode"],
                        "htmlLength": normalized["htmlLength"],
                        "captureMode": "rendered-search-page",
                        "hasScreenshot": bool(normalized["screenshotPath"]),
                    },
                    track_stats=False,
                )
                if any(
                    [
                        normalized["matchedLinks"],
                        normalized["title"],
                        normalized["bodyTextSample"],
                        normalized["screenshotPath"],
                    ]
                ):
                    evidence_rows.append(normalized)
                    break
        return evidence_rows

    def _host_health_status(self, host: str) -> str:
        with self._state_lock:
            stats = dict(self._host_stats.get(host, {}))
        requests = float(stats.get("requests", 0.0))
        failures = float(stats.get("failures", 0.0))
        avg_latency = float(stats.get("totalLatencyMs", 0.0)) / requests if requests else 0.0
        if requests >= 2 and (failures >= 1 or failures / requests >= 0.25 or avg_latency >= 2500):
            return "degraded"
        if requests >= 2 and failures == 0 and avg_latency <= 1000:
            return "healthy"
        return "observing"

    def _recommended_timeout_seconds(self, host: str, base_timeout: float) -> float:
        with self._state_lock:
            stats = dict(self._host_stats.get(host, {}))
        requests = float(stats.get("requests", 0.0))
        failures = float(stats.get("failures", 0.0))
        avg_latency = float(stats.get("totalLatencyMs", 0.0)) / requests if requests else 0.0
        timeout = base_timeout
        if requests >= 2 and failures >= 1:
            timeout *= 1.2
        if requests >= 2 and (failures / requests >= 0.25 or avg_latency >= 2500):
            timeout *= 1.7
        elif requests >= 2 and failures == 0 and avg_latency <= 800:
            timeout *= 0.9
        return round(max(4.0, min(14.0, timeout)), 1)

    def _recommended_query_depth(self, host: str, base_depth: int) -> int:
        with self._state_lock:
            stats = dict(self._host_stats.get(host, {}))
        requests = float(stats.get("requests", 0.0))
        failures = float(stats.get("failures", 0.0))
        avg_results = float(stats.get("totalResults", 0.0)) / requests if requests else 0.0
        depth = base_depth
        if host == "search.bilibili.com" and requests < BILIBILI_HOST_STABILITY_MIN_REQUESTS:
            return max(2, min(10, depth))
        if requests >= 2 and failures / requests >= 0.4:
            depth -= 2
        elif requests >= 2 and failures == 0 and avg_results >= 2.0:
            depth += 1
        elif requests >= 3 and avg_results < 0.5:
            depth -= 1
        return max(2, min(10, depth))

    def _recommended_result_depth(self, host: str, base_depth: int) -> int:
        with self._state_lock:
            stats = dict(self._host_stats.get(host, {}))
        requests = float(stats.get("requests", 0.0))
        avg_results = float(stats.get("totalResults", 0.0)) / requests if requests else 0.0
        if requests >= 2 and avg_results >= 4.0:
            return min(base_depth + 2, 12)
        if requests >= 3 and avg_results < 1.0:
            return max(base_depth - 2, 4)
        return base_depth

    def _should_skip_host(self, host: str, *, min_requests: int = 3) -> bool:
        with self._state_lock:
            stats = dict(self._host_stats.get(host, {}))
        requests = int(stats.get("requests", 0.0))
        successes = int(stats.get("successes", 0.0))
        failures = int(stats.get("failures", 0.0))
        if is_bilibili_host(host):
            min_requests = max(min_requests, BILIBILI_HOST_STABILITY_MIN_REQUESTS)
        if requests < min_requests:
            return False
        if successes == 0 and failures >= min_requests:
            return True
        return failures / max(1, requests) >= 0.85

    def _browser_search_timeout_seconds(self, host: str) -> float:
        if host == "search.bilibili.com":
            recommended = self._recommended_timeout_seconds(host, BILIBILI_BROWSER_SEARCH_BASE_TIMEOUT)
            return round(min(BILIBILI_BROWSER_SEARCH_MAX_TIMEOUT, max(BILIBILI_BROWSER_SEARCH_BASE_TIMEOUT, recommended)), 1)
        return min(4.0, self._recommended_timeout_seconds(host, 4.0))

    def _get_http_client(self) -> httpx.AsyncClient:
        client = self._client
        if client is not None:
            return client
        current_loop = asyncio.get_running_loop()
        client = getattr(self._thread_local, "client", None)
        client_loop = getattr(self._thread_local, "client_loop", None)
        if client is None or client_loop is not current_loop:
            client = httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True, timeout=6.0)
            self._thread_local.client = client
            self._thread_local.client_loop = current_loop
        return client

    def _platform_clients(self) -> PlatformSearchClients:
        return PlatformSearchClients(self._platform_search_config, self._get_http_client())

    def _request_headers_for_url(self, url: str) -> dict[str, str] | None:
        host = urlparse(url).netloc.lower()
        if "bilibili.com" not in host and "b23.tv" not in host:
            return None
        headers = {
            "referer": self._platform_search_config.bilibili.referer or "https://www.bilibili.com",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        if compact(self._platform_search_config.bilibili.user_agent):
            headers["user-agent"] = self._platform_search_config.bilibili.user_agent
        if compact(self._platform_search_config.bilibili.cookie):
            headers["cookie"] = self._platform_search_config.bilibili.cookie
        return headers

    def _is_platform_api_disabled(self, api_label: str) -> bool:
        with self._state_lock:
            return compact(api_label).lower() in self._disabled_platform_apis

    def _disable_platform_api(self, api_label: str) -> None:
        normalized = compact(api_label).lower()
        if normalized:
            with self._state_lock:
                self._disabled_platform_apis.add(normalized)

    def _can_use_youtube_api(self) -> bool:
        return bool(
            self._platform_search_config.enabled
            and self._platform_search_config.youtube.enabled
            and compact(self._platform_search_config.youtube.api_key)
        )

    def _can_use_apple_music_api(self) -> bool:
        return bool(
            self._platform_search_config.enabled
            and self._platform_search_config.apple_music.enabled
            and compact(self._platform_search_config.apple_music.developer_token)
        )

    def _can_use_apple_music_public_api(self) -> bool:
        return bool(
            self._platform_search_config.enabled
            and self._platform_search_config.apple_music.enabled
            and self._platform_search_config.apple_music.use_itunes_fallback
        )

    def _can_use_bilibili_api(self) -> bool:
        return bool(
            self._platform_search_config.enabled
            and self._platform_search_config.bilibili.enabled
            and (
                compact(self._platform_search_config.bilibili.cookie)
                or compact(self._platform_search_config.bilibili.user_agent)
                or compact(self._platform_search_config.bilibili.storage_state_path)
            )
        )

    async def inspect_existing_links(self, draft: DraftRecordingEntry, profile: RetrievalProfile) -> list[dict[str, Any]]:
        del profile
        self._reset_warnings()
        semaphore = asyncio.Semaphore(4)
        tasks = [
            self._fetch_page_record(link["url"], "Existing Link", "existing-link", draft, semaphore)
            for link in draft.existing_links[:3]
        ]
        return [item for item in await asyncio.gather(*tasks, return_exceptions=False) if item]

    async def search_high_quality(self, draft: DraftRecordingEntry, profile: RetrievalProfile) -> list[dict[str, Any]]:
        self._reset_warnings()
        profiles = self._profile_loader.load(category=profile.category, tags=profile.tags)
        return await self._search_hosts(draft, profile, profiles.high_quality[:HOST_SEARCH_DEPTH], source_kind="high-quality")

    async def search_streaming(self, draft: DraftRecordingEntry, profile: RetrievalProfile) -> list[dict[str, Any]]:
        self._reset_warnings()
        self._thread_local.current_draft = draft
        profiles = self._profile_loader.load(category=profile.category, tags=profile.tags)
        streaming_hosts = dedupe_streaming_hosts_for_execution(
            sorted(profiles.streaming[:HOST_SEARCH_DEPTH], key=lambda host: streaming_host_priority(host.url))
        )

        async def run_host(host: SourceProfileEntry) -> tuple[SourceProfileEntry, list[dict[str, str]]]:
            normalized_host = normalize_host(host.url)
            if self._should_skip_host(normalized_host, min_requests=2):
                self._warn(f"{normalized_host} 连续失败，当前请求暂时跳过。")
                return host, []
            timeout_seconds = self._streaming_host_timeout_seconds(draft, profile, host)
            try:
                host_rows = await asyncio.wait_for(self._search_streaming_host(draft, profile, host), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                self._warn(f"{normalized_host} 资源平台搜索超时。")
                self._record_access_event(
                    url=host.url,
                    operation="streaming-host",
                    ok=False,
                    duration_ms=timeout_seconds * 1000,
                    source_kind="streaming",
                    source_label=normalized_host,
                    error="timeout",
                    timeout_seconds=timeout_seconds,
                )
                return host, []
            return host, host_rows

        primary_hosts = [host for host in streaming_hosts if streaming_host_priority(host.url)[0] == 0]
        auxiliary_hosts = [host for host in streaming_hosts if streaming_host_priority(host.url)[0] != 0]
        apple_auxiliary_hosts = [host for host in auxiliary_hosts if "apple.com" in normalize_host(host.url)]

        primary_results = await asyncio.gather(*(run_host(host) for host in primary_hosts), return_exceptions=False)
        host_results = list(primary_results)
        auxiliary_executed = False
        if should_search_auxiliary_streaming_hosts(primary_results):
            auxiliary_results = await asyncio.gather(*(run_host(host) for host in auxiliary_hosts), return_exceptions=False)
            host_results.extend(auxiliary_results)
            auxiliary_executed = True

        rows = merge_streaming_host_rows(host_results)
        initial_depth = HYDRATE_DEPTH
        if should_expand_initial_streaming_window(host_results):
            initial_depth = min(len(rows), HYDRATE_DEPTH + 4)
        hydrated_rows = await self._hydrate_results(draft, rows[:initial_depth], "streaming")
        if len(rows) > initial_depth and not any(
            float(row.get("same_recording_score", 0.0) or 0.0) >= LOW_CONFIDENCE_THRESHOLD for row in hydrated_rows
        ):
            extended_depth = min(len(rows), initial_depth + 6)
            hydrated_rows = await self._hydrate_results(draft, rows[:extended_depth], "streaming")
        if apple_auxiliary_hosts and not auxiliary_executed and should_probe_apple_auxiliary_hosts(hydrated_rows):
            self._record_access_event(
                url="https://music.apple.com/search",
                operation="search-strategy",
                ok=True,
                duration_ms=0.0,
                source_kind="streaming",
                source_label="Apple Music Search",
                details={
                    "strategy": "apple-auxiliary-probe",
                    "reason": "primary_results_lack_platform_diversity",
                    "selectedHosts": [host.url for host in apple_auxiliary_hosts],
                },
                track_stats=False,
            )
            apple_results = await asyncio.gather(*(run_host(host) for host in apple_auxiliary_hosts), return_exceptions=False)
            host_results.extend(apple_results)
            rows = merge_streaming_host_rows(host_results)
            extended_depth = min(len(rows), max(initial_depth, HYDRATE_DEPTH + 2))
            hydrated_rows = await self._hydrate_results(draft, rows[:extended_depth], "streaming")
        return hydrated_rows

    def _streaming_host_timeout_seconds(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host: SourceProfileEntry,
    ) -> float:
        normalized_host = normalize_host(host.url)
        base_timeout = self._recommended_timeout_seconds(normalized_host, 10.0)
        query_count = len(self._queries_for_host(draft, profile, host))
        query_depth = min(query_count, self._recommended_query_depth(normalized_host, HOST_QUERY_DEPTH))
        timeout_scale = 1.0 + max(0, query_depth - 1) * 0.28
        timeout_seconds = min(30.0, max(10.0, base_timeout * timeout_scale))
        if any(token in normalized_host for token in ("youtube.com", "youtu.be", "bilibili.com", "b23.tv", "apple.com")):
            timeout_seconds = min(timeout_seconds, PRIMARY_STREAMING_HOST_TIMEOUT_CAP)
        return round(timeout_seconds, 1)

    async def search_fallback(self, draft: DraftRecordingEntry, profile: RetrievalProfile) -> list[dict[str, Any]]:
        self._reset_warnings()
        fallback_queries = ensure_catalogue_hints(profile.queries, draft=draft)
        tasks = [
            self._search_query_via_engines(
                query=query,
                source_label="Web Search",
                source_kind="search",
            )
            for query in fallback_queries[:HOST_QUERY_DEPTH]
        ]
        rows: list[dict[str, str]] = []
        for group in await asyncio.gather(*tasks, return_exceptions=False):
            rows.extend(group)
            rows = dedupe_rows(rows)
            if len(rows) >= HYDRATE_DEPTH:
                break
        return await self._hydrate_results(draft, rows[:HYDRATE_DEPTH], "search")

    async def _search_hosts(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        hosts: list[SourceProfileEntry],
        source_kind: str,
    ) -> list[dict[str, Any]]:
        tasks = []
        total_query_budget = HIGH_QUALITY_TOTAL_ENGINE_QUERY_BUDGET if source_kind == "high-quality" else None
        for host in hosts:
            normalized_host = normalize_host(host.url)
            if self._should_skip_host(normalized_host, min_requests=2):
                self._warn(f"{normalized_host} 已因连续失败暂时跳过。")
                continue
            query_depth = self._recommended_query_depth(normalized_host, HOST_QUERY_DEPTH)
            if source_kind == "high-quality":
                query_depth = min(query_depth, HIGH_QUALITY_QUERY_DEPTH)
            for query in self._queries_for_host(draft, profile, host)[:query_depth]:
                if total_query_budget is not None and len(tasks) >= total_query_budget:
                    break
                tasks.append(
                    self._search_query_via_engines(
                        query=f"site:{normalized_host} {query}",
                        source_label=f"Site Search {normalized_host}",
                        source_kind=source_kind,
                    )
                )
            if total_query_budget is not None and len(tasks) >= total_query_budget:
                break
        rows: list[dict[str, str]] = []
        for group in await asyncio.gather(*tasks, return_exceptions=False):
            rows.extend(group)
        return await self._hydrate_results(draft, rows[:HYDRATE_DEPTH], source_kind)

    async def _search_streaming_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host: SourceProfileEntry,
    ) -> list[dict[str, str]]:
        normalized_host = normalize_host(host.url)
        queries = self._queries_for_host(draft, profile, host)
        if "youtube.com" in normalized_host or "youtu.be" in normalized_host:
            return await self._search_youtube(queries)
        if "bilibili.com" in normalized_host:
            return await self._search_bilibili(queries)
        if "apple.com" in normalized_host:
            return await self._search_apple_music(queries)
        return await self._search_hosts(draft, profile, [host], source_kind="streaming")

    async def _search_query_via_engines(self, *, query: str, source_label: str, source_kind: str) -> list[dict[str, str]]:
        engines = [
            ("bing", f"https://www.bing.com/search?q={quote_plus(query)}", extract_bing_result_links),
        ]
        active_engines = []
        for engine_name, engine_url, parser in engines:
            if self._should_skip_host(urlparse(engine_url).netloc.lower(), min_requests=3):
                continue
            active_engines.append((engine_name, engine_url, parser))
        if not active_engines:
            active_engines.append(("bing", f"https://www.bing.com/search?q={quote_plus(query)}", extract_bing_result_links))

        async def run_engine(engine_name: str, url: str, parser) -> list[dict[str, str]]:
            started = time.perf_counter()
            try:
                html_text = await self._fetch_text(
                    url,
                    operation="search-engine",
                    query=query,
                    source_kind=source_kind,
                    source_label=source_label,
                    timeout_seconds=self._recommended_timeout_seconds(urlparse(url).netloc.lower(), 6.0),
                )
            except Exception as error:
                self._warn(f"{source_label} 在 {engine_name} 检索失败：{error}")
                self._record_access_event(
                    url=url,
                    operation="search-engine",
                    ok=False,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    source_kind=source_kind,
                    source_label=source_label,
                    query=query,
                    error=str(error),
                )
                return []
            links = parser(html_text)
            self._record_access_event(
                url=url,
                operation="search-engine",
                ok=True,
                duration_ms=(time.perf_counter() - started) * 1000,
                source_kind=source_kind,
                source_label=source_label,
                query=query,
                result_count=len(links[:ENGINE_RESULT_DEPTH]),
            )
            return [
                {"url": link, "source_label": f"{source_label} via {engine_name}", "source_kind": source_kind}
                for link in links[:ENGINE_RESULT_DEPTH]
            ]

        rows: list[dict[str, str]] = []
        for group in await asyncio.gather(
            *(run_engine(engine_name, engine_url, parser) for engine_name, engine_url, parser in active_engines),
            return_exceptions=False,
        ):
            rows.extend(group)
        return dedupe_rows(rows)

    async def _search_youtube(self, queries: list[str]) -> list[dict[str, str]]:
        html_query_depth = min(len(queries), self._recommended_query_depth("www.youtube.com", HOST_QUERY_DEPTH) + 1)
        builders = [
            lambda query: f"https://www.youtube.com/results?search_query={quote_plus(query)}",
            lambda query: f"https://www.youtube.com/results?sp=EgIQAQ%253D%253D&search_query={quote_plus(query)}",
        ]
        strategy_label = (
            "youtube-api-first"
            if self._can_use_youtube_api() and not self._is_platform_api_disabled("YouTube API Search")
            else "youtube-html-fallback"
        )
        self._record_access_event(
            url="https://www.youtube.com/results",
            operation="search-strategy",
            ok=True,
            duration_ms=0.0,
            source_kind="streaming",
            source_label="YouTube Search",
            query=" || ".join(queries[:html_query_depth]),
            details={
                "strategy": strategy_label,
                "htmlQueryDepth": html_query_depth,
                "selectedQueries": list(queries[:html_query_depth]),
            },
            track_stats=False,
        )
        rows = await self._search_streaming_platform(
            queries=queries[:html_query_depth],
            url_builders=builders,
            parser=extract_youtube_result_links,
            source_label="YouTube Search",
            api_search=self._run_youtube_api_search if self._can_use_youtube_api() else None,
            api_source_label="YouTube API Search",
        )
        if self._can_use_youtube_api() and not self._is_platform_api_disabled("YouTube API Search"):
            self._record_access_event(
                url="https://www.youtube.com/results",
                operation="search-layer-summary",
                ok=True,
                duration_ms=0.0,
                source_kind="streaming",
                source_label="YouTube Search",
                details={
                    "strategy": strategy_label,
                    "primaryResultCount": len(rows),
                    "engineResultCount": 0,
                },
                track_stats=False,
            )
            return rows
        engine_rows = await self._search_platform_via_site_engines(
            queries[: max(4, html_query_depth)],
            site_hosts=["www.youtube.com", "youtu.be"],
            source_label="YouTube Search",
        )
        self._record_access_event(
            url="https://www.youtube.com/results",
            operation="search-layer-summary",
            ok=True,
            duration_ms=0.0,
            source_kind="streaming",
            source_label="YouTube Search",
            details={
                "strategy": strategy_label,
                "primaryResultCount": len(rows),
                "engineResultCount": len(engine_rows),
            },
            track_stats=False,
        )
        if rows and engine_rows and self._row_overlap_count(rows, engine_rows) == 0:
            rendered_evidence = await self._capture_rendered_search_evidence(
                queries=list(queries[: max(4, html_query_depth)]),
                url_builders=builders,
                url_patterns=[r"https://www\.youtube\.com/watch\?v=[0-9A-Za-z_-]+"],
                source_label="YouTube Search",
                max_queries=1,
            )
            rendered_primary_overlap = self._rendered_overlap_count(rows, rendered_evidence)
            rendered_engine_overlap = self._rendered_overlap_count(engine_rows, rendered_evidence)
            if rendered_evidence and rendered_primary_overlap == 0 and rendered_engine_overlap > 0:
                self._record_search_anomaly(
                    url="https://www.youtube.com/results",
                    source_label="YouTube Search",
                    strategy=strategy_label,
                    anomaly_type="parser_mismatch",
                    primary_rows=rows,
                    engine_rows=engine_rows,
                    selected_queries=list(queries[: max(4, html_query_depth)]),
                    rendered_evidence=rendered_evidence,
                    extra_details={
                        "overlapCount": rendered_primary_overlap,
                        "alternateOverlapCount": rendered_engine_overlap,
                    },
                )
        if engine_rows and not rows:
            rendered_evidence = await self._capture_rendered_search_evidence(
                queries=list(queries[: max(4, html_query_depth)]),
                url_builders=builders,
                url_patterns=[r"https://www\.youtube\.com/watch\?v=[0-9A-Za-z_-]+"],
                source_label="YouTube Search",
            )
            self._record_search_anomaly(
                url="https://www.youtube.com/results",
                source_label="YouTube Search",
                strategy=strategy_label,
                anomaly_type="engine_only_recovery",
                primary_rows=rows,
                engine_rows=engine_rows,
                selected_queries=list(queries[: max(4, html_query_depth)]),
                rendered_evidence=rendered_evidence,
            )
        return dedupe_rows([*rows, *engine_rows])[:HYDRATE_DEPTH]

    async def _search_bilibili(self, queries: list[str]) -> list[dict[str, str]]:
        prioritized_queries = queries
        html_query_depth = min(
            len(prioritized_queries),
            min(BILIBILI_PRIMARY_QUERY_DEPTH, self._recommended_query_depth("search.bilibili.com", HOST_QUERY_DEPTH) + 1),
        )
        builders = [
            lambda query: f"https://search.bilibili.com/all?keyword={quote_plus(query)}",
            lambda query: f"https://search.bilibili.com/video?keyword={quote_plus(query)}",
        ]
        browser_queries = prepare_bilibili_browser_queries(prioritized_queries, max_queries=BILIBILI_BROWSER_QUERY_DEPTH)
        self._record_access_event(
            url="https://search.bilibili.com/video",
            operation="search-strategy",
            ok=True,
            duration_ms=0.0,
            source_kind="streaming",
            source_label="Bilibili Search",
            query=" || ".join(prioritized_queries[:html_query_depth]),
            details={
                "strategy": "bilibili-mixed",
                "htmlQueryDepth": html_query_depth,
                "selectedQueries": list(prioritized_queries[:html_query_depth]),
                "selectedBrowserQueries": list(browser_queries),
                "selectedBrowserQueryCount": len(browser_queries),
            },
            track_stats=False,
        )
        browser_rows = await self._search_platform_via_browser_pages(
            queries=browser_queries,
            url_builders=builders,
            source_label="Bilibili Search",
            url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
        )
        primary_queries = prioritized_queries[:html_query_depth]
        if browser_rows:
            # When browser search already finds hits, probe the most focused browser query once via
            # the primary/API path so anomaly reporting compares like-for-like queries and we can
            # still merge any complementary API results into the final candidate pool.
            primary_queries = dedupe_text([*browser_queries[:1], *prioritized_queries[:1]])[:1]
        rows: list[dict[str, str]] = []
        if primary_queries:
            rows = await self._search_streaming_platform(
                queries=primary_queries,
                url_builders=builders,
                parser=extract_bilibili_result_links,
                source_label="Bilibili Search",
                api_search=self._run_bilibili_api_search if self._can_use_bilibili_api() else None,
                api_source_label="Bilibili API Search",
            )
        force_chamber_duo_second_pass = should_force_chamber_duo_second_pass_for_bilibili(
            queries=prioritized_queries,
            api_rows=rows,
            browser_rows=browser_rows,
        )
        force_chamber_solo_second_pass = should_force_chamber_solo_second_pass_for_bilibili(
            queries=prioritized_queries,
            api_rows=rows,
            browser_rows=browser_rows,
        )
        force_chamber_second_pass = force_chamber_duo_second_pass or force_chamber_solo_second_pass
        if (not rows and not browser_rows) or force_chamber_second_pass:
            remaining_queries = [
                query
                for query in prioritized_queries
                if query not in primary_queries and query not in browser_queries
            ]
            chamber_probe_queries: list[str] = []
            if force_chamber_duo_second_pass:
                chamber_probe_queries.extend(
                    build_bilibili_chamber_duo_trailer_probe_queries(prioritized_queries)
                )
            if force_chamber_solo_second_pass:
                chamber_probe_queries.extend(
                    build_bilibili_chamber_solo_opus_probe_queries(prioritized_queries)
                )
            if force_chamber_second_pass:
                chamber_probe_queries = dedupe_text(chamber_probe_queries)
            second_pass_primary_queries = dedupe_text(
                [*chamber_probe_queries, *remaining_queries]
            )[:BILIBILI_SECOND_PASS_QUERY_DEPTH]
            second_pass_browser_queries = prepare_bilibili_browser_queries(
                remaining_queries,
                max_queries=min(BILIBILI_SECOND_PASS_QUERY_DEPTH, BILIBILI_BROWSER_QUERY_DEPTH),
            )
            if second_pass_browser_queries or second_pass_primary_queries:
                self._record_access_event(
                    url="https://search.bilibili.com/video",
                    operation="search-strategy",
                    ok=True,
                    duration_ms=0.0,
                    source_kind="streaming",
                    source_label="Bilibili Search",
                    query=" || ".join(remaining_queries[:BILIBILI_SECOND_PASS_QUERY_DEPTH]),
                    details={
                        "strategy": "bilibili-second-pass",
                        "htmlQueryDepth": len(second_pass_primary_queries),
                        "selectedQueries": list(second_pass_primary_queries),
                        "selectedBrowserQueries": list(second_pass_browser_queries),
                        "selectedBrowserQueryCount": len(second_pass_browser_queries),
                        "forceChamberDuoSecondPass": force_chamber_duo_second_pass,
                        "forceChamberSoloSecondPass": force_chamber_solo_second_pass,
                    },
                    track_stats=False,
                )
                if second_pass_browser_queries:
                    second_pass_browser_rows = await self._search_platform_via_browser_pages(
                        queries=second_pass_browser_queries,
                        url_builders=builders,
                        source_label="Bilibili Search",
                        url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
                    )
                    browser_rows = dedupe_rows([*browser_rows, *second_pass_browser_rows])
                if second_pass_primary_queries and (not rows or force_chamber_second_pass):
                    second_pass_rows = await self._search_streaming_platform(
                        queries=second_pass_primary_queries,
                        url_builders=builders,
                        parser=extract_bilibili_result_links,
                        source_label="Bilibili Search",
                        api_search=self._run_bilibili_api_search if self._can_use_bilibili_api() else None,
                        api_source_label="Bilibili API Search",
                    )
                    rows = dedupe_rows([*rows, *second_pass_rows])
        engine_rows: list[dict[str, str]] = []
        if not rows and not browser_rows:
            engine_rows = await self._search_platform_via_site_engines(
                prioritized_queries[:BILIBILI_ENGINE_QUERY_DEPTH],
                site_hosts=["www.bilibili.com", "m.bilibili.com", "b23.tv"],
                source_label="Bilibili Search",
            )
        self._record_access_event(
            url="https://search.bilibili.com/video",
            operation="search-layer-summary",
            ok=True,
            duration_ms=0.0,
            source_kind="streaming",
            source_label="Bilibili Search",
            details={
                "strategy": "bilibili-mixed",
                "apiResultCount": len(rows),
                "browserResultCount": len(browser_rows),
                "engineResultCount": len(engine_rows),
                "primaryExecuted": bool(primary_queries),
                "executedPrimaryQueries": list(primary_queries),
            },
            track_stats=False,
        )
        parser_mismatch = False
        if rows and browser_rows and self._row_overlap_count(rows, browser_rows) == 0:
            rendered_evidence = await self._capture_rendered_search_evidence(
                queries=list(browser_queries),
                url_builders=builders,
                url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
                source_label="Bilibili Search",
                max_queries=1,
            )
            rendered_primary_overlap = self._rendered_overlap_count(rows, rendered_evidence)
            rendered_browser_overlap = self._rendered_overlap_count(browser_rows, rendered_evidence)
            if rendered_evidence and rendered_primary_overlap == 0 and rendered_browser_overlap > 0:
                parser_mismatch = True
                self._record_search_anomaly(
                    url="https://search.bilibili.com/video",
                    source_label="Bilibili Search",
                    strategy="bilibili-mixed",
                    anomaly_type="parser_mismatch",
                    primary_rows=rows,
                    browser_rows=browser_rows,
                    engine_rows=engine_rows,
                    selected_queries=list(prioritized_queries[:html_query_depth]),
                    selected_browser_queries=list(browser_queries),
                    rendered_evidence=rendered_evidence,
                    extra_details={
                        "overlapCount": rendered_primary_overlap,
                        "alternateOverlapCount": rendered_browser_overlap,
                    },
                )
        if browser_rows and not rows:
            rendered_evidence = await self._capture_rendered_search_evidence(
                queries=list(browser_queries),
                url_builders=builders,
                url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
                source_label="Bilibili Search",
            )
            self._record_search_anomaly(
                url="https://search.bilibili.com/video",
                source_label="Bilibili Search",
                strategy="bilibili-mixed",
                anomaly_type="browser_outperformed_primary",
                primary_rows=rows,
                browser_rows=browser_rows,
                engine_rows=engine_rows,
                selected_queries=list(prioritized_queries[:html_query_depth]),
                selected_browser_queries=list(browser_queries),
                rendered_evidence=rendered_evidence,
                extra_details={
                    "primaryExecuted": bool(primary_queries),
                    "executedPrimaryQueries": list(primary_queries),
                },
            )
        elif engine_rows and not rows and not browser_rows:
            rendered_evidence = await self._capture_rendered_search_evidence(
                queries=list(browser_queries or prioritized_queries[:html_query_depth]),
                url_builders=builders,
                url_patterns=[r"https://www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?"],
                source_label="Bilibili Search",
            )
            self._record_search_anomaly(
                url="https://search.bilibili.com/video",
                source_label="Bilibili Search",
                strategy="bilibili-mixed",
                anomaly_type="engine_only_recovery",
                primary_rows=rows,
                browser_rows=browser_rows,
                engine_rows=engine_rows,
                selected_queries=list(prioritized_queries[:html_query_depth]),
                selected_browser_queries=list(browser_queries),
                rendered_evidence=rendered_evidence,
            )
        if force_chamber_second_pass and rows:
            promoted_probe_rows = select_bilibili_chamber_duo_bundle_probe_rows(
                rows,
                queries=prioritized_queries,
                limit=1,
            )
            if promoted_probe_rows:
                browser_rows = dedupe_rows([*promoted_probe_rows, *browser_rows])
        return merge_bilibili_search_rows(
            rows,
            browser_rows,
            engine_rows,
            parser_mismatch=parser_mismatch,
        )

    async def _search_apple_music(self, queries: list[str]) -> list[dict[str, str]]:
        rows = await self._search_streaming_platform(
            queries=queries,
            url_builders=[
                lambda query: f"https://classical.music.apple.com/search?term={quote_plus(query)}",
                lambda query: f"https://music.apple.com/search?term={quote_plus(query)}",
            ],
            parser=extract_apple_music_result_links,
            source_label="Apple Music Search",
            api_search=self._run_apple_music_api_search
            if self._can_use_apple_music_api() or self._can_use_apple_music_public_api()
            else None,
            api_source_label="Apple Music API Search",
        )
        if rows:
            return rows
        return await self._search_platform_via_site_engines(
            queries,
            site_hosts=["classical.music.apple.com", "music.apple.com"],
            source_label="Apple Music Search",
        )

    async def _search_streaming_platform(
        self,
        *,
        queries: list[str],
        url_builder=None,
        url_builders=None,
        parser,
        source_label: str,
        api_search=None,
        api_source_label: str = "",
    ) -> list[dict[str, str]]:
        builders = list(url_builders or ([] if url_builder is None else [url_builder]))
        if not builders:
            return []
        sample_url = builders[0](queries[0]) if queries else ""
        host = urlparse(sample_url).netloc.lower()
        query_depth = self._recommended_query_depth(host, HOST_QUERY_DEPTH)
        result_depth = self._recommended_result_depth(host, STREAMING_RESULT_DEPTH)
        if api_search is None or self._is_platform_api_disabled(api_source_label or source_label):
            query_boost, result_boost = self._html_budget_boost(source_label)
            query_depth = min(10, query_depth + query_boost)
            result_depth = min(12, result_depth + result_boost)

        async def run_query(query: str) -> list[dict[str, str]]:
            if api_search is not None and not self._is_platform_api_disabled(api_source_label or source_label):
                api_started = time.perf_counter()
                try:
                    api_result = await api_search(query, result_depth)
                except Exception as error:
                    api_url = self._platform_api_marker_url(api_source_label or source_label)
                    if should_disable_platform_api(error):
                        self._disable_platform_api(api_source_label or source_label)
                    self._warn(f"{api_source_label or source_label} API 搜索失败：{error}")
                    self._record_access_event(
                        url=api_url,
                        operation="streaming-api-search",
                        ok=False,
                        duration_ms=(time.perf_counter() - api_started) * 1000,
                        source_kind="streaming",
                        source_label=api_source_label or source_label,
                        query=query,
                        error=str(error),
                    )
                else:
                    self._record_access_event(
                        url=api_result.endpoint_url,
                        operation="streaming-api-search",
                        ok=True,
                        duration_ms=(time.perf_counter() - api_started) * 1000,
                        source_kind="streaming",
                        source_label=api_source_label or source_label,
                        query=query,
                        result_count=len(api_result.links[:result_depth]),
                    )
                    if api_result.links:
                        if api_result.rows:
                            rows: list[dict[str, Any]] = []
                            for item in api_result.rows[:result_depth]:
                                url = compact(item.get("url"))
                                if not url:
                                    continue
                                rows.append(
                                    {
                                        "url": url,
                                        "source_label": api_source_label or source_label,
                                        "source_kind": "streaming",
                                        "title": compact(item.get("title")),
                                        "description": compact(item.get("description")),
                                        "uploader": compact(item.get("uploader")),
                                        "duration_seconds": int(item.get("duration_seconds") or 0),
                                        "view_count": int(item.get("view_count") or 0),
                                        "bvid": compact(item.get("bvid")),
                                    }
                                )
                            if rows:
                                return rows
                        return [
                            {
                                "url": link,
                                "source_label": api_source_label or source_label,
                                "source_kind": "streaming",
                            }
                            for link in api_result.links[:result_depth]
                        ]

            rows: list[dict[str, str]] = []
            for build_url in builders:
                request_url = build_url(query)
                started = time.perf_counter()
                try:
                    html_text = await self._fetch_text(
                        request_url,
                        operation="streaming-search",
                        query=query,
                        source_kind="streaming",
                        source_label=source_label,
                        timeout_seconds=self._recommended_timeout_seconds(urlparse(request_url).netloc.lower(), 6.0),
                    )
                except Exception as error:
                    self._warn(f"{source_label} 搜索失败：{error}")
                    self._record_access_event(
                        url=request_url,
                        operation="streaming-search",
                        ok=False,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        source_kind="streaming",
                        source_label=source_label,
                        query=query,
                        error=str(error),
                    )
                    continue
                links = parser(html_text)[:result_depth]
                self._record_access_event(
                    url=request_url,
                    operation="streaming-search",
                    ok=True,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    source_kind="streaming",
                    source_label=source_label,
                    query=query,
                    result_count=len(links),
                )
                rows.extend(
                    {
                        "url": link,
                        "source_label": source_label,
                        "source_kind": "streaming",
                    }
                    for link in links
                )
                rows = dedupe_rows(rows)
                if links or len(rows) >= result_depth:
                    break
            return rows[:result_depth]
        selected_queries = list(queries[:query_depth])
        if api_search is not None:
            groups: list[list[dict[str, str]]] = []
            for query in selected_queries:
                groups.append(await run_query(query))
        else:
            groups = await asyncio.gather(*(run_query(query) for query in selected_queries), return_exceptions=False)
        if should_merge_streaming_query_coverage(source_label):
            return merge_streaming_query_groups(groups, limit=HYDRATE_DEPTH)
        rows: list[dict[str, str]] = []
        for group in groups:
            rows.extend(group)
            rows = dedupe_rows(rows)
            if len(rows) >= HYDRATE_DEPTH:
                break
        return rows

    def _platform_api_marker_url(self, source_label: str) -> str:
        lowered = compact(source_label).lower()
        if "youtube" in lowered:
            return "https://www.googleapis.com/youtube/v3/search"
        if "apple" in lowered:
            return f"https://api.music.apple.com/v1/catalog/{self._platform_search_config.apple_music.storefront}/search"
        if "bilibili" in lowered:
            return "https://api.bilibili.com/x/web-interface/wbi/search/type"
        return "https://api.example.invalid/search"

    def _html_budget_boost(self, source_label: str) -> tuple[int, int]:
        lowered = compact(source_label).lower()
        if "youtube" in lowered:
            return 2, 2
        if "bilibili" in lowered:
            return 1, 2
        if "apple" in lowered:
            return 1, 1
        return 0, 0

    async def _search_platform_via_site_engines(
        self,
        queries: list[str],
        *,
        site_hosts: list[str],
        source_label: str,
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for query in queries[: min(len(queries), HOST_QUERY_DEPTH)]:
            for host in site_hosts:
                engine_rows = await self._search_query_via_engines(
                    query=f"site:{host} {query}",
                    source_label=f"{source_label} Engine Fallback",
                    source_kind="streaming",
                )
                filtered = [
                    row
                    for row in engine_rows
                    if any(site_host in urlparse(row.get("url", "")).netloc.lower() for site_host in site_hosts)
                ]
                rows.extend(filtered)
                rows = dedupe_rows(rows)
                if len(rows) >= HYDRATE_DEPTH:
                    return rows[:HYDRATE_DEPTH]
        return rows[:HYDRATE_DEPTH]

    async def _search_platform_via_browser_pages(
        self,
        *,
        queries: list[str],
        url_builders: list,
        source_label: str,
        url_patterns: list[str],
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        if not queries or not url_builders:
            return rows

        sample_url = url_builders[0](queries[0])
        host = urlparse(sample_url).netloc.lower()
        lowered_label = compact(source_label).lower()
        bilibili_search = "bilibili" in lowered_label
        query_depth = len(queries) if bilibili_search else 1
        result_depth = min(HYDRATE_DEPTH, self._recommended_result_depth(host, STREAMING_RESULT_DEPTH) + 2)
        if bilibili_search:
            query_rows_list: list[list[dict[str, str]]] = []
            query_row_queries: list[str] = []
            browser_url_builders = list(url_builders[:2] or url_builders[:1])
            skip_all_search_builder = False
            for query in queries[:query_depth]:
                query_rows: list[dict[str, str]] = []
                query_timeout_failures = 0
                builders_for_query = list(browser_url_builders)
                if skip_all_search_builder:
                    filtered_builders = []
                    for build_url in builders_for_query:
                        sample_search_url = build_url(query)
                        if "search.bilibili.com/all?" in sample_search_url:
                            continue
                        filtered_builders.append(build_url)
                    builders_for_query = filtered_builders or builders_for_query[-1:]
                for build_url in builders_for_query:
                    search_url = build_url(query)
                    started = time.perf_counter()
                    try:
                        links = await self._browser_fetcher.fetch_links(
                            search_url,
                            url_patterns=url_patterns,
                            timeout_seconds=self._browser_search_timeout_seconds(urlparse(search_url).netloc.lower()),
                        )
                    except (AttributeError, BrowserFetchUnavailable, RuntimeError, TimeoutError) as error:
                        self._warn(f"{source_label} 浏览器搜索回退失败: {error}")
                        self._record_access_event(
                            url=search_url,
                            operation="browser-search",
                            ok=False,
                            duration_ms=(time.perf_counter() - started) * 1000,
                            source_kind="streaming",
                            source_label=f"{source_label} Browser Search",
                            query=query,
                            error=str(error),
                        )
                        if "search.bilibili.com/all?" in search_url:
                            skip_all_search_builder = True
                        if isinstance(error, TimeoutError):
                            query_timeout_failures += 1
                        continue
                    self._record_access_event(
                        url=search_url,
                        operation="browser-search",
                        ok=True,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        source_kind="streaming",
                        source_label=f"{source_label} Browser Search",
                        query=query,
                        result_count=len(links[:result_depth]),
                    )
                    query_rows.extend(
                        {
                            "url": link,
                            "source_label": f"{source_label} Browser Search",
                            "source_kind": "streaming",
                        }
                        for link in links[:result_depth]
                    )
                    query_rows = dedupe_rows(query_rows)
                    if links:
                        break
                if query_rows:
                    query_rows_list.append(query_rows[:result_depth])
                    query_row_queries.append(query)
                    continue
                if builders_for_query and query_timeout_failures >= len(builders_for_query):
                    break
            return merge_bilibili_browser_query_rows(
                query_rows_list,
                queries=query_row_queries,
                result_depth=result_depth,
            )

        for query in queries[:query_depth]:
            builders_to_use = url_builders[:1]
            for build_url in builders_to_use:
                search_url = build_url(query)
                started = time.perf_counter()
                try:
                    links = await self._browser_fetcher.fetch_links(
                        search_url,
                        url_patterns=url_patterns,
                        timeout_seconds=self._browser_search_timeout_seconds(urlparse(search_url).netloc.lower()),
                    )
                except (AttributeError, BrowserFetchUnavailable, RuntimeError, TimeoutError) as error:
                    self._warn(f"{source_label} 浏览器搜索回退失败：{error}")
                    self._record_access_event(
                        url=search_url,
                        operation="browser-search",
                        ok=False,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        source_kind="streaming",
                        source_label=f"{source_label} Browser Search",
                        query=query,
                        error=str(error),
                    )
                    continue
                self._record_access_event(
                    url=search_url,
                    operation="browser-search",
                    ok=True,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    source_kind="streaming",
                    source_label=f"{source_label} Browser Search",
                    query=query,
                    result_count=len(links[:result_depth]),
                )
                rows.extend(
                    {
                        "url": link,
                        "source_label": f"{source_label} Browser Search",
                        "source_kind": "streaming",
                    }
                    for link in links[:result_depth]
                )
                rows = dedupe_rows(rows)
                if len(rows) >= result_depth:
                    return rows[:result_depth]
                if links:
                    break
        return rows[:result_depth]

    async def _run_youtube_api_search(self, query: str, result_depth: int):
        return await self._platform_clients().search_youtube(query, result_limit=result_depth)

    async def _run_apple_music_api_search(self, query: str, result_depth: int):
        clients = self._platform_clients()
        if self._can_use_apple_music_api():
            try:
                return await clients.search_apple_music(query, result_limit=result_depth)
            except Exception:
                if not self._can_use_apple_music_public_api():
                    raise
        return await clients.search_apple_music_public(query, result_limit=result_depth)

    async def _run_bilibili_api_search(self, query: str, result_depth: int):
        return await self._platform_clients().search_bilibili(query, result_limit=result_depth)

    async def _hydrate_results(
        self,
        draft: DraftRecordingEntry,
        rows: list[dict[str, str]],
        source_kind: str,
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(4)
        tasks = [
            self._fetch_page_record(
                item["url"],
                item["source_label"],
                source_kind,
                draft,
                semaphore,
                seed_data=item,
            )
            for item in dedupe_rows(rows)
        ]
        return [item for item in await asyncio.gather(*tasks, return_exceptions=False) if item]

    def _queries_for_host(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        host: SourceProfileEntry,
    ) -> list[str]:
        latin_lead_pool = self._expand_person_terms(
            dedupe_text([*draft.query_lead_names_latin, *draft.query_lead_names]),
            prefer_latin=True,
        )
        latin_leads = prioritize_person_query_terms(
            [*[value for value in latin_lead_pool if looks_latin(value)], *latin_lead_pool]
        )
        latin_ensembles = self._expand_ensemble_terms(
            draft.ensemble_names_latin or [value for value in draft.ensemble_names if looks_latin(value)],
            prefer_full_names=not host.is_chinese,
        )
        zh_leads = self._expand_person_terms(
            [value for value in draft.query_lead_names if contains_cjk(value)],
            prefer_latin=False,
        )
        zh_ensembles = dedupe_text([value for value in draft.ensemble_names if contains_cjk(value)])

        latin_queries = build_queries(
            work_query=build_work_query(draft, prefer_latin=True),
            composer_query=compact(draft.composer_name_latin),
            lead_terms=latin_leads,
            ensemble_terms=latin_ensembles,
            title=draft.title,
            performance_date_text=draft.performance_date_text,
        )
        if not latin_queries:
            latin_queries = profile.latin_queries or profile.queries
        is_chamber_profile = profile.category in {"chamber_solo", "chamber_duo"}
        if is_chamber_profile:
            latin_leads = prioritize_chamber_collaboration_query_terms(draft, latin_leads)
        chamber_latin_anchor_queries = (
            build_chamber_work_anchor_queries(
                draft,
                prefer_latin=True,
                composer_query=compact(draft.composer_name_latin or draft.composer_name),
                lead_terms=dedupe_text([*draft.query_lead_names_latin, *draft.query_lead_names]),
            )
            if is_chamber_profile
            else []
        )

        if host.is_chinese:
            chamber_cjk_anchor_queries = (
                build_chamber_work_anchor_queries(
                    draft,
                    prefer_latin=False,
                    composer_query=compact(draft.composer_name or draft.composer_name_latin),
                    lead_terms=dedupe_text([*draft.query_lead_names, *draft.query_lead_names_latin]),
                )
                if is_chamber_profile
                else []
            )
            chamber_named_work_surname_queries = (
                build_chamber_named_work_surname_rescue_queries(draft)
                if is_chamber_profile
                else []
            )
            chamber_named_work_full_name_year_queries = (
                build_chamber_named_work_full_name_year_rescue_queries(draft)
                if is_chamber_profile
                else []
            )
            chamber_solo_opus_queries = (
                build_chamber_solo_opus_queries(
                    draft,
                    composer_query=compact(draft.composer_name_latin or draft.composer_name),
                    lead_terms=dedupe_text([*draft.query_lead_names_latin, *draft.query_lead_names]),
                )
                if is_chamber_profile
                else []
            )
            bilingual_primary_terms = prioritize_person_query_terms(
                draft.primary_names_latin or [value for value in latin_leads if looks_latin(value)]
            )
            decade_rescue_queries = build_chinese_host_decade_rescue_queries(draft)
            generic_bundle_rescue_queries = select_generic_plural_bundle_rescue_queries(
                decade_rescue_queries,
                draft=draft,
            )
            primary_work_rescue_queries = build_chinese_host_primary_work_rescue_queries(draft)
            primary_year_anchor_queries = build_chinese_host_primary_year_anchor_queries(draft)
            cjk_context_rescue_queries = build_chinese_host_cjk_context_rescue_queries(
                draft,
                ensemble_terms=dedupe_text([*zh_ensembles, *latin_ensembles]),
            )
            bundle_context_queries = build_chinese_host_bundle_context_queries(
                draft,
                ensemble_terms=dedupe_text([*latin_ensembles, *zh_ensembles]),
            )
            primary_only_queries = self._primary_only_queries_for_host(
                draft,
                host,
                composer_query=compact(draft.composer_name),
            )
            bilingual_primary_queries = build_queries(
                work_query=build_work_query(draft, prefer_latin=False),
                composer_query=compact(draft.composer_name),
                lead_terms=bilingual_primary_terms[:2],
                ensemble_terms=dedupe_text([*zh_ensembles, *latin_ensembles]),
                title=draft.title,
                performance_date_text=draft.performance_date_text,
            )
            zh_queries = build_queries(
                work_query=build_work_query(draft, prefer_latin=False),
                composer_query=compact(draft.composer_name),
                lead_terms=zh_leads,
                ensemble_terms=zh_ensembles,
                title=draft.title,
                performance_date_text=draft.performance_date_text,
            )
            mixed_queries = build_queries(
                work_query=build_work_query(draft, prefer_latin=False) or build_work_query(draft, prefer_latin=True),
                composer_query=compact(draft.composer_name_latin or draft.composer_name),
                lead_terms=dedupe_text([*zh_leads, *latin_leads]),
                ensemble_terms=dedupe_text([*zh_ensembles, *latin_ensembles]),
                title=draft.title,
                performance_date_text=draft.performance_date_text,
            )
            alias_queries = self._alias_queries_for_host(
                draft,
                host,
                lead_terms=dedupe_text([*zh_leads, *latin_leads]),
                ensemble_terms=dedupe_text([*zh_ensembles, *latin_ensembles]),
            )
            bilingual_alias_queries = self._alias_queries_for_host(
                draft,
                host,
                lead_terms=latin_leads,
                ensemble_terms=dedupe_text([*zh_ensembles, *latin_ensembles]),
            )
            generated_queries = prioritize_platform_queries(
                [
                    *chamber_cjk_anchor_queries[:2],
                    *chamber_named_work_surname_queries[:2],
                    *chamber_named_work_full_name_year_queries[:2],
                    *chamber_solo_opus_queries[:2],
                    *chamber_latin_anchor_queries[:7],
                    *cjk_context_rescue_queries[:1],
                    *bundle_context_queries[:1],
                    *decade_rescue_queries[:2],
                    *generic_bundle_rescue_queries[:1],
                    *primary_work_rescue_queries[:2],
                    *primary_year_anchor_queries[:1],
                    *primary_only_queries[:3],
                    *bilingual_primary_queries[:3],
                    *bilingual_alias_queries[:3],
                    *zh_queries[:3],
                    *latin_queries[:3],
                    *profile.mixed_queries[:1],
                    *mixed_queries[:2],
                    *alias_queries[:2],
                ],
                draft=draft,
                prefer_cjk=True,
            )
            final_queries = ensure_catalogue_hints([
                *profile.zh_queries[:2],
                *chamber_cjk_anchor_queries[:2],
                *chamber_named_work_surname_queries[:2],
                *chamber_named_work_full_name_year_queries[:2],
                *chamber_solo_opus_queries[:2],
                *chamber_latin_anchor_queries[:7],
                *cjk_context_rescue_queries[:1],
                *decade_rescue_queries[:2],
                *generic_bundle_rescue_queries[:1],
                *bundle_context_queries[:1],
                *primary_work_rescue_queries[:2],
                *primary_year_anchor_queries[:1],
                *primary_only_queries[:3],
                *bilingual_primary_queries[:1],
                *bilingual_alias_queries[:1],
                *alias_queries[:1],
                *profile.latin_queries[:1],
                *generated_queries,
            ], draft=draft)
            named_duo_anchor_query = next(
                (
                    query
                    for query in chamber_cjk_anchor_queries
                    if is_chamber_named_work_duo_anchor_query(query, draft)
                ),
                "",
            )
            if named_duo_anchor_query and named_duo_anchor_query not in final_queries[:2]:
                final_queries = [query for query in final_queries if query != named_duo_anchor_query]
                final_queries = [named_duo_anchor_query, *final_queries]
            mixed_compact_duo_query = next(
                (
                    query
                    for query in chamber_cjk_anchor_queries
                    if is_chamber_named_work_compact_duo_query(query, draft)
                ),
                "",
            )
            if mixed_compact_duo_query and mixed_compact_duo_query not in final_queries[:8]:
                final_queries = [query for query in final_queries if query != mixed_compact_duo_query]
                final_queries = [*final_queries[:1], mixed_compact_duo_query, *final_queries[1:]]
            latin_compact_duo_query = next(
                (
                    query
                    for query in chamber_latin_anchor_queries
                    if is_chamber_latin_compact_duo_anchor_query(query, draft)
                ),
                "",
            )
            if latin_compact_duo_query and latin_compact_duo_query not in final_queries[:6]:
                final_queries = [query for query in final_queries if query != latin_compact_duo_query]
                insert_index = min(3, len(final_queries))
                final_queries = [*final_queries[:insert_index], latin_compact_duo_query, *final_queries[insert_index:]]
            solo_opus_anchor_query = next(
                (
                    query
                    for query in chamber_solo_opus_queries
                    if is_chamber_latin_solo_opus_anchor_query(query, draft)
                ),
                "",
            )
            if solo_opus_anchor_query and solo_opus_anchor_query not in final_queries[:6]:
                final_queries = [query for query in final_queries if query != solo_opus_anchor_query]
                insert_index = min(1, len(final_queries))
                final_queries = [*final_queries[:insert_index], solo_opus_anchor_query, *final_queries[insert_index:]]
            chamber_bundle_trailer_query = next(
                (
                    query
                    for query in chamber_latin_anchor_queries
                    if "trailer" in query.lower() and bilibili_query_is_chamber_duo_bundle_anchor(query)
                ),
                "",
            )
            trailer_is_schneiderhan_probe = (
                "schneiderhan" in chamber_bundle_trailer_query.lower()
                and "seemann" in chamber_bundle_trailer_query.lower()
            )
            if (
                chamber_bundle_trailer_query
                and trailer_is_schneiderhan_probe
                and chamber_bundle_trailer_query not in final_queries[:10]
            ):
                final_queries = [query for query in final_queries if query != chamber_bundle_trailer_query]
                insert_index = min(5, len(final_queries))
                final_queries = [*final_queries[:insert_index], chamber_bundle_trailer_query, *final_queries[insert_index:]]
            chamber_bundle_year_query = next(
                (
                    query
                    for query in chamber_latin_anchor_queries
                    if bilibili_query_is_chamber_duo_bundle_anchor(query)
                    and re.search(r"\b(?:18|19|20)\d{2}\b", query.lower())
                ),
                "",
            )
            year_is_schneiderhan_probe = (
                "schneiderhan" in chamber_bundle_year_query.lower()
                and "seemann" in chamber_bundle_year_query.lower()
            )
            if (
                chamber_bundle_year_query
                and year_is_schneiderhan_probe
                and chamber_bundle_year_query not in final_queries[:10]
            ):
                final_queries = [query for query in final_queries if query != chamber_bundle_year_query]
                insert_index = min(6, len(final_queries))
                final_queries = [*final_queries[:insert_index], chamber_bundle_year_query, *final_queries[insert_index:]]
            solo_named_work_cjk_query = next(
                (
                    query
                    for query in sorted(
                        chamber_cjk_anchor_queries,
                        key=chamber_named_work_solo_cjk_rescue_rank,
                    )
                    if is_chamber_named_work_solo_cjk_rescue_query(query, draft)
                ),
                "",
            )
            if solo_named_work_cjk_query and solo_named_work_cjk_query not in final_queries[:6]:
                final_queries = [query for query in final_queries if query != solo_named_work_cjk_query]
                insert_index = min(2, len(final_queries))
                final_queries = [*final_queries[:insert_index], solo_named_work_cjk_query, *final_queries[insert_index:]]
            anchor_pool = dedupe_text([
                *profile.zh_queries[:2],
                *profile.queries[:3],
                *profile.latin_queries[:3],
                *profile.mixed_queries[:2],
                *zh_queries[:3],
                *primary_only_queries[:3],
                *primary_year_anchor_queries[:1],
                *bilingual_primary_queries[:3],
                *bilingual_alias_queries[:2],
                *alias_queries[:2],
                *decade_rescue_queries[:2],
                *primary_work_rescue_queries[:2],
                *chamber_solo_opus_queries[:2],
                *generated_queries,
            ])
            anchor_candidates = [
                query for query in primary_year_anchor_queries if bilibili_query_is_primary_year_anchor(query)
            ]
            if not anchor_candidates:
                anchor_candidates = [query for query in anchor_pool if bilibili_query_is_primary_year_anchor(query)]
            if anchor_candidates:
                anchor_query = min(
                    anchor_candidates,
                    key=lambda query: (0 if contains_cjk(query) else 1, len(compact(query)), query),
                )
                if anchor_query not in final_queries[:10]:
                    if len(final_queries) >= 10:
                        replace_window = min(len(final_queries), 10)
                        replace_index = next(
                            (
                                index
                                for index in range(replace_window - 1, -1, -1)
                                if not bilibili_query_is_primary_work_rescue(final_queries[index])
                                and not bilibili_query_has_collaboration_signal(final_queries[index])
                                and not is_chamber_named_work_duo_anchor_query(final_queries[index], draft)
                            ),
                            replace_window - 1,
                        )
                        final_queries[replace_index] = anchor_query
                    else:
                        final_queries.append(anchor_query)
                    final_queries = dedupe_text(final_queries)
            has_explicit_year_context = bool(
                re.search(r"\b(?:18|19|20)\d{2}\b", compact(draft.performance_date_text).lower())
            )
            exact_collaboration_candidates = [
                query
                for query in dedupe_text([
                    *latin_queries[:20],
                    *profile.latin_queries[:20],
                    *profile.queries[:20],
                    *generated_queries,
                ])
                if bilibili_query_is_exact_collaboration_anchor(query)
            ]
            if exact_collaboration_candidates and not has_explicit_year_context:
                exact_collaboration_query = exact_collaboration_candidates[0]
                if exact_collaboration_query not in final_queries[:10]:
                    if len(final_queries) >= 10:
                        replace_window = min(len(final_queries), 10)
                        replace_index = next(
                            (
                                index
                                for index in range(replace_window - 1, -1, -1)
                                if not bilibili_query_is_primary_work_rescue(final_queries[index])
                                and not bilibili_query_is_primary_year_anchor(final_queries[index])
                                and not bilibili_query_is_exact_collaboration_anchor(final_queries[index])
                                and not bilibili_query_has_collaboration_signal(final_queries[index])
                                and not is_chamber_named_work_duo_anchor_query(final_queries[index], draft)
                            ),
                            replace_window - 1,
                        )
                        final_queries[replace_index] = exact_collaboration_query
                    else:
                        final_queries.append(exact_collaboration_query)
                    final_queries = dedupe_text(final_queries)
            return final_queries[:10]

        primary_only_queries = self._primary_only_queries_for_host(
            draft,
            host,
            composer_query=compact(draft.composer_name_latin),
        )
        chamber_named_work_surname_queries = build_chamber_named_work_surname_rescue_queries(draft)
        collaboration_rescue_queries = build_collaboration_surname_rescue_queries(draft)
        alias_queries = self._alias_queries_for_host(
            draft,
            host,
            lead_terms=latin_leads,
            ensemble_terms=latin_ensembles,
        )
        generated_queries = prioritize_platform_queries(
            [
                *chamber_latin_anchor_queries[:7],
                *chamber_named_work_surname_queries[:3],
                *chamber_latin_anchor_queries[6:8],
                *collaboration_rescue_queries[:3],
                *primary_only_queries[:4],
                *alias_queries[:4],
                *latin_queries[:4],
            ],
            draft=draft,
            prefer_cjk=False,
        )
        return ensure_catalogue_hints([
            *chamber_latin_anchor_queries[:7],
            *chamber_named_work_surname_queries[:3],
            *chamber_latin_anchor_queries[6:8],
            *collaboration_rescue_queries[:3],
            *primary_only_queries[:4],
            *alias_queries[:1],
            *profile.queries[:2],
            *profile.latin_queries[:2],
            *generated_queries,
        ], draft=draft)[:10]

    def _primary_only_queries_for_host(
        self,
        draft: DraftRecordingEntry,
        host: SourceProfileEntry,
        *,
        composer_query: str,
    ) -> list[str]:
        work_query = build_work_query(draft, prefer_latin=not host.is_chinese)
        normalized_work = compact(work_query).lower()
        if not normalized_work:
            return []
        if "concerto" not in normalized_work and "协奏曲" not in work_query:
            return []
        latin_primary_terms: list[str] = []
        if host.is_chinese:
            primary_terms = dedupe_text([*getattr(draft, "primary_names", []), *getattr(draft, "primary_names_latin", [])])
            latin_primary_terms = prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))[:1]
        else:
            primary_terms = dedupe_text([*getattr(draft, "primary_names_latin", []), *getattr(draft, "primary_names", [])])
        if not primary_terms:
            return []
        queries: list[str] = []
        if host.is_chinese:
            queries.extend(build_chinese_host_decade_rescue_queries(draft))
        queries.extend(
            build_queries(
                work_query=work_query,
                composer_query=composer_query,
                lead_terms=primary_terms[:1],
                ensemble_terms=[],
                title=draft.title,
                performance_date_text=draft.performance_date_text,
            )
        )
        alias_values = build_work_aliases(draft.work_title_latin)
        alias_values.update(build_work_aliases(draft.work_title))
        for alias in sorted(alias_values):
            normalized_alias = compact(alias)
            if not normalized_alias or normalized_alias.lower() == normalized_work:
                continue
            if host.is_chinese and not contains_cjk(normalized_alias):
                continue
            if not host.is_chinese and not looks_latin(normalized_alias):
                continue
            queries.extend(
                build_queries(
                    work_query=normalized_alias,
                    composer_query=composer_query,
                    lead_terms=primary_terms[:1],
                    ensemble_terms=[],
                    title=draft.title,
                    performance_date_text=draft.performance_date_text,
                )[:2]
            )
        if host.is_chinese and latin_primary_terms:
            queries.extend(
                build_queries(
                    work_query=work_query,
                    composer_query=composer_query,
                    lead_terms=latin_primary_terms,
                    ensemble_terms=[],
                    title=draft.title,
                    performance_date_text=draft.performance_date_text,
                )[:4]
            )
            for alias in sorted(alias_values):
                normalized_alias = compact(alias)
                if not normalized_alias or not contains_cjk(normalized_alias):
                    continue
                queries.extend(
                    build_queries(
                        work_query=normalized_alias,
                        composer_query=composer_query,
                        lead_terms=latin_primary_terms,
                        ensemble_terms=[],
                        title=draft.title,
                        performance_date_text=draft.performance_date_text,
                    )[:2]
                )
        filtered_queries = []
        required_leads = {compact(primary_terms[0]).lower()}
        required_leads.update(compact(value).lower() for value in latin_primary_terms if compact(value))
        required_leads.update(
            compact(extract_person_query_keyword(value)).lower()
            for value in latin_primary_terms
            if compact(extract_person_query_keyword(value))
        )
        catalogue = compact(draft.catalogue).lower()
        for query in dedupe_text(queries):
            lowered = compact(query).lower()
            if required_leads and not any(required_lead in lowered for required_lead in required_leads if required_lead):
                continue
            if catalogue and catalogue not in lowered and "concerto" not in lowered and "协奏曲" not in query and "klavierkonzert" not in lowered:
                continue
            filtered_queries.append(query)
        return prioritize_platform_queries(filtered_queries, draft=draft, prefer_cjk=host.is_chinese)

    def _alias_queries_for_host(
        self,
        draft: DraftRecordingEntry,
        host: SourceProfileEntry,
        *,
        lead_terms: list[str],
        ensemble_terms: list[str],
    ) -> list[str]:
        aliases = build_work_aliases(draft.work_title_latin)
        aliases.update(build_work_aliases(draft.work_title))
        if host.is_chinese:
            work_aliases = [alias for alias in aliases if contains_cjk(alias)]
            composer_query = compact(draft.composer_name)
        else:
            work_aliases = [alias for alias in aliases if looks_latin(alias)]
            composer_query = compact(draft.composer_name_latin)
        queries: list[str] = []
        base_queries = {
            compact(build_work_query(draft, prefer_latin=not host.is_chinese)),
            compact(build_work_query(draft, prefer_latin=host.is_chinese)),
        }
        ordered_aliases = sorted(
            work_aliases,
            key=lambda alias: (
                any(word in alias.lower() for word in ("sonata", "concerto", "symphony")),
                len(alias),
            ),
        )
        for alias in ordered_aliases:
            normalized_alias = compact(alias)
            if not normalized_alias or normalized_alias in base_queries:
                continue
            queries.extend(
                build_queries(
                    work_query=normalized_alias,
                    composer_query=composer_query,
                    lead_terms=lead_terms,
                    ensemble_terms=ensemble_terms,
                    title=draft.title,
                    performance_date_text=draft.performance_date_text,
                )[:4]
            )
        return dedupe_text(queries)

    def _expand_ensemble_terms(self, values: list[str], *, prefer_full_names: bool) -> list[str]:
        expanded: list[str] = []
        for value in values:
            expansions = self._orchestra_alias_loader.expand(value)
            if prefer_full_names:
                expansions = sorted(expansions, key=lambda item: (is_probable_abbreviation(item), len(item)))
            expanded.extend(expansions)
        return dedupe_text(expanded)

    def _expand_person_terms(self, values: list[str], *, prefer_latin: bool) -> list[str]:
        expanded: list[str] = []
        for value in values:
            expansions = self._person_alias_loader.expand(value)
            if prefer_latin:
                expansions = sorted(expansions, key=lambda item: (contains_cjk(item), len(item)))
            else:
                expansions = sorted(expansions, key=lambda item: (not contains_cjk(item), len(item)))
            expanded.extend(expansions)
        return dedupe_text(expanded)

    async def _fetch_page_record(
        self,
        url: str,
        source_label: str,
        source_kind: str,
        draft: DraftRecordingEntry,
        semaphore: asyncio.Semaphore,
        *,
        seed_data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        async with semaphore:
            platform = detect_platform(url)
            html_text = ""
            bilibili_metadata: dict[str, Any] = {}
            youtube_metadata: dict[str, Any] = {}
            seed = seed_data or {}
            seed_title = strip_html(compact(seed.get("title")))
            seed_description = strip_html(compact(seed.get("description")))
            seed_uploader = compact(seed.get("uploader"))
            seed_bvid = compact(seed.get("bvid"))
            duration_seconds = int(seed.get("duration_seconds") or 0)
            uploader = seed_uploader
            view_count = int(seed.get("view_count") or 0)
            title = seed_title
            description = seed_description
            body_text = compact(seed.get("body_text")) or compact(
                " ".join(part for part in [seed_title, seed_description, seed_uploader] if compact(part))
            )
            image_url = resolve_image_url(url, compact(seed.get("image_url")))
            canonical_url = canonicalize_bilibili_video_url(url, seed_bvid)
            fetch_timeout = self._recommended_timeout_seconds(urlparse(url).netloc.lower(), 6.0)
            seed_detail_ready = platform == "bilibili" and not metadata_is_insufficient(title, description, body_text) and duration_seconds > 0 and view_count > 0 and compact(uploader)
            if platform == "bilibili" and not seed_detail_ready and self._can_use_bilibili_api() and not self._is_platform_api_disabled("Bilibili Detail API"):
                detail_started = time.perf_counter()
                try:
                    detail = await self._platform_clients().fetch_bilibili_video_detail(url)
                except Exception as error:
                    if should_disable_platform_api(error):
                        self._disable_platform_api("Bilibili Detail API")
                    self._warn(f"Bilibili Detail API 获取失败：{error}")
                    self._record_access_event(
                        url=url,
                        operation="detail-api-fetch",
                        ok=False,
                        duration_ms=(time.perf_counter() - detail_started) * 1000,
                        source_kind=source_kind,
                        source_label="Bilibili Detail API",
                        error=str(error),
                    )
                else:
                    if detail is not None:
                        bilibili_metadata = build_bilibili_metadata_from_detail(detail)
                    self._record_access_event(
                        url=detail.endpoint_url if detail is not None else url,
                        operation="detail-api-fetch",
                        ok=True,
                        duration_ms=(time.perf_counter() - detail_started) * 1000,
                        source_kind=source_kind,
                        source_label="Bilibili Detail API",
                    )

            detail_ready = platform == "bilibili" and not metadata_is_insufficient(
                compact(bilibili_metadata.get("title")) or title,
                compact(bilibili_metadata.get("description")) or description,
                compact(bilibili_metadata.get("body_text")) or body_text,
            ) and int(bilibili_metadata.get("duration_seconds", 0) or 0) > 0 and int(
                bilibili_metadata.get("view_count", 0) or 0
            ) > 0 and compact(bilibili_metadata.get("uploader"))

            if not detail_ready and not seed_detail_ready:
                started = time.perf_counter()
                try:
                    html_text = await self._fetch_text(
                        url,
                        operation="fetch-page",
                        source_kind=source_kind,
                        source_label=source_label,
                        timeout_seconds=fetch_timeout,
                    )
                except Exception as error:
                    self._warn(f"{normalize_host(url)} 访问失败：{error}")
                    self._record_access_event(
                        url=url,
                        operation="fetch-page",
                        ok=False,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        source_kind=source_kind,
                        source_label=source_label,
                        error=str(error),
                        timeout_seconds=fetch_timeout,
                    )
                    html_text = ""

                if platform == "bilibili" and html_text:
                    html_bilibili_metadata = extract_bilibili_structured_metadata(html_text)
                    for key, value in html_bilibili_metadata.items():
                        if not compact(bilibili_metadata.get(key)):
                            bilibili_metadata[key] = value
                if platform == "youtube" and html_text:
                    youtube_metadata = extract_youtube_structured_metadata(html_text)
            title = strip_html(
                extract_meta_content(html_text, "og:title")
                or compact(youtube_metadata.get("title"))
                or compact(bilibili_metadata.get("title"))
                or title
                or extract_title(html_text)
            )
            if platform == "youtube":
                title = strip_generic_youtube_watch_suffix(title)
            description = strip_html(
                extract_meta_content(html_text, "og:description")
                or extract_meta_content(html_text, "description", attr="name")
                or compact(youtube_metadata.get("description"))
                or compact(bilibili_metadata.get("description"))
                or description
            )
            if platform == "apple_music" and is_generic_apple_player_text(title):
                title = seed_title or title
            if platform == "apple_music" and is_generic_apple_player_text(description):
                description = seed_description or description
            body_text = (
                compact(youtube_metadata.get("body_text"))
                or compact(bilibili_metadata.get("body_text"))
                or body_text
                or (strip_html(html_text)[:4000] if html_text else "")
            )
            youtube_generic_shell = False
            if platform == "youtube":
                youtube_generic_shell = is_generic_youtube_watch_text(title) or (
                    is_generic_youtube_watch_text(description) and is_generic_youtube_watch_text(body_text)
                )
                if is_generic_youtube_watch_text(title):
                    title = seed_title or ""
                if is_generic_youtube_watch_text(description):
                    description = seed_description or ""
            if platform == "bilibili":
                description = sanitize_bilibili_metadata_text(description)
                body_text = sanitize_bilibili_metadata_text(body_text)
            image_url = resolve_image_url(
                url,
                extract_meta_content(html_text, "og:image")
                or extract_meta_content(html_text, "twitter:image", attr="name")
                or compact(bilibili_metadata.get("image_url"))
                or image_url
                or extract_first_image_src(html_text, url),
            )
            canonical_url = canonicalize_bilibili_video_url(canonical_url, compact(bilibili_metadata.get("bvid")) or seed_bvid)
            duration_seconds = (
                extract_duration_seconds(html_text)
                or int(youtube_metadata.get("duration_seconds", 0) or 0)
                or int(bilibili_metadata.get("duration_seconds", 0) or 0)
            )
            if duration_seconds <= 0:
                duration_seconds = int(seed.get("duration_seconds") or 0)
            uploader = (
                extract_uploader_name(html_text)
                or compact(youtube_metadata.get("uploader"))
                or compact(bilibili_metadata.get("uploader"))
            )
            if not compact(uploader):
                uploader = seed_uploader
            view_count = (
                extract_view_count(html_text)
                or int(youtube_metadata.get("view_count", 0) or 0)
                or int(bilibili_metadata.get("view_count", 0) or 0)
            )
            if view_count <= 0:
                view_count = int(seed.get("view_count") or 0)

            bilibili_suspicious_metadata = platform == "bilibili" and bilibili_metadata_needs_browser_fallback(
                title=title,
                description=description,
                body_text=body_text,
                duration_seconds=duration_seconds,
                draft=draft,
            )
            browser_metadata_needed = metadata_is_insufficient(title, description, body_text) or youtube_generic_shell or (
                platform == "bilibili" and (duration_seconds <= 0 or view_count <= 0 or not compact(uploader) or bilibili_suspicious_metadata)
            )
            if browser_metadata_needed:
                browser_started = time.perf_counter()
                try:
                    browser_timeout = self._recommended_timeout_seconds(urlparse(url).netloc.lower(), 6.0)
                    browser_payload = await self._browser_fetcher.fetch_page(url, timeout_seconds=browser_timeout)
                    self._record_access_event(
                        url=url,
                        operation="browser-fallback",
                        ok=True,
                        duration_ms=(time.perf_counter() - browser_started) * 1000,
                        source_kind=source_kind,
                        source_label=source_label,
                        timeout_seconds=browser_timeout,
                    )
                    browser_title = compact(browser_payload.get("title"))
                    browser_description = compact(browser_payload.get("description"))
                    if platform == "apple_music" and is_generic_apple_player_text(browser_title):
                        browser_title = ""
                    if platform == "apple_music" and is_generic_apple_player_text(browser_description):
                        browser_description = ""
                    if platform == "youtube" and is_generic_youtube_watch_text(browser_title):
                        browser_title = ""
                    if platform == "youtube" and is_generic_youtube_watch_text(browser_description):
                        browser_description = ""
                    if platform == "youtube":
                        browser_title = strip_generic_youtube_watch_suffix(browser_title)
                    title = browser_title or title
                    description = browser_description or description
                    body_text = compact(browser_payload.get("bodyText")) or body_text
                    if platform == "bilibili":
                        description = sanitize_bilibili_metadata_text(description)
                        body_text = sanitize_bilibili_metadata_text(body_text)
                    image_url = resolve_image_url(url, browser_payload.get("imageUrl") or image_url)
                    uploader = compact(browser_payload.get("uploader")) or uploader
                    canonical_url = canonicalize_bilibili_video_url(
                        canonical_url,
                        compact(browser_payload.get("bvid")),
                    )
                    browser_duration_seconds = int(
                        browser_payload.get("durationSeconds", browser_payload.get("duration_seconds", 0)) or 0
                    )
                    browser_view_count = int(
                        browser_payload.get("viewCount", browser_payload.get("view_count", 0)) or 0
                    )
                    if platform == "bilibili" and bilibili_suspicious_metadata:
                        duration_seconds = browser_duration_seconds or duration_seconds
                        view_count = browser_view_count or view_count
                    else:
                        duration_seconds = max(duration_seconds, browser_duration_seconds)
                        view_count = max(view_count, browser_view_count)
                except (BrowserFetchUnavailable, RuntimeError, TimeoutError) as error:
                    self._warn(f"{normalize_host(url)} 浏览器回退失败：{error}")
                    self._record_access_event(
                        url=url,
                        operation="browser-fallback",
                        ok=False,
                        duration_ms=(time.perf_counter() - browser_started) * 1000,
                        source_kind=source_kind,
                        source_label=source_label,
                        error=str(error),
                    )

        summary_text = " ".join(part for part in [title, description] if part)
        combined = " ".join(part for part in [summary_text, body_text] if part)
        bilibili_field_year_guard = platform == "bilibili" and bilibili_metadata_needs_browser_fallback(
            title=title,
            description=description,
            body_text=body_text,
            duration_seconds=duration_seconds,
            draft=draft,
        )
        score_inputs = dedupe_text([summary_text, combined])
        match_score = max(
            (
                score_recording_match(
                    candidate_text,
                    url,
                    draft,
                    duration_seconds=duration_seconds,
                    uploader=uploader,
                )
                for candidate_text in score_inputs
            ),
            default=0.0,
        )
        source_images = []
        if image_url:
            source_images.append(
                {
                    "src": image_url,
                    "title": title or draft.title,
                    "sourceUrl": url,
                    "sourceKind": source_kind,
                    "attribution": source_label,
                }
            )

        return {
            "url": canonical_url,
            "source_label": source_label,
            "source_kind": source_kind,
            "title": title,
            "description": description,
            "platform": platform,
            "weight": 0.9 if source_kind in {"existing-link", "high-quality"} else 0.68,
            "same_recording_score": match_score,
            "duration_seconds": duration_seconds,
            "uploader": uploader,
            "view_count": view_count,
            "fields": {
                "performanceDateText": (
                    compact(draft.performance_date_text)
                    if bilibili_field_year_guard
                    else (
                        extract_performance_date_text(summary_text)
                        or extract_performance_date_text(combined)
                        or draft.performance_date_text
                    )
                ),
                "venueText": extract_venue(combined),
                "albumTitle": title if title and not title.startswith("http") else "",
                "label": extract_label(summary_text or combined),
                "releaseDate": extract_release_date(summary_text or combined),
            },
            "images": source_images,
        }

    async def _fetch_text(
        self,
        url: str,
        *,
        operation: str = "fetch-page",
        query: str = "",
        source_kind: str = "",
        source_label: str = "",
        timeout_seconds: float | None = None,
    ) -> str:
        normalized_url = compact(url)
        with self._state_lock:
            cached = self._text_cache.get(normalized_url)
        if cached is not None:
            if operation == "fetch-page":
                self._record_access_event(
                    url=url,
                    operation=operation,
                    ok=True,
                    duration_ms=0.0,
                    source_kind=source_kind,
                    source_label=source_label,
                    query=query,
                    result_count=0,
                    cache_hit=True,
                    timeout_seconds=timeout_seconds,
                )
            return cached
        client = self._get_http_client()
        started = time.perf_counter()
        response = await client.get(url, timeout=timeout_seconds, headers=self._request_headers_for_url(url))
        response.raise_for_status()
        with self._state_lock:
            self._text_cache[normalized_url] = response.text
        if operation == "fetch-page":
            self._record_access_event(
                url=url,
                operation=operation,
                ok=True,
                duration_ms=(time.perf_counter() - started) * 1000,
                source_kind=source_kind,
                source_label=source_label,
                query=query,
                status_code=response.status_code,
                timeout_seconds=timeout_seconds,
            )
        return response.text


def metadata_is_insufficient(title: str, description: str, body_text: str) -> bool:
    return not compact(title) and not compact(description) and len(compact(body_text)) < 80


def bilibili_metadata_needs_browser_fallback(
    *,
    title: str,
    description: str,
    body_text: str,
    duration_seconds: int,
    draft: DraftRecordingEntry,
) -> bool:
    if duration_seconds < 21600:
        return False
    normalized_context = normalize_text(" ".join(part for part in [title, description, body_text] if compact(part)))
    work_aliases = {
        normalize_text(value)
        for value in [
            draft.work_title,
            draft.work_title_latin,
            draft.catalogue,
            *build_work_aliases(draft),
        ]
        if compact(value)
    }
    if any(alias and alias in normalized_context for alias in work_aliases):
        return False
    return len(normalize_text(description)) <= 20


def should_disable_platform_api(error: Exception) -> bool:
    if isinstance(error, httpx.HTTPStatusError):
        status_code = error.response.status_code
        body = ""
        try:
            body = error.response.text.lower()
        except Exception:
            body = ""
        if status_code in {401, 403, 412, 429}:
            return True
        if "quota" in body or "rate" in body:
            return True
    message = compact(error).lower()
    return any(token in message for token in ("quota", "rate limit", "403", "412", "429"))


def normalize_host(value: str) -> str:
    return compact(value).replace("https://", "").replace("http://", "").strip("/")


def is_bilibili_host(value: str) -> bool:
    normalized = normalize_host(value).lower()
    return "bilibili.com" in normalized or "b23.tv" in normalized


def canonicalize_bilibili_video_url(url: str, bvid: str = "") -> str:
    normalized_url = compact(url)
    normalized_bvid = compact(bvid)
    host = urlparse(normalized_url).netloc.lower()
    if "bilibili.com" not in host or not normalized_bvid:
        return normalized_url
    return f"https://www.bilibili.com/video/{normalized_bvid}/"


def streaming_host_priority(value: str) -> tuple[int, str]:
    normalized = normalize_host(value).lower()
    if "youtube.com" in normalized or "youtu.be" in normalized:
        return (0, f"1-{normalized}")
    if "bilibili.com" in normalized or "b23.tv" in normalized:
        return (0, f"0-{normalized}")
    if "apple.com" in normalized:
        return (0, f"2-{normalized}")
    return (1, normalized)


def dedupe_streaming_hosts_for_execution(hosts: list[SourceProfileEntry]) -> list[SourceProfileEntry]:
    deduped: list[SourceProfileEntry] = []
    seen: set[str] = set()
    for host in hosts:
        normalized = normalize_host(host.url).lower()
        key = "apple_music" if "apple.com" in normalized else normalized
        if key in seen:
            continue
        seen.add(key)
        deduped.append(host)
    return deduped


def merge_streaming_host_rows(
    host_results: list[tuple[SourceProfileEntry, list[dict[str, str]]]],
) -> list[dict[str, str]]:
    coverage_rows: list[dict[str, str]] = []
    all_rows: list[dict[str, str]] = []
    priority_hosts = {host.url for host, _ in host_results if streaming_host_priority(host.url)[0] == 0}
    for host, rows in host_results:
        if host.url in priority_hosts:
            coverage_rows.extend(rows[:2])
    coverage_rows = dedupe_rows(coverage_rows)

    multiple_hosts = len(host_results) > 1
    for host, rows in host_results:
        normalized_host = normalize_host(host.url)
        if not multiple_hosts:
            per_host_cap = HYDRATE_DEPTH
        elif "youtube.com" in normalized_host or "youtu.be" in normalized_host:
            per_host_cap = 10
        elif "bilibili.com" in normalized_host or "b23.tv" in normalized_host:
            per_host_cap = 10
        else:
            per_host_cap = 4
        all_rows.extend(rows[:per_host_cap])
    return dedupe_rows([*coverage_rows, *all_rows])


def merge_bilibili_search_rows(
    api_rows: list[dict[str, str]],
    browser_rows: list[dict[str, str]],
    engine_rows: list[dict[str, str]],
    *,
    parser_mismatch: bool = False,
) -> list[dict[str, str]]:
    if parser_mismatch:
        trusted_urls = {
            compact(row.get("url")).rstrip("/").lower()
            for row in [*browser_rows, *engine_rows]
            if compact(row.get("url"))
        }
        api_rows = [
            row
            for row in api_rows
            if compact(row.get("url")).rstrip("/").lower() in trusted_urls
        ]
    return dedupe_rows([*browser_rows, *api_rows, *engine_rows])[:HYDRATE_DEPTH]


def rrf_consensus_query_rows(
    query_rows_list: list[list[dict[str, str]]],
    *,
    result_depth: int,
    query_weights: list[float] | None = None,
    rank_constant: int = 10,
    rank_window: int | None = None,
) -> list[dict[str, str]]:
    score_by_key: dict[str, float] = {}
    occurrence_by_key: dict[str, int] = {}
    best_rank_by_key: dict[str, int] = {}
    first_seen_by_key: dict[str, tuple[int, int]] = {}
    representative_by_key: dict[str, dict[str, str]] = {}
    window = rank_window if rank_window is not None else max(result_depth * 4, 8)

    for query_index, rows in enumerate(query_rows_list):
        query_seen: set[str] = set()
        weight = 1.0
        if query_weights and query_index < len(query_weights):
            weight = query_weights[query_index]
        for rank, row in enumerate(rows[:window]):
            url = compact(row.get("url"))
            if not url:
                continue
            key = url.lower()
            representative_by_key.setdefault(key, row)
            score_by_key[key] = score_by_key.get(key, 0.0) + (weight / (rank_constant + rank + 1))
            if key not in query_seen:
                occurrence_by_key[key] = occurrence_by_key.get(key, 0) + 1
                query_seen.add(key)
            best_rank_by_key[key] = min(best_rank_by_key.get(key, rank), rank)
            first_seen_by_key.setdefault(key, (query_index, rank))

    consensus_keys = [key for key, count in occurrence_by_key.items() if count >= 2]
    ordered_keys = sorted(
        consensus_keys,
        key=lambda key: (
            -occurrence_by_key[key],
            -score_by_key[key],
            best_rank_by_key[key],
            first_seen_by_key[key][0],
            first_seen_by_key[key][1],
        ),
    )
    return [representative_by_key[key] for key in ordered_keys[:result_depth]]


def merge_bilibili_browser_query_rows(
    query_rows_list: list[list[dict[str, str]]],
    *,
    queries: list[str] | None = None,
    result_depth: int,
) -> list[dict[str, str]]:
    if queries and len(queries) == len(query_rows_list):
        bundle_priority_rows: list[dict[str, str]] = []
        prioritized_rows: list[dict[str, str]] = []
        rescue_rows: list[dict[str, str]] = []
        coverage_rows: list[dict[str, str]] = []
        all_rows: list[dict[str, str]] = []
        focused_indices = sorted(
            range(len(queries)),
            key=lambda index: (bilibili_query_focus_rank(queries[index]), index),
        )
        query_weights = [1.0] * len(query_rows_list)
        for bonus, index in zip((0.35, 0.2, 0.1), focused_indices[:3]):
            query_weights[index] += bonus
        deep_indices = focused_indices[:2]
        medium_indices = focused_indices[2:3]
        consensus_rows = rrf_consensus_query_rows(
            query_rows_list,
            result_depth=result_depth,
            query_weights=query_weights,
        )
        for index, rows in enumerate(query_rows_list):
            coverage_rows.extend(rows[:1])
            if len(query_rows_list) >= 5 and index < 2:
                coverage_rows.extend(rows[1:2])
        seen_urls = {
            compact(row.get("url")).lower()
            for row in [*consensus_rows, *coverage_rows]
            if compact(row.get("url"))
        }
        for index in focused_indices:
            duplicate_prefix = 0
            for row in query_rows_list[index][1:4]:
                url = compact(row.get("url")).lower()
                if not url:
                    continue
                if url in seen_urls:
                    duplicate_prefix += 1
                    continue
                if duplicate_prefix > 0:
                    rescue_rows.append(row)
                    seen_urls.add(url)
                break
        prioritized_groups: list[list[dict[str, str]]] = []
        bundle_indices = [
            index for index, query in enumerate(queries) if bilibili_query_is_chamber_duo_bundle_anchor(query)
        ]
        for index in bundle_indices[:1]:
            bundle_priority_rows.extend(query_rows_list[index][:5])
        for rank, index in enumerate(deep_indices):
            prioritized_groups.append(query_rows_list[index][1 : max(2, result_depth - (3 * (rank + 1)) + 1)])
        for index in medium_indices:
            prioritized_groups.append(query_rows_list[index][1:3])
        if prioritized_groups:
            max_group_len = max(len(group) for group in prioritized_groups)
            for offset in range(max_group_len):
                for group in prioritized_groups:
                    if offset < len(group):
                        prioritized_rows.append(group[offset])
        for index, rows in enumerate(query_rows_list):
            all_rows.extend(rows)
        return dedupe_rows([*consensus_rows, *bundle_priority_rows, *coverage_rows, *rescue_rows, *prioritized_rows, *all_rows])[:result_depth]

    prioritized_rows: list[dict[str, str]] = []
    coverage_rows: list[dict[str, str]] = []
    all_rows: list[dict[str, str]] = []
    for index, rows in enumerate(query_rows_list):
        if index == 0:
            prioritized_rows.extend(rows[: max(1, result_depth - 3)])
        elif index == 1:
            prioritized_rows.extend(rows[: max(1, result_depth - 6)])
        else:
            coverage_rows.extend(rows[:1])
        all_rows.extend(rows)
    return dedupe_rows([*prioritized_rows, *coverage_rows, *all_rows])[:result_depth]


def merge_streaming_query_groups(
    groups: list[list[dict[str, str]]],
    *,
    limit: int,
    coverage_per_query: int = 2,
) -> list[dict[str, str]]:
    coverage_rows: list[dict[str, str]] = []
    all_rows: list[dict[str, str]] = []
    for group in groups:
        coverage_rows.extend(group[:coverage_per_query])
        all_rows.extend(group)
    return dedupe_rows([*coverage_rows, *all_rows])[:limit]


def should_merge_streaming_query_coverage(source_label: str) -> bool:
    lowered = compact(source_label).lower()
    return "youtube" in lowered


def bilibili_query_specificity(query: str) -> tuple[int, int, int]:
    normalized = compact(query)
    return (
        sum(character.isdigit() for character in normalized),
        len(normalized.split()),
        len(normalized),
    )


def bilibili_queries_have_chamber_duo_signal(queries: list[str]) -> bool:
    chamber_markers = (
        "spring",
        "appassionata",
        "sonata",
        "violin sonata",
        "piano sonata",
        "sonatas",
        "\u6625\u5929",
        "\u70ed\u60c5",
        "\u594f\u9e23\u66f2",
        "\u5c0f\u63d0\u7434\u594f\u9e23\u66f2",
        "\u94a2\u7434\u594f\u9e23\u66f2",
    )
    normalized_queries = [compact(query) for query in queries if compact(query)]
    if not normalized_queries:
        return False
    has_chamber_marker = any(
        any(marker in query.lower() or marker in query for marker in chamber_markers)
        for query in normalized_queries
    )
    if not has_chamber_marker:
        return False
    return any(bilibili_query_has_collaboration_signal(query) for query in normalized_queries)


def build_bilibili_chamber_duo_trailer_probe_queries(queries: list[str]) -> list[str]:
    probe_queries: list[str] = []
    bundle_markers = (
        "sonatas",
        "quartets",
        "trios",
        "sonata collection",
        "quartet collection",
        "trio collection",
    )
    for query in dedupe_text(queries):
        normalized = compact(query)
        if not normalized or contains_cjk(normalized) or "/" in normalized:
            continue
        lowered = normalized.lower()
        if "trailer" in lowered:
            continue
        if "complete" not in lowered or not any(marker in lowered for marker in bundle_markers):
            continue
        if not bilibili_query_has_collaboration_signal(normalized):
            continue
        year_stripped = compact(re.sub(r"\b(?:18|19|20)\d{2}\b", "", normalized))
        complete_offset = year_stripped.lower().find("complete ")
        focused_query = year_stripped[complete_offset:] if complete_offset >= 0 else year_stripped
        focused_query = compact(re.sub(r"\s+", " ", focused_query))
        if not focused_query:
            continue
        probe_queries.append(f"{focused_query} trailer")
    return dedupe_text(probe_queries)


def bilibili_query_is_chamber_solo_opus_probe(query: str) -> bool:
    normalized = compact(query)
    if not normalized or "/" in normalized:
        return False
    lowered = normalized.lower()
    has_named_marker = any(marker in lowered or marker in normalized for marker in ("appassionata", "spring", "热情", "春天"))
    has_opus_marker = bool(re.search(r"\bop\.?\s*(?:24|57)\b", lowered, re.I)) or "op24" in lowered or "op57" in lowered
    return has_named_marker or has_opus_marker


def build_bilibili_chamber_solo_opus_probe_queries(queries: list[str]) -> list[str]:
    probe_queries = [
        query
        for query in dedupe_text(queries)
        if bilibili_query_is_chamber_solo_opus_probe(query)
    ]
    ordered = sorted(
        probe_queries,
        key=lambda query: (
            0 if not re.search(r"[\u4e00-\u9fff]", query) else 1,
            0 if re.search(r"\bop\.?\s*(?:24|57)\b", compact(query), re.I) else 1,
            bilibili_query_focus_rank(query),
            len(compact(query)),
        ),
    )
    return dedupe_text(ordered)[:2]


def bilibili_chamber_duo_person_tokens(queries: list[str]) -> list[str]:
    normalized_queries = [compact(query) for query in queries if compact(query)]
    if not normalized_queries:
        return []
    stop_tokens = {
        "beethoven",
        "spring",
        "appassionata",
        "sonata",
        "sonatas",
        "violin",
        "piano",
        "ludwig",
        "van",
        "no",
        "op",
        "major",
        "minor",
        "complete",
        "collection",
    }
    counts: dict[str, int] = {}
    for query in normalized_queries:
        for token in re.findall(r"[A-Za-z]{3,}", query.lower()):
            if token in stop_tokens:
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked_tokens = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    selected = [token for token, count in ranked_tokens if count >= 2][:4]
    return selected


def bilibili_rows_have_chamber_duo_person_match(
    rows: list[dict[str, Any]],
    *,
    person_tokens: list[str],
) -> bool:
    if len(person_tokens) < 2:
        return False
    for row in rows:
        fields = row.get("fields") or {}
        haystack = normalize_text(
            " ".join(
                [
                    compact(row.get("title")),
                    compact(row.get("description")),
                    compact(fields.get("albumTitle")),
                    compact(fields.get("notes")),
                ]
            )
        )
        if not haystack:
            continue
        if sum(1 for token in person_tokens if token in haystack) >= 2:
            return True
    return False


def should_force_chamber_duo_second_pass_for_bilibili(
    *,
    queries: list[str],
    api_rows: list[dict[str, Any]],
    browser_rows: list[dict[str, Any]],
) -> bool:
    if not bilibili_queries_have_chamber_duo_signal(queries):
        return False
    person_tokens = bilibili_chamber_duo_person_tokens(queries)
    if len(person_tokens) < 2:
        return False
    return not bilibili_rows_have_chamber_duo_person_match(
        [*api_rows, *browser_rows],
        person_tokens=person_tokens,
    )


def bilibili_chamber_solo_person_tokens(queries: list[str]) -> list[str]:
    normalized_queries = [compact(query) for query in queries if compact(query)]
    if not normalized_queries:
        return []
    stop_tokens = {
        "beethoven",
        "spring",
        "appassionata",
        "sonata",
        "sonatas",
        "violin",
        "piano",
        "ludwig",
        "van",
        "no",
        "op",
        "major",
        "minor",
        "complete",
        "collection",
    }
    counts: dict[str, int] = {}
    cjk_stop_markers = (
        "热情",
        "春天",
        "奏鸣曲",
        "钢琴",
        "小提琴",
        "贝多芬",
        "路德维希",
        "第二十三",
        "第五",
        "op",
    )
    cjk_stop_full_tokens = {
        "贝多芬",
        "路德维希",
        "凡贝多芬",
    }
    for query in normalized_queries:
        for token in re.findall(r"[A-Za-z]{3,}", query.lower()):
            if token in stop_tokens:
                continue
            counts[token] = counts.get(token, 0) + 1
        for chunk in re.findall(r"[\u4e00-\u9fff·]{2,}", query):
            normalized_chunk = compact(chunk.replace("·", ""))
            if (
                not normalized_chunk
                or len(normalized_chunk) < 3
                or normalized_chunk in cjk_stop_full_tokens
                or any(marker in normalized_chunk for marker in cjk_stop_markers)
            ):
                continue
            counts[normalized_chunk] = counts.get(normalized_chunk, 0) + 1
        cjk_keyword = compact(extract_cjk_person_query_keyword(query))
        if (
            cjk_keyword
            and len(cjk_keyword) >= 3
            and cjk_keyword not in cjk_stop_full_tokens
            and not any(marker in cjk_keyword for marker in cjk_stop_markers)
        ):
            counts[cjk_keyword] = counts.get(cjk_keyword, 0) + 1
    ranked_tokens = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    selected = [
        token
        for token, count in ranked_tokens
        if count >= 2 or (contains_cjk(token) and count >= 1)
    ][:3]
    return selected


def bilibili_rows_have_chamber_solo_named_work_match(
    rows: list[dict[str, Any]],
    *,
    person_tokens: list[str],
) -> bool:
    if not person_tokens:
        return False
    work_markers = (
        "appassionata",
        "spring",
        "op57",
        "op.57",
        "op 57",
        "op24",
        "op.24",
        "op 24",
        "热情",
        "春天",
    )
    for row in rows:
        fields = row.get("fields") or {}
        haystack = normalize_text(
            " ".join(
                [
                    compact(row.get("title")),
                    compact(row.get("description")),
                    compact(fields.get("albumTitle")),
                    compact(fields.get("notes")),
                ]
            )
        )
        if not haystack:
            continue
        has_person = any(token in haystack for token in person_tokens)
        has_work = any(marker in haystack for marker in work_markers)
        if has_person and has_work:
            return True
    return False


def should_force_chamber_solo_second_pass_for_bilibili(
    *,
    queries: list[str],
    api_rows: list[dict[str, Any]],
    browser_rows: list[dict[str, Any]],
) -> bool:
    if not any(bilibili_query_is_chamber_solo_opus_probe(query) for query in queries):
        return False
    person_tokens = bilibili_chamber_solo_person_tokens(queries)
    if not person_tokens:
        return False
    return not bilibili_rows_have_chamber_solo_named_work_match(
        [*api_rows, *browser_rows],
        person_tokens=person_tokens,
    )


def bilibili_row_has_chamber_duo_bundle_signal(
    row: dict[str, Any],
    *,
    person_tokens: list[str],
) -> bool:
    if len(person_tokens) < 2:
        return False
    fields = row.get("fields") or {}
    haystack = normalize_text(
        " ".join(
            [
                compact(row.get("title")),
                compact(row.get("description")),
                compact(fields.get("albumTitle")),
                compact(fields.get("notes")),
            ]
        )
    )
    if not haystack:
        return False
    has_complete_marker = "complete" in haystack or "全集" in haystack
    has_bundle_marker = any(
        marker in haystack
        for marker in (
            "sonata",
            "sonatas",
            "奏鸣曲",
            "quartet",
            "quartets",
            "四重奏",
            "trio",
            "trios",
            "三重奏",
        )
    )
    if not has_complete_marker or not has_bundle_marker:
        return False
    return sum(1 for token in person_tokens if token in haystack) >= 2


def select_bilibili_chamber_duo_bundle_probe_rows(
    rows: list[dict[str, Any]],
    *,
    queries: list[str],
    limit: int = 1,
) -> list[dict[str, Any]]:
    if limit <= 0 or not rows:
        return []
    if not bilibili_queries_have_chamber_duo_signal(queries):
        return []
    person_tokens = bilibili_chamber_duo_person_tokens(queries)
    if len(person_tokens) < 2:
        return []
    matches = [
        row
        for row in rows
        if bilibili_row_has_chamber_duo_bundle_signal(
            row,
            person_tokens=person_tokens,
        )
    ]
    if not matches:
        return []
    ordered = sorted(
        matches,
        key=lambda row: (
            0 if "bilibili.com/video/BV" in compact(row.get("url")) else 1,
            0 if "complete" in normalize_text(compact(row.get("title"))) else 1,
            len(compact(row.get("title"))),
        ),
    )
    return dedupe_rows(ordered)[:limit]


def select_bilibili_browser_queries(queries: list[str], *, max_queries: int = 6) -> list[str]:
    candidates = dedupe_text([compact(query) for query in queries if compact(query)])
    if len(candidates) <= max_queries:
        return candidates
    if max_queries <= 0:
        return []

    head_count = min(2, len(candidates), max_queries)
    selected_indices: set[int] = set(range(head_count))

    tail_capacity = max_queries - len(selected_indices)
    tail_count = min(1, tail_capacity, max(0, len(candidates) - head_count))
    if tail_count:
        selected_indices.update(range(len(candidates) - tail_count, len(candidates)))

    remaining_slots = max_queries - len(selected_indices)
    if remaining_slots > 0:
        middle_indices = [index for index in range(head_count, len(candidates) - tail_count) if index not in selected_indices]
        if middle_indices:
            focused_middle = min(
                middle_indices,
                key=lambda index: (bilibili_query_focus_rank(candidates[index]), index),
            )
            selected_indices.add(focused_middle)
        remaining_slots = max_queries - len(selected_indices)
        if remaining_slots > 0:
            middle_indices = [
                index for index in range(head_count, len(candidates) - tail_count) if index not in selected_indices
            ]
            collaboration_candidates = [
                index for index in middle_indices if bilibili_query_has_collaboration_signal(candidates[index])
            ]
            if collaboration_candidates:
                collaboration_middle = min(
                    collaboration_candidates,
                    key=lambda index: (bilibili_query_collaboration_rank(candidates[index]), index),
                )
                selected_indices.add(collaboration_middle)
        remaining_slots = max_queries - len(selected_indices)
        if remaining_slots > 0:
            middle_indices = [
                index for index in range(head_count, len(candidates) - tail_count) if index not in selected_indices
            ]
            ranked_middle = sorted(
                middle_indices,
                key=lambda index: (bilibili_query_context_rank(candidates[index]), -index),
                reverse=True,
            )
            selected_indices.update(ranked_middle[:remaining_slots])

    return [candidates[index] for index in sorted(selected_indices)]


def prepare_bilibili_browser_queries(queries: list[str], *, max_queries: int = 3) -> list[str]:
    seeded = select_bilibili_browser_queries(queries, max_queries=max(max_queries + 2, 6))
    pool = dedupe_text([*seeded, *queries])
    ranked = sorted(pool, key=bilibili_browser_runtime_rank)
    selected = list(ranked[:max_queries])
    if not any(bilibili_query_is_primary_work_rescue(query) for query in selected):
        rescue_pool = [query for query in pool if bilibili_query_is_primary_work_rescue(query)]
        if rescue_pool:
            rescue_query = min(dedupe_text(rescue_pool), key=bilibili_primary_work_rescue_rank)
            rescue_is_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", compact(rescue_query).lower()))
            if rescue_is_decade_bucket and any(bilibili_query_is_primary_year_anchor(query) for query in selected):
                if max_queries < 4 or not selected:
                    return selected[:max_queries]
                replace_index = len(selected) - 1
            else:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                    ),
                    len(selected) - 1,
                )
            if rescue_query not in selected:
                selected[replace_index] = rescue_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    primary_year_anchor_pool = [query for query in pool if bilibili_query_is_primary_year_anchor(query)]
    if primary_year_anchor_pool:
        best_primary_year_anchor = min(
            dedupe_text(primary_year_anchor_pool),
            key=lambda query: (0 if contains_cjk(query) else 1, len(compact(query).split()), len(compact(query)), query),
        )
        if best_primary_year_anchor not in selected:
            protected_collaboration_query = ""
            collaboration_selected = [query for query in selected if bilibili_query_has_collaboration_signal(query)]
            if collaboration_selected:
                protected_collaboration_query = min(collaboration_selected, key=bilibili_query_collaboration_rank)
            replace_index = next(
                (
                    index
                    for index in range(len(selected) - 1, -1, -1)
                    if not bilibili_query_is_primary_work_rescue(selected[index])
                    and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                    and selected[index] != protected_collaboration_query
                ),
                len(selected) - 1,
            )
            selected[replace_index] = best_primary_year_anchor
            selected = dedupe_text(selected)
            if len(selected) < max_queries:
                for query in ranked:
                    if query in selected:
                        continue
                    selected.append(query)
                    if len(selected) >= max_queries:
                        break
    if max_queries >= 4 and not any(bilibili_query_is_generic_plural_bundle_rescue(query) for query in selected):
        bundle_pool = [query for query in pool if bilibili_query_is_generic_plural_bundle_rescue(query)]
        if bundle_pool:
            bundle_query = min(
                dedupe_text(bundle_pool),
                key=lambda query: bilibili_generic_plural_bundle_rescue_rank(query),
            )
            if bundle_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                    ),
                    next(
                        (
                            index
                            for index in range(len(selected) - 1, -1, -1)
                            if bilibili_query_is_primary_year_anchor(selected[index])
                            and not bilibili_query_is_primary_work_rescue(selected[index])
                        ),
                        next(
                            (
                                index
                                for index in range(len(selected) - 1, -1, -1)
                                if bilibili_query_is_primary_year_anchor(selected[index])
                            ),
                            next(
                                (
                                    index
                                    for index in range(len(selected) - 1, -1, -1)
                                    if not bilibili_query_is_primary_work_rescue(selected[index])
                                ),
                                len(selected) - 1,
                            ),
                        ),
                    ),
                )
                selected[replace_index] = bundle_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4:
        year_collaboration_pool = [query for query in pool if bilibili_query_is_year_collaboration_rescue(query)]
        if year_collaboration_pool:
            year_collaboration_query = min(
                dedupe_text(year_collaboration_pool),
                key=bilibili_year_collaboration_rescue_rank,
            )
            if year_collaboration_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = year_collaboration_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_exact_collaboration_anchor(query) for query in selected):
        collaboration_pool = [query for query in pool if bilibili_query_is_exact_collaboration_anchor(query)]
        if collaboration_pool:
            collaboration_query = min(
                dedupe_text(collaboration_pool),
                key=bilibili_exact_collaboration_anchor_rank,
            )
            if collaboration_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_has_collaboration_signal(selected[index])
                        and not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                    ),
                    next(
                        (
                            index
                            for index in range(len(selected) - 1, -1, -1)
                            if not bilibili_query_has_collaboration_signal(selected[index])
                            and not bilibili_query_is_primary_work_rescue(selected[index])
                            and not bilibili_query_is_primary_year_anchor(selected[index])
                        ),
                        len(selected) - 1,
                    ),
                )
                selected[replace_index] = collaboration_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_mixed_compact_duo_anchor(query) for query in selected):
        mixed_compact_pool = [query for query in pool if bilibili_query_is_mixed_compact_duo_anchor(query)]
        if mixed_compact_pool:
            mixed_compact_query = min(
                dedupe_text(mixed_compact_pool),
                key=lambda query: (bilibili_query_focus_rank(query), len(compact(query)), query),
            )
            if mixed_compact_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = mixed_compact_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_chamber_duo_bundle_anchor(query) for query in selected):
        chamber_bundle_pool = [query for query in pool if bilibili_query_is_chamber_duo_bundle_anchor(query)]
        if chamber_bundle_pool:
            chamber_bundle_query = min(
                dedupe_text(chamber_bundle_pool),
                key=lambda query: (
                    chamber_duo_bundle_query_rank(query),
                    bilibili_query_focus_rank(query),
                    len(compact(query)),
                    query,
                ),
            )
            if chamber_bundle_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                        and not bilibili_query_is_mixed_compact_duo_anchor(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = chamber_bundle_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_chamber_named_work_surname_rescue(query) for query in selected):
        chamber_named_work_pool = [query for query in pool if bilibili_query_is_chamber_named_work_surname_rescue(query)]
        if chamber_named_work_pool:
            chamber_named_work_query = min(
                dedupe_text(chamber_named_work_pool),
                key=lambda query: (bilibili_query_focus_rank(query), len(compact(query)), query),
            )
            if chamber_named_work_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                        and not bilibili_query_is_mixed_compact_duo_anchor(selected[index])
                        and not bilibili_query_is_chamber_duo_bundle_anchor(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = chamber_named_work_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_chamber_named_work_full_name_year_rescue(query) for query in selected):
        chamber_full_name_year_pool = [
            query for query in pool if bilibili_query_is_chamber_named_work_full_name_year_rescue(query)
        ]
        if chamber_full_name_year_pool:
            chamber_full_name_year_query = min(
                dedupe_text(chamber_full_name_year_pool),
                key=lambda query: (bilibili_query_focus_rank(query), len(compact(query)), query),
            )
            if chamber_full_name_year_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                        and not bilibili_query_is_mixed_compact_duo_anchor(selected[index])
                        and not bilibili_query_is_chamber_duo_bundle_anchor(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = chamber_full_name_year_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_chamber_named_work_solo_cjk_rescue(query) for query in selected):
        chamber_solo_cjk_pool = [query for query in pool if bilibili_query_is_chamber_named_work_solo_cjk_rescue(query)]
        if chamber_solo_cjk_pool:
            chamber_solo_cjk_query = min(
                dedupe_text(chamber_solo_cjk_pool),
                key=chamber_named_work_solo_cjk_rescue_rank,
            )
            if chamber_solo_cjk_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                        and not bilibili_query_is_mixed_compact_duo_anchor(selected[index])
                        and not bilibili_query_is_chamber_duo_bundle_anchor(selected[index])
                        and not bilibili_query_is_chamber_named_work_surname_rescue(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = chamber_solo_cjk_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    if max_queries >= 4 and not any(bilibili_query_is_cjk_orchestral_context_anchor(query) for query in selected):
        orchestral_context_pool = [query for query in pool if bilibili_query_is_cjk_orchestral_context_anchor(query)]
        if orchestral_context_pool:
            orchestral_context_query = min(
                dedupe_text(orchestral_context_pool),
                key=bilibili_cjk_orchestral_context_rank,
            )
            if orchestral_context_query not in selected:
                replace_index = next(
                    (
                        index
                        for index in range(len(selected) - 1, -1, -1)
                        if not bilibili_query_is_primary_work_rescue(selected[index])
                        and not bilibili_query_is_primary_year_anchor(selected[index])
                        and not bilibili_query_is_generic_plural_bundle_rescue(selected[index])
                        and not bilibili_query_is_exact_collaboration_anchor(selected[index])
                        and not bilibili_query_is_mixed_compact_duo_anchor(selected[index])
                        and not bilibili_query_is_chamber_duo_bundle_anchor(selected[index])
                    ),
                    len(selected) - 1,
                )
                selected[replace_index] = orchestral_context_query
                selected = dedupe_text(selected)
                if len(selected) < max_queries:
                    for query in ranked:
                        if query in selected:
                            continue
                        selected.append(query)
                        if len(selected) >= max_queries:
                            break
    return selected[:max_queries]


def bilibili_query_is_primary_work_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized:
        return False
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "\u4e50\u56e2",
        "\u7231\u4e50",
        "\u4ea4\u54cd",
    )
    work_markers = (
        "concerto",
        "concertos",
        "klavierkonzert",
        "\u94a2\u534f",
        "\u534f\u594f\u66f2",
    )
    has_work_marker = any(marker in lowered or marker in normalized for marker in work_markers)
    if not has_work_marker:
        return False
    has_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", lowered))
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_ensemble = any(marker in lowered or marker in normalized for marker in ensemble_markers)
    has_explicit_duo = "/" in normalized or " - " in normalized
    has_cjk_short_work = "\u94a2\u534f" in normalized or "\u534f\u594f\u66f2" in normalized
    if has_decade_bucket and not has_explicit_duo and not has_ensemble:
        return True
    return has_year and not has_explicit_duo and not has_ensemble and (token_count <= 4 or has_cjk_short_work)


def bilibili_primary_work_rescue_rank(query: str) -> tuple[int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    has_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", lowered))
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_cjk_short_work = "\u94a2\u534f" in normalized or "\u534f\u594f\u66f2" in normalized
    return (
        0 if has_cjk_short_work and has_year else 1 if has_decade_bucket else 2 if has_year else 3,
        token_count,
        len(normalized),
        0 if "/" not in normalized else 1,
    )


def bilibili_query_is_primary_year_anchor(query: str) -> bool:
    normalized = compact(query)
    if not normalized:
        return False
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", lowered))
    work_markers = (
        "concerto",
        "concertos",
        "klavierkonzert",
        "\u94a2\u534f",
        "\u534f\u594f\u66f2",
    )
    has_month = any(
        month in lowered
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    )
    has_work_marker = any(marker in lowered or marker in normalized for marker in work_markers)
    return has_year and not has_decade_bucket and not has_month and not has_work_marker and "/" not in normalized and token_count <= 4


def bilibili_query_is_generic_plural_bundle_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized:
        return False
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "\u4e50\u56e2",
        "\u7231\u4e50",
        "\u4ea4\u54cd",
    )
    if "concertos" not in lowered:
        return False
    if "/" in normalized or " - " in normalized:
        return False
    if any(marker in lowered or marker in normalized for marker in ensemble_markers):
        return False
    return token_count <= 4


def bilibili_generic_plural_bundle_rescue_rank(query: str) -> tuple[int, int, int]:
    normalized = compact(query)
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    return (
        0 if "piano concertos" in normalized.lower() else 1,
        token_count,
        len(normalized),
    )


def bilibili_query_is_exact_collaboration_anchor(query: str) -> bool:
    normalized = compact(query)
    if not normalized or not bilibili_query_has_collaboration_signal(normalized):
        return False
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    work_markers = (
        "concerto",
        "concertos",
        "klavierkonzert",
        "\u94a2\u534f",
        "\u534f\u594f\u66f2",
    )
    has_work_marker = any(marker in lowered or marker in normalized for marker in work_markers)
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", lowered))
    has_month = any(
        month in lowered
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    )
    latin_name_tokens = [
        token
        for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized)
        if len(token) > 1
        and token.lower()
        not in {
            "piano",
            "concerto",
            "concertos",
            "op",
            "robert",
            "schumann",
            "orchestra",
            "philharmonic",
            "symphony",
            "ensemble",
            "budapest",
            "bppo",
        }
    ]
    return (
        has_work_marker
        and not has_year
        and not has_decade_bucket
        and not has_month
        and token_count <= 12
        and len(latin_name_tokens) >= 4
    )


def bilibili_exact_collaboration_anchor_rank(query: str) -> tuple[int, int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "\u4e50\u56e2",
        "\u7231\u4e50",
        "\u4ea4\u54cd",
        "bppo",
    )
    return (
        0 if "op." in lowered else 1,
        0 if not any(marker in lowered or marker in normalized for marker in ensemble_markers) else 1,
        0 if "/" not in normalized else 1,
        len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized)),
        len(normalized),
    )


def bilibili_query_focus_rank(query: str) -> tuple[int, int, int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    latin_tokens = re.findall(r"[A-Za-z]{3,}", normalized)
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "乐团",
        "爱乐",
        "交响",
    )
    work_markers = (
        "concerto",
        "concertos",
        "klavierkonzert",
        "钢协",
        "协奏曲",
    )
    has_cjk_work_marker = any(marker in normalized for marker in ("钢协", "协奏曲"))
    has_work_marker = any(marker in lowered or marker in normalized for marker in work_markers)
    return (
        0 if has_cjk_work_marker and latin_tokens else 1 if has_work_marker else 2,
        0 if len(latin_tokens) >= 2 else 1,
        0 if any(character.isdigit() for character in normalized) else 1,
        0 if not any(marker in lowered for marker in ensemble_markers) else 1,
        len(normalized.split()),
        len(normalized),
    )


def bilibili_browser_runtime_rank(query: str) -> tuple[int, int, int, int, tuple[int, int, int, int, int, int], int]:
    normalized = compact(query)
    lowered = normalized.lower()
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    has_month = any(
        month in lowered
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    )
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_decade_bucket = bool(re.search(r"\b(?:18|19|20)\d0s\b", lowered))
    has_collaboration = bilibili_query_has_collaboration_signal(normalized)
    very_long = len(normalized) > 56 or token_count > 10
    longish = len(normalized) > 40 or token_count > 7
    has_runtime_heavy_context = has_month or (has_year and len(normalized) > 32)
    return (
        0 if has_year and has_collaboration else 1 if has_year else 2 if has_decade_bucket else 3,
        1 if very_long else 0,
        1 if longish else 0,
        1 if has_runtime_heavy_context else 0,
        1 if "/" in normalized else 0,
        bilibili_query_focus_rank(normalized),
        len(normalized),
    )


def bilibili_query_has_collaboration_signal(query: str) -> bool:
    normalized = compact(query)
    if "/" in normalized:
        return True
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    return len(latin_tokens) >= 3


def bilibili_query_is_mixed_compact_duo_anchor(query: str) -> bool:
    normalized = compact(query)
    if not normalized or not contains_cjk(normalized) or "/" in normalized:
        return False
    if any(character.isdigit() for character in normalized):
        return False
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    if not 2 <= len(latin_tokens) <= 3:
        return False
    lowered = normalized.lower()
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "涔愬洟",
        "鐖变箰",
        "浜ゅ搷",
    )
    chamber_markers = (
        "\u594f\u9e23\u66f2",
        "\u4e09\u91cd\u594f",
        "\u56db\u91cd\u594f",
        "\u6625\u5929",
        "\u70ed\u60c5",
        "sonata",
        "trio",
        "quartet",
        "spring",
        "appassionata",
    )
    return not any(marker in lowered for marker in ensemble_markers) and any(
        marker in lowered for marker in chamber_markers
    )


def bilibili_query_is_chamber_duo_bundle_anchor(query: str) -> bool:
    normalized = compact(query)
    if not normalized or contains_cjk(normalized) or "/" in normalized:
        return False
    lowered = normalized.lower()
    bundle_markers = (
        "violin sonatas",
        "piano sonatas",
        "sonatas",
        "violin sonata collection",
        "piano sonata collection",
        "sonata collection",
        "quartets",
        "quartet collection",
        "trios",
        "trio collection",
    )
    if not any(marker in lowered for marker in bundle_markers):
        return False
    # Keep chamber bundle anchors with an explicit release year (e.g. 1959),
    # while rejecting other numeric fragments (e.g. movement/opus numbers).
    without_years = re.sub(r"\b(?:18|19|20)\d{2}\b", "", lowered)
    if any(character.isdigit() for character in without_years):
        return False
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    return len(latin_tokens) >= 4


def bilibili_query_is_chamber_named_work_surname_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized or contains_cjk(normalized) or "/" in normalized:
        return False
    lowered = normalized.lower()
    if not any(alias in lowered for alias in ("appassionata", "spring")):
        return False
    if not any(composer in lowered for composer in ("beethoven",)):
        return False
    if any(marker in lowered for marker in ("concerto", "symphony", "quartet", "trio")):
        return False
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    return 3 <= len(latin_tokens) <= 4


def bilibili_query_is_chamber_named_work_full_name_year_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized or contains_cjk(normalized) or "/" in normalized:
        return False
    lowered = normalized.lower()
    if not any(alias in lowered for alias in ("appassionata", "spring")):
        return False
    if not re.search(r"\b(?:18|19|20)\d{2}\b", lowered):
        return False
    if any(marker in lowered for marker in ("concerto", "symphony", "quartet", "trio", "collection", "complete")):
        return False
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    return 3 <= len(latin_tokens) <= 5


def bilibili_query_is_chamber_named_work_solo_cjk_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized or not contains_cjk(normalized):
        return False
    if "/" in normalized:
        return False
    lowered = normalized.lower()
    if not any(marker in normalized for marker in ("钢琴奏鸣曲", "小提琴奏鸣曲", "op57", "op.57", "op24", "op.24")):
        return False
    if any(marker in lowered for marker in ("concerto", "symphony", "quartet", "trio")):
        return False
    return len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized)) >= 2


def bilibili_query_is_year_collaboration_rescue(query: str) -> bool:
    normalized = compact(query)
    if not normalized or not bilibili_query_has_collaboration_signal(normalized):
        return False
    lowered = normalized.lower()
    if not re.search(r"\b(?:18|19|20)\d{2}\b", lowered):
        return False
    if any(
        month in lowered
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    ):
        return False
    return any(
        marker in lowered or marker in normalized
        for marker in ("concerto", "concertos", "klavierkonzert", "钢协", "协奏曲")
    )


def bilibili_year_collaboration_rescue_rank(query: str) -> tuple[int, int, int, int]:
    normalized = compact(query)
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    long_latin_tokens = [token for token in latin_tokens if len(token) > 2 and not token.isupper()]
    acronym_tokens = [token for token in latin_tokens if token.isupper() and len(token) <= 3]
    return (
        0 if "/" not in normalized else 1,
        0 if not acronym_tokens else 1,
        0 if len(long_latin_tokens) >= 3 else 1,
        len(normalized),
    )


def bilibili_query_is_cjk_orchestral_context_anchor(query: str) -> bool:
    normalized = compact(query)
    if not normalized or "/" in normalized:
        return False
    lowered = normalized.lower()
    if not re.search(r"\b(?:18|19|20)\d{2}\b", lowered):
        return False
    if any(
        marker in lowered or marker in normalized
        for marker in (
            "concerto",
            "concertos",
            "klavierkonzert",
            "sonata",
            "quartet",
            "trio",
            "钢协",
            "协奏曲",
            "奏鸣曲",
            "四重奏",
            "三重奏",
            "海顿",
            "布鲁克纳",
            "贝多芬",
            "舒伯特",
            "舒曼",
        )
    ):
        return False
    if any(
        month in lowered
        for month in (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    ):
        return False
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    cjk_chunks = re.findall(r"[\u4e00-\u9fff]+", normalized)
    if token_count < 4 or token_count > 10:
        return False
    if len(cjk_chunks) < 2:
        return False
    return True


def bilibili_cjk_orchestral_context_rank(query: str) -> tuple[int, int, int, int]:
    normalized = compact(query)
    token_count = len(re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", normalized))
    cjk_chunk_count = len(re.findall(r"[\u4e00-\u9fff]+", normalized))
    return (
        0 if not re.search(r"[A-Za-z]", normalized) else 1,
        token_count,
        -cjk_chunk_count,
        len(normalized),
    )


def bilibili_query_collaboration_rank(query: str) -> tuple[int, int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    latin_tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if len(token) > 1]
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "乐团",
        "爱乐",
        "交响",
    )
    return (
        0 if any(character.isdigit() for character in normalized) else 1,
        0 if len(latin_tokens) >= 3 else 1,
        0 if "/" in normalized else 1,
        0 if not any(marker in lowered for marker in ensemble_markers) else 1,
        len(normalized),
    )


def bilibili_query_context_rank(query: str) -> tuple[int, int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    ensemble_markers = (
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "乐团",
        "爱乐",
        "交响",
    )
    return (
        1 if any(marker in lowered for marker in ensemble_markers) else 0,
        1 if "/" not in normalized else 0,
        *bilibili_query_specificity(normalized),
    )


def should_search_auxiliary_streaming_hosts(
    host_results: list[tuple[SourceProfileEntry, list[dict[str, str]]]],
) -> bool:
    non_empty = [(host, rows) for host, rows in host_results if rows]
    if len(non_empty) < 2:
        return True
    merged = merge_streaming_host_rows(non_empty)
    return len(merged) < 4


def should_probe_apple_auxiliary_hosts(hydrated_rows: list[dict[str, Any]]) -> bool:
    if not hydrated_rows:
        return True
    if any(compact(row.get("platform")) == "apple_music" for row in hydrated_rows):
        return False
    strong_rows = [
        row
        for row in hydrated_rows
        if float(row.get("same_recording_score", 0.0) or 0.0) >= LOW_CONFIDENCE_THRESHOLD
    ]
    if len(strong_rows) < 2:
        return True
    strong_platforms = {
        compact(row.get("platform")) or detect_platform(compact(row.get("url")))
        for row in strong_rows
        if compact(row.get("platform")) or compact(row.get("url"))
    }
    return len(strong_platforms) < 2


def should_expand_initial_streaming_window(
    host_results: list[tuple[SourceProfileEntry, list[dict[str, str]]]],
) -> bool:
    priority_non_empty = [
        (host, rows)
        for host, rows in host_results
        if rows and streaming_host_priority(host.url)[0] == 0
    ]
    if len(priority_non_empty) < 2:
        return False
    for host, rows in priority_non_empty:
        normalized_host = normalize_host(host.url)
        if "apple.com" in normalized_host:
            return True
        if ("bilibili.com" in normalized_host or "b23.tv" in normalized_host) and len(rows) >= 9:
            return True
    return False


def prioritize_platform_queries(values: list[str], *, draft: DraftRecordingEntry, prefer_cjk: bool) -> list[str]:
    catalogue = compact(draft.catalogue).lower()
    work_title = compact(draft.work_title_latin or draft.work_title).lower()
    composer = compact(draft.composer_name_latin or draft.composer_name).lower()
    lead_slots = build_lead_slots(draft)
    prefer_collaboration = has_sparse_collaboration_hint(draft, lead_slots)

    def sort_key(query: str) -> tuple[int, int, int, int, int]:
        lowered = compact(query).lower()
        collaboration_rank = 1
        if prefer_collaboration and count_query_lead_slot_hits(lowered, lead_slots) >= 2:
            collaboration_rank = 0
        return (
            collaboration_rank,
            0 if contains_cjk(lowered) == prefer_cjk else 1,
            0 if catalogue and catalogue in lowered else 1,
            0 if work_title and work_title in lowered else 1,
            0 if composer and composer in lowered else 1,
            len(lowered),
        )

    return sorted(dedupe_text(values), key=sort_key)


def ensure_catalogue_hints(values: list[str], *, draft: DraftRecordingEntry) -> list[str]:
    return dedupe_text([append_catalogue_hint(query, draft=draft) for query in values if compact(query)])


def append_catalogue_hint(query: str, *, draft: DraftRecordingEntry) -> str:
    normalized_query = compact(query)
    catalogue = compact(draft.catalogue)
    if not normalized_query or not catalogue:
        return normalized_query
    normalized_catalogue = normalize_text(catalogue)
    if normalized_catalogue and normalized_catalogue in normalize_text(normalized_query):
        return normalized_query
    if query_is_generic_plural_bundle_rescue(normalized_query, draft=draft):
        return normalized_query
    if not query_mentions_requested_work(normalized_query, draft=draft):
        return normalized_query
    return f"{normalized_query} {catalogue}"


def select_generic_plural_bundle_rescue_queries(
    values: list[str],
    *,
    draft: DraftRecordingEntry,
) -> list[str]:
    return dedupe_text([
        query for query in values if query_is_generic_plural_bundle_rescue(query, draft=draft)
    ])


def query_is_generic_plural_bundle_rescue(query: str, *, draft: DraftRecordingEntry) -> bool:
    if not compact(draft.performance_date_text):
        return False
    if not (draft.secondary_names or draft.secondary_names_latin):
        return False
    if not (draft.ensemble_names or draft.ensemble_names_latin):
        return False
    normalized_query = compact(query)
    if not bilibili_query_is_generic_plural_bundle_rescue(normalized_query):
        return False
    if "/" in normalized_query or " - " in normalized_query:
        return False
    haystack = normalize_text(normalized_query)
    primary_values = dedupe_text([*draft.primary_names_latin[:2], *draft.primary_names[:2]])
    if primary_values and not any(name_matches(haystack, value) for value in primary_values):
        return False
    composer_values = dedupe_text([draft.composer_name_latin, draft.composer_name])
    if any(name_matches(haystack, value) for value in composer_values if compact(value)):
        return False
    secondary_values = dedupe_text([*draft.secondary_names_latin[:2], *draft.secondary_names[:2]])
    if any(name_matches(haystack, value) for value in secondary_values if compact(value)):
        return False
    ensemble_values = dedupe_text([*draft.ensemble_names_latin[:2], *draft.ensemble_names[:2]])
    if any(ensemble_matches(haystack, value) for value in ensemble_values if compact(value)):
        return False
    return True


def query_mentions_requested_work(query: str, *, draft: DraftRecordingEntry) -> bool:
    normalized_query = normalize_text(query)
    if not normalized_query:
        return False
    alias_values = {
        normalize_text(alias)
        for alias in [
            *build_work_aliases(draft.work_title_latin),
            *build_work_aliases(draft.work_title),
            compact(build_work_query(draft, prefer_latin=True)),
            compact(build_work_query(draft, prefer_latin=False)),
        ]
        if compact(alias)
    }
    alias_values.discard(normalize_text(compact(draft.catalogue)))
    if any(alias in normalized_query for alias in alias_values if len(alias.replace(" ", "")) >= 4):
        return True
    return any(marker in normalized_query or marker in query for marker in requested_work_form_markers(draft))


def requested_work_form_markers(draft: DraftRecordingEntry) -> tuple[str, ...]:
    work_text = normalize_text(f"{draft.work_title_latin} {draft.work_title}")
    markers: list[str] = []
    if "concerto" in work_text or "\u534f\u594f\u66f2" in compact(draft.work_title):
        markers.extend(["concerto", "concertos", "klavierkonzert", "\u94a2\u534f", "\u534f\u594f\u66f2"])
    if "sonata" in work_text or "\u594f\u9e23\u66f2" in compact(draft.work_title):
        markers.extend(["sonata", "\u594f\u9e23\u66f2"])
    if "symphony" in work_text or "\u4ea4\u54cd\u66f2" in compact(draft.work_title):
        markers.extend(["symphony", "sym", "\u4ea4\u54cd\u66f2"])
    if "quartet" in work_text or "\u56db\u91cd\u594f" in compact(draft.work_title):
        markers.extend(["quartet", "\u56db\u91cd\u594f"])
    if "trio" in work_text or "\u4e09\u91cd\u594f" in compact(draft.work_title):
        markers.extend(["trio", "\u4e09\u91cd\u594f"])
    return tuple(dict.fromkeys(markers))


def count_query_lead_slot_hits(query: str, lead_slots: list[list[str]]) -> int:
    normalized_query = normalize_text(query)
    hits = 0
    for slot in lead_slots:
        if any(normalize_text(value) in normalized_query for value in slot if compact(value)):
            hits += 1
    return hits


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        url = compact(row.get("url"))
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def dedupe_text(values: list[str]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values:
        normalized = compact(value)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(normalized)
    return items


def prioritize_person_query_terms(values: list[str]) -> list[str]:
    deduped = dedupe_text(values)

    def bucket(normalized: str) -> int:
        token_count = len([token for token in normalized.split() if token])
        if "/" in normalized:
            return 3
        if 2 <= token_count <= 3:
            return 0
        if token_count >= 4:
            return 1
        return 2

    return [
        value
        for _, value in sorted(
            enumerate(deduped),
            key=lambda item: (bucket(item[1]), len(compact(item[1])), item[0]),
        )
    ]


def prioritize_chamber_collaboration_query_terms(
    draft: DraftRecordingEntry,
    values: list[str],
) -> list[str]:
    if compact(getattr(draft, "work_type_hint", "")).lower() not in {"chamber_solo", "chamber_duo"}:
        return dedupe_text(values)
    lead_slots = build_lead_slots(draft)
    if len(lead_slots) < 2:
        return dedupe_text(values)

    collaboration_terms: list[str] = []
    for value in dedupe_text(values):
        haystack = normalize_text(value)
        if not looks_latin(value):
            continue
        if all(any(name_matches(haystack, candidate) for candidate in slot if compact(candidate)) for slot in lead_slots[:2]):
            collaboration_terms.append(value)
    return dedupe_text([*collaboration_terms, *values])


def is_probable_abbreviation(value: str) -> bool:
    normalized = compact(value)
    return bool(normalized) and normalized.upper() == normalized and len(normalized) <= 6 and " " not in normalized


def strip_html(value: str) -> str:
    cleaned = re.sub(r"<script[\s\S]*?</script>", " ", value or "", flags=re.I)
    cleaned = re.sub(r"<style[\s\S]*?</style>", " ", cleaned, flags=re.I)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    return html.unescape(cleaned).replace("\n", " ").strip()


def extract_meta_content(html_text: str, key: str, attr: str = "property") -> str:
    patterns = [
        re.compile(rf'<meta[^>]+{attr}=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']', re.I),
        re.compile(rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+{attr}=["\']{re.escape(key)}["\']', re.I),
    ]
    for pattern in patterns:
        match = pattern.search(html_text or "")
        if match:
            return html.unescape(match.group(1))
    return ""


def extract_title(html_text: str) -> str:
    match = re.search(r"<title>([^<]+)</title>", html_text or "", re.I)
    return html.unescape(match.group(1).strip()) if match else ""


def extract_first_image_src(html_text: str, base_url: str) -> str:
    for match in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', html_text or "", re.I):
        src = resolve_image_url(base_url, match.group(1))
        if src:
            return src
    return ""


def resolve_image_url(base_url: str, value: str | None) -> str:
    src = compact(value)
    if not src or src.startswith("data:"):
        return ""
    return urljoin(base_url, src)


def extract_duration_seconds(html_text: str) -> int:
    patterns = [
        r'"lengthSeconds":"(\d+)"',
        r'"durationSeconds":"(\d+)"',
        r'"duration":"PT(?:(\d+)M)?(?:(\d+)S)?"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text or "", re.I)
        if not match:
            continue
        if len(match.groups()) == 1:
            return int(match.group(1))
        minutes = int(match.group(1) or 0)
        seconds = int(match.group(2) or 0)
        total = minutes * 60 + seconds
        if total:
            return total
    return 0


def extract_json_object_after_marker(html_text: str, marker: str) -> dict[str, Any]:
    index = html_text.find(marker)
    if index < 0:
        return {}
    start = index + len(marker)
    while start < len(html_text) and html_text[start].isspace():
        start += 1
    if start >= len(html_text) or html_text[start] != "{":
        return {}

    depth = 0
    in_string = False
    escaped = False
    for end in range(start, len(html_text)):
        char = html_text[end]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                payload = html_text[start : end + 1]
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    return {}
                return parsed if isinstance(parsed, dict) else {}
    return {}


def extract_bilibili_structured_metadata(html_text: str) -> dict[str, Any]:
    state = extract_json_object_after_marker(html_text, "window.__INITIAL_STATE__=")
    video_data = state.get("videoData") if isinstance(state, dict) else {}
    if not isinstance(video_data, dict):
        return {}
    owner = video_data.get("owner") if isinstance(video_data.get("owner"), dict) else {}
    stats = video_data.get("stat") if isinstance(video_data.get("stat"), dict) else {}
    pages = video_data.get("pages") if isinstance(video_data.get("pages"), list) else []
    parts = " ".join(compact(item.get("part")) for item in pages[:4] if isinstance(item, dict))
    description = compact(video_data.get("desc"))
    return {
        "title": compact(video_data.get("title")),
        "description": description,
        "body_text": compact(" ".join(part for part in [compact(video_data.get("title")), description, compact(owner.get("name")), parts] if part)),
        "image_url": compact(video_data.get("pic")),
        "uploader": compact(owner.get("name")),
        "bvid": compact(video_data.get("bvid")),
        "duration_seconds": int(video_data.get("duration") or 0),
        "view_count": int(stats.get("view") or 0),
    }


def extract_youtube_structured_metadata(html_text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for marker in (
        "var ytInitialPlayerResponse = ",
        "var ytInitialPlayerResponse=",
        "ytInitialPlayerResponse = ",
        "ytInitialPlayerResponse=",
    ):
        payload = extract_json_object_after_marker(html_text, marker)
        if payload:
            break
    if not payload:
        return {}

    video_details = payload.get("videoDetails") if isinstance(payload.get("videoDetails"), dict) else {}
    microformat = payload.get("microformat") if isinstance(payload.get("microformat"), dict) else {}
    player_microformat = (
        microformat.get("playerMicroformatRenderer") if isinstance(microformat.get("playerMicroformatRenderer"), dict) else {}
    )
    description_runs = player_microformat.get("description") if isinstance(player_microformat.get("description"), dict) else {}
    description = compact(video_details.get("shortDescription"))
    if not description:
        description = compact(description_runs.get("simpleText"))
    title = compact(video_details.get("title")) or compact(player_microformat.get("title"))
    uploader = compact(video_details.get("author")) or compact(player_microformat.get("ownerChannelName"))
    duration_seconds = int(video_details.get("lengthSeconds") or player_microformat.get("lengthSeconds") or 0)
    view_count = int(video_details.get("viewCount") or player_microformat.get("viewCount") or 0)

    overlay_payload = extract_json_object_after_marker(html_text, '"videoDetails":')
    overlay_renderer = (
        overlay_payload.get("playerOverlayVideoDetailsRenderer")
        if isinstance(overlay_payload.get("playerOverlayVideoDetailsRenderer"), dict)
        else {}
    )
    overlay_title = extract_renderer_text(overlay_renderer.get("title"))
    overlay_subtitle = overlay_renderer.get("subtitle") if isinstance(overlay_renderer.get("subtitle"), dict) else {}
    overlay_runs = overlay_subtitle.get("runs") if isinstance(overlay_subtitle.get("runs"), list) else []
    overlay_texts = [compact(item.get("text")) for item in overlay_runs if isinstance(item, dict) and compact(item.get("text"))]
    overlay_uploader = overlay_texts[0] if overlay_texts else ""
    overlay_view_count = 0
    for value in overlay_texts[1:]:
        numeric_value = extract_first_number(value)
        if numeric_value > 0:
            overlay_view_count = numeric_value
            break
    overlay_duration_seconds = extract_duration_seconds_from_accessibility_label(
        extract_meta_content(html_text, "lengthText", attr="name")
    ) or extract_duration_seconds_from_accessibility_label(extract_youtube_length_label(html_text))

    title = title or overlay_title
    uploader = uploader or overlay_uploader
    duration_seconds = duration_seconds or overlay_duration_seconds
    view_count = view_count or overlay_view_count
    return {
        "title": title,
        "description": description,
        "body_text": compact(" ".join(part for part in [title, description, uploader] if part)),
        "uploader": uploader,
        "duration_seconds": duration_seconds,
        "view_count": view_count,
    }


def extract_renderer_text(value: Any) -> str:
    if isinstance(value, dict):
        simple_text = compact(value.get("simpleText"))
        if simple_text:
            return simple_text
        runs = value.get("runs")
        if isinstance(runs, list):
            return compact("".join(compact(item.get("text")) for item in runs if isinstance(item, dict)))
        return ""
    return compact(value)


def extract_first_number(value: str) -> int:
    match = re.search(r"(\d[\d,\.]*)", compact(value))
    if not match:
        return 0
    digits = re.sub(r"[^\d]", "", match.group(1))
    return int(digits) if digits else 0


def extract_duration_seconds_from_accessibility_label(value: str) -> int:
    numbers = [int(token) for token in re.findall(r"\d+", compact(value))]
    if len(numbers) >= 3:
        return numbers[-3] * 3600 + numbers[-2] * 60 + numbers[-1]
    if len(numbers) == 2:
        return numbers[0] * 60 + numbers[1]
    if len(numbers) == 1:
        return numbers[0]
    return 0


def extract_youtube_length_label(html_text: str) -> str:
    patterns = (
        r'"lengthText":\{"accessibility":\{"accessibilityData":\{"label":"([^"]+)"',
        r'"lengthText":\{"simpleText":"([^"]+)"',
    )
    for pattern in patterns:
        match = re.search(pattern, html_text or "", re.I)
        if match:
            return html.unescape(match.group(1))
    return ""


def extract_uploader_name(html_text: str) -> str:
    for pattern in (r'"ownerChannelName":"([^"]+)"', r'"author":"([^"]+)"'):
        match = re.search(pattern, html_text or "", re.I)
        if match:
            return html.unescape(match.group(1))
    return ""


def extract_view_count(html_text: str) -> int:
    match = re.search(r'"viewCount":"(\d+)"', html_text or "", re.I)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except ValueError:
        return 0


def extract_duckduckgo_result_links(html_text: str) -> list[str]:
    matches = re.finditer(r'result__a[^>]+href="([^"]+)"', html_text or "", re.I)
    return [html.unescape(match.group(1)) for match in matches if compact(match.group(1)).startswith("http")]


def extract_bing_result_links(html_text: str) -> list[str]:
    matches = re.finditer(r'<li class="b_algo"[\s\S]{0,1200}?<a href="([^"]+)"', html_text or "", re.I)
    links: list[str] = []
    for match in matches:
        link = html.unescape(match.group(1))
        decoded = decode_bing_redirect(link)
        if compact(decoded).startswith("http"):
            links.append(decoded)
    return links


def extract_youtube_result_links(html_text: str) -> list[str]:
    video_ids = re.findall(r'"videoRenderer"[\s\S]{0,600}?"videoId":"([^"]+)"', html_text or "")
    return [f"https://www.youtube.com/watch?v={video_id}" for video_id in video_ids]


def extract_bilibili_result_links(html_text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def append_url(value: str) -> None:
        normalized = compact(value)
        if normalized.startswith("//"):
            normalized = f"https:{normalized}"
        if not normalized.startswith("http"):
            return
        if normalized in seen:
            return
        seen.add(normalized)
        urls.append(normalized)

    for pattern in (
        r'"arcurl":"([^"]+)"',
        r'arcurl:"([^"]+)"',
    ):
        for match in re.finditer(pattern, html_text or ""):
            encoded = match.group(1)
            try:
                decoded = json.loads(f'"{encoded}"')
            except json.JSONDecodeError:
                continue
            append_url(decoded)

    for match in re.finditer(r'href="(//www\.bilibili\.com/video/(?:BV[0-9A-Za-z]+|av\d+)/?)"', html_text or "", re.I):
        append_url(match.group(1))

    return urls


def extract_apple_music_result_links(html_text: str) -> list[str]:
    links: list[str] = []
    for match in re.finditer(r'"url":"(https:\\/\\/(?:classical\.)?music\.apple\.com[^"]+)"', html_text or ""):
        try:
            link = json.loads(f'"{match.group(1)}"')
        except json.JSONDecodeError:
            continue
        if "/album/" in link or "/song/" in link or "/playlist/" in link:
            links.append(link)
    return links


def decode_bing_redirect(url: str) -> str:
    target = compact(url)
    if "bing.com/ck/a" not in target:
        return target
    match = re.search(r"[?&]u=([^&]+)", target)
    if not match:
        return target
    encoded = match.group(1)
    if encoded.startswith("a1"):
        encoded = encoded[2:]
    padding = "=" * ((4 - len(encoded) % 4) % 4)
    try:
        decoded = base64.b64decode(encoded + padding).decode("utf-8")
    except Exception:
        return target
    return decoded if decoded.startswith("http") else target


def detect_platform(url: str) -> str:
    normalized = url.lower()
    if "youtube.com" in normalized or "youtu.be" in normalized:
        return "youtube"
    if "bilibili.com" in normalized:
        return "bilibili"
    if "apple.com" in normalized:
        return "apple_music"
    if "spotify.com" in normalized:
        return "spotify"
    if "qobuz.com" in normalized:
        return "qobuz"
    return "other"


def is_generic_apple_player_text(value: str) -> bool:
    lowered = compact(value).lower().replace("\xa0", " ")
    if not lowered:
        return False
    generic_markers = (
        "apple music 网页播放器",
        "apple music web player",
        "在 apple music 上畅听数千万首歌曲",
        "listen to millions of songs ad-free",
    )
    return any(marker in lowered for marker in generic_markers)


def is_generic_youtube_watch_text(value: str) -> bool:
    lowered = compact(value).lower().replace("\xa0", " ")
    if not lowered:
        return False
    if lowered in {"- youtube", "youtube"}:
        return True
    generic_markers = (
        "youtube でお気に入りの動画や音楽を楽しみ",
        "在 youtube 上畅享你喜爱的视频和音乐",
        "enjoy the videos and music you love",
        "upload original content",
        "share it all with friends, family, and the world",
        "share your videos with friends, family, and the world",
    )
    return any(marker in lowered for marker in generic_markers)


def strip_generic_youtube_watch_suffix(value: str) -> str:
    normalized = compact(value)
    if not normalized:
        return ""
    return re.sub(r"\s*-\s*YouTube\s*$", "", normalized, flags=re.I).strip()


def score_recording_match(
    text: str,
    url: str,
    draft: DraftRecordingEntry,
    *,
    duration_seconds: int = 0,
    uploader: str = "",
) -> float:
    haystack = normalize_text(f"{text} {url} {uploader}")
    score = 0.2
    years_in_text = extract_year_mentions(haystack)

    work_text = draft.work_title_latin or draft.work_title
    work_tokens = tokenize(work_text)
    work_core_tokens = tokenize(strip_catalogue_text(work_text))
    work_aliases = build_work_aliases(draft.work_title_latin)
    work_aliases.update(build_work_aliases(draft.work_title))
    named_work_aliases = build_named_work_aliases(draft.work_title_latin)
    named_work_aliases.update(build_named_work_aliases(draft.work_title))
    work_matched = False
    if work_tokens and contains_tokens(haystack, work_tokens):
        work_matched = True
    elif work_core_tokens and contains_tokens(haystack, work_core_tokens):
        work_matched = True
    elif work_aliases and any(alias in haystack for alias in work_aliases):
        work_matched = True
    if work_matched:
        score += 0.25

    catalogue_tokens = tokenize(draft.catalogue)
    if catalogue_tokens and contains_tokens(haystack, catalogue_tokens):
        score += 0.12

    composer_matched = False
    composer_values = dedupe_text([draft.composer_name_latin, draft.composer_name])
    for composer_value in composer_values:
        composer_tokens = tokenize(composer_value)
        if composer_tokens and (contains_tokens(haystack, composer_tokens) or name_matches(haystack, composer_value)):
            composer_matched = True
            break
    if composer_matched:
        score += 0.08
    elif composer_values and work_matched and any(looks_latin(value) for value in composer_values):
        score -= 0.24

    lead_slots = build_lead_slots(draft)
    sparse_collaboration_hint = has_sparse_collaboration_hint(draft, lead_slots)
    lead_hits = 0
    for slot in lead_slots:
        if any(name_matches(haystack, lead) for lead in slot):
            lead_hits += 1
    if lead_hits:
        score += 0.18 + 0.08 * max(0, lead_hits - 1)
    if len(lead_slots) >= 2 and lead_hits < len(lead_slots):
        collaboration_separators = [" / ", "&", " and ", " with "]
        if any(separator in haystack for separator in collaboration_separators):
            penalty = 0.24 * (len(lead_slots) - lead_hits)
            if sparse_collaboration_hint and has_collaboration_marker(text) and work_matched and lead_hits >= 1:
                penalty *= 0.15
            score -= penalty
        elif has_explicit_collaborator_marker(text):
            score -= 0.18 * (len(lead_slots) - lead_hits)
    if len(lead_slots) >= 2 and not draft.ensemble_names and not draft.ensemble_names_latin:
        if lead_hits == len(lead_slots):
            score += 0.18
        elif lead_hits == 1:
            if sparse_collaboration_hint and has_collaboration_marker(text) and work_matched:
                score += 0.08
                if has_complete_work_tracklist(haystack):
                    score += 0.18
                elif named_work_aliases and any(alias in haystack for alias in named_work_aliases):
                    score += 0.08
            else:
                score -= 0.1
    elif has_collaboration_marker(draft.title) and not draft.ensemble_names and not draft.ensemble_names_latin:
        if has_collaboration_marker(text) and lead_hits >= 1 and work_matched:
            score += 0.18
            if named_work_aliases and any(alias in haystack for alias in named_work_aliases):
                score += 0.06
        elif lead_hits >= 1 and not has_collaboration_marker(text):
            score -= 0.12

    group_hits = 0
    for group in dedupe_text([*draft.ensemble_names[:2], *draft.ensemble_names_latin[:2]])[:4]:
        if ensemble_matches(haystack, group):
            group_hits += 1
    if group_hits:
        score += 0.15
    has_explicit_secondary = bool(draft.secondary_names or draft.secondary_names_latin)
    if (
        has_explicit_secondary
        and len(lead_slots) >= 2
        and lead_hits == 1
        and group_hits
        and work_matched
        and not sparse_collaboration_hint
    ):
        score -= 0.45
    if work_matched and group_hits and (not lead_slots or lead_hits >= len(lead_slots)):
        score += 0.06

    reference_years = infer_reference_years(draft)
    reference_year_set = set(reference_years)
    has_reference_year_hit = bool(reference_year_set.intersection(years_in_text))
    if has_reference_year_hit:
        score += 0.08
    if reference_year_set and years_in_text and reference_year_set.isdisjoint(years_in_text):
        score -= 0.32
    performance_context_tokens = extract_performance_context_tokens(draft.performance_date_text)
    if performance_context_tokens and contains_tokens(haystack, performance_context_tokens):
        score += 0.1
        if has_specific_date_context_tokens(performance_context_tokens) and lead_hits >= 1 and has_reference_year_hit:
            score += 0.08
            if work_matched:
                score += 0.08
    if sparse_collaboration_hint and work_matched and lead_hits >= 1 and has_complete_work_tracklist(haystack):
        score += 0.08
        if has_reference_year_hit:
            score += 0.04
    score += score_catalogue_fit(draft, haystack)

    score += score_recording_container_preference(
        haystack=haystack,
        url=url,
        work_matched=work_matched,
        lead_hits=lead_hits,
    )
    if any(marker in haystack for marker in ("new edition", "restored", "remaster", "reissue", "alt take")):
        score -= 0.12
    if "provided to youtube by" in haystack:
        score -= 0.1
    score += score_duration_fit(draft, haystack, duration_seconds)

    negative_patterns = [
        "biography",
        "discography of",
        "born ",
        "composer profile",
        "work details",
        "人物简介",
        "生平",
        "作品介绍",
    ]
    if any(pattern in haystack for pattern in negative_patterns):
        score -= 0.28

    if (work_tokens or work_aliases) and not work_matched:
        score -= 0.22
    if lead_slots and lead_hits == 0:
        score -= 0.1
    if draft.ensemble_names and group_hits == 0:
        score -= 0.08

    return max(0.0, min(0.97, score))


def score_duration_fit(draft: DraftRecordingEntry, haystack: str, duration_seconds: int) -> float:
    minimum = estimate_full_work_min_duration_seconds(draft)
    if duration_seconds <= 0 or minimum <= 0:
        return 0.0
    if duration_seconds >= int(minimum * 0.85):
        return 0.05 if "full" in haystack or "complete" in haystack else 0.02
    if duration_seconds < max(240, int(minimum * 0.35)):
        return -0.38
    if duration_seconds < int(minimum * 0.6):
        return -0.18
    return 0.0


def score_catalogue_fit(draft: DraftRecordingEntry, haystack: str) -> float:
    catalogue = normalize_text(draft.catalogue)
    if not catalogue:
        return 0.0
    if catalogue in haystack:
        return 0.06
    requested_numbers = extract_catalogue_markers(catalogue)
    if not requested_numbers:
        return 0.0
    seen_numbers = extract_catalogue_markers(haystack)
    if seen_numbers and requested_numbers.isdisjoint(seen_numbers):
        return -0.2
    return 0.0


def score_recording_container_preference(
    *,
    haystack: str,
    url: str,
    work_matched: bool,
    lead_hits: int,
) -> float:
    lowered = haystack.lower()
    score = 0.0
    is_apple_track = "music.apple.com" in compact(url).lower() and "?i=" in compact(url).lower()
    if looks_like_single_movement(haystack):
        if is_apple_track and work_matched and lead_hits >= 1:
            score -= 0.02 if looks_like_first_chapter_extract(f"{haystack} {url}") else 0.06
        elif looks_like_first_chapter_extract(f"{haystack} {url}") and work_matched and lead_hits >= 1:
            score -= 0.08
        else:
            score -= 0.34
    elif looks_like_multi_work_compilation(haystack):
        if is_apple_track and work_matched and lead_hits >= 1:
            score += 0.0
        elif work_matched and lead_hits >= 1:
            score -= 0.12
        else:
            score -= 0.34
    elif has_complete_work_tracklist(haystack) or any(marker in lowered for marker in ("full", "complete", "full performance")):
        score += 0.04

    if re.search(r"[?&]p=\d+", url, re.I):
        score -= 0.04
    return score


def has_complete_work_tracklist(haystack: str) -> bool:
    numbered_sections = len(re.findall(r"(?:^|\D)([1-6])\.", haystack))
    movement_terms = sum(
        1
        for token in ("allegro", "adagio", "andante", "scherzo", "rondo", "presto", "larghetto", "largo")
        if token in haystack
    )
    if numbered_sections >= 3 and movement_terms >= 2:
        return True
    return movement_terms >= 3


def infer_reference_year(draft: DraftRecordingEntry) -> str:
    years = infer_reference_years(draft)
    return years[0] if years else ""


def infer_reference_years(draft: DraftRecordingEntry) -> list[str]:
    years: list[str] = []
    for value in (draft.performance_date_text, draft.title, draft.raw_text):
        years.extend(re.findall(r"((?:17|18|19|20)\d{2})", compact(value)))
    return dedupe_text(years)


def extract_decade_bucket(value: str) -> str:
    year = extract_year(value)
    if not year:
        return ""
    return f"{year[:3]}0s"


def extract_person_query_keyword(value: str) -> str:
    normalized = compact(value)
    if not looks_latin(normalized):
        return ""
    tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if token]
    if len(tokens) >= 2 and len(tokens[-1]) >= 4:
        surname_particles = {"da", "de", "del", "della", "der", "di", "du", "la", "le", "ten", "ter", "van", "von"}
        surname_parts = [tokens[-1]]
        index = len(tokens) - 2
        while index > 0 and tokens[index].casefold() in surname_particles:
            surname_parts.insert(0, tokens[index])
            index -= 1
        return " ".join(surname_parts)
    return normalized


def is_distinctive_person_query_keyword(value: str) -> bool:
    normalized = compact(value)
    if not normalized:
        return False
    condensed = re.sub(r"[^A-Za-z\u4e00-\u9fff]+", "", normalized)
    if not condensed:
        return False
    if contains_cjk(condensed):
        return len(condensed) >= 3
    return len(condensed) >= 8


def extract_cjk_person_query_keyword(value: str) -> str:
    normalized = compact(value)
    if not contains_cjk(normalized):
        return ""
    separator_pattern = r"[·•・／/|,，、\s]+"
    segments = [segment.strip() for segment in re.split(separator_pattern, normalized) if segment.strip()]
    if not segments:
        return ""
    return segments[-1]


def build_chinese_host_decade_rescue_queries(draft: DraftRecordingEntry) -> list[str]:
    work_text = compact(draft.work_title_latin)
    if "concerto" not in normalize_text(work_text):
        return []
    decade_bucket = extract_decade_bucket(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    if not decade_bucket:
        return []
    primary_keywords = dedupe_text(
        [
            extract_person_query_keyword(value)
            for value in prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))
        ]
    )
    if not primary_keywords:
        return []
    composer_keyword = extract_person_query_keyword(draft.composer_name_latin or draft.composer_name)
    work_candidates: list[str] = []
    stripped_work = compact(strip_catalogue_text(work_text))
    if looks_latin(stripped_work) and "concerto" in normalize_text(stripped_work):
        work_candidates.append(stripped_work)
    work_candidates.extend(
        sorted(
            {
                alias
                for alias in build_work_aliases(work_text)
                if looks_latin(alias) and "concerto" in alias
            },
            key=lambda alias: (len(alias.split()), len(alias)),
        )
    )
    queries: list[str] = []
    for primary_keyword in primary_keywords[:2]:
        for work_candidate in dedupe_text(work_candidates)[:2]:
            candidate_variants = [work_candidate]
            if "Piano Concerto" in work_candidate and "Piano Concertos" not in work_candidate:
                candidate_variants.append(work_candidate.replace("Piano Concerto", "Piano Concertos"))
            deduped_variants = dedupe_text(candidate_variants)
            if composer_keyword:
                for candidate_variant in deduped_variants:
                    queries.append(f"{primary_keyword} {composer_keyword} {candidate_variant} {decade_bucket}")
            for candidate_variant in deduped_variants:
                queries.append(f"{primary_keyword} {candidate_variant} {decade_bucket}")
    return dedupe_text(queries)


def build_chinese_host_primary_work_rescue_queries(draft: DraftRecordingEntry) -> list[str]:
    work_text = normalize_text(draft.work_title_latin)
    if "concerto" not in work_text:
        return []
    collaboration_queries = build_collaboration_surname_rescue_queries(draft)
    primary_names = dedupe_text(prioritize_person_query_terms(getattr(draft, "primary_names_latin", [])))
    if not primary_names:
        return collaboration_queries
    primary_keywords = dedupe_text(
        [
            extract_person_query_keyword(value)
            for value in prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))
        ]
    )
    composer_keyword_latin = extract_person_query_keyword(draft.composer_name_latin or draft.composer_name)
    composer_keyword_cjk = extract_cjk_person_query_keyword(draft.composer_name)
    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    stripped_work_latin = compact(strip_catalogue_text(draft.work_title_latin))
    secondary_keywords = dedupe_text(
        [
            extract_person_query_keyword(value)
            for value in prioritize_person_query_terms(
                [
                    *getattr(draft, "secondary_names_latin", []),
                    *getattr(draft, "lead_names_latin", [])[1:],
                ]
            )
        ]
    )
    queries: list[str] = collaboration_queries[:1]
    if (
        primary_keywords
        and composer_keyword_latin
        and stripped_work_latin
        and reference_year
        and any(is_distinctive_person_query_keyword(primary_keyword) for primary_keyword in primary_keywords[:2])
    ):
        for primary_keyword in primary_keywords[:2]:
            if not is_distinctive_person_query_keyword(primary_keyword):
                continue
            queries.append(f"{primary_keyword} {composer_keyword_latin} {stripped_work_latin} {reference_year}")
    for primary_name in primary_names[:2]:
        if composer_keyword_latin:
            queries.append(f"{primary_name} {composer_keyword_latin} concerto")
        if composer_keyword_cjk:
            queries.append(f"{primary_name} {composer_keyword_cjk}钢协")
        for secondary_keyword in secondary_keywords[:1]:
            if secondary_keyword.casefold() == primary_name.casefold():
                continue
            if composer_keyword_latin:
                queries.append(f"{primary_name} {secondary_keyword} {composer_keyword_latin} concerto")
            else:
                queries.append(f"{primary_name} {secondary_keyword} concerto")
    if primary_keywords and secondary_keywords and reference_year:
        for primary_keyword in primary_keywords[:1]:
            for secondary_keyword in secondary_keywords[:1]:
                if secondary_keyword.casefold() == primary_keyword.casefold():
                    continue
                queries.append(f"{primary_keyword} {secondary_keyword} concerto {reference_year}")
    queries.extend(collaboration_queries[1:])
    return dedupe_text(queries)


def build_chinese_host_primary_year_anchor_queries(draft: DraftRecordingEntry) -> list[str]:
    reference_year = extract_year(
        draft.performance_date_text or draft.title or draft.raw_text or draft.source_line or draft.item_id
    )
    if not reference_year:
        return []
    primary_values = dedupe_text([
        *getattr(draft, "primary_names", [])[:2],
        *getattr(draft, "primary_names_latin", [])[:2],
        *draft.lead_names[:1],
        *draft.lead_names_latin[:1],
    ])
    queries: list[str] = []
    for value in primary_values:
        normalized = compact(value)
        if not normalized:
            continue
        query_term = normalized if contains_cjk(normalized) else extract_person_query_keyword(normalized)
        query_term = compact(query_term)
        if not query_term:
            continue
        queries.append(f"{query_term} {reference_year}")
    return dedupe_text(queries)


def chamber_work_alias_rank(alias: str) -> tuple[int, int, int]:
    normalized = normalize_text(alias)
    named_aliases = {"appassionata", "spring"}
    return (
        0 if normalized in named_aliases else 1,
        0 if "no." not in normalized and "no " not in normalized else 1,
        len(normalized),
    )


def build_chamber_compound_work_candidates(values: list[str]) -> list[str]:
    normalized_values = dedupe_text([compact(value) for value in values if compact(value)])
    named_values = [value for value in normalized_values if normalize_text(value) in {"appassionata", "spring"}]
    numbered_values = [
        value
        for value in normalized_values
        if re.search(r"\bsonata\b", normalize_text(value)) and re.search(r"\bno\.?\s*\d+\b", normalize_text(value))
    ]
    queries: list[str] = []
    for named_value in named_values[:2]:
        for numbered_value in numbered_values[:2]:
            if normalize_text(named_value) in normalize_text(numbered_value):
                continue
            queries.append(f"{named_value} {numbered_value}")
    return dedupe_text(queries)


def build_chamber_duo_compact_lead_terms(draft: DraftRecordingEntry) -> list[str]:
    primary_values = dedupe_text(getattr(draft, "primary_names_latin", [])[:2])
    secondary_values = dedupe_text(getattr(draft, "secondary_names_latin", [])[:2])
    if not primary_values or not secondary_values:
        return []

    queries: list[str] = []
    for primary_value in primary_values:
        for secondary_value in secondary_values:
            primary_keyword = compact(extract_person_query_keyword(primary_value))
            secondary_keyword = compact(extract_person_query_keyword(secondary_value))
            if not primary_keyword or not secondary_keyword or primary_keyword.casefold() == secondary_keyword.casefold():
                continue
            queries.extend(
                [
                    f"{primary_keyword} {secondary_keyword}",
                    f"{primary_keyword}-{secondary_keyword}",
                    f"{primary_keyword} / {secondary_keyword}",
                ]
            )
    return dedupe_text(queries)


def build_chamber_year_anchor_queries(
    draft: DraftRecordingEntry,
    *,
    composer_query: str,
    work_candidates: list[str],
    lead_terms: list[str],
) -> list[str]:
    year_candidates = extract_year_candidates(
        " ".join(
            [
                compact(draft.performance_date_text),
                compact(draft.title),
                compact(draft.raw_text),
                compact(draft.source_line),
            ]
        )
    )
    if not year_candidates or not work_candidates or not lead_terms:
        return []

    queries: list[str] = []
    composer_queries: list[str] = []
    for work_candidate in work_candidates[:1]:
        for lead_term in lead_terms[:1]:
            for year_candidate in year_candidates[:2]:
                queries.append(f"{work_candidate} {lead_term} {year_candidate}")
                if composer_query:
                    composer_queries.append(f"{composer_query} {work_candidate} {lead_term} {year_candidate}")
    return dedupe_text([*queries, *composer_queries])


def build_chamber_solo_opus_queries(
    draft: DraftRecordingEntry,
    *,
    composer_query: str,
    lead_terms: list[str],
) -> list[str]:
    if getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", []):
        return []
    context = normalize_text(
        " ".join(
            [
                compact(draft.work_title_latin),
                compact(draft.work_title),
                compact(draft.title),
                compact(draft.raw_text),
                compact(draft.source_line),
            ]
        )
    )
    opus_values = dedupe_text(re.findall(r"\bop\.?\s*(\d+)\b", context))
    if not opus_values:
        return []

    queries: list[str] = []
    for lead_term in dedupe_text(lead_terms)[:3]:
        normalized_lead = compact(lead_term)
        if not normalized_lead or not looks_latin(normalized_lead) or "/" in normalized_lead:
            continue
        for opus in opus_values[:1]:
            queries.append(f"{normalized_lead} op {opus}")
            if composer_query:
                queries.append(f"{composer_query} {normalized_lead} op {opus}")
    return dedupe_text(queries)


def build_chamber_duo_bundle_queries(
    draft: DraftRecordingEntry,
    *,
    composer_query: str,
    compact_duo_lead_terms: list[str],
) -> list[str]:
    if not (getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", [])):
        return []

    work_text = normalize_text(draft.work_title_latin or draft.work_title)
    bundle_terms: list[str] = []
    if "violin sonata" in work_text:
        bundle_terms = ["complete violin sonatas", "violin sonatas", "violin sonata collection"]
    elif "piano sonata" in work_text:
        bundle_terms = ["complete piano sonatas", "piano sonatas", "piano sonata collection"]
    elif "sonata" in work_text:
        bundle_terms = ["complete sonatas", "sonatas", "sonata collection"]
    elif "quartet" in work_text:
        bundle_terms = ["complete quartets", "quartets", "quartet collection"]
    elif "trio" in work_text:
        bundle_terms = ["complete trios", "trios", "trio collection"]
    if not bundle_terms:
        return []

    year_candidates = extract_year_candidates(
        " ".join(
            [
                compact(draft.performance_date_text),
                compact(draft.title),
                compact(draft.raw_text),
                compact(draft.source_line),
            ]
        )
    )

    queries: list[str] = []
    for lead_term in compact_duo_lead_terms[:2]:
        if "/" in lead_term:
            continue
        for bundle_term in bundle_terms[:3]:
            is_complete_bundle = bundle_term.startswith("complete ")
            if composer_query:
                if year_candidates:
                    queries.append(f"{composer_query} {bundle_term} {lead_term} {year_candidates[0]}")
                queries.append(f"{composer_query} {lead_term} {bundle_term}")
            queries.append(f"{lead_term} {bundle_term}")
            if is_complete_bundle:
                queries.append(f"{lead_term} {bundle_term} trailer")
    ranked_queries = sorted(
        dedupe_text(queries),
        key=chamber_duo_bundle_query_rank,
    )
    return ranked_queries


def chamber_duo_bundle_query_rank(query: str) -> tuple[int, int, int, int]:
    normalized = compact(query)
    lowered = normalized.lower()
    has_complete = "complete" in lowered
    has_year = bool(re.search(r"\b(?:18|19|20)\d{2}\b", lowered))
    has_trailer = "trailer" in lowered
    has_hyphenated_duo = bool(re.search(r"\b[A-Za-z]+-[A-Za-z]+\b", normalized))
    return (
        0
        if has_complete and has_year and not has_hyphenated_duo
        else 1
        if has_complete and has_trailer
        else 2
        if has_complete and has_year
        else 3
        if has_complete
        else 4
        if has_year
        else 5,
        1 if has_hyphenated_duo else 0,
        len(lowered.split()),
        len(lowered),
    )


def build_chamber_work_anchor_queries(
    draft: DraftRecordingEntry,
    *,
    prefer_latin: bool,
    composer_query: str,
    lead_terms: list[str],
) -> list[str]:
    work_text = normalize_text(draft.work_title_latin or draft.work_title)
    if not any(token in work_text for token in ("sonata", "quartet", "trio", "奏鸣曲", "四重奏", "三重奏")):
        return []

    work_candidates: list[str] = []
    work_candidates.extend(build_work_aliases(draft.work_title_latin or draft.work_title))
    work_candidates.append(build_work_query(draft, prefer_latin=prefer_latin))
    if prefer_latin:
        filtered_work_candidates = [value for value in work_candidates if looks_latin(value)]
        filtered_leads = [value for value in dedupe_text(lead_terms) if looks_latin(value) or "/" in value]
        filtered_leads = prioritize_chamber_anchor_lead_terms(draft, filtered_leads)
    else:
        filtered_work_candidates = [value for value in work_candidates if contains_cjk(value)]
        filtered_leads = dedupe_text(lead_terms)
    ranked_work_candidates = dedupe_text(
        sorted(
            [compact(value) for value in filtered_work_candidates if compact(value)],
            key=chamber_work_alias_rank,
        )
    )
    compound_work_candidates: list[str] = []
    if prefer_latin:
        leading_candidates = ranked_work_candidates[:1]
        trailing_candidates = ranked_work_candidates[1:]
        compound_work_candidates = build_chamber_compound_work_candidates(ranked_work_candidates)
        ranked_work_candidates = dedupe_text([
            *leading_candidates,
            *compound_work_candidates,
            *trailing_candidates,
        ])
    if not ranked_work_candidates or not filtered_leads:
        return []

    queries: list[str] = []
    for work_candidate in ranked_work_candidates[:3]:
        for lead_term in filtered_leads[:4]:
            queries.append(f"{work_candidate} {lead_term}")
            if composer_query:
                queries.append(f"{composer_query} {work_candidate} {lead_term}")
    compact_duo_lead_terms = build_chamber_duo_compact_lead_terms(draft)
    if prefer_latin:
        compact_duo_queries: list[str] = []
        for work_candidate in ranked_work_candidates[:2]:
            for lead_term in compact_duo_lead_terms[:2]:
                compact_duo_queries.append(f"{work_candidate} {lead_term}")
                if composer_query:
                    compact_duo_queries.append(f"{composer_query} {work_candidate} {lead_term}")
        bundle_queries = build_chamber_duo_bundle_queries(
            draft,
            composer_query=composer_query,
            compact_duo_lead_terms=compact_duo_lead_terms,
        )
        year_anchor_queries = build_chamber_year_anchor_queries(
            draft,
            composer_query=composer_query,
            work_candidates=dedupe_text([*compound_work_candidates, *ranked_work_candidates]),
            lead_terms=dedupe_text([*compact_duo_lead_terms, *filtered_leads]),
        )
        queries = [
            *queries[:2],
            *compact_duo_queries[:1],
            *year_anchor_queries[:2],
            *bundle_queries[:2],
            *compact_duo_queries[1:2],
            *queries[2:],
        ]
    elif compact_duo_lead_terms:
        mixed_compact_duo_queries: list[str] = []
        for work_candidate in ranked_work_candidates[:2]:
            for lead_term in compact_duo_lead_terms[:2]:
                mixed_compact_duo_queries.extend(
                    [
                        f"{work_candidate} {lead_term}",
                        f"{lead_term} {work_candidate}",
                    ]
                )
                if composer_query:
                    mixed_compact_duo_queries.append(f"{composer_query} {work_candidate} {lead_term}")
        queries = [*queries[:1], *mixed_compact_duo_queries[:1], *queries[1:]]
    return dedupe_text(queries)


def prioritize_chamber_anchor_lead_terms(
    draft: DraftRecordingEntry,
    values: list[str],
) -> list[str]:
    deduped = dedupe_text(values)
    if compact(getattr(draft, "work_type_hint", "")).lower() not in {"chamber_solo", "chamber_duo"}:
        return deduped

    collaboration_terms = sorted(
        [
            value
            for value in deduped
            if looks_latin(value) and query_mentions_primary_and_secondary_names(value, draft)
        ],
        key=lambda value: ("/" in value, len([token for token in value.split() if token]), len(compact(value))),
    )
    return dedupe_text([*collaboration_terms, *deduped])


def query_mentions_primary_and_secondary_names(query: str, draft: DraftRecordingEntry) -> bool:
    haystack = normalize_text(query)
    primary_values = dedupe_text([*getattr(draft, "primary_names_latin", [])[:3], *getattr(draft, "primary_names", [])[:2]])
    secondary_values = dedupe_text([*getattr(draft, "secondary_names_latin", [])[:3], *getattr(draft, "secondary_names", [])[:2]])
    return bool(primary_values) and bool(secondary_values) and any(
        name_matches(haystack, value) for value in primary_values if compact(value)
    ) and any(name_matches(haystack, value) for value in secondary_values if compact(value))


def is_chamber_named_work_compact_duo_query(query: str, draft: DraftRecordingEntry) -> bool:
    normalized_query = compact(query)
    if not normalized_query:
        return False
    compact_duo_terms = build_chamber_duo_compact_lead_terms(draft)
    if not any(term in normalized_query for term in compact_duo_terms[:2]):
        return False
    cjk_named_aliases = [
        alias
        for alias in build_named_work_aliases(draft.work_title_latin or draft.work_title)
        if contains_cjk(alias)
    ]
    return any(alias in normalized_query for alias in cjk_named_aliases)


def is_chamber_named_work_duo_anchor_query(query: str, draft: DraftRecordingEntry) -> bool:
    normalized_query = compact(query)
    if not normalized_query:
        return False
    cjk_named_aliases = [
        alias
        for alias in build_named_work_aliases(draft.work_title_latin or draft.work_title)
        if contains_cjk(alias)
    ]
    if not any(alias in normalized_query for alias in cjk_named_aliases):
        return False
    if is_chamber_named_work_compact_duo_query(normalized_query, draft):
        return True
    haystack = normalize_text(normalized_query)
    primary_values = dedupe_text([*getattr(draft, "primary_names_latin", [])[:2], *getattr(draft, "primary_names", [])[:2]])
    secondary_values = dedupe_text([*getattr(draft, "secondary_names_latin", [])[:2], *getattr(draft, "secondary_names", [])[:2]])
    return bool(primary_values) and bool(secondary_values) and any(
        name_matches(haystack, value) for value in primary_values if compact(value)
    ) and any(name_matches(haystack, value) for value in secondary_values if compact(value))


def is_chamber_latin_compact_duo_anchor_query(query: str, draft: DraftRecordingEntry) -> bool:
    normalized_query = compact(query)
    if not normalized_query or contains_cjk(normalized_query):
        return False
    compact_duo_terms = build_chamber_duo_compact_lead_terms(draft)
    if not any(term in normalized_query for term in compact_duo_terms[:2]):
        return False
    return query_mentions_requested_work(normalized_query, draft=draft)


def is_chamber_latin_solo_opus_anchor_query(query: str, draft: DraftRecordingEntry) -> bool:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return False
    if getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", []):
        return False
    normalized_query = compact(query)
    if not normalized_query or contains_cjk(normalized_query):
        return False
    query_opus_values = set(re.findall(r"\bop\.?\s*(\d+)\b", normalized_query, re.I))
    if not query_opus_values:
        return False
    draft_context = normalize_text(
        " ".join(
            [
                compact(draft.work_title_latin),
                compact(draft.work_title),
                compact(draft.title),
                compact(draft.raw_text),
                compact(draft.source_line),
            ]
        )
    )
    draft_opus_values = set(re.findall(r"\bop\.?\s*(\d+)\b", draft_context, re.I))
    if draft_opus_values and not (query_opus_values & draft_opus_values):
        return False
    haystack = normalize_text(normalized_query)
    primary_values = dedupe_text([*getattr(draft, "primary_names_latin", [])[:2], *getattr(draft, "primary_names", [])[:1]])
    if primary_values and not any(name_matches(haystack, value) for value in primary_values if compact(value)):
        return False
    return True


def is_chamber_named_work_solo_cjk_rescue_query(query: str, draft: DraftRecordingEntry) -> bool:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return False
    if getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", []):
        return False
    normalized_query = compact(query)
    if not normalized_query or not contains_cjk(normalized_query):
        return False
    if not query_mentions_requested_work(normalized_query, draft=draft):
        return False
    haystack = normalize_text(normalized_query)
    primary_values = dedupe_text([*getattr(draft, "primary_names", [])[:2], *getattr(draft, "primary_names_latin", [])[:1]])
    if primary_values and not any(name_matches(haystack, value) for value in primary_values if compact(value)):
        return False
    cjk_specific_aliases = [
        alias
        for alias in build_named_work_aliases(draft.work_title_latin or draft.work_title)
        if contains_cjk(alias) and any(marker in alias for marker in ("钢琴奏鸣曲", "小提琴奏鸣曲", "op57", "op.57", "op24", "op.24"))
    ]
    return any(alias in normalized_query for alias in cjk_specific_aliases)


def chamber_named_work_solo_cjk_rescue_rank(query: str) -> tuple[int, int, int]:
    normalized = compact(query)
    return (
        0 if any(marker in normalized for marker in ("钢琴奏鸣曲", "小提琴奏鸣曲")) else 1,
        0 if re.search(r"op\.?\s*(?:24|57)", normalized, re.I) else 1,
        len(normalized),
    )


def build_ensemble_bundle_query_keywords(values: list[str]) -> list[str]:
    stopwords = {
        "orchestra",
        "philharmonic",
        "symphony",
        "ensemble",
        "choir",
        "national",
        "state",
        "royal",
        "the",
        "of",
        "de",
        "la",
    }
    keywords: list[str] = []
    for value in values:
        normalized = compact(value)
        if not normalized:
            continue
        if contains_cjk(normalized):
            for marker in ("交响乐团", "管弦乐团", "爱乐乐团", "乐团"):
                if marker not in normalized:
                    continue
                trimmed = compact(normalized.replace(marker, ""))
                if len(trimmed) >= 2:
                    keywords.append(trimmed)
            continue
        tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", normalized) if token]
        if not tokens:
            continue
        uppercase_tokens = [token for token in tokens if token.isupper() and 2 <= len(token) <= 5]
        if uppercase_tokens:
            keywords.append(uppercase_tokens[0])
        acronym = build_acronym(tokens)
        if 2 <= len(acronym) <= 5:
            keywords.append(acronym)
        significant = [token for token in tokens if token.lower() not in stopwords]
        if significant:
            keywords.append(significant[0])
        if len(significant) >= 2 and len(significant[0]) + len(significant[1]) <= 24:
            keywords.append(f"{significant[0]} {significant[1]}")
    return dedupe_text(keywords)


def build_cjk_ensemble_context_keywords(values: list[str]) -> list[str]:
    markers = (
        "国家爱乐乐团",
        "国家交响乐团",
        "国家管弦乐团",
        "国家乐团",
        "爱乐乐团",
        "交响乐团",
        "管弦乐团",
        "爱乐乐队",
        "交响乐队",
        "管弦乐队",
        "乐团",
        "乐队",
    )
    keywords: list[str] = []
    for value in values:
        normalized = compact(value)
        if not normalized or not contains_cjk(normalized):
            continue
        for marker in markers:
            if marker not in normalized:
                continue
            trimmed = compact(normalized.replace(marker, ""))
            if len(trimmed) >= 2:
                keywords.append(trimmed)
        if 2 <= len(normalized) <= 6:
            keywords.append(normalized)
    return dedupe_text(keywords)


def build_chinese_host_cjk_context_rescue_queries(
    draft: DraftRecordingEntry,
    *,
    ensemble_terms: list[str],
) -> list[str]:
    work_text = normalize_text(draft.work_title_latin or draft.work_title)
    if "concerto" not in work_text:
        return []
    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    if not reference_year:
        return []
    composer_keyword_cjk = extract_cjk_person_query_keyword(draft.composer_name)
    if not composer_keyword_cjk:
        return []
    work_shorthand = f"{composer_keyword_cjk}钢协"
    primary_keywords = dedupe_text(
        extract_cjk_person_query_keyword(value)
        for value in [
            *getattr(draft, "primary_names", []),
            *getattr(draft, "lead_names", [])[:1],
        ]
    )
    secondary_keywords = dedupe_text(
        extract_cjk_person_query_keyword(value)
        for value in [
            *getattr(draft, "secondary_names", []),
            *getattr(draft, "lead_names", [])[1:],
        ]
    )
    ensemble_keywords = build_cjk_ensemble_context_keywords(ensemble_terms)
    if not primary_keywords or not ensemble_keywords:
        return []

    queries: list[str] = []
    for ensemble_keyword in ensemble_keywords[:2]:
        for primary_keyword in primary_keywords[:1]:
            queries.append(f"{primary_keyword} {ensemble_keyword} {reference_year} {work_shorthand}")
            for secondary_keyword in secondary_keywords[:1]:
                if secondary_keyword == primary_keyword:
                    continue
                queries.append(
                    f"{secondary_keyword} {primary_keyword} {ensemble_keyword} {reference_year} {work_shorthand}"
                )
    return dedupe_text(queries)


def build_chinese_host_bundle_context_queries(
    draft: DraftRecordingEntry,
    *,
    ensemble_terms: list[str],
) -> list[str]:
    work_text = normalize_text(draft.work_title_latin or draft.work_title)
    if "concerto" not in work_text:
        return []
    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    if not reference_year:
        return []
    primary_names = prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))[:2]
    primary_keywords = dedupe_text(extract_person_query_keyword(value) for value in primary_names)
    secondary_keywords = dedupe_text(
        extract_person_query_keyword(value)
        for value in prioritize_person_query_terms(
            [
                *getattr(draft, "secondary_names_latin", []),
                *getattr(draft, "lead_names_latin", [])[1:],
            ]
        )
    )
    composer_keyword = extract_person_query_keyword(draft.composer_name_latin or draft.composer_name)
    ensemble_keywords = build_ensemble_bundle_query_keywords(ensemble_terms)
    if not primary_names or not ensemble_keywords:
        return []

    work_keywords = dedupe_text(
        alias
        for alias in build_work_aliases(draft.work_title_latin or draft.work_title)
        if alias in {"piano concerto", "piano concertos", "klavierkonzert"}
    )

    queries: list[str] = []
    for ensemble_keyword in ensemble_keywords[:2]:
        for primary_name in primary_names[:1]:
            if composer_keyword:
                queries.append(f"{primary_name} {ensemble_keyword} {composer_keyword} concerto {reference_year}")
        for primary_keyword in primary_keywords[:2]:
            if composer_keyword:
                queries.append(f"{primary_keyword} {ensemble_keyword} {composer_keyword} concerto {reference_year}")
            for secondary_keyword in secondary_keywords[:1]:
                if secondary_keyword.casefold() == primary_keyword.casefold():
                    continue
                if composer_keyword:
                    queries.append(
                        f"{primary_keyword} {secondary_keyword} {ensemble_keyword} {composer_keyword} concerto {reference_year}"
                    )
            for work_keyword in work_keywords[:2]:
                queries.append(f"{primary_keyword} {ensemble_keyword} {work_keyword} {reference_year}")
    return dedupe_text(queries)


def build_collaboration_surname_rescue_queries(draft: DraftRecordingEntry) -> list[str]:
    work_text = normalize_text(draft.work_title_latin or draft.work_title)
    if "concerto" not in work_text:
        return []
    primary_keywords = dedupe_text(
        [
            extract_person_query_keyword(value)
            for value in prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))
        ]
    )
    secondary_keywords = dedupe_text(
        [
            extract_person_query_keyword(value)
            for value in prioritize_person_query_terms(
                [
                    *getattr(draft, "secondary_names_latin", []),
                    *getattr(draft, "lead_names_latin", [])[1:],
                ]
            )
        ]
    )
    if not primary_keywords or not secondary_keywords:
        return []
    composer_keyword = extract_person_query_keyword(draft.composer_name_latin or draft.composer_name)
    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    work_keywords = dedupe_text(
        alias
        for alias in build_work_aliases(draft.work_title_latin or draft.work_title)
        if alias in {"piano concerto", "klavierkonzert"}
    )

    queries: list[str] = []
    for primary_keyword in primary_keywords[:2]:
        for secondary_keyword in secondary_keywords[:2]:
            if secondary_keyword.casefold() == primary_keyword.casefold():
                continue
            if composer_keyword and reference_year:
                queries.append(f"{primary_keyword} {secondary_keyword} {composer_keyword} concerto {reference_year}")
            if composer_keyword:
                queries.append(f"{primary_keyword} {secondary_keyword} {composer_keyword} concerto")
            if reference_year:
                queries.append(f"{primary_keyword} {secondary_keyword} concerto {reference_year}")
                for work_keyword in work_keywords[:1]:
                    queries.append(f"{primary_keyword} {secondary_keyword} {work_keyword} {reference_year}")
    return dedupe_text(queries)


def build_chamber_named_work_surname_rescue_queries(draft: DraftRecordingEntry) -> list[str]:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return []
    if getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", []):
        return []

    named_aliases = dedupe_text(
        alias
        for alias in build_named_work_aliases(draft.work_title_latin or draft.work_title)
        if looks_latin(alias) and normalize_text(alias) in {"appassionata", "spring"}
    )
    if not named_aliases:
        return []

    primary_keywords = dedupe_text(
        extract_person_query_keyword(value)
        for value in prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))
    )
    if not primary_keywords:
        return []

    composer_keyword = extract_person_query_keyword(draft.composer_name_latin or draft.composer_name)
    composer_surname = compact(composer_keyword).split()[-1] if compact(composer_keyword) else ""
    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)

    queries: list[str] = []
    for primary_keyword in primary_keywords[:2]:
        for named_alias in named_aliases[:1]:
            if composer_surname:
                queries.append(f"{primary_keyword} {named_alias} {composer_surname}")
            if reference_year:
                queries.append(f"{named_alias} {primary_keyword} {reference_year}")
                if composer_surname:
                    queries.append(f"{named_alias} {primary_keyword} {composer_surname} {reference_year}")
    return dedupe_text(queries)


def build_chamber_named_work_full_name_year_rescue_queries(draft: DraftRecordingEntry) -> list[str]:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return []
    if getattr(draft, "secondary_names", []) or getattr(draft, "secondary_names_latin", []):
        return []

    named_aliases = dedupe_text(
        alias
        for alias in build_named_work_aliases(draft.work_title_latin or draft.work_title)
        if looks_latin(alias) and normalize_text(alias) in {"appassionata", "spring"}
    )
    if not named_aliases:
        return []

    reference_year = extract_year(draft.performance_date_text or draft.title or draft.raw_text or draft.source_line)
    if not reference_year:
        return []

    primary_names = dedupe_text(
        compact(value)
        for value in prioritize_person_query_terms(getattr(draft, "primary_names_latin", []))
        if looks_latin(value)
    )
    if not primary_names:
        return []

    queries: list[str] = []
    for primary_name in primary_names[:2]:
        for named_alias in named_aliases[:1]:
            queries.append(f"{named_alias} {primary_name}")
            queries.append(f"{named_alias} {primary_name} {reference_year}")
    return dedupe_text(queries)


def estimate_full_work_min_duration_seconds(draft: DraftRecordingEntry) -> int:
    work_text = normalize_text(f"{draft.work_title_latin} {draft.work_title}")
    if any(token in work_text for token in ("symphony", "交响曲", "concerto", "协奏曲", "variations", "变奏曲")):
        return 900
    if any(token in work_text for token in ("sonata", "奏鸣曲", "quartet", "四重奏", "trio", "三重奏")):
        return 720
    if any(token in work_text for token in ("mass", "requiem", "opera", "suite", "组曲")):
        return 900
    return 0


def name_matches(haystack: str, value: str) -> bool:
    tokens = tokenize(value)
    if not tokens:
        return False
    if contains_tokens(haystack, tokens):
        return True
    surname = tokens[-1]
    if len(surname) >= 4 and surname in haystack:
        return True
    initials = "".join(token[0] for token in tokens if token)
    if len(initials) >= 2:
        initials_pattern = r"\b" + r"[\.\s]+".join(re.escape(char.lower()) for char in initials) + r"\.?\b"
        if re.search(initials_pattern, haystack, re.I):
            return True
    return False


def ensemble_matches(haystack: str, value: str) -> bool:
    tokens = tokenize(value)
    if not tokens:
        return False
    if contains_tokens(haystack, tokens):
        return True
    acronym = build_acronym(tokens)
    compact_haystack = haystack.replace(".", "").replace(" ", "")
    if acronym and acronym.lower() in compact_haystack:
        return True
    if is_compact_acronym(value) and acronym_sequence_matches(haystack, value):
        return True
    significant = [token for token in tokens if token not in {"orchestra", "philharmonic", "symphony", "ensemble", "choir"}]
    return bool(significant) and all(token in haystack for token in significant)


def build_lead_slots(draft: DraftRecordingEntry) -> list[list[str]]:
    slots: list[list[str]] = []
    primary = dedupe_text([*getattr(draft, "primary_names", []), *getattr(draft, "primary_names_latin", [])])
    secondary = dedupe_text([*getattr(draft, "secondary_names", []), *getattr(draft, "secondary_names_latin", [])])
    if primary:
        slots.append(primary)
    if secondary:
        slots.append(secondary)
    if not slots:
        lead_names = draft.lead_names[:2]
        lead_names_latin = draft.lead_names_latin[:2]
        count = max(len(lead_names), len(lead_names_latin))
        for index in range(count):
            slot = dedupe_text(
                [
                    lead_names[index] if index < len(lead_names) else "",
                    lead_names_latin[index] if index < len(lead_names_latin) else "",
                ]
            )
            if slot:
                slots.append(slot)
    return slots


def has_sparse_collaboration_hint(draft: DraftRecordingEntry, lead_slots: list[list[str]]) -> bool:
    title_hint = has_collaboration_marker(draft.title) or has_sparse_title_duo_separator(draft.title)
    if not title_hint or len(lead_slots) < 2:
        return False
    for slot in lead_slots[1:]:
        if any(looks_latin(value) for value in slot):
            continue
        if any(contains_cjk(value) for value in slot):
            return True
    return False


def has_sparse_title_duo_separator(value: str) -> bool:
    normalized = compact(value)
    if " - " not in normalized:
        return False
    segments = [segment.strip() for segment in normalized.split(" - ") if compact(segment)]
    return len(segments) >= 2


def has_explicit_collaborator_marker(value: str) -> bool:
    normalized = compact(value).lower()
    return has_collaboration_marker(normalized) or " - " in normalized or ", " in normalized


def build_acronym(tokens: list[str]) -> str:
    significant = [token for token in tokens if len(token) >= 2]
    if len(significant) < 2:
        return ""
    return "".join(token[0] for token in significant)


def is_compact_acronym(value: str) -> bool:
    normalized = compact(value).replace(".", "").replace(" ", "")
    return 2 <= len(normalized) <= 6 and normalized.isalpha() and normalized.upper() == normalized


def acronym_sequence_matches(haystack: str, value: str) -> bool:
    acronym = compact(value).replace(".", "").replace(" ", "").lower()
    if len(acronym) < 2:
        return False
    words = [token for token in tokenize(haystack) if len(token) >= 2]
    if len(words) < len(acronym):
        return False
    target = list(acronym)
    for index in range(len(words) - len(target) + 1):
        window = words[index : index + len(target)]
        initials = "".join(word[0] for word in window)
        if initials == acronym:
            return True
    return False


def build_work_aliases(value: str) -> set[str]:
    text = compact(value)
    aliases: set[str] = set()
    normalized = normalize_text(text)
    if normalized:
        aliases.add(normalized)
    stripped = normalize_text(strip_catalogue_text(text))
    if stripped:
        aliases.add(stripped)
    stripped_key_text = normalize_text(strip_work_key_text(text))
    if stripped_key_text:
        aliases.add(stripped_key_text)
    aliases.update(build_chinese_work_shorthand_aliases(text, normalized))
    aliases.update(build_generic_work_aliases(text))
    aliases.update(build_keyed_work_aliases(text))
    aliases.update(build_named_work_aliases(text))

    arabic_match = re.search(r"\u7b2c?\s*(\d+)\s*(\u4ea4\u54cd\u66f2|\u534f\u594f\u66f2|\u594f\u9e23\u66f2)", text)
    chinese_match = re.search(
        r"\u7b2c?\s*([\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\u767e\u4e24]+)\s*(\u4ea4\u54cd\u66f2|\u534f\u594f\u66f2|\u594f\u9e23\u66f2)",
        text,
    )
    number = ""
    form = ""
    if arabic_match:
        number = arabic_match.group(1)
        form = arabic_match.group(2)
    elif chinese_match:
        number = str(chinese_number_to_int(chinese_match.group(1)))
        form = chinese_match.group(2)

    if not number or not form:
        return aliases

    english_form = {
        "\u4ea4\u54cd\u66f2": "symphony",
        "\u534f\u594f\u66f2": "concerto",
        "\u594f\u9e23\u66f2": "sonata",
    }.get(form, "")
    if not english_form:
        return aliases

    aliases.add(f"{english_form} no {number}")
    aliases.add(f"{english_form} no. {number}")
    aliases.add(f"{english_form} no.{number}")
    aliases.add(f"{english_form} n {number}")
    aliases.add(f"{english_form} {number}")
    if english_form == "symphony":
        aliases.add(f"sym {number}")
        aliases.add(f"sym{number}")
    return aliases


def strip_work_key_text(value: str) -> str:
    text = compact(value)
    if not text:
        return ""
    stripped = re.sub(r"^[A-Ga-g]\s*(?:\u5927\u8c03|\u5c0f\u8c03)", "", text)
    stripped = re.sub(r"\b(?:in\s+)?[A-Ga-g][#b-]?\s*(?:major|minor|maj|min)\b", "", stripped, flags=re.I)
    stripped = re.sub(r"^[\s,;:()\-]+|[\s,;:()\-]+$", "", stripped)
    return compact(stripped)


def build_generic_work_aliases(value: str) -> set[str]:
    text = compact(value)
    normalized = normalize_text(text)
    aliases: set[str] = set()
    stripped_text = strip_work_key_text(text)
    stripped_normalized = normalize_text(stripped_text)
    if stripped_normalized:
        aliases.add(stripped_normalized)
    if "\u534f\u594f\u66f2" in text and "\u94a2\u7434" in text:
        aliases.add(normalize_text("\u94a2\u7434\u534f\u594f\u66f2"))
        aliases.add(normalize_text("\u94a2\u534f"))
    if "concerto" in normalized and "piano" in normalized:
        aliases.add("piano concerto")
    return {alias for alias in aliases if alias}


def build_chinese_work_shorthand_aliases(text: str, normalized: str) -> set[str]:
    aliases: set[str] = set()
    if "协奏曲" not in text:
        return aliases
    if "piano" not in infer_concerto_instruments(text, normalized):
        return aliases
    aliases.add("钢协")
    cn_match = re.search(r"([A-Ga-g])\s*(大调|小调)", text)
    if cn_match:
        aliases.add(f"{cn_match.group(1).lower()}{cn_match.group(2)}钢协")
    return aliases


def build_keyed_work_aliases(value: str) -> set[str]:
    text = compact(value)
    if not text:
        return set()
    normalized = normalize_text(text)
    aliases: set[str] = set()
    is_concerto = "concerto" in normalized or "协奏曲" in text
    is_sonata = "sonata" in normalized or "奏鸣曲" in text
    is_symphony = "symphony" in normalized or "交响曲" in text

    if "协奏曲" in text:
        is_concerto = True
    if "奏鸣曲" in text:
        is_sonata = True
    if "交响曲" in text:
        is_symphony = True

    key_names = extract_key_aliases(text)
    if not key_names:
        return aliases

    for key_name in key_names:
        if is_concerto:
            concerto_instruments = infer_concerto_instruments(text, normalized)
            for instrument in concerto_instruments:
                aliases.add(f"{instrument} concerto")
                aliases.add(f"{instrument} concerto {key_name}")
                aliases.add(f"{instrument} concerto in {key_name}")
            aliases.add(f"concerto {key_name}")
            if "piano" in concerto_instruments:
                aliases.add(f"klavierkonzert {key_name}")
        if is_sonata:
            aliases.add(f"sonata {key_name}")
            aliases.add(f"sonata in {key_name}")
        if is_symphony:
            aliases.add(f"symphony {key_name}")
            aliases.add(f"symphony in {key_name}")
    return {normalize_text(alias) for alias in aliases if compact(alias)}


def infer_concerto_instruments(text: str, normalized: str) -> list[str]:
    instrument_map = [
        ("violin", ("小提琴", "violin")),
        ("piano", ("钢琴", "鋼琴", "piano", "klavier")),
        ("cello", ("大提琴", "cello")),
        ("flute", ("长笛", "flute")),
    ]
    return [
        label
        for label, markers in instrument_map
        if any(marker in text or marker in normalized for marker in markers)
    ]


def build_named_work_aliases(value: str) -> set[str]:
    text = compact(value)
    normalized = normalize_text(text)
    aliases: set[str] = set()
    nickname_map = {
        "appassionata": ["appassionata", "热情"],
        "spring": ["spring", "春天"],
    }
    for alias, markers in nickname_map.items():
        if any(marker in normalized or marker in text for marker in markers):
            aliases.add(alias)
            if alias == "appassionata":
                aliases.add("热情奏鸣曲")
            if alias == "spring":
                aliases.add("春天奏鸣曲")
    if "piano sonata no 23" in normalized or "op.57" in normalized or "op57" in normalized:
        aliases.add("appassionata")
        aliases.add("热情奏鸣曲")
        aliases.add("第二十三钢琴奏鸣曲")
        aliases.add("第二十三号钢琴奏鸣曲")
        aliases.add("热情op57")
        aliases.add("热情op.57")
    if "violin sonata no 5" in normalized or "op.24" in normalized or "op24" in normalized:
        aliases.add("spring")
        aliases.add("春天奏鸣曲")
    return aliases


def extract_key_aliases(value: str) -> set[str]:
    text = compact(value)
    aliases: set[str] = set()
    latin_match = re.search(r"\b([a-g])\s*(?:-| )?(major|minor|maj|min)\b", text, flags=re.I)
    if latin_match:
        note = latin_match.group(1).lower()
        mode = latin_match.group(2).lower()
        if mode == "maj":
            mode = "major"
        elif mode == "min":
            mode = "minor"
        aliases.add(f"{note} {mode}")
        aliases.add(f"{note}-{mode}")

    cn_match = re.search(r"([A-Ga-g])\s*(大调|小调)", text)
    if cn_match:
        note = cn_match.group(1).lower()
        mode = "major" if cn_match.group(2) == "大调" else "minor"
        aliases.add(f"{note} {mode}")
        aliases.add(f"{note}-{mode}")

    return aliases


def extract_catalogue_markers(value: str) -> set[str]:
    text = normalize_text(value)
    markers: set[str] = set()
    for prefix in ("op", "kv", "k", "bwv", "hob", "d", "wab", "sz", "rv"):
        for number in re.findall(rf"\b{prefix}\s*\.?\s*(\d+[a-z]?)\b", text):
            markers.add(f"{prefix}{number}")
    return markers


def chinese_number_to_int(value: str) -> int:
    digits = {
        "\u96f6": 0,
        "\u4e00": 1,
        "\u4e8c": 2,
        "\u4e24": 2,
        "\u4e09": 3,
        "\u56db": 4,
        "\u4e94": 5,
        "\u516d": 6,
        "\u4e03": 7,
        "\u516b": 8,
        "\u4e5d": 9,
    }
    units = {"\u5341": 10, "\u767e": 100}
    total = 0
    current = 0
    for char in value:
        if char in digits:
            current = digits[char]
        elif char in units:
            unit = units[char]
            total += (current or 1) * unit
            current = 0
    return total + current


def strip_catalogue_text(value: str) -> str:
    return re.sub(r"\b(?:op|k|bwv|hob|d|wab)\.?\s*\d+[a-z]?\b", "", compact(value), flags=re.I).strip(" ,.;:-")


def normalize_text(value: str) -> str:
    normalized = compact(value).lower()
    normalized = re.sub(r"№\s*(\d+)", r" no \1", normalized)
    normalized = re.sub(r"\bn\s*[°º]\s*(\d+)", r" no \1", normalized)
    normalized = re.sub(r"\bno\.?\s*(\d+)", r" no \1", normalized)
    normalized = re.sub(r"\bsym\s*\.?\s*(\d+)", r" sym \1", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def tokenize(value: str) -> list[str]:
    return [
        token
        for token in re.split(r"[^a-z0-9\u4e00-\u9fff]+", normalize_text(value))
        if len(token) >= 2 or token.isdigit()
    ]


def contains_tokens(haystack: str, tokens: list[str]) -> bool:
    return bool(tokens) and all(token in haystack for token in tokens)


def extract_year(value: str) -> str:
    match = re.search(r"(19\d{2}|20\d{2})", value or "")
    return match.group(1) if match else ""


def extract_performance_date_text(value: str) -> str:
    normalized = compact(value)
    if not normalized:
        return ""

    month_names = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    english_month_aliases = {
        "jan": "January",
        "january": "January",
        "feb": "February",
        "february": "February",
        "mar": "March",
        "march": "March",
        "apr": "April",
        "april": "April",
        "may": "May",
        "jun": "June",
        "june": "June",
        "jul": "July",
        "july": "July",
        "aug": "August",
        "august": "August",
        "sep": "September",
        "sept": "September",
        "september": "September",
        "oct": "October",
        "october": "October",
        "nov": "November",
        "november": "November",
        "dec": "December",
        "december": "December",
    }
    english_month_pattern = (
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    )

    cjk_match = re.search(r"((?:19|20)\d{2})\s*年\s*(1[0-2]|0?[1-9])\s*月(?:\s*(3[01]|[12]?\d)\s*日)?", normalized)
    if cjk_match:
        year, month_text, day_text = cjk_match.groups()
        month_name = month_names.get(int(month_text))
        if month_name:
            if day_text:
                return f"{month_name} {int(day_text)}, {year}"
            return f"{month_name}, {year}"

    full_english_match = re.search(
        rf"\b{english_month_pattern}\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,)?\s+((?:19|20)\d{{2}})\b",
        normalized,
        re.I,
    )
    if full_english_match:
        month_text, day_text, year = full_english_match.groups()
        month_name = english_month_aliases.get(month_text.rstrip(".").lower())
        if month_name:
            return f"{month_name} {int(day_text)}, {year}"

    english_month_year_match = re.search(
        rf"\b{english_month_pattern}\.?\s*,?\s+((?:19|20)\d{{2}})\b",
        normalized,
        re.I,
    )
    if english_month_year_match:
        month_text, year = english_month_year_match.groups()
        month_name = english_month_aliases.get(month_text.rstrip(".").lower())
        if month_name:
            return f"{month_name}, {year}"

    return extract_year(normalized)


def extract_year_candidates(value: str) -> list[str]:
    return dedupe_text([match.group(1) for match in re.finditer(r"(19\d{2}|20\d{2})", value or "")])


def extract_year_mentions(value: str) -> set[str]:
    return {match.group(1) for match in re.finditer(r"(19\d{2}|20\d{2})", value or "")}


def extract_performance_context_tokens(value: str) -> list[str]:
    normalized = re.sub(r"(19\d{2}|20\d{2})", " ", value or "")
    tokens = tokenize(normalized)
    stopwords = {
        "live",
        "recorded",
        "recording",
    }
    return [token for token in tokens if token not in stopwords]


def has_specific_date_context_tokens(tokens: list[str]) -> bool:
    month_tokens = {
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    }
    return any(token in month_tokens for token in tokens) and any(token.isdigit() for token in tokens)


def extract_release_date(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2}(?:[-/.]\d{2}){0,2})", value or "")
    return match.group(1) if match else ""


def extract_label(value: str) -> str:
    patterns = [
        re.compile(r"label[:：]?\s*([^|,/.]+)", re.I),
        re.compile(r"厂牌[:：]?\s*([^|,/.]+)", re.I),
        re.compile(r"发行商[:：]?\s*([^|,/.]+)", re.I),
    ]
    for pattern in patterns:
        match = pattern.search(value or "")
        if match:
            return match.group(1).strip()
    return ""


def extract_venue(value: str) -> str:
    patterns = [
        re.compile(r"(?:venue|location|live at)[:：]?\s*([^|,/]+)", re.I),
        re.compile(r"(?:地点|现场|录于)[:：]?\s*([^|,/]+)", re.I),
    ]
    for pattern in patterns:
        match = pattern.search(value or "")
        if match:
            return match.group(1).strip()
    return ""


def looks_like_single_movement(value: str) -> bool:
    lowered = value or ""
    roman_markers = {
        marker.lower()
        for marker in re.findall(r"(?:^|[^a-z])(i{1,3}|iv|v)\.\s", lowered, re.I)
    }
    arabic_markers = {
        marker
        for marker in re.findall(r"(?:^|[\s(:\-–—])([1-9])\.\s", lowered, re.I)
    }
    numbered_markers = {
        marker
        for marker in re.findall(r"\b([1-9][0-9]{0,1})(?:st|nd|rd|th)\s+movement\b", lowered, re.I)
    }
    movement_heading_count = len(roman_markers) + len(arabic_markers) + len(numbered_markers)
    movement_terms = [
        "allegro",
        "adagio",
        "andante",
        "scherzo",
        "rondo",
        "presto",
        "largo",
    ]
    distinct_movement_terms = sum(1 for term in movement_terms if re.search(rf"\b{term}\b", lowered, re.I))
    if distinct_movement_terms >= 3:
        return False
    if movement_heading_count >= 2:
        return False
    if movement_heading_count >= 1 and re.search(r"\b(full|complete)\b", lowered, re.I):
        return False
    if movement_heading_count >= 1 and distinct_movement_terms >= 1:
        return True

    patterns = [
        r":\s*i\.\s",
        r":\s*ii\.\s",
        r":\s*iii\.\s",
        r":\s*iv\.\s",
        r":\s*v\.\s",
        r"1st movement",
        r"2nd movement",
        r"3rd movement",
        r"4th movement",
        r"\bgoldberg variations\b.*:\s*aria\b",
        r"allegro con brio",
        r"andante",
        r"adagio",
        r"scherzo",
        r"\baria\b",
    ]
    return any(re.search(pattern, value, re.I) for pattern in patterns)


def looks_like_first_chapter_extract(value: str) -> bool:
    patterns = [
        r"(?:^|[\s(:\-–—])(i|1)\.\s",
        r"\b1st movement\b",
        r"\bfirst movement\b",
        r"[?&]p=1\b",
    ]
    return any(re.search(pattern, value or "", re.I) for pattern in patterns)


def looks_like_multi_work_compilation(value: str) -> bool:
    patterns = [
        r"nos?\.\s*\d+\s*(?:and|&)\s*\d+",
        r"\bnos?\s*\d+\s*,\s*\d+",
        r"\bnos?\s*\d+\s*/\s*\d+",
        r"symphonies\s+nos?\.",
        r"\bsonatas\b",
        r" overture",
        r" overtures",
        r" works /",
        r" works by",
    ]
    return any(re.search(pattern, value, re.I) for pattern in patterns)
