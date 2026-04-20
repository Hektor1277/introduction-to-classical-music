from __future__ import annotations

import atexit
import asyncio
import hashlib
import time
import sys
import weakref
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


class BrowserFetchUnavailable(RuntimeError):
    pass


BROWSER_DIAGNOSTIC_MAX_FILES = 40
BROWSER_DIAGNOSTIC_MAX_TOTAL_BYTES = 20 * 1024 * 1024
BROWSER_DIAGNOSTIC_MAX_AGE_SECONDS = 7 * 24 * 60 * 60

_SEARCH_LINK_PAYLOAD_SCRIPT = """(patterns) => {
    const regexes = (patterns || []).map((pattern) => new RegExp(pattern, 'i'));
    const toHref = (node) => {
      try {
        return new URL(node.getAttribute('href') || '', window.location.href).href;
      } catch (error) {
        return '';
      }
    };
    const anchors = Array.from(document.querySelectorAll('a[href]'));
    const values = anchors
      .map((node) => toHref(node))
      .filter((href) => /^https?:/i.test(href));
    const matches = (href) => !regexes.length || regexes.some((pattern) => pattern.test(href));
    const filtered = values.filter((href) => matches(href));
    const host = window.location.hostname || '';
    let resultCardLinks = [];
    if (host.includes('search.bilibili.com')) {
      const selectors = [
        '.bili-video-card a[href]',
        '.video-list-item a[href]',
        '[class*="video-card"] a[href]',
        '[class*="video-item"] a[href]',
        '[class*="search-result"] a[href]',
      ];
      const collected = selectors.flatMap((selector) =>
        Array.from(document.querySelectorAll(selector)).map((node) => toHref(node))
      );
      resultCardLinks = collected.filter((href) => /^https?:/i.test(href) && matches(href));
    }
    const bodyText = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
    return {
      title: String(document.title || '').trim(),
      allLinks: Array.from(new Set(filtered)),
      resultCardLinks: Array.from(new Set(resultCardLinks)),
      anchorCount: values.length,
      bodyTextSample: bodyText.slice(0, 500),
    };
}"""


@dataclass(slots=True)
class BrowserPageSnapshot:
    title: str
    description: str
    body_text: str
    image_url: str
    uploader: str
    bvid: str
    duration_seconds: int
    view_count: int


@dataclass(slots=True)
class _LoopBrowserState:
    browser: Any = None
    playwright: Any = None
    playwright_manager: Any = None
    shared_bilibili_context: Any = None
    browser_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    context_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PlaywrightBrowserFetcher:
    def __init__(
        self,
        max_concurrency: int = 2,
        *,
        bilibili_cookie: str = "",
        bilibili_user_agent: str = "",
        bilibili_referer: str = "https://www.bilibili.com",
        bilibili_storage_state_path: str = "",
    ) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._bilibili_cookie = bilibili_cookie.strip()
        self._bilibili_user_agent = bilibili_user_agent.strip()
        self._bilibili_referer = bilibili_referer.strip() or "https://www.bilibili.com"
        self._bilibili_storage_state_path = bilibili_storage_state_path.strip()
        self._loop_states: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopBrowserState] = weakref.WeakKeyDictionary()
        self._atexit_registered = False
        atexit.register(self._aclose_sync)
        self._atexit_registered = True

    async def fetch_page(self, url: str, timeout_seconds: float | None = None) -> dict[str, str]:
        async with self._semaphore:
            effective_timeout = timeout_seconds or 8.0
            return await self._run_with_timeout(self._fetch_page_inner(url, timeout_seconds=effective_timeout), effective_timeout)

    async def fetch_links(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        async with self._semaphore:
            effective_timeout = timeout_seconds or 8.0
            return await self._run_with_timeout(
                self._fetch_links_inner(url, url_patterns or [], timeout_seconds=effective_timeout),
                effective_timeout,
            )

    async def fetch_search_evidence(
        self,
        url: str,
        *,
        url_patterns: list[str] | None = None,
        timeout_seconds: float | None = None,
        capture_screenshot: bool = False,
    ) -> dict[str, Any]:
        async with self._semaphore:
            effective_timeout = timeout_seconds or 8.0
            return await self._run_with_timeout(
                self._fetch_search_evidence_inner(
                    url,
                    url_patterns or [],
                    timeout_seconds=effective_timeout,
                    capture_screenshot=capture_screenshot,
                ),
                effective_timeout,
            )

    async def _run_with_timeout(self, coroutine: Any, timeout_seconds: float):
        task = asyncio.create_task(coroutine)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_seconds)
        except asyncio.TimeoutError as error:
            task.cancel()
            task.add_done_callback(_silence_task_exception)
            with suppress(asyncio.CancelledError, Exception):
                await task
            raise TimeoutError("browser fetch timed out") from error

    async def _fetch_page_inner(self, url: str, *, timeout_seconds: float) -> dict[str, str]:
        try:
            from playwright.async_api import Error as PlaywrightError
        except ImportError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable("playwright is not installed") from error

        try:
            async with self._open_page(url) as page:
                await self._goto_with_retry(page, url, PlaywrightError, timeout_seconds=timeout_seconds)
                await self._wait_for_page_snapshot(page, url)

                snapshot = BrowserPageSnapshot(
                    **(
                        await page.evaluate(
                            """() => {
                                const host = window.location.hostname || '';
                                const fromMeta = () => (
                                  document.querySelector('meta[property="og:description"]')?.content
                                  || document.querySelector('meta[name="description"]')?.content
                                  || ''
                                ).trim();
                                const imageFromMeta = () => {
                                  const fromMeta = document.querySelector('meta[property="og:image"]')?.content
                                    || document.querySelector('meta[name="twitter:image"]')?.content;
                                  if (fromMeta) {
                                    return fromMeta.trim();
                                  }
                                  const image = Array.from(document.images || [])
                                    .find((node) => node?.src && node.width >= 240 && node.height >= 240);
                                  return image?.src?.trim() || '';
                                };
                                if (host.includes('bilibili.com')) {
                                  const video = window.__INITIAL_STATE__?.videoData || {};
                                  const owner = video.owner?.name || '';
                                  const description = (video.desc || fromMeta()).trim();
                                  const bodyParts = [
                                    video.title || document.title || '',
                                    description,
                                    owner,
                                    Array.isArray(video.pages) ? video.pages.map((item) => item?.part || '').join(' ') : '',
                                  ];
                                  return {
                                    title: String(video.title || document.title || '').trim(),
                                    description,
                                    body_text: bodyParts.filter(Boolean).join(' ').slice(0, 4000).trim(),
                                    image_url: String(video.pic || imageFromMeta()).trim(),
                                    uploader: String(owner || '').trim(),
                                    bvid: String(video.bvid || '').trim(),
                                    duration_seconds: Number(video.duration || 0) || 0,
                                    view_count: Number(video.stat?.view || 0) || 0,
                                  };
                                }
                                const text = document.body?.innerText || '';
                                return {
                                  title: String(document.title || '').trim(),
                                  description: fromMeta(),
                                  body_text: text.slice(0, 4000).trim(),
                                  image_url: imageFromMeta(),
                                  uploader: '',
                                  bvid: '',
                                  duration_seconds: 0,
                                  view_count: 0,
                                };
                            }""",
                        )
                    ),
                )
                return {
                    "title": snapshot.title,
                    "description": snapshot.description,
                    "bodyText": snapshot.body_text,
                    "imageUrl": snapshot.image_url,
                    "uploader": snapshot.uploader,
                    "bvid": snapshot.bvid,
                    "durationSeconds": snapshot.duration_seconds,
                    "viewCount": snapshot.view_count,
                }
        except PlaywrightError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable(str(error)) from error

    async def _fetch_links_inner(self, url: str, url_patterns: list[str], *, timeout_seconds: float) -> list[str]:
        try:
            from playwright.async_api import Error as PlaywrightError
        except ImportError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable("playwright is not installed") from error

        try:
            async with self._open_page(url) as page:
                await self._goto_with_retry(page, url, PlaywrightError, timeout_seconds=timeout_seconds)
                await self._wait_for_link_results(page, url)

                payload = await page.evaluate(
                    _SEARCH_LINK_PAYLOAD_SCRIPT,
                    url_patterns,
                )
                normalized = normalize_search_result_payload(url, payload)
                return [str(value).strip() for value in normalized.get("matchedLinks") or [] if str(value or "").strip()]
        except PlaywrightError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable(str(error)) from error

    async def _fetch_search_evidence_inner(
        self,
        url: str,
        url_patterns: list[str],
        *,
        timeout_seconds: float,
        capture_screenshot: bool,
    ) -> dict[str, Any]:
        try:
            from playwright.async_api import Error as PlaywrightError
        except ImportError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable("playwright is not installed") from error

        try:
            async with self._open_page(url) as page:
                await self._goto_with_retry(page, url, PlaywrightError, timeout_seconds=timeout_seconds)
                await self._wait_for_link_results(page, url)
                payload = await page.evaluate(
                    _SEARCH_LINK_PAYLOAD_SCRIPT,
                    url_patterns,
                )
                normalized = normalize_search_result_payload(url, payload)
                html_length = len(await page.content())
                screenshot_path = ""
                if capture_screenshot:
                    screenshot_path = await self._save_search_screenshot(page, url)
                return {
                    "title": str(normalized.get("title") or "").strip(),
                    "matchedLinks": [
                        str(value).strip()
                        for value in normalized.get("matchedLinks") or []
                        if str(value or "").strip()
                    ],
                    "matchedLinkCount": int(normalized.get("matchedLinkCount", 0) or 0),
                    "anchorCount": int(normalized.get("anchorCount", 0) or 0),
                    "resultCardCount": int(normalized.get("resultCardCount", 0) or 0),
                    "extractionMode": str(normalized.get("extractionMode") or "").strip(),
                    "bodyTextSample": str(normalized.get("bodyTextSample") or "").strip(),
                    "htmlLength": html_length,
                    "screenshotPath": screenshot_path,
                }
        except PlaywrightError as error:  # pragma: no cover - environment dependent
            raise BrowserFetchUnavailable(str(error)) from error

    async def aclose(self) -> None:
        for state in list(self._loop_states.values()):
            async with state.context_lock:
                await self._safe_close(state.shared_bilibili_context)
                state.shared_bilibili_context = None
            async with state.browser_lock:
                await self._safe_close(state.browser)
                state.browser = None
                with suppress(Exception):
                    if state.playwright is not None:
                        await state.playwright.stop()
                state.playwright_manager = None
                state.playwright = None
        self._loop_states = weakref.WeakKeyDictionary()
        if self._atexit_registered:
            with suppress(Exception):
                atexit.unregister(self._aclose_sync)
            self._atexit_registered = False

    def _aclose_sync(self) -> None:
        if all(
            state.browser is None and state.shared_bilibili_context is None and state.playwright_manager is None
            for state in self._loop_states.values()
        ):
            return
        with suppress(BaseException):
            asyncio.run(self.aclose())

    def _get_loop_state(self) -> _LoopBrowserState:
        loop = asyncio.get_running_loop()
        state = self._loop_states.get(loop)
        if state is None:
            state = _LoopBrowserState()
            self._loop_states[loop] = state
        return state

    async def _get_browser(self) -> Any:
        state = self._get_loop_state()
        if state.browser is not None:
            return state.browser
        async with state.browser_lock:
            if state.browser is not None:
                return state.browser
            try:
                from playwright.async_api import async_playwright
            except ImportError as error:  # pragma: no cover - environment dependent
                raise BrowserFetchUnavailable("playwright is not installed") from error
            state.playwright_manager = async_playwright()
            state.playwright = await state.playwright_manager.start()
            launch_options: dict[str, Any] = {"headless": True}
            if sys.platform == "win32":
                launch_options["channel"] = "msedge"
            state.browser = await state.playwright.chromium.launch(**launch_options)
            return state.browser

    async def _get_context(self, url: str) -> tuple[Any, bool]:
        browser = await self._get_browser()
        if self._should_reuse_shared_context(url):
            state = self._get_loop_state()
            async with state.context_lock:
                if state.shared_bilibili_context is None:
                    state.shared_bilibili_context = await browser.new_context(**self._context_options_for_url(url))
                return state.shared_bilibili_context, True
        return await browser.new_context(**self._context_options_for_url(url)), False

    @asynccontextmanager
    async def _open_page(self, url: str):
        context, shared_context = await self._get_context(url)
        page = None
        try:
            page = await context.new_page()
            yield page
        finally:
            await self._safe_close(page)
            if not shared_context:
                await self._safe_close(context)

    def _context_options_for_url(self, url: str) -> dict[str, Any]:
        host = urlparse(url).netloc.lower()
        options: dict[str, Any] = {
            "viewport": {"width": 1440, "height": 960},
        }
        if "bilibili.com" not in host and "b23.tv" not in host:
            return options

        headers = {
            "Referer": self._bilibili_referer,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        if self._bilibili_cookie:
            headers["Cookie"] = self._bilibili_cookie
        options["extra_http_headers"] = headers
        options["locale"] = "zh-CN"
        if self._bilibili_user_agent:
            options["user_agent"] = self._bilibili_user_agent
        if self._bilibili_storage_state_path:
            storage_state = Path(self._bilibili_storage_state_path)
            if storage_state.is_file():
                options["storage_state"] = str(storage_state)
        return options

    def _should_reuse_shared_context(self, url: str) -> bool:
        host = urlparse(url).netloc.lower()
        return "bilibili.com" in host or "b23.tv" in host

    async def _wait_for_page_snapshot(self, page: Any, url: str) -> None:
        host = urlparse(url).netloc.lower()
        if "bilibili.com" in host:
            with suppress(Exception):
                await page.wait_for_function(
                    "() => Boolean(window.__INITIAL_STATE__ && window.__INITIAL_STATE__.videoData)",
                    timeout=4000,
                )
            await page.wait_for_timeout(800)
            return
        await page.wait_for_timeout(300)

    async def _wait_for_link_results(self, page: Any, url: str) -> None:
        host = urlparse(url).netloc.lower()
        if "search.bilibili.com" in host:
            with suppress(Exception):
                await page.wait_for_function(
                    """() => Array.from(document.querySelectorAll('a[href]')).some((node) => {
                        const href = node?.href || '';
                        return /\\/video\\/(?:BV[0-9A-Za-z]+|av\\d+)/i.test(href);
                    })""",
                    timeout=4500,
                )
            await page.wait_for_timeout(700)
            return
        await page.wait_for_timeout(800)

    async def _goto_with_retry(
        self,
        page: Any,
        url: str,
        playwright_error: type[Exception],
        *,
        timeout_seconds: float,
    ) -> None:
        goto_timeout_ms = max(5000, int(timeout_seconds * 1000))
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            return
        except playwright_error as error:
            if not should_retry_navigation_error(str(error), url):
                raise
        await page.wait_for_timeout(450)
        await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)

    async def _safe_close(self, handle: Any) -> None:
        with suppress(Exception):
            if handle is not None:
                await handle.close()

    async def _save_search_screenshot(self, page: Any, url: str) -> str:
        output_dir = Path(__file__).resolve().parents[2] / "output" / "browser-diagnostics"
        output_dir.mkdir(parents=True, exist_ok=True)
        parsed = urlparse(url)
        host_slug = _slugify_filename_fragment(parsed.netloc or "page")
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
        target = output_dir / f"{host_slug}-{digest}.png"
        with suppress(Exception):
            await page.screenshot(path=str(target), full_page=True)
        prune_browser_diagnostic_files(output_dir)
        return str(target) if target.exists() else ""


def _silence_task_exception(task: asyncio.Task) -> None:
    with suppress(BaseException):
        task.exception()


def should_retry_navigation_error(message: str, url: str) -> bool:
    host = urlparse(url).netloc.lower()
    if "bilibili.com" not in host and "b23.tv" not in host:
        return False
    lowered = (message or "").lower()
    return "err_aborted" in lowered or "frame was detached" in lowered


def _slugify_filename_fragment(value: str) -> str:
    cleaned = "".join(character if character.isalnum() else "-" for character in (value or "").strip().lower())
    collapsed = "-".join(segment for segment in cleaned.split("-") if segment)
    return collapsed or "page"


def normalize_search_result_payload(url: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = payload or {}
    all_links = _clean_http_links(raw.get("allLinks") or raw.get("matchedLinks") or [])
    result_card_links = _clean_http_links(raw.get("resultCardLinks") or [])
    host = urlparse(url).netloc.lower()

    extraction_mode = "anchor-scan"
    matched_links = list(all_links)
    if "search.bilibili.com" in host:
        all_links = [value for value in all_links if _is_bilibili_video_link(value)]
        result_card_links = [value for value in result_card_links if _is_bilibili_video_link(value)]
        matched_links = list(all_links)
    if "search.bilibili.com" in host and result_card_links:
        matched_links = _dedupe_preserve_order([*result_card_links, *all_links])
        extraction_mode = "result-card-priority"

    return {
        "title": str(raw.get("title") or "").strip(),
        "matchedLinks": matched_links[:8],
        "matchedLinkCount": len(matched_links),
        "anchorCount": int(raw.get("anchorCount", 0) or 0),
        "resultCardCount": len(result_card_links),
        "extractionMode": extraction_mode,
        "bodyTextSample": str(raw.get("bodyTextSample") or "").strip(),
    }


def prune_browser_diagnostic_files(
    output_dir: Path,
    *,
    max_files: int = BROWSER_DIAGNOSTIC_MAX_FILES,
    max_total_bytes: int = BROWSER_DIAGNOSTIC_MAX_TOTAL_BYTES,
    max_age_seconds: float = BROWSER_DIAGNOSTIC_MAX_AGE_SECONDS,
) -> None:
    files = [path for path in output_dir.glob("*.png") if path.is_file()]
    if not files:
        return

    now = time.time()
    for path in files:
        with suppress(OSError):
            if max_age_seconds > 0 and now - path.stat().st_mtime > max_age_seconds:
                path.unlink(missing_ok=True)

    files = [path for path in output_dir.glob("*.png") if path.is_file()]
    if not files:
        return

    files.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)

    if max_files > 0:
        for stale in files[max_files:]:
            with suppress(OSError):
                stale.unlink(missing_ok=True)
        files = files[:max_files]

    if max_total_bytes <= 0:
        for path in files:
            with suppress(OSError):
                path.unlink(missing_ok=True)
        return

    total_size = 0
    kept: list[Path] = []
    for path in files:
        with suppress(OSError):
            size = path.stat().st_size
            if total_size + size <= max_total_bytes or not kept:
                kept.append(path)
                total_size += size
                continue
            path.unlink(missing_ok=True)


def _clean_http_links(values: list[Any]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text.startswith("http://") or text.startswith("https://"):
            cleaned.append(text)
    return _dedupe_preserve_order(cleaned)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        normalized = value.rstrip("/")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(value)
    return deduped


def _is_bilibili_video_link(value: str) -> bool:
    lowered = value.lower()
    return "/video/bv" in lowered or "/video/av" in lowered
