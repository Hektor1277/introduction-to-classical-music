from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

from app.services.browser_fetcher import PlaywrightBrowserFetcher, should_retry_navigation_error


def test_bilibili_context_options_include_storage_state_when_file_exists(tmp_path: Path) -> None:
    storage_path = tmp_path / "bilibili-state.json"
    storage_path.write_text("{}", encoding="utf-8")
    fetcher = PlaywrightBrowserFetcher(
        bilibili_cookie="SESSDATA=abc",
        bilibili_user_agent="UA/1.0",
        bilibili_storage_state_path=str(storage_path),
    )

    options = fetcher._context_options_for_url("https://search.bilibili.com/all?keyword=test")

    assert options["storage_state"] == str(storage_path)
    assert options["user_agent"] == "UA/1.0"
    assert options["extra_http_headers"]["Cookie"] == "SESSDATA=abc"


def test_bilibili_urls_reuse_shared_context() -> None:
    fetcher = PlaywrightBrowserFetcher()

    assert fetcher._should_reuse_shared_context("https://search.bilibili.com/all?keyword=test") is True
    assert fetcher._should_reuse_shared_context("https://www.bilibili.com/video/BV1abc/") is True
    assert fetcher._should_reuse_shared_context("https://www.youtube.com/results?search_query=test") is False


def test_should_retry_navigation_error_only_for_transient_bilibili_navigation_failures() -> None:
    assert should_retry_navigation_error(
        'net::ERR_ABORTED; maybe frame was detached?',
        "https://search.bilibili.com/all?keyword=test",
    ) is True
    assert should_retry_navigation_error(
        "frame was detached during navigation",
        "https://www.bilibili.com/video/BV1abc/",
    ) is True
    assert should_retry_navigation_error(
        "Target page, context or browser has been closed",
        "https://search.bilibili.com/all?keyword=test",
    ) is False
    assert should_retry_navigation_error(
        "net::ERR_ABORTED",
        "https://www.youtube.com/results?search_query=test",
    ) is False


def test_bilibili_shared_context_isolated_per_event_loop() -> None:
    fetcher = PlaywrightBrowserFetcher()
    fetcher._get_browser = _fake_get_browser  # type: ignore[method-assign]

    async def open_once() -> None:
        async with fetcher._open_page("https://www.bilibili.com/video/BV1abc/"):
            return None

    asyncio.run(open_once())
    asyncio.run(open_once())


def test_browser_instance_isolated_per_event_loop(monkeypatch) -> None:
    state = _FakePlaywrightState()
    fake_module = types.SimpleNamespace(Error=RuntimeError, async_playwright=lambda: _FakePlaywrightManager(state))
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_module)

    fetcher = PlaywrightBrowserFetcher()

    async def open_once() -> None:
        async with fetcher._open_page("https://www.youtube.com/watch?v=test"):
            return None

    asyncio.run(open_once())
    asyncio.run(open_once())

    assert state.launch_count == 2


async def _fake_get_browser():
    return _LoopAgnosticBrowser()


class _LoopAgnosticBrowser:
    async def new_context(self, **kwargs):
        del kwargs
        return _LoopBoundContext()


class _LoopBoundContext:
    def __init__(self) -> None:
        self._owner_loop = asyncio.get_running_loop()

    async def new_page(self):
        if asyncio.get_running_loop() is not self._owner_loop:
            raise RuntimeError("'NoneType' object has no attribute 'send'")
        return _FakePage()

    async def close(self) -> None:
        return None


class _FakePage:
    async def close(self) -> None:
        return None


class _FakePlaywrightState:
    def __init__(self) -> None:
        self.launch_count = 0


class _FakePlaywrightManager:
    def __init__(self, state: _FakePlaywrightState) -> None:
        self._state = state

    async def start(self):
        return _FakePlaywright(self._state)


class _FakePlaywright:
    def __init__(self, state: _FakePlaywrightState) -> None:
        self.chromium = _FakeChromium(state)

    async def stop(self) -> None:
        return None


class _FakeChromium:
    def __init__(self, state: _FakePlaywrightState) -> None:
        self._state = state

    async def launch(self, **kwargs):
        del kwargs
        self._state.launch_count += 1
        return _LoopBoundBrowser()


class _LoopBoundBrowser:
    def __init__(self) -> None:
        self._owner_loop = asyncio.get_running_loop()

    async def new_context(self, **kwargs):
        del kwargs
        if asyncio.get_running_loop() is not self._owner_loop:
            raise RuntimeError("'NoneType' object has no attribute 'send'")
        return _LoopBoundContext()

    async def close(self) -> None:
        return None
