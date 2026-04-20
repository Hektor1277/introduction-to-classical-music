from __future__ import annotations

import json
from pathlib import Path

from app.services.platform_search_config import load_platform_search_config


def test_load_platform_search_config_reads_platform_credentials(tmp_path: Path) -> None:
    path = tmp_path / "platform-search.local.json"
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "youtube": {
                    "enabled": True,
                    "apiKey": "yt-secret",
                    "regionCode": "US",
                    "maxResults": 8,
                },
                "appleMusic": {
                    "enabled": True,
                    "developerToken": "apple-secret",
                    "storefront": "jp",
                    "useItunesFallback": True,
                },
                "bilibili": {
                    "enabled": True,
                    "cookie": "SESSDATA=abc; buvid3=def",
                    "userAgent": "TestAgent/1.0",
                    "referer": "https://www.bilibili.com",
                    "storageStatePath": "config/bilibili-state.json",
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_platform_search_config(path)

    assert config.enabled is True
    assert config.youtube.api_key == "yt-secret"
    assert config.youtube.region_code == "US"
    assert config.youtube.max_results == 8
    assert config.apple_music.developer_token == "apple-secret"
    assert config.apple_music.storefront == "jp"
    assert config.apple_music.use_itunes_fallback is True
    assert config.bilibili.cookie == "SESSDATA=abc; buvid3=def"
    assert config.bilibili.user_agent == "TestAgent/1.0"
    assert config.bilibili.storage_state_path == "config/bilibili-state.json"


def test_load_platform_search_config_allows_environment_overrides(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "platform-search.local.json"
    path.write_text(json.dumps({"enabled": True, "youtube": {"enabled": True, "apiKey": "old"}}), encoding="utf-8")
    monkeypatch.setenv("RECORDING_RETRIEVAL_YOUTUBE_API_KEY", "env-youtube")
    monkeypatch.setenv("RECORDING_RETRIEVAL_APPLE_DEVELOPER_TOKEN", "env-apple")
    monkeypatch.setenv("RECORDING_RETRIEVAL_BILIBILI_COOKIE", "SESSDATA=env-cookie")

    config = load_platform_search_config(path)

    assert config.youtube.api_key == "env-youtube"
    assert config.apple_music.developer_token == "env-apple"
    assert config.bilibili.cookie == "SESSDATA=env-cookie"


def test_load_platform_search_config_ignores_placeholder_values(tmp_path: Path) -> None:
    path = tmp_path / "platform-search.local.json"
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "youtube": {"enabled": True, "apiKey": "AIza-real"},
                "appleMusic": {"enabled": True, "developerToken": "YOUR_APPLE_MUSIC_DEVELOPER_TOKEN"},
                "bilibili": {"enabled": True, "cookie": "SESSDATA=...; buvid3=..."},
            }
        ),
        encoding="utf-8",
    )

    config = load_platform_search_config(path)

    assert config.youtube.api_key == "AIza-real"
    assert config.apple_music.developer_token == ""
    assert config.bilibili.cookie == ""
