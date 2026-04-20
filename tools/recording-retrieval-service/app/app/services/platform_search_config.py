from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def runtime_config_root() -> Path:
    app_data_dir = str(os.getenv("ICM_APP_DATA_DIR", "")).strip()
    if app_data_dir:
        return Path(app_data_dir).resolve() / "retrieval-service" / "config"
    return Path(__file__).resolve().parents[2] / "config"


@dataclass(slots=True)
class YouTubeSearchConfig:
    enabled: bool = True
    api_key: str = ""
    region_code: str = "US"
    max_results: int = 8


@dataclass(slots=True)
class AppleMusicSearchConfig:
    enabled: bool = True
    developer_token: str = ""
    storefront: str = "us"
    use_itunes_fallback: bool = True


@dataclass(slots=True)
class BilibiliSearchConfig:
    enabled: bool = True
    cookie: str = ""
    user_agent: str = ""
    referer: str = "https://www.bilibili.com"
    storage_state_path: str = ""


@dataclass(slots=True)
class PlatformSearchConfig:
    enabled: bool = True
    youtube: YouTubeSearchConfig = field(default_factory=YouTubeSearchConfig)
    apple_music: AppleMusicSearchConfig = field(default_factory=AppleMusicSearchConfig)
    bilibili: BilibiliSearchConfig = field(default_factory=BilibiliSearchConfig)


def default_platform_search_config_path() -> Path:
    return runtime_config_root() / "platform-search.local.json"


def load_platform_search_config(path: Path | None = None) -> PlatformSearchConfig:
    config_path = path or default_platform_search_config_path()
    payload: dict[str, Any] = {}
    if config_path.is_file():
        payload = json.loads(config_path.read_text(encoding="utf-8"))

    youtube_payload = dict(payload.get("youtube") or {})
    apple_payload = dict(payload.get("appleMusic") or payload.get("apple_music") or {})
    bilibili_payload = dict(payload.get("bilibili") or {})

    storage_state_path = _string_value(
        os.getenv("RECORDING_RETRIEVAL_BILIBILI_STORAGE_STATE_PATH", ""),
        bilibili_payload.get("storageStatePath"),
    )

    return PlatformSearchConfig(
        enabled=_bool_value(payload.get("enabled"), True),
        youtube=YouTubeSearchConfig(
            enabled=_bool_value(youtube_payload.get("enabled"), True),
            api_key=_string_value(os.getenv("RECORDING_RETRIEVAL_YOUTUBE_API_KEY", ""), youtube_payload.get("apiKey")),
            region_code=_string_value(os.getenv("RECORDING_RETRIEVAL_YOUTUBE_REGION_CODE", ""), youtube_payload.get("regionCode"), "US"),
            max_results=_int_value(os.getenv("RECORDING_RETRIEVAL_YOUTUBE_MAX_RESULTS", ""), youtube_payload.get("maxResults"), 8, minimum=1, maximum=25),
        ),
        apple_music=AppleMusicSearchConfig(
            enabled=_bool_value(apple_payload.get("enabled"), True),
            developer_token=_string_value(
                os.getenv("RECORDING_RETRIEVAL_APPLE_DEVELOPER_TOKEN", ""),
                apple_payload.get("developerToken"),
            ),
            storefront=_string_value(os.getenv("RECORDING_RETRIEVAL_APPLE_STOREFRONT", ""), apple_payload.get("storefront"), "us"),
            use_itunes_fallback=_bool_value(
                _bool_from_env(os.getenv("RECORDING_RETRIEVAL_APPLE_USE_ITUNES_FALLBACK"), True),
                apple_payload.get("useItunesFallback"),
            ),
        ),
        bilibili=BilibiliSearchConfig(
            enabled=_bool_value(bilibili_payload.get("enabled"), True),
            cookie=_string_value(os.getenv("RECORDING_RETRIEVAL_BILIBILI_COOKIE", ""), bilibili_payload.get("cookie")),
            user_agent=_string_value(
                os.getenv("RECORDING_RETRIEVAL_BILIBILI_USER_AGENT", ""),
                bilibili_payload.get("userAgent"),
            ),
            referer=_string_value(
                os.getenv("RECORDING_RETRIEVAL_BILIBILI_REFERER", ""),
                bilibili_payload.get("referer"),
                "https://www.bilibili.com",
            ),
            storage_state_path=storage_state_path,
        ),
    )


def _string_value(*values: Any) -> str:
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or _looks_like_placeholder(cleaned):
            continue
        return cleaned
    return ""


def _bool_value(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"", "0", "false", "no", "off"}


def _bool_from_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return _bool_value(value, default)


def _int_value(*values: Any, minimum: int, maximum: int) -> int:
    for value in values:
        if value is None or str(value).strip() == "":
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        return max(minimum, min(maximum, parsed))
    return minimum


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip()
    if not normalized:
        return True
    upper = normalized.upper()
    return (
        "YOUR_" in upper
        or normalized == "..."
        or "...;" in normalized
        or "SESSDATA=..." in normalized
        or "BUVID3=..." in upper
    )
