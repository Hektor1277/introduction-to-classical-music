from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from dataclasses import field
from urllib.parse import quote

import httpx

from app.services.platform_search_config import PlatformSearchConfig


def parse_bilibili_duration_seconds(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value).strip()
    if not text:
        return 0
    if text.isdigit():
        return max(0, int(text))
    if ":" not in text:
        return 0
    parts = text.split(":")
    if not all(part.isdigit() for part in parts):
        return 0
    total = 0
    for part in parts:
        total = total * 60 + int(part)
    return max(0, total)


@dataclass(slots=True)
class ApiSearchResult:
    endpoint_url: str
    links: list[str]
    rows: list[dict[str, object]] = field(default_factory=list)


@dataclass(slots=True)
class BilibiliVideoDetail:
    endpoint_url: str
    title: str
    description: str
    image_url: str
    uploader: str
    bvid: str
    duration_seconds: int
    view_count: int
    page_parts: list[str]


WBI_MIXIN_KEY_INDEX = [
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    48,
    7,
    16,
    24,
    55,
    40,
    61,
    26,
    17,
    0,
    1,
    60,
    51,
    30,
    4,
    22,
    25,
    54,
    21,
    56,
    59,
    6,
    63,
    57,
    62,
    11,
    36,
    20,
    34,
    44,
    52,
]


class PlatformSearchClients:
    def __init__(self, config: PlatformSearchConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
        self._bilibili_wbi_keys: tuple[str, str] | None = None
        self._bilibili_wbi_key_time = 0.0
        self._bilibili_seeded = False

    async def search_youtube(self, query: str, *, result_limit: int) -> ApiSearchResult:
        endpoint = "https://www.googleapis.com/youtube/v3/search"
        response = await self._client.get(
            endpoint,
            params={
                "part": "snippet",
                "type": "video",
                "q": query,
                "key": self._config.youtube.api_key,
                "maxResults": min(max(1, result_limit), self._config.youtube.max_results),
                "regionCode": self._config.youtube.region_code,
            },
        )
        response.raise_for_status()
        payload = response.json()
        links = []
        for item in payload.get("items", []):
            video_id = str(((item.get("id") or {}).get("videoId") or "")).strip()
            if video_id:
                links.append(f"https://www.youtube.com/watch?v={video_id}")
        return ApiSearchResult(endpoint_url=str(response.request.url), links=links)

    async def search_apple_music(self, query: str, *, result_limit: int) -> ApiSearchResult:
        endpoint = f"https://api.music.apple.com/v1/catalog/{self._config.apple_music.storefront}/search"
        response = await self._client.get(
            endpoint,
            params={
                "term": query,
                "types": "songs,albums",
                "limit": min(max(1, result_limit), 25),
            },
            headers={
                "authorization": f"Bearer {self._config.apple_music.developer_token}",
                "origin": "https://music.apple.com",
            },
        )
        response.raise_for_status()
        payload = response.json()
        links: list[str] = []
        rows: list[dict[str, object]] = []
        for bucket_name in ("songs", "albums", "playlists"):
            bucket = (((payload.get("results") or {}).get(bucket_name) or {}).get("data") or [])
            for item in bucket:
                attributes = (item.get("attributes") or {}) if isinstance(item, dict) else {}
                url = str(attributes.get("url") or "").strip()
                if url:
                    links.append(url)
                    title = str(attributes.get("name") or "").strip()
                    artist_name = str(attributes.get("artistName") or "").strip()
                    album_name = str(attributes.get("albumName") or "").strip()
                    composer_name = str(attributes.get("composerName") or "").strip()
                    editorial_notes = attributes.get("editorialNotes") if isinstance(attributes.get("editorialNotes"), dict) else {}
                    note_text = str(
                        editorial_notes.get("standard") or editorial_notes.get("short") or ""
                    ).strip()
                    description = " | ".join(
                        part for part in [artist_name, album_name, composer_name, note_text] if part
                    )
                    rows.append(
                        {
                            "url": url,
                            "title": title,
                            "description": description,
                            "uploader": artist_name,
                            "duration_seconds": int(attributes.get("durationInMillis") or 0) // 1000,
                        }
                    )
        return ApiSearchResult(endpoint_url=str(response.request.url), links=links, rows=rows)

    async def search_apple_music_public(self, query: str, *, result_limit: int) -> ApiSearchResult:
        endpoint = "https://itunes.apple.com/search"
        response = await self._client.get(
            endpoint,
            params={
                "term": query,
                "media": "music",
                "limit": min(max(1, result_limit), 25),
            },
        )
        response.raise_for_status()
        payload = response.json()
        links: list[str] = []
        rows: list[dict[str, object]] = []
        for item in payload.get("results", []):
            url = str(item.get("trackViewUrl") or item.get("collectionViewUrl") or "").strip()
            if not url:
                continue
            links.append(url)
            title = str(item.get("trackName") or item.get("collectionName") or "").strip()
            artist_name = str(item.get("artistName") or "").strip()
            collection_name = str(item.get("collectionName") or "").strip()
            genre_name = str(item.get("primaryGenreName") or "").strip()
            release_date = str(item.get("releaseDate") or "").strip()
            description = " | ".join(
                part for part in [artist_name, collection_name, genre_name, release_date] if part
            )
            rows.append(
                {
                    "url": url,
                    "title": title,
                    "description": description,
                    "uploader": artist_name,
                    "duration_seconds": int(item.get("trackTimeMillis") or 0) // 1000,
                }
            )
        return ApiSearchResult(endpoint_url=str(response.request.url), links=links, rows=rows)

    async def search_bilibili(self, query: str, *, result_limit: int) -> ApiSearchResult:
        await self._seed_bilibili_session()
        endpoint = "https://api.bilibili.com/x/web-interface/wbi/search/type"
        img_key, sub_key = await self._load_bilibili_wbi_keys()
        mixin_key = self._derive_bilibili_mixin_key(img_key, sub_key)
        params = self._sign_bilibili_wbi_params(
            {
                "search_type": "video",
                "keyword": query,
                "page": 1,
                "page_size": min(max(1, result_limit), 20),
                "order": "totalrank",
            },
            mixin_key,
        )
        response = await self._client.get(
            endpoint,
            params=params,
            headers=self._bilibili_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        code = int(payload.get("code") or 0)
        if code != 0:
            message = str(payload.get("message") or payload.get("msg") or "unknown error").strip()
            raise RuntimeError(f"Bilibili WBI search failed with code {code}: {message}")
        links = []
        rows: list[dict[str, object]] = []
        for item in (((payload.get("data") or {}).get("result") or [])[:result_limit]):
            url = str(item.get("arcurl") or "").strip()
            if url:
                links.append(url)
                rows.append(
                    {
                        "url": url,
                        "title": str(item.get("title") or "").strip(),
                        "description": str(item.get("description") or item.get("desc") or "").strip(),
                        "uploader": str(item.get("author") or item.get("up_name") or "").strip(),
                        "duration_seconds": parse_bilibili_duration_seconds(item.get("duration")),
                        "view_count": int(item.get("play") or item.get("view") or 0),
                        "bvid": str(item.get("bvid") or "").strip(),
                    }
                )
        return ApiSearchResult(endpoint_url=str(response.request.url), links=links, rows=rows)

    async def fetch_bilibili_video_detail(self, url: str) -> BilibiliVideoDetail | None:
        bvid_match = re.search(r"/(BV[0-9A-Za-z]+)/?", url, re.I)
        aid_match = re.search(r"/av(\d+)/?", url, re.I)
        if not bvid_match and not aid_match:
            return None
        await self._seed_bilibili_session()
        endpoint = "https://api.bilibili.com/x/web-interface/view"
        params: dict[str, str] = {}
        if bvid_match:
            params["bvid"] = bvid_match.group(1)
        elif aid_match:
            params["aid"] = aid_match.group(1)
        response = await self._client.get(
            endpoint,
            params=params,
            headers=self._bilibili_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        code = int(payload.get("code") or 0)
        if code != 0:
            message = str(payload.get("message") or payload.get("msg") or "unknown error").strip()
            raise RuntimeError(f"Bilibili view detail failed with code {code}: {message}")
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return None
        owner = data.get("owner") if isinstance(data.get("owner"), dict) else {}
        stat = data.get("stat") if isinstance(data.get("stat"), dict) else {}
        pages = data.get("pages") if isinstance(data.get("pages"), list) else []
        return BilibiliVideoDetail(
            endpoint_url=str(response.request.url),
            title=str(data.get("title") or "").strip(),
            description=str(data.get("desc") or "").strip(),
            image_url=str(data.get("pic") or "").strip(),
            uploader=str(owner.get("name") or "").strip(),
            bvid=str(data.get("bvid") or params.get("bvid") or "").strip(),
            duration_seconds=parse_bilibili_duration_seconds(data.get("duration")),
            view_count=int(stat.get("view") or 0),
            page_parts=[str(page.get("part") or "").strip() for page in pages if isinstance(page, dict)],
        )

    def _bilibili_headers(self) -> dict[str, str]:
        headers = {
            "referer": self._config.bilibili.referer or "https://www.bilibili.com",
            "user-agent": self._config.bilibili.user_agent or "Mozilla/5.0",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        if self._config.bilibili.cookie:
            headers["cookie"] = self._config.bilibili.cookie
        return headers

    async def _seed_bilibili_session(self) -> None:
        if self._bilibili_seeded:
            return
        self._bilibili_seeded = True
        response = await self._client.get("https://www.bilibili.com", headers=self._bilibili_headers())
        response.raise_for_status()

    async def _load_bilibili_wbi_keys(self) -> tuple[str, str]:
        if self._bilibili_wbi_keys and time.time() - self._bilibili_wbi_key_time < 600:
            return self._bilibili_wbi_keys
        response = await self._client.get(
            "https://api.bilibili.com/x/web-interface/nav",
            headers=self._bilibili_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        wbi_img = data.get("wbi_img") or {}
        img_url = str(wbi_img.get("img_url") or "").strip()
        sub_url = str(wbi_img.get("sub_url") or "").strip()
        img_key = self._extract_bilibili_wbi_key(img_url)
        sub_key = self._extract_bilibili_wbi_key(sub_url)
        if not img_key or not sub_key:
            raise RuntimeError("Bilibili nav response missing wbi keys")
        self._bilibili_wbi_keys = (img_key, sub_key)
        self._bilibili_wbi_key_time = time.time()
        return self._bilibili_wbi_keys

    @staticmethod
    def _extract_bilibili_wbi_key(url: str) -> str:
        match = re.search(r"/([^/]+)\.[a-zA-Z0-9]+(?:\?|$)", url)
        return match.group(1) if match else ""

    @staticmethod
    def _derive_bilibili_mixin_key(img_key: str, sub_key: str) -> str:
        joined = img_key + sub_key
        return "".join(joined[index] for index in WBI_MIXIN_KEY_INDEX if index < len(joined))[:32]

    @staticmethod
    def _sign_bilibili_wbi_params(params: dict[str, object], mixin_key: str) -> dict[str, object]:
        signed: dict[str, object] = {
            key: re.sub(r"[!'()*]", "", str(value))
            for key, value in params.items()
            if value is not None
        }
        signed["wts"] = int(time.time())
        canonical = "&".join(
            f"{quote(str(key), safe='-_.~')}={quote(str(signed[key]), safe='-_.~')}"
            for key in sorted(signed)
        )
        signed["w_rid"] = hashlib.md5(f"{canonical}{mixin_key}".encode("utf-8")).hexdigest()
        return signed
