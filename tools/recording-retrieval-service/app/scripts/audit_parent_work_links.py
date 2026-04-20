from __future__ import annotations

import argparse
import asyncio
import re
import json
from html import unescape
from pathlib import Path
from urllib.parse import urlparse

import httpx

from app.services.http_sources import score_recording_match
from app.services.parent_work_eval import (
    build_recording_scenarios,
    canonicalize_url,
    classify_target_link_audit,
    find_work_id,
    list_work_ids_with_supported_targets,
    load_library_indices,
    normalize_ground_truth_platform,
    summarize_link_audit,
)
from app.services.pipeline import InputNormalizer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-id", default="")
    parser.add_argument("--work-ids", default="")
    parser.add_argument("--title-latin", default="Piano Concerto, Op.54")
    parser.add_argument("--title", default="")
    parser.add_argument("--all-works", action="store_true")
    parser.add_argument("--limit-works", type=int, default=0)
    parser.add_argument(
        "--output",
        default="output/parent_work_eval_schumann_op54_link_audit.json",
    )
    return parser


def resolve_selected_work_ids(args: argparse.Namespace, recordings: dict[str, dict], works: dict[str, dict]) -> list[str]:
    explicit_work_ids = [value.strip() for value in str(args.work_ids or "").split(",") if value.strip()]
    if explicit_work_ids:
        missing = [work_id for work_id in explicit_work_ids if work_id not in works]
        if missing:
            raise KeyError(f"unknown work ids: {', '.join(missing)}")
        return explicit_work_ids
    if args.all_works:
        work_ids = list_work_ids_with_supported_targets(recordings)
        if args.limit_works > 0:
            work_ids = work_ids[: args.limit_works]
        return work_ids
    return [
        find_work_id(
            works=works,
            work_id=args.work_id,
            title_latin=args.title_latin,
            title=args.title,
        )
    ]


def extract_bilibili_bvid(url: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]
    if len(parts) >= 2 and parts[0] == "video":
        return parts[1]
    return ""


async def fetch_youtube_oembed(client: httpx.AsyncClient, url: str) -> dict[str, object]:
    response = await client.get(
        "https://www.youtube.com/oembed",
        params={"url": url, "format": "json"},
    )
    if response.status_code != 200:
        return {
            "available": False,
            "statusCode": response.status_code,
            "resolvedCanonical": "",
            "title": "",
            "description": "",
            "uploader": "",
            "message": response.text[:200],
        }
    payload = response.json()
    return {
        "available": True,
        "statusCode": response.status_code,
        "resolvedCanonical": canonicalize_url(url),
        "title": str(payload.get("title") or "").strip(),
        "description": "",
        "uploader": str(payload.get("author_name") or "").strip(),
        "message": "",
    }


async def fetch_bilibili_view(client: httpx.AsyncClient, url: str) -> dict[str, object]:
    bvid = extract_bilibili_bvid(url)
    response = await client.get(
        "https://api.bilibili.com/x/web-interface/view",
        params={"bvid": bvid},
        headers={
            "referer": "https://www.bilibili.com",
            "user-agent": "Mozilla/5.0",
        },
    )
    if response.status_code != 200:
        return {
            "available": False,
            "statusCode": response.status_code,
            "resolvedCanonical": "",
            "title": "",
            "description": "",
            "uploader": "",
            "message": response.text[:200],
        }
    payload = response.json()
    if int(payload.get("code") or 0) != 0:
        return {
            "available": False,
            "statusCode": response.status_code,
            "resolvedCanonical": "",
            "title": "",
            "description": "",
            "uploader": "",
            "message": str(payload.get("message") or payload.get("msg") or "").strip(),
        }
    data = payload.get("data") or {}
    owner = data.get("owner") if isinstance(data.get("owner"), dict) else {}
    resolved_bvid = str(data.get("bvid") or bvid).strip()
    return {
        "available": True,
        "statusCode": response.status_code,
        "resolvedCanonical": f"bilibili:{resolved_bvid}" if resolved_bvid else "",
        "title": str(data.get("title") or "").strip(),
        "description": str(data.get("desc") or "").strip(),
        "uploader": str(owner.get("name") or "").strip(),
        "message": "",
    }


def _extract_meta_content(html_text: str, property_name: str) -> str:
    patterns = [
        rf'<meta[^>]+property="{re.escape(property_name)}"[^>]+content="([^"]*)"',
        rf"<meta[^>]+property='{re.escape(property_name)}'[^>]+content='([^']*)'",
        rf'<meta[^>]+content="([^"]*)"[^>]+property="{re.escape(property_name)}"',
        rf"<meta[^>]+content='([^']*)'[^>]+property='{re.escape(property_name)}'",
        rf'<meta[^>]+name="{re.escape(property_name)}"[^>]+content="([^"]*)"',
        rf"<meta[^>]+name='{re.escape(property_name)}'[^>]+content='([^']*)'",
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text or "", flags=re.IGNORECASE)
        if match:
            return unescape(match.group(1)).strip()
    return ""


async def fetch_apple_music_page(client: httpx.AsyncClient, url: str) -> dict[str, object]:
    response = await client.get(url)
    if response.status_code != 200:
        return {
            "available": False,
            "statusCode": response.status_code,
            "resolvedCanonical": "",
            "title": "",
            "description": "",
            "uploader": "",
            "message": response.text[:200],
        }
    html_text = response.text or ""
    return {
        "available": True,
        "statusCode": response.status_code,
        "resolvedCanonical": canonicalize_url(url),
        "title": _extract_meta_content(html_text, "og:title"),
        "description": _extract_meta_content(html_text, "og:description"),
        "uploader": _extract_meta_content(html_text, "og:site_name"),
        "message": "",
    }


async def audit_link(
    client: httpx.AsyncClient,
    *,
    platform: str,
    url: str,
    drafts: list[object],
) -> dict[str, object]:
    normalized_platform = normalize_ground_truth_platform(platform)
    if normalized_platform == "youtube":
        metadata = await fetch_youtube_oembed(client, url)
    elif normalized_platform == "bilibili":
        metadata = await fetch_bilibili_view(client, url)
    elif normalized_platform == "apple_music":
        metadata = await fetch_apple_music_page(client, url)
    else:
        return {
            "available": False,
            "statusCode": 0,
            "resolvedCanonical": "",
            "title": "",
            "description": "",
            "uploader": "",
            "message": "unsupported platform",
            "auditStatus": "unsupported_platform",
            "matchScore": None,
        }
    match_score: float | None = None
    variant_scores: list[float] = []
    if bool(metadata.get("available")):
        text = " ".join(
            part for part in [str(metadata.get("title") or ""), str(metadata.get("description") or "")] if part
        )
        variant_scores = [
            round(
                score_recording_match(text, url, draft, uploader=str(metadata.get("uploader") or "")),
                4,
            )
            for draft in drafts
        ]
        if variant_scores:
            match_score = max(variant_scores)
    return {
        **metadata,
        "matchScore": match_score,
        "variantScores": variant_scores,
        "auditStatus": classify_target_link_audit(
            available=bool(metadata.get("available")),
            match_score=match_score,
        ),
    }


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    recordings, works, composers = load_library_indices()
    selected_work_ids = resolve_selected_work_ids(args, recordings, works)
    normalizer = InputNormalizer()
    rows: list[dict[str, object]] = []
    per_work_payloads: list[dict[str, object]] = []

    async with httpx.AsyncClient(
        timeout=20.0,
        follow_redirects=True,
        headers={"user-agent": "Mozilla/5.0"},
    ) as client:
        for work_id in selected_work_ids:
            work = works[work_id]
            composer = composers[work["composerId"]]
            work_rows: list[dict[str, object]] = []
            selected_recordings = [
                recording for recording in recordings.values() if recording.get("workId") == work_id
            ]
            selected_recordings.sort(key=lambda item: str(item.get("title") or item["id"]))
            for recording in selected_recordings:
                scenarios = build_recording_scenarios(recording, work, composer)
                drafts = [normalizer.normalize(scenario.item) for scenario in scenarios]
                primary_scenario = scenarios[0]
                for link in recording.get("links") or []:
                    platform = normalize_ground_truth_platform(str(link.get("platform") or ""))
                    if platform not in {"youtube", "bilibili", "apple_music"}:
                        continue
                    audit = await audit_link(
                        client,
                        platform=platform,
                        url=str(link.get("url") or "").strip(),
                        drafts=drafts,
                    )
                    row = {
                        "workId": work_id,
                        "workTitle": str(work.get("title") or "").strip(),
                        "workTitleLatin": str(work.get("titleLatin") or "").strip(),
                        "composerNameLatin": str(composer.get("nameLatin") or "").strip(),
                        "recordingId": recording["id"],
                        "recordingTitle": str(recording.get("title") or "").strip(),
                        "sourceLine": primary_scenario.item.source_line,
                        "platform": platform,
                        "url": str(link.get("url") or "").strip(),
                        "canonical": canonicalize_url(str(link.get("url") or "").strip()),
                        **audit,
                    }
                    rows.append(row)
                    work_rows.append(row)
            per_work_payloads.append(
                {
                    "workId": work_id,
                    "title": str(work.get("title") or ""),
                    "titleLatin": str(work.get("titleLatin") or ""),
                    "composerName": str(composer.get("name") or ""),
                    "composerNameLatin": str(composer.get("nameLatin") or ""),
                    "recordingCount": len({row["recordingId"] for row in work_rows}),
                    "targetLinkCount": len(work_rows),
                    "summary": summarize_link_audit(work_rows),
                }
            )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "scope": {
            "allWorks": bool(args.all_works),
            "workCount": len(selected_work_ids),
        },
        "summary": summarize_link_audit(rows),
        "works": per_work_payloads,
        "results": rows,
    }
    if len(per_work_payloads) == 1:
        payload["work"] = {
            "workId": per_work_payloads[0]["workId"],
            "title": per_work_payloads[0]["title"],
            "titleLatin": per_work_payloads[0]["titleLatin"],
            "composerName": per_work_payloads[0]["composerName"],
            "composerNameLatin": per_work_payloads[0]["composerNameLatin"],
        }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(output_path.as_posix())


if __name__ == "__main__":
    asyncio.run(main())
