from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync a Bilibili Cookie header from a Playwright storage state file into platform-search.local.json."
    )
    parser.add_argument(
        "--storage-state",
        default="config/bilibili-storage-state.json",
        help="Path to the captured Bilibili storage state JSON.",
    )
    parser.add_argument(
        "--config",
        default="config/platform-search.local.json",
        help="Path to the local platform search config JSON.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_bilibili_cookie_header(storage_state: dict[str, Any]) -> str:
    cookies = storage_state.get("cookies") or []
    pairs: list[str] = []
    seen: set[str] = set()
    for cookie in cookies:
        domain = str(cookie.get("domain") or "").lower()
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "").strip()
        if not name or not value:
            continue
        if "bilibili.com" not in domain and "hdslb.com" not in domain and "b23.tv" not in domain:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def update_config(config_path: Path, cookie_header: str) -> None:
    payload = load_json(config_path)
    bilibili = dict(payload.get("bilibili") or {})
    bilibili["cookie"] = cookie_header
    payload["bilibili"] = bilibili
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    storage_state_path = (root / args.storage_state).resolve()
    config_path = (root / args.config).resolve()

    if not storage_state_path.is_file():
        raise FileNotFoundError(f"Storage state not found: {storage_state_path}")
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    storage_state = load_json(storage_state_path)
    cookie_header = build_bilibili_cookie_header(storage_state)
    if not cookie_header:
        raise RuntimeError("No usable Bilibili cookies were found in the storage state.")

    update_config(config_path, cookie_header)
    print(f"Updated {config_path}")
    print(f"Synced {len(cookie_header.split('; '))} cookie pairs from {storage_state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
