from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Bilibili storage state for browser reuse.")
    parser.add_argument(
        "--output",
        default="config/bilibili-storage-state.json",
        help="Path to write the Playwright storage state JSON.",
    )
    parser.add_argument(
        "--channel",
        default="msedge" if sys.platform == "win32" else "",
        help="Optional browser channel, for example msedge or chrome.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as error:
        raise SystemExit("Playwright is not installed in the current environment.") from error

    print("即将打开有头浏览器，请手动登录 Bilibili。")
    print("登录完成并确认首页已经是登录态后，回到终端按 Enter 保存 storage state。")
    print(f"输出文件: {output_path}")

    with sync_playwright() as playwright:
        launch_options: dict[str, object] = {"headless": False}
        if args.channel.strip():
            launch_options["channel"] = args.channel.strip()
        browser = playwright.chromium.launch(**launch_options)
        context = browser.new_context(
            locale="zh-CN",
            viewport={"width": 1440, "height": 960},
            extra_http_headers={
                "Referer": "https://www.bilibili.com",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
        )
        page = context.new_page()
        page.goto("https://www.bilibili.com", wait_until="domcontentloaded", timeout=30000)
        input("登录完成后按 Enter 保存状态...")
        context.storage_state(path=str(output_path))
        context.close()
        browser.close()

    print(f"已保存: {output_path}")
    print("后续请将 platform-search.local.json 中的 bilibili.storageStatePath 指向该文件。")


if __name__ == "__main__":
    main()
