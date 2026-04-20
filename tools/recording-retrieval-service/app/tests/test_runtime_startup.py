from __future__ import annotations

import pytest

from app import main


def test_resolve_ui_root_finds_pyinstaller_onedir_assets(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle_dir = tmp_path / "portable"
    ui_root = bundle_dir / "_internal" / "app" / "ui"
    (ui_root / "assets").mkdir(parents=True)
    (ui_root / "index.html").write_text("<html>ok</html>", encoding="utf-8")

    monkeypatch.setattr(main.sys, "frozen", True, raising=False)
    monkeypatch.delattr(main.sys, "_MEIPASS", raising=False)
    monkeypatch.setattr(
        main.sys,
        "executable",
        str(bundle_dir / "recording-retrieval-service.exe"),
        raising=False,
    )

    assert main.resolve_ui_root() == ui_root


def test_handle_existing_service_reuses_running_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    opened_urls: list[str] = []

    monkeypatch.setattr(
        main,
        "probe_existing_service",
        lambda host, port: main.ServiceProbe(
            occupied=True,
            is_ours=True,
            health_ok=True,
            ui_ok=True,
        ),
    )
    monkeypatch.setattr(main.webbrowser, "open", opened_urls.append)

    should_exit = main.handle_existing_service("ui", "127.0.0.1", 4780)

    assert should_exit is True
    assert opened_urls == ["http://127.0.0.1:4780/"]


def test_handle_existing_service_rejects_broken_existing_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        main,
        "probe_existing_service",
        lambda host, port: main.ServiceProbe(
            occupied=True,
            is_ours=True,
            health_ok=True,
            ui_ok=False,
        ),
    )

    with pytest.raises(RuntimeError, match="UI is unavailable"):
        main.handle_existing_service("ui", "127.0.0.1", 4780)
