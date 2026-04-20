from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

import app.main as main_module
from app.main import create_app


def test_profile_targets_endpoint_returns_editable_source_files() -> None:
    client = TestClient(create_app())

    response = client.get("/ui/profile-targets")

    assert response.status_code == 200
    payload = response.json()
    assert Path(payload["highQuality"]["path"]).name == "high-quality.txt"
    assert Path(payload["streaming"]["path"]).name == "streaming.txt"
    assert Path(payload["llmConfig"]["path"]).name == "llm.local.json"
    assert Path(payload["orchestraAliases"]["path"]).name == "orchestra-abbreviations.txt"
    assert Path(payload["personAliases"]["path"]).name == "person-name-aliases.txt"


def test_open_profile_endpoint_supports_all_profile_groups(monkeypatch) -> None:
    opened: list[Path] = []

    def fake_launcher(path: Path) -> None:
        opened.append(path)

    monkeypatch.setattr(main_module, "launch_path", fake_launcher)
    client = TestClient(create_app())

    for group in ("high-quality", "streaming", "llm-config", "orchestra-aliases", "person-aliases"):
        response = client.post(f"/ui/open-profile/{group}")
        assert response.status_code == 200

    assert [path.name for path in opened] == [
        "high-quality.txt",
        "streaming.txt",
        "llm.local.json",
        "orchestra-abbreviations.txt",
        "person-name-aliases.txt",
    ]


def test_analyze_text_endpoint_returns_concerto_specific_fields() -> None:
    client = TestClient(create_app())

    response = client.post(
        "/ui/analyze-text",
        json={
            "rawText": "舒曼 | a小调钢琴协奏曲 op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | 1960",
            "workTypeHint": "concerto",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["composerName"] == "舒曼"
    assert payload["workTitle"] == "a小调钢琴协奏曲"
    assert payload["primaryPerson"] == "Annie Fischer"
    assert payload["secondaryPerson"] == "Kletzki"
    assert payload["groupName"] == "Budapest Philharmonic Orchestra"
    assert payload["performanceDateText"] == "1960"
