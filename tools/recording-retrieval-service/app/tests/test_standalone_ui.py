from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import create_app


def test_index_page_serves_workspace_display_cover_and_role_fields() -> None:
    client = TestClient(create_app())

    response = client.get("/")

    assert response.status_code == 200
    assert "workspace-panel" in response.text
    assert "display-panel" in response.text
    assert "文本分析" in response.text
    assert "开始搜索" in response.text
    assert "刷新条目" in response.text
    assert "人物 / 指挥" in response.text
    assert "第二关键信息" in response.text
    assert "打开人物映射文档" in response.text
    assert "cover-frame" in response.text
    assert "variant-tabs" in response.text


def test_static_asset_contains_dynamic_role_and_profile_actions() -> None:
    client = TestClient(create_app())

    response = client.get("/assets/app.js")

    assert response.status_code == 200
    assert "applyWorkTypeProfile" in response.text
    assert "secondaryPersonLatin" in response.text
    assert "renderCover" in response.text
    assert 'openProfile("person-aliases")' in response.text
