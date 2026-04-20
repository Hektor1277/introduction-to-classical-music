from __future__ import annotations

from app.services.input_analysis import analyze_raw_text


def test_analyze_raw_text_supports_structured_pipe_format_for_orchestral() -> None:
    result = analyze_raw_text(
        raw_text="柴可夫斯基 | 第五交响曲 | Otto Klemperer | London Symphony Orchestra | 1964",
        work_type_hint="orchestral",
    )

    assert result["composerName"] == "柴可夫斯基"
    assert result["workTitle"] == "第五交响曲"
    assert result["primaryPerson"] == "Otto Klemperer"
    assert result["groupName"] == "London Symphony Orchestra"
    assert result["performanceDateText"] == "1964"


def test_analyze_raw_text_accepts_missing_group_with_dash_placeholder() -> None:
    result = analyze_raw_text(
        raw_text="柴可夫斯基 | 第五交响曲 | Albert Coates | - | 1922",
        work_type_hint="orchestral",
    )

    assert result["primaryPerson"] == "Albert Coates"
    assert result["groupName"] == ""
    assert result["performanceDateText"] == "1922"


def test_analyze_raw_text_supports_concerto_role_split() -> None:
    result = analyze_raw_text(
        raw_text="舒曼 | a小调钢琴协奏曲 op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | 1960",
        work_type_hint="concerto",
    )

    assert result["composerName"] == "舒曼"
    assert result["workTitle"] == "a小调钢琴协奏曲"
    assert result["catalogue"].lower() == "op.54"
    assert result["primaryPerson"] == "Annie Fischer"
    assert result["secondaryPerson"] == "Kletzki"
    assert result["groupName"] == "Budapest Philharmonic Orchestra"
    assert result["performanceDateText"] == "1960"


def test_analyze_raw_text_supports_short_free_text_fallback() -> None:
    result = analyze_raw_text(
        raw_text="柴可夫斯基第五交响曲\nKlemperer 1964 LSO",
        work_type_hint="orchestral",
    )

    assert result["composerName"] == "柴可夫斯基"
    assert result["workTitle"] == "第五交响曲"
    assert result["primaryPerson"] == "Klemperer"
    assert result["groupName"] == "LSO"
    assert result["performanceDateText"] == "1964"


def test_analyze_raw_text_supports_concerto_free_text_without_pipes() -> None:
    result = analyze_raw_text(
        raw_text="舒曼 a小调钢琴协奏曲 Annie Fischer Kletzki Budapest Philharmonic Orchestra",
        work_type_hint="concerto",
    )

    assert result["composerName"] == "舒曼"
    assert result["workTitle"] == "a小调钢琴协奏曲"
    assert result["primaryPerson"] == "Annie Fischer"
    assert result["secondaryPerson"] == "Kletzki"
    assert result["groupName"] == "Budapest Philharmonic Orchestra"


def test_analyze_raw_text_supports_chinese_work_line_plus_latin_people_line() -> None:
    result = analyze_raw_text(
        raw_text="柴可夫斯基第五交响曲\nKempe 1964 LSO",
        work_type_hint="orchestral",
    )

    assert result["composerName"] == "柴可夫斯基"
    assert result["workTitle"] == "第五交响曲"
    assert result["primaryPerson"] == "Kempe"
    assert result["groupName"] == "LSO"
    assert result["performanceDateText"] == "1964"
