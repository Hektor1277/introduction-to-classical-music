from __future__ import annotations

from pathlib import Path

import pytest

from app.services.parent_work_eval import (
    build_allowed_targets_by_recording,
    build_recording_scenarios,
    build_work_dataset,
    canonicalize_url,
    classify_target_link_audit,
    categorize_result_reason,
    evaluate_hit_metrics,
    find_work_id as resolve_work_id,
    load_selector_values,
    load_library_indices,
    list_work_ids_with_supported_targets,
    supported_target_urls,
    summarize_link_audit,
    summarize_results,
    workspace_root,
)


def find_work_id(*, works: dict[str, dict], work_id: str = "", title_latin: str = "", title: str = "") -> str:
    try:
        return resolve_work_id(works=works, work_id=work_id, title_latin=title_latin, title=title)
    except KeyError:
        selector = title_latin or title or work_id or "<unknown>"
        pytest.skip(f"public repository regression dataset does not include required work selector: {selector}")


def test_build_recording_scenarios_for_concerto_produces_full_and_partial_variants() -> None:
    recording = {
        "id": "recording-1",
        "workId": "work-1",
        "title": "Example Recording",
        "performanceDateText": "March 3, 1942 Berlin",
        "credits": [
            {"role": "orchestra", "displayName": "Berlin Philharmonic Orchestra", "personId": "person-orch"},
            {"role": "conductor", "displayName": "Wilhelm Furtwangler", "personId": "person-cond"},
            {"role": "soloist", "displayName": "Walter Gieseking", "personId": "person-solo"},
        ],
        "links": [
            {"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"},
            {"platform": "bilibili", "url": "https://www.bilibili.com/video/BV1xx411c7mD"},
        ],
    }
    work = {
        "id": "work-1",
        "composerId": "composer-1",
        "title": "a小调钢琴协奏曲",
        "titleLatin": "Piano Concerto, Op.54",
        "catalogue": "Op.54",
    }
    composer = {"id": "composer-1", "name": "罗伯特·舒曼", "nameLatin": "Robert Schumann"}

    scenarios = build_recording_scenarios(recording, work, composer)

    assert [scenario.variant for scenario in scenarios] == ["full", "partial"]

    full_item = scenarios[0].item
    assert full_item.work_type_hint == "concerto"
    assert full_item.seed.performance_date_text == "March 3, 1942 Berlin"
    assert [credit.role for credit in full_item.seed.credits] == ["soloist", "conductor", "orchestra"]
    assert scenarios[0].target_urls == ["youtube:abc123xyz01", "bilibili:BV1xx411c7mD"]
    assert scenarios[0].evaluable is True

    partial_item = scenarios[1].item
    assert partial_item.seed.performance_date_text == ""
    assert [credit.role for credit in partial_item.seed.credits] == ["soloist"]
    assert partial_item.item_id == "recording-1-partial"


def test_build_recording_scenarios_strips_performance_date_from_partial_title() -> None:
    recording = {
        "id": "recording-sonoda-2023",
        "workId": "work-1",
        "title": "園田高弘 - 2023.10.31",
        "performanceDateText": "2023.10.31",
        "credits": [
            {"role": "soloist", "displayName": "園田高弘", "personId": "person-sonoda", "label": "钢琴"},
        ],
        "links": [
            {"platform": "bilibili", "url": "https://www.bilibili.com/video/BV1ouNbzqEQt/"},
        ],
    }
    work = {
        "id": "work-1",
        "composerId": "composer-1",
        "title": "第二十三号奏鸣曲，热情",
        "titleLatin": "Piano Sonata No.23, Op.57",
        "catalogue": "Op.57",
    }
    composer = {"id": "composer-1", "name": "路德维希·凡·贝多芬", "nameLatin": "Ludwig van Beethoven"}

    scenarios = build_recording_scenarios(recording, work, composer)

    assert scenarios[0].item.seed.title == "園田高弘 - 2023.10.31"
    assert scenarios[1].item.seed.title == "園田高弘"
    assert scenarios[1].item.seed.performance_date_text == ""


def test_build_recording_scenarios_keeps_partial_title_date_for_concerto_profiles() -> None:
    recording = {
        "id": "recording-1",
        "workId": "work-1",
        "title": "Walter Gieseking - March 3, 1942 Berlin",
        "performanceDateText": "March 3, 1942 Berlin",
        "credits": [
            {"role": "soloist", "displayName": "Walter Gieseking", "personId": "person-solo"},
            {"role": "conductor", "displayName": "Wilhelm Furtwangler", "personId": "person-cond"},
            {"role": "orchestra", "displayName": "Berlin Philharmonic Orchestra", "personId": "person-orch"},
        ],
        "links": [
            {"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"},
        ],
    }
    work = {
        "id": "work-1",
        "composerId": "composer-1",
        "title": "a小调钢琴协奏曲",
        "titleLatin": "Piano Concerto, Op.54",
        "catalogue": "Op.54",
    }
    composer = {"id": "composer-1", "name": "罗伯特·舒曼", "nameLatin": "Robert Schumann"}

    scenarios = build_recording_scenarios(recording, work, composer)

    assert scenarios[1].item.work_type_hint == "concerto"
    assert scenarios[1].item.seed.title == "Walter Gieseking - March 3, 1942 Berlin"


def test_build_recording_scenarios_supports_variant_specific_target_urls() -> None:
    recording = {
        "id": "recording-sonoda-2023",
        "workId": "work-1",
        "title": "園田高弘 - 2023.10.31",
        "performanceDateText": "2023.10.31",
        "credits": [
            {"role": "soloist", "displayName": "園田高弘", "personId": "person-sonoda", "label": "钢琴"},
        ],
        "links": [
            {"platform": "bilibili", "url": "https://www.bilibili.com/video/BV1ouNbzqEQt/"},
        ],
        "evalTargetUrlsByVariant": {
            "full": [
                "https://www.bilibili.com/video/BV1ouNbzqEQt/",
            ],
            "partial": [
                "https://www.bilibili.com/video/BV1oZ421u7M6/",
            ],
        },
    }
    work = {
        "id": "work-1",
        "composerId": "composer-1",
        "title": "第二十三号奏鸣曲，热情",
        "titleLatin": "Piano Sonata No.23, Op.57",
        "catalogue": "Op.57",
    }
    composer = {"id": "composer-1", "name": "路德维希·凡·贝多芬", "nameLatin": "Ludwig van Beethoven"}

    scenarios = build_recording_scenarios(recording, work, composer)

    assert scenarios[0].target_urls == ["bilibili:BV1ouNbzqEQt"]
    assert scenarios[1].target_urls == ["bilibili:BV1oZ421u7M6"]


def test_canonicalize_url_normalizes_apple_music_and_classical_track_urls() -> None:
    assert (
        canonicalize_url("https://music.apple.com/us/album/demo-album/123456789?i=987654321&uo=4")
        == "apple_music:/us/album/demo-album/123456789?i=987654321"
    )
    assert (
        canonicalize_url("https://classical.music.apple.com/us/work/demo-work/123456789?i=987654321&l=en-US")
        == "apple_music:/us/work/demo-work/123456789?i=987654321"
    )


def test_supported_target_urls_accepts_apple_music_platform_aliases() -> None:
    recording = {
        "links": [
            {"platform": "apple-music", "url": "https://music.apple.com/cn/album/demo/123?i=456"},
            {"platform": "apple_music", "url": "https://classical.music.apple.com/cn/album/demo/123?i=456&uo=4"},
            {"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"},
        ]
    }

    assert supported_target_urls(recording) == [
        "apple_music:/cn/album/demo/123?i=456",
        "youtube:abc123xyz01",
    ]


def test_supported_target_urls_can_filter_to_allowed_canonicals() -> None:
    recording = {
        "links": [
            {"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"},
            {"platform": "bilibili", "url": "https://www.bilibili.com/video/BV1xx411c7mD/"},
        ]
    }

    assert supported_target_urls(recording, allowed_canonicals={"bilibili:BV1xx411c7mD"}) == [
        "bilibili:BV1xx411c7mD"
    ]


def test_build_allowed_targets_by_recording_keeps_only_allowed_audit_statuses() -> None:
    rows = [
        {
            "recordingId": "recording-1",
            "canonical": "youtube:abc123xyz01",
            "auditStatus": "available",
        },
        {
            "recordingId": "recording-1",
            "canonical": "bilibili:BV1xx411c7mD",
            "auditStatus": "available_but_suspicious",
        },
        {
            "recordingId": "recording-2",
            "canonical": "youtube:def456uvw89",
            "auditStatus": "unavailable",
        },
    ]

    assert build_allowed_targets_by_recording(rows, allowed_statuses={"available"}) == {
        "recording-1": {"youtube:abc123xyz01"}
    }


def test_build_work_dataset_uses_corrected_goldberg_yamane_metadata() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenarios = build_work_dataset(work_id=work_id, recordings=recordings, works=works, composers=composers)

    full_scenario = next(
        scenario
        for scenario in scenarios
        if scenario.item.item_id == "recording-第5号小提琴奏鸣曲-春天-席蒙戈尔德贝格-and-山根美代子-full"
    )

    assert full_scenario.item.seed.title == "席蒙·戈尔德贝格 - 山根美代子 - June, 1991"
    assert full_scenario.item.seed.performance_date_text == "June, 1991"
    assert full_scenario.item.seed.credits[1].display_name == "山根美代子"


def test_build_work_dataset_uses_variant_specific_targets_for_actual_sonoda_recording() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Piano Sonata No.23, Op.57")
    scenarios = build_work_dataset(work_id=work_id, recordings=recordings, works=works, composers=composers)

    full_scenario = next(
        scenario
        for scenario in scenarios
        if scenario.item.item_id == "recording-第二十三号奏鸣曲-热情-園田高弘2023-full"
    )
    partial_scenario = next(
        scenario
        for scenario in scenarios
        if scenario.item.item_id == "recording-第二十三号奏鸣曲-热情-園田高弘2023-partial"
    )

    assert full_scenario.target_urls == ["bilibili:BV1ouNbzqEQt"]
    assert partial_scenario.target_urls == ["bilibili:BV1oZ421u7M6"]


def test_build_work_dataset_includes_equivalent_full_upload_for_schneiderhan_seemann() -> None:
    recordings, works, composers = load_library_indices()
    work_id = find_work_id(works=works, title_latin="Violin Sonata No.5, Op.24")
    scenarios = build_work_dataset(work_id=work_id, recordings=recordings, works=works, composers=composers)

    matched = [
        scenario
        for scenario in scenarios
        if "bilibili:BV1qW4y1f7Ng" in scenario.target_urls
        and "youtube:3otH1b0icCg" in scenario.target_urls
    ]
    full_scenario = next(scenario for scenario in matched if scenario.variant == "full")
    partial_scenario = next(scenario for scenario in matched if scenario.variant == "partial")

    assert "bilibili:BV1NSH9zNE2S" in full_scenario.target_urls
    assert "bilibili:BV1NSH9zNE2S" in partial_scenario.target_urls


def test_list_work_ids_with_supported_targets_only_returns_truth_backed_works() -> None:
    recordings = {
        "recording-1": {
            "id": "recording-1",
            "workId": "work-1",
            "links": [{"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"}],
        },
        "recording-2": {
            "id": "recording-2",
            "workId": "work-2",
            "links": [{"platform": "spotify", "url": "https://open.spotify.com/track/demo"}],
        },
        "recording-3": {
            "id": "recording-3",
            "workId": "work-1",
            "links": [],
        },
    }

    assert list_work_ids_with_supported_targets(recordings) == ["work-1"]


def test_summarize_results_groups_hits_by_variant_and_tracks_evaluable_cases() -> None:
    summary = summarize_results(
        [
            {
                "variant": "full",
                "evaluable": True,
                "finalHit": True,
                "candidateHit": True,
                "relaxedFinalHit": True,
                "relaxedCandidateHit": True,
                "versionFinalHit": True,
                "versionCandidateHit": True,
                "strictMissReason": "none",
            },
            {
                "variant": "full",
                "evaluable": True,
                "finalHit": False,
                "candidateHit": True,
                "relaxedFinalHit": True,
                "relaxedCandidateHit": True,
                "versionFinalHit": True,
                "versionCandidateHit": True,
                "strictMissReason": "same_platform_alt_upload",
            },
            {
                "variant": "partial",
                "evaluable": True,
                "finalHit": False,
                "candidateHit": False,
                "relaxedFinalHit": False,
                "relaxedCandidateHit": False,
                "versionFinalHit": True,
                "versionCandidateHit": True,
                "strictMissReason": "recall_miss",
            },
            {
                "variant": "partial",
                "evaluable": False,
                "finalHit": False,
                "candidateHit": False,
                "relaxedFinalHit": False,
                "relaxedCandidateHit": False,
                "versionFinalHit": False,
                "versionCandidateHit": False,
                "strictMissReason": "not_evaluable",
            },
        ]
    )

    assert summary["overall"] == {
        "total": 4,
        "evaluable": 3,
        "finalHit": 1,
        "candidateHit": 2,
        "relaxedFinalHit": 2,
        "relaxedCandidateHit": 2,
        "versionFinalHit": 3,
        "versionCandidateHit": 3,
    }
    assert summary["byVariant"]["full"] == {
        "total": 2,
        "evaluable": 2,
        "finalHit": 1,
        "candidateHit": 2,
        "relaxedFinalHit": 2,
        "relaxedCandidateHit": 2,
        "versionFinalHit": 2,
        "versionCandidateHit": 2,
    }
    assert summary["byVariant"]["partial"] == {
        "total": 2,
        "evaluable": 1,
        "finalHit": 0,
        "candidateHit": 0,
        "relaxedFinalHit": 0,
        "relaxedCandidateHit": 0,
        "versionFinalHit": 1,
        "versionCandidateHit": 1,
    }
    assert summary["strictMissReasons"] == {
        "same_platform_alt_upload": 1,
        "recall_miss": 1,
    }


def test_workspace_root_points_to_parent_project_root() -> None:
    assert (workspace_root() / "data" / "library" / "works.json").exists()


def test_load_selector_values_reads_newline_delimited_file_and_dedupes(tmp_path: Path) -> None:
    values_file = tmp_path / "item_ids.txt"
    values_file.write_text(
        "\n".join(
            [
                "recording-a小调钢琴协奏曲-鲁普-and-朱里尼1980-full",
                "",
                " recording-a小调钢琴协奏曲-鲁普-and-朱里尼1980-partial ",
                "recording-a小调钢琴协奏曲-鲁普-and-朱里尼1980-full",
            ]
        ),
        encoding="utf-8",
    )

    assert load_selector_values(
        inline_values="recording-a小调钢琴协奏曲-福斯特-1953-full",
        values_file=str(values_file),
    ) == [
        "recording-a小调钢琴协奏曲-福斯特-1953-full",
        "recording-a小调钢琴协奏曲-鲁普-and-朱里尼1980-full",
        "recording-a小调钢琴协奏曲-鲁普-and-朱里尼1980-partial",
    ]


def test_build_work_dataset_can_filter_targets_by_audit_results() -> None:
    recordings = {
        "recording-1": {
            "id": "recording-1",
            "workId": "work-1",
            "title": "Example Recording",
            "performanceDateText": "March 3, 1942 Berlin",
            "credits": [
                {"role": "soloist", "displayName": "Walter Gieseking", "personId": "person-solo"},
                {"role": "conductor", "displayName": "Wilhelm Furtwangler", "personId": "person-cond"},
            ],
            "links": [
                {"platform": "youtube", "url": "https://www.youtube.com/watch?v=abc123xyz01"},
                {"platform": "bilibili", "url": "https://www.bilibili.com/video/BV1xx411c7mD/"},
            ],
        }
    }
    works = {
        "work-1": {
            "id": "work-1",
            "composerId": "composer-1",
            "title": "a小调钢琴协奏曲",
            "titleLatin": "Piano Concerto, Op.54",
            "catalogue": "Op.54",
        }
    }
    composers = {"composer-1": {"id": "composer-1", "name": "罗伯特·舒曼", "nameLatin": "Robert Schumann"}}

    scenarios = build_work_dataset(
        work_id="work-1",
        recordings=recordings,
        works=works,
        composers=composers,
        allowed_targets_by_recording={"recording-1": {"youtube:abc123xyz01"}},
    )

    assert len(scenarios) == 2
    assert scenarios[0].target_urls == ["youtube:abc123xyz01"]
    assert scenarios[1].target_urls == ["youtube:abc123xyz01"]
    assert all(scenario.evaluable is True for scenario in scenarios)


def test_evaluate_hit_metrics_counts_high_confidence_same_platform_alt_upload_as_relaxed_hit() -> None:
    metrics = evaluate_hit_metrics(
        targets=["bilibili:BV1target1234"],
        final_links=[
            {
                "canonical": "bilibili:BV1alt567890",
                "platform": "bilibili",
                "confidence": 0.81,
                "title": "Cortot / Fricsay / Schumann Piano Concerto",
            }
        ],
        candidate_links=[],
    )

    assert metrics["finalHit"] is False
    assert metrics["candidateHit"] is False
    assert metrics["relaxedFinalHit"] is True
    assert metrics["relaxedCandidateHit"] is True
    assert metrics["finalMatchType"] == "same_platform_alt_upload"
    assert metrics["candidateMatchType"] == "same_platform_alt_upload"


def test_evaluate_hit_metrics_rejects_low_confidence_same_platform_alt_upload() -> None:
    metrics = evaluate_hit_metrics(
        targets=["bilibili:BV1target1234"],
        final_links=[],
        candidate_links=[
            {
                "canonical": "bilibili:BV1alt567890",
                "platform": "bilibili",
                "confidence": 0.47,
                "title": "Schumann Piano Concerto",
            }
        ],
    )

    assert metrics["relaxedFinalHit"] is False
    assert metrics["relaxedCandidateHit"] is False
    assert metrics["candidateMatchType"] == "none"


def test_evaluate_hit_metrics_counts_high_confidence_cross_platform_version_hit_separately() -> None:
    metrics = evaluate_hit_metrics(
        targets=["bilibili:BV1target1234"],
        final_links=[
            {
                "canonical": "youtube:araujochum1977",
                "platform": "youtube",
                "confidence": 0.97,
                "title": "Schumann: Piano Concerto in A minor, Op. 54 - Claudio Arrau, RCO, Eugen Jochum. Rec. 1977",
            }
        ],
        candidate_links=[],
    )

    assert metrics["finalHit"] is False
    assert metrics["relaxedFinalHit"] is False
    assert metrics["versionFinalHit"] is True
    assert metrics["versionCandidateHit"] is True
    assert metrics["finalVersionMatchType"] == "cross_platform_version_equivalent"
    assert metrics["candidateVersionMatchType"] == "cross_platform_version_equivalent"


def test_evaluate_hit_metrics_counts_apple_music_same_platform_alt_upload_as_relaxed_hit() -> None:
    metrics = evaluate_hit_metrics(
        targets=["apple_music:/us/album/demo-album/123456789?i=111"],
        final_links=[
            {
                "canonical": "apple_music:/us/album/demo-album/123456789?i=222",
                "platform": "apple_music",
                "confidence": 0.84,
                "title": "Schumann: Piano Concerto in A Minor, Op. 54",
            }
        ],
        candidate_links=[],
    )

    assert metrics["finalHit"] is False
    assert metrics["relaxedFinalHit"] is True
    assert metrics["versionFinalHit"] is True
    assert metrics["finalMatchType"] == "same_platform_alt_upload"


def test_categorize_result_reason_distinguishes_alt_upload_from_real_recall_miss() -> None:
    alt_upload_reason = categorize_result_reason(
        {
            "evaluable": True,
            "finalHit": False,
            "candidateHit": False,
            "relaxedFinalHit": True,
            "relaxedCandidateHit": True,
            "warnings": [],
        }
    )
    recall_reason = categorize_result_reason(
        {
            "evaluable": True,
            "finalHit": False,
            "candidateHit": False,
            "relaxedFinalHit": False,
            "relaxedCandidateHit": False,
            "warnings": [],
        }
    )

    assert alt_upload_reason == "same_platform_alt_upload"
    assert recall_reason == "recall_miss"


def test_classify_target_link_audit_distinguishes_unavailable_suspicious_and_healthy() -> None:
    assert classify_target_link_audit(available=False, match_score=None) == "unavailable"
    assert classify_target_link_audit(available=True, match_score=0.12) == "available_but_suspicious"
    assert classify_target_link_audit(available=True, match_score=0.39) == "available"


def test_summarize_link_audit_counts_statuses_and_platforms() -> None:
    summary = summarize_link_audit(
        [
            {"platform": "bilibili", "available": True, "auditStatus": "available"},
            {"platform": "youtube", "available": True, "auditStatus": "available_but_suspicious"},
            {"platform": "youtube", "available": False, "auditStatus": "unavailable"},
        ]
    )

    assert summary == {
        "total": 3,
        "available": 2,
        "unavailable": 1,
        "byStatus": {
            "available": 1,
            "available_but_suspicious": 1,
            "unavailable": 1,
        },
        "byPlatform": {
            "bilibili": {
                "total": 1,
                "available": 1,
                "unavailable": 0,
            },
            "youtube": {
                "total": 2,
                "available": 1,
                "unavailable": 1,
            },
        },
    }
