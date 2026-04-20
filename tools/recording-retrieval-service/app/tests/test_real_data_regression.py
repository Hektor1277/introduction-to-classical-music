from __future__ import annotations

from scripts.real_data_regression import build_access_report_payload, canonicalize_url, sample_scenarios


def test_build_access_report_payload_summarizes_scenarios_and_hosts() -> None:
    payload = build_access_report_payload(
        scenario_access={
            "spring-lead-only": [
                {
                    "host": "www.youtube.com",
                    "operation": "streaming-search",
                    "ok": True,
                    "durationMs": 820,
                    "resultCount": 8,
                },
                {
                    "host": "music.apple.com",
                    "operation": "fetch-page",
                    "ok": False,
                    "durationMs": 4100,
                    "error": "timeout",
                },
            ]
        },
        host_summary={
            "www.youtube.com": {
                "requests": 4,
                "successes": 4,
                "failures": 0,
                "avgLatencyMs": 780,
                "recommendedTimeoutSeconds": 6.0,
                "recommendedQueryDepth": 6,
                "status": "healthy",
            },
            "music.apple.com": {
                "requests": 3,
                "successes": 1,
                "failures": 2,
                "avgLatencyMs": 3200,
                "recommendedTimeoutSeconds": 10.0,
                "recommendedQueryDepth": 3,
                "status": "degraded",
            },
        },
    )

    assert payload["scenarioCount"] == 1
    assert payload["eventCount"] == 2
    assert payload["hosts"]["music.apple.com"]["status"] == "degraded"
    assert payload["scenarios"]["spring-lead-only"]["failedEvents"] == 1


def test_sample_scenarios_include_expanded_sparse_variants() -> None:
    scenario_ids = {scenario.scenario_id for scenario in sample_scenarios()}

    assert len(scenario_ids) >= 15
    assert {
        "bohm-conductor-only",
        "heifetz-full",
        "heifetz-lead-only",
        "gieseking-full",
        "karajan-alpine-full",
        "bernstein-fantastique-conductor-only",
        "kreisler-spring-full",
    } <= scenario_ids


def test_real_data_regression_canonicalize_url_supports_apple_music() -> None:
    assert (
        canonicalize_url("https://music.apple.com/cn/album/demo/123456789?i=987654321&uo=4")
        == "apple_music:/cn/album/demo/123456789?i=987654321"
    )
