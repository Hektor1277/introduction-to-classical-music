from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from app.services.parent_work_eval import (
    build_allowed_targets_by_recording,
    build_work_dataset,
    categorize_result_reason,
    canonicalize_url,
    evaluate_hit_metrics,
    find_work_id,
    list_work_ids_with_supported_targets,
    load_library_indices,
    scenario_to_dict,
    summarize_results,
    workspace_root,
)
from app.services.retrieval import build_default_retriever


def build_access_report_payload(
    *,
    scenario_access: dict[str, list[dict]],
    host_summary: dict[str, dict],
) -> dict[str, object]:
    scenarios: dict[str, dict[str, object]] = {}
    event_count = 0
    for scenario_id, events in scenario_access.items():
        event_count += len(events)
        scenarios[scenario_id] = {
            "eventCount": len(events),
            "failedEvents": sum(1 for event in events if not event.get("ok", False)),
            "slowEvents": sum(1 for event in events if float(event.get("durationMs", 0.0) or 0.0) >= 3000),
            "hosts": sorted({str(event.get("host", "")).strip() for event in events if str(event.get("host", "")).strip()}),
            "events": events,
        }
    return {
        "scenarioCount": len(scenario_access),
        "eventCount": event_count,
        "scenarios": scenarios,
        "hosts": host_summary,
    }


def summarize_results_by_work(results: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for result in results:
        work_id = str(result.get("workId") or "").strip()
        if not work_id:
            continue
        grouped.setdefault(work_id, []).append(result)
    return {work_id: summarize_results(items) for work_id, items in grouped.items()}


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


def load_allowed_targets_from_audit(
    audit_report_path: str,
    *,
    allowed_statuses: set[str],
) -> dict[str, set[str]]:
    if not audit_report_path.strip():
        return {}
    payload = json.loads(Path(audit_report_path).read_text(encoding="utf-8"))
    rows = payload.get("results") or []
    if not isinstance(rows, list):
        return {}
    return build_allowed_targets_by_recording(rows, allowed_statuses=allowed_statuses)


async def run_scenario(retriever, scenario) -> dict[str, object]:
    try:
        deadline = time.monotonic() + 55
        result = await asyncio.wait_for(retriever.retrieve(scenario.item, deadline=deadline), timeout=70)
    except TimeoutError:
        return {
            "itemId": scenario.item.item_id,
            "recordingId": scenario.recording_id,
            "workId": scenario.item.work_id,
            "variant": scenario.variant,
            "workTypeHint": scenario.item.work_type_hint,
            "sourceLine": scenario.item.source_line,
            "evaluable": scenario.evaluable,
            "targets": scenario.target_urls,
            "status": "timeout",
            "confidence": 0.0,
            "finalLinks": [],
            "candidateLinks": [],
            "finalHit": False,
            "candidateHit": False,
            "relaxedFinalHit": False,
            "relaxedCandidateHit": False,
            "versionFinalHit": False,
            "versionCandidateHit": False,
            "finalMatchType": "none",
            "candidateMatchType": "none",
            "finalVersionMatchType": "none",
            "candidateVersionMatchType": "none",
            "strictMissReason": "timeout",
            "warnings": ["scenario timeout after internal 55s / external 70s"],
        }
    final_link_details = [
        {
            "canonical": canonicalize_url(link.url),
            "url": link.url,
            "title": link.title or "",
            "confidence": float(link.confidence or 0.0),
            "platform": link.platform or "",
        }
        for link in result.result.links
    ]
    candidate_link_details = [
        {
            "canonical": canonicalize_url(link.url),
            "url": link.url,
            "title": link.title or "",
            "confidence": float(link.confidence or 0.0),
            "platform": link.platform or "",
        }
        for link in result.link_candidates
    ]
    hit_metrics = evaluate_hit_metrics(
        targets=scenario.target_urls,
        final_links=final_link_details,
        candidate_links=candidate_link_details,
    )
    payload = {
        "itemId": scenario.item.item_id,
        "recordingId": scenario.recording_id,
        "workId": scenario.item.work_id,
        "variant": scenario.variant,
        "workTypeHint": scenario.item.work_type_hint,
        "sourceLine": scenario.item.source_line,
        "evaluable": scenario.evaluable,
        "targets": scenario.target_urls,
        "status": result.status,
        "confidence": result.confidence,
        "finalLinks": [item["canonical"] for item in final_link_details if item["canonical"]],
        "candidateLinks": [item["canonical"] for item in candidate_link_details if item["canonical"]],
        "finalLinkDetails": final_link_details,
        "candidateLinkDetails": candidate_link_details,
        "warnings": result.warnings,
    }
    payload.update(hit_metrics)
    payload["strictMissReason"] = categorize_result_reason(payload)
    return payload


async def main(args: argparse.Namespace) -> None:
    recordings, works, composers = load_library_indices()
    selected_work_ids = resolve_selected_work_ids(args, recordings, works)
    allowed_statuses = {
        status.strip()
        for status in args.allowed_audit_statuses.split(",")
        if status.strip()
    } or {"available"}
    allowed_targets_by_recording = load_allowed_targets_from_audit(
        args.audit_report,
        allowed_statuses=allowed_statuses,
    )
    scenarios = []
    for work_id in selected_work_ids:
        scenarios.extend(
            build_work_dataset(
                work_id=work_id,
                recordings=recordings,
                works=works,
                composers=composers,
                allowed_targets_by_recording=allowed_targets_by_recording or None,
            )
        )
    if args.limit:
        scenarios = scenarios[: args.limit]

    work_payloads = []
    for work_id in selected_work_ids:
        work = works[work_id]
        composer = composers[work["composerId"]]
        work_payloads.append(
            {
                "workId": work_id,
                "title": work.get("title", ""),
                "titleLatin": work.get("titleLatin", ""),
                "catalogue": work.get("catalogue", ""),
                "composerName": composer.get("name", ""),
                "composerNameLatin": composer.get("nameLatin", ""),
            }
        )
    dataset_payload: dict[str, object] = {
        "scope": {
            "allWorks": bool(args.all_works),
            "workCount": len(selected_work_ids),
            "allowedAuditStatuses": sorted(allowed_statuses),
            "auditReport": args.audit_report,
        },
        "works": work_payloads,
        "recordingCount": len({scenario.recording_id for scenario in scenarios}),
        "scenarioCount": len(scenarios),
        "variants": sorted({scenario.variant for scenario in scenarios}),
        "samples": [scenario_to_dict(scenario) for scenario in scenarios],
    }
    if len(work_payloads) == 1:
        dataset_payload["work"] = work_payloads[0]
    dataset_output = Path(args.dataset_output)
    dataset_output.parent.mkdir(parents=True, exist_ok=True)
    dataset_output.write_text(json.dumps(dataset_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    retriever = build_default_retriever()
    results: list[dict[str, object]] = []
    scenario_access: dict[str, list[dict]] = {}
    try:
        for scenario in scenarios:
            results.append(await run_scenario(retriever, scenario))
            scenario_access[scenario.item.item_id] = list(getattr(retriever, "consume_access_events", lambda: [])())
    finally:
        close_retriever = getattr(retriever, "aclose", None)
        if callable(close_retriever):
            await close_retriever()

    results_payload = {
        "scope": dataset_payload["scope"],
        "works": work_payloads,
        "summary": summarize_results(results),
        "summaryByWork": summarize_results_by_work(results),
        "results": results,
    }
    if len(work_payloads) == 1:
        results_payload["work"] = work_payloads[0]
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    access_report_path = Path(args.access_report)
    access_report_path.parent.mkdir(parents=True, exist_ok=True)
    access_report_path.write_text(
        json.dumps(
            build_access_report_payload(
                scenario_access=scenario_access,
                host_summary=dict(getattr(retriever, "get_access_summary", lambda: {})().get("hosts", {})),
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps(results_payload["summary"], ensure_ascii=False, indent=2))
    print(str(dataset_output))
    print(str(output_path))
    print(str(access_report_path))


if __name__ == "__main__":
    default_stem = "parent_work_eval_schumann_op54"
    output_dir = Path(__file__).resolve().parents[1] / "output"
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-id", default="")
    parser.add_argument("--work-ids", default="")
    parser.add_argument("--title-latin", default="Piano Concerto, Op.54")
    parser.add_argument("--title", default="")
    parser.add_argument("--all-works", action="store_true")
    parser.add_argument("--limit-works", type=int, default=0)
    parser.add_argument("--audit-report", default="")
    parser.add_argument("--allowed-audit-statuses", default="available")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dataset-output", default=str(output_dir / f"{default_stem}_dataset.json"))
    parser.add_argument("--output", default=str(output_dir / f"{default_stem}_results.json"))
    parser.add_argument("--access-report", default=str(output_dir / f"{default_stem}_access.json"))
    parsed_args = parser.parse_args()
    asyncio.run(main(parsed_args))
