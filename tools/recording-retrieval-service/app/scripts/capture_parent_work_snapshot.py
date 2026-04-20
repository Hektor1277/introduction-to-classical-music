from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.frozen_replay import RecordingLlmClient, RecordingSourceProvider
from app.services.parent_work_eval import (
    build_work_dataset,
    canonicalize_url,
    categorize_result_reason,
    evaluate_hit_metrics,
    find_work_id,
    list_work_ids_with_supported_targets,
    load_selector_values,
    load_library_indices,
    summarize_results,
)
from app.services.retrieval import build_default_retriever


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def build_work_payloads(selected_work_ids: list[str], works: dict[str, dict], composers: dict[str, dict]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for work_id in selected_work_ids:
        work = works[work_id]
        composer = composers[work["composerId"]]
        payloads.append(
            {
                "workId": work_id,
                "title": work.get("title", ""),
                "titleLatin": work.get("titleLatin", ""),
                "catalogue": work.get("catalogue", ""),
                "composerName": composer.get("name", ""),
                "composerNameLatin": composer.get("nameLatin", ""),
            }
        )
    return payloads


async def capture_scenario(retriever, scenario, source_recorder: RecordingSourceProvider, llm_recorder: RecordingLlmClient | None) -> dict[str, object]:
    deadline = time.monotonic() + 55
    result = await asyncio.wait_for(retriever.retrieve(scenario.item, deadline=deadline), timeout=70)
    stage_snapshots = source_recorder.consume_stage_snapshots().get(scenario.item.item_id, {})
    llm_payload = None
    if llm_recorder is not None:
        llm_payload = llm_recorder.consume_synthesis_snapshots().get(scenario.item.item_id)
    response_payload = result.model_dump(by_alias=True)
    final_link_details = [
        {
            "canonical": canonicalize_url(item.get("url", "").strip()),
            "url": item.get("url", "").strip(),
            "title": item.get("title", ""),
            "confidence": float(item.get("confidence") or 0.0),
            "platform": item.get("platform", ""),
        }
        for item in response_payload["result"].get("links", [])
    ]
    candidate_link_details = [
        {
            "canonical": canonicalize_url(item.get("url", "").strip()),
            "url": item.get("url", "").strip(),
            "title": item.get("title", ""),
            "confidence": float(item.get("confidence") or 0.0),
            "platform": item.get("platform", ""),
        }
        for item in response_payload.get("linkCandidates", [])
    ]
    hit_metrics = evaluate_hit_metrics(
        targets=scenario.target_urls,
        final_links=final_link_details,
        candidate_links=candidate_link_details,
    )
    return {
        "variant": scenario.variant,
        "recordingId": scenario.recording_id,
        "itemId": scenario.item.item_id,
        "workTypeHint": scenario.item.work_type_hint,
        "sourceLine": scenario.item.source_line,
        "evaluable": scenario.evaluable,
        "targetUrls": scenario.target_urls,
        "item": scenario.item.model_dump(by_alias=True),
        "stagePayloads": stage_snapshots,
        "llmSynthesis": llm_payload,
        "capturedResponse": response_payload,
        "capturedMetrics": hit_metrics,
    }


async def main(args: argparse.Namespace) -> None:
    recordings, works, composers = load_library_indices()
    selected_work_ids = resolve_selected_work_ids(args, recordings, works)
    scenarios = []
    for work_id in selected_work_ids:
        scenarios.extend(build_work_dataset(work_id=work_id, recordings=recordings, works=works, composers=composers))

    selected_item_ids = set(
        load_selector_values(
            inline_values=str(args.item_ids or ""),
            values_file=str(args.item_id_file or ""),
        )
    )
    if selected_item_ids:
        scenarios = [scenario for scenario in scenarios if scenario.item.item_id in selected_item_ids]
    if args.limit > 0:
        scenarios = scenarios[: args.limit]

    work_payloads = build_work_payloads(selected_work_ids, works, composers)
    retriever = build_default_retriever()
    source_recorder = RecordingSourceProvider(retriever._source_provider)
    retriever._source_provider = source_recorder
    llm_recorder = None
    if retriever._llm_client is not None:
        llm_recorder = RecordingLlmClient(retriever._llm_client)
        retriever._llm_client = llm_recorder

    samples: list[dict[str, object]] = []
    summary_inputs: list[dict[str, object]] = []
    try:
        for scenario in scenarios:
            sample = await capture_scenario(retriever, scenario, source_recorder, llm_recorder)
            samples.append(sample)
            metrics = sample["capturedMetrics"]
            summary_inputs.append(
                {
                    "variant": sample["variant"],
                    "evaluable": sample["evaluable"],
                    "finalHit": metrics["finalHit"],
                    "candidateHit": metrics["candidateHit"],
                    "relaxedFinalHit": metrics["relaxedFinalHit"],
                    "relaxedCandidateHit": metrics["relaxedCandidateHit"],
                    "versionFinalHit": metrics["versionFinalHit"],
                    "versionCandidateHit": metrics["versionCandidateHit"],
                    "strictMissReason": categorize_result_reason(
                        {
                            "evaluable": sample["evaluable"],
                            "status": sample["capturedResponse"]["status"],
                            "finalHit": metrics["finalHit"],
                            "candidateHit": metrics["candidateHit"],
                            "relaxedFinalHit": metrics["relaxedFinalHit"],
                            "relaxedCandidateHit": metrics["relaxedCandidateHit"],
                            "versionFinalHit": metrics["versionFinalHit"],
                            "versionCandidateHit": metrics["versionCandidateHit"],
                            "finalMatchType": metrics["finalMatchType"],
                            "candidateMatchType": metrics["candidateMatchType"],
                            "finalVersionMatchType": metrics["finalVersionMatchType"],
                            "candidateVersionMatchType": metrics["candidateVersionMatchType"],
                            "finalLinks": [
                                canonicalize_url(item.get("url", ""))
                                for item in sample["capturedResponse"]["result"].get("links", [])
                            ],
                            "candidateLinks": [
                                canonicalize_url(item.get("url", ""))
                                for item in sample["capturedResponse"].get("linkCandidates", [])
                            ],
                        }
                    ),
                }
            )
    finally:
        close_retriever = getattr(retriever, "aclose", None)
        if callable(close_retriever):
            await close_retriever()

    payload = {
        "capturedAt": now_iso(),
        "scope": {
            "allWorks": bool(args.all_works),
            "workCount": len(selected_work_ids),
            "scenarioCount": len(scenarios),
            "llmEnabled": llm_recorder is not None,
        },
        "works": work_payloads,
        "summary": summarize_results(summary_inputs),
        "samples": samples,
    }
    if len(work_payloads) == 1:
        payload["work"] = work_payloads[0]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(str(output_path))


if __name__ == "__main__":
    default_stem = "parent_work_eval_schumann_op54_snapshot"
    output_dir = PROJECT_ROOT / "output"
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-id", default="")
    parser.add_argument("--work-ids", default="")
    parser.add_argument("--title-latin", default="Piano Concerto, Op.54")
    parser.add_argument("--title", default="")
    parser.add_argument("--all-works", action="store_true")
    parser.add_argument("--limit-works", type=int, default=0)
    parser.add_argument("--item-ids", default="")
    parser.add_argument("--item-id-file", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", default=str(output_dir / f"{default_stem}.json"))
    parsed_args = parser.parse_args()
    asyncio.run(main(parsed_args))
