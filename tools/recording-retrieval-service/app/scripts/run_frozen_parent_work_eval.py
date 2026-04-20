from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.models.protocol import RetrievalItem
from app.services.frozen_replay import FrozenLlmClient, FrozenSourceProvider
from app.services.parent_work_eval import (
    canonicalize_url,
    categorize_result_reason,
    evaluate_hit_metrics,
    load_selector_values,
    summarize_results,
)
from app.services.pipeline import RetrievalPipeline


async def run_sample(pipeline: RetrievalPipeline, sample: dict[str, object]) -> dict[str, object]:
    item = RetrievalItem.model_validate(sample["item"])
    result = await asyncio.wait_for(pipeline.retrieve(item, deadline=time.monotonic() + 55), timeout=70)
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
    metrics = evaluate_hit_metrics(
        targets=sample.get("targetUrls") or [],
        final_links=final_link_details,
        candidate_links=candidate_link_details,
    )
    payload = {
        "itemId": item.item_id,
        "recordingId": sample.get("recordingId", ""),
        "variant": sample.get("variant", ""),
        "workTypeHint": sample.get("workTypeHint", ""),
        "sourceLine": sample.get("sourceLine", ""),
        "evaluable": bool(sample.get("evaluable")),
        "targets": list(sample.get("targetUrls") or []),
        "status": result.status,
        "confidence": result.confidence,
        "finalLinks": [item["canonical"] for item in final_link_details if item["canonical"]],
        "candidateLinks": [item["canonical"] for item in candidate_link_details if item["canonical"]],
        "finalLinkDetails": final_link_details,
        "candidateLinkDetails": candidate_link_details,
        "warnings": result.warnings,
    }
    payload.update(metrics)
    payload["strictMissReason"] = categorize_result_reason(payload)
    return payload


async def main(args: argparse.Namespace) -> None:
    snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    samples = list(snapshot.get("samples") or [])
    if args.limit > 0:
        samples = samples[: args.limit]

    selected_item_ids = set(
        load_selector_values(
            inline_values=str(args.item_ids or ""),
            values_file=str(args.item_id_file or ""),
        )
    )
    if selected_item_ids:
        samples = [sample for sample in samples if str(sample.get("itemId") or "") in selected_item_ids]

    stage_snapshots = {
        str(sample.get("itemId") or ""): dict(sample.get("stagePayloads") or {})
        for sample in samples
    }
    llm_enabled = bool(snapshot.get("scope", {}).get("llmEnabled"))
    llm_snapshots = {
        str(sample.get("itemId") or ""): dict(sample.get("llmSynthesis") or {})
        for sample in samples
        if sample.get("llmSynthesis") is not None
    }
    pipeline = RetrievalPipeline(
        source_provider=FrozenSourceProvider(stage_snapshots),
        llm_client=FrozenLlmClient(llm_snapshots) if llm_enabled else None,
    )

    results: list[dict[str, object]] = []
    try:
        for sample in samples:
            results.append(await run_sample(pipeline, sample))
    finally:
        close = getattr(pipeline, "aclose", None)
        if callable(close):
            await close()

    payload = {
        "snapshot": str(Path(args.snapshot).resolve()),
        "scope": snapshot.get("scope", {}),
        "works": snapshot.get("works", []),
        "summary": summarize_results(results),
        "results": results,
    }
    if snapshot.get("work") is not None:
        payload["work"] = snapshot["work"]
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(str(output_path))


if __name__ == "__main__":
    output_dir = PROJECT_ROOT / "output"
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--item-ids", default="")
    parser.add_argument("--item-id-file", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", default=str(output_dir / "parent_work_eval_frozen_results.json"))
    parsed_args = parser.parse_args()
    asyncio.run(main(parsed_args))
