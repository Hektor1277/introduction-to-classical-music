from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path

from app.models.protocol import Credit, LinkSeed, RetrievalItem, Seed
from app.services.parent_work_eval import canonicalize_url
from app.services.retrieval import build_default_retriever


REQUESTED_FIELDS = [
    "links",
    "images",
    "performanceDateText",
    "venueText",
    "albumTitle",
    "label",
    "releaseDate",
    "notes",
]


@dataclass(slots=True)
class RoleInput:
    role: str
    display_name: str
    label: str = ""


@dataclass(slots=True)
class Scenario:
    scenario_id: str
    recording_id: str
    work_type_hint: str
    primary: RoleInput | None = None
    secondary: RoleInput | None = None
    group: RoleInput | None = None
    performance_date_text: str = ""


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def load_json(name: str) -> list[dict]:
    return json.loads((workspace_root() / "data" / "library" / name).read_text(encoding="utf-8"))


def recording_index() -> dict[str, dict]:
    return {item["id"]: item for item in load_json("recordings.json")}


def composer_index() -> dict[str, dict]:
    return {item["id"]: item for item in load_json("composers.json")}


def work_index() -> dict[str, dict]:
    return {item["id"]: item for item in load_json("works.json")}


def sample_scenarios() -> list[Scenario]:
    return [
        Scenario(
            scenario_id="bohm-full",
            recording_id="recording-第七交响曲-伯姆1976",
            work_type_hint="orchestral",
            primary=RoleInput("conductor", "卡尔·伯姆", "Karl Böhm"),
            group=RoleInput("orchestra", "维也纳爱乐乐团", "Wiener Philharmoniker"),
            performance_date_text="February 2-5 1976",
        ),
        Scenario(
            scenario_id="bohm-no-date",
            recording_id="recording-第七交响曲-伯姆1976",
            work_type_hint="orchestral",
            primary=RoleInput("conductor", "卡尔·伯姆", "Karl Böhm"),
            group=RoleInput("orchestra", "维也纳爱乐乐团", "Wiener Philharmoniker"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="bohm-conductor-only",
            recording_id="recording-第七交响曲-伯姆1976",
            work_type_hint="orchestral",
            primary=RoleInput("conductor", "卡尔·伯姆", "Karl Böhm"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="annie-full",
            recording_id="recording-a小调钢琴协奏曲-安妮-and-克列茨基",
            work_type_hint="concerto",
            primary=RoleInput("soloist", "安妮·费舍尔", "Annie Fischer"),
            secondary=RoleInput("conductor", "保罗·克列茨基", "Kletzki"),
            group=RoleInput("orchestra", "布达佩斯爱乐乐团", "Budapest Philharmonic Orchestra"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="annie-no-group",
            recording_id="recording-a小调钢琴协奏曲-安妮-and-克列茨基",
            work_type_hint="concerto",
            primary=RoleInput("soloist", "安妮·费舍尔", "Annie Fischer"),
            secondary=RoleInput("conductor", "保罗·克列茨基", "Kletzki"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="arrau-full",
            recording_id="recording-第二十三号奏鸣曲-热情-克劳迪奥阿劳1970",
            work_type_hint="chamber_solo",
            primary=RoleInput("soloist", "克劳迪奥·阿劳", "Claudio Arrau"),
            performance_date_text="Beethovenfest Bonn 1970",
        ),
        Scenario(
            scenario_id="arrau-no-date",
            recording_id="recording-第二十三号奏鸣曲-热情-克劳迪奥阿劳1970",
            work_type_hint="chamber_solo",
            primary=RoleInput("soloist", "克劳迪奥·阿劳", "Claudio Arrau"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="spring-full",
            recording_id="recording-第5号小提琴奏鸣曲-春天-让富尼埃-and-吉内特多延",
            work_type_hint="chamber_solo",
            primary=RoleInput("soloist", "让·富尼埃", "Jean Fournier"),
            secondary=RoleInput("soloist", "吉内特·多延", "Ginette Doyen"),
            performance_date_text="early '50s",
        ),
        Scenario(
            scenario_id="spring-lead-only",
            recording_id="recording-第5号小提琴奏鸣曲-春天-让富尼埃-and-吉内特多延",
            work_type_hint="chamber_solo",
            primary=RoleInput("soloist", "让·富尼埃", "Jean Fournier"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="heifetz-full",
            recording_id="recording-d大调小提琴协奏曲-海菲兹-and-托斯卡尼尼1940",
            work_type_hint="concerto",
            primary=RoleInput("soloist", "亚莎·海菲兹", "Jascha Heifetz"),
            secondary=RoleInput("conductor", "阿尔图罗·托斯卡尼尼", "Arturo Toscanini"),
            performance_date_text="March 11, 1940",
        ),
        Scenario(
            scenario_id="heifetz-lead-only",
            recording_id="recording-d大调小提琴协奏曲-海菲兹-and-托斯卡尼尼1940",
            work_type_hint="concerto",
            primary=RoleInput("soloist", "亚莎·海菲兹", "Jascha Heifetz"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="gieseking-full",
            recording_id="recording-a小调钢琴协奏曲-吉泽金-and-富特文格勒1942",
            work_type_hint="concerto",
            primary=RoleInput("soloist", "吉泽金", "Walter Gieseking"),
            secondary=RoleInput("conductor", "富特文格勒", "Wilhelm Furtwangler"),
            group=RoleInput("orchestra", "柏林爱乐乐团", "Berlin Philharmonic Orchestra"),
            performance_date_text="March 3, 1942 Berlin",
        ),
        Scenario(
            scenario_id="karajan-alpine-full",
            recording_id="recording-阿尔卑斯山交响曲-卡拉扬1982",
            work_type_hint="orchestral",
            primary=RoleInput("conductor", "卡拉扬", "Herbert von Karajan"),
            group=RoleInput("orchestra", "柏林爱乐乐团", "Berlin Philharmonic Orchestra"),
            performance_date_text="August 28, 1982 Salzburg",
        ),
        Scenario(
            scenario_id="bernstein-fantastique-conductor-only",
            recording_id="recording-幻想交响曲-伯恩斯坦1977",
            work_type_hint="orchestral",
            primary=RoleInput("conductor", "伯恩斯坦", "Leonard Bernstein"),
            performance_date_text="",
        ),
        Scenario(
            scenario_id="kreisler-spring-full",
            recording_id="recording-第5号小提琴奏鸣曲-春天-弗里茨克莱斯勒-and-弗朗茨鲁普",
            work_type_hint="chamber_solo",
            primary=RoleInput("soloist", "克莱斯勒", "Fritz Kreisler"),
            secondary=RoleInput("soloist", "鲁普", "Franz Rupp"),
            performance_date_text="1935",
        ),
    ]


def build_source_line(recording: dict, work: dict, composer: dict, scenario: Scenario) -> str:
    parts = [
        composer.get("name", ""),
        work.get("title", ""),
        scenario.primary.label or scenario.primary.display_name if scenario.primary else "",
        scenario.secondary.label or scenario.secondary.display_name if scenario.secondary else "",
        scenario.group.label or scenario.group.display_name if scenario.group else "",
        scenario.performance_date_text or "-",
    ]
    return " | ".join(part for part in parts if part != "")


def build_item(recording: dict, work: dict, composer: dict, scenario: Scenario) -> RetrievalItem:
    credits = [
        Credit(role=role.role, displayName=role.display_name, label=role.label)
        for role in [scenario.primary, scenario.secondary, scenario.group]
        if role is not None
    ]
    seed = Seed(
        title=recording.get("title") or build_source_line(recording, work, composer, scenario),
        composerName=composer.get("name", ""),
        composerNameLatin=composer.get("nameLatin", ""),
        workTitle=work.get("title", ""),
        workTitleLatin=work.get("titleLatin", ""),
        catalogue=work.get("catalogue", ""),
        performanceDateText=scenario.performance_date_text,
        venueText="",
        albumTitle="",
        label="",
        releaseDate="",
        credits=credits,
        links=[],
        notes="",
    )
    return RetrievalItem(
        itemId=scenario.scenario_id,
        recordingId=recording["id"],
        workId=recording["workId"],
        composerId=work["composerId"],
        workTypeHint=scenario.work_type_hint,
        sourceLine=build_source_line(recording, work, composer, scenario),
        seed=seed,
        requestedFields=REQUESTED_FIELDS,
    )


async def run_scenario(retriever, recording: dict, work: dict, composer: dict, scenario: Scenario) -> dict:
    item = build_item(recording, work, composer, scenario)
    target_urls = [canonicalize_url(link["url"]) for link in recording.get("links", []) if link.get("url")]
    try:
        deadline = time.monotonic() + 55
        result = await asyncio.wait_for(retriever.retrieve(item, deadline=deadline), timeout=70)
    except TimeoutError:
        return {
            "scenarioId": scenario.scenario_id,
            "recordingId": recording["id"],
            "workTypeHint": scenario.work_type_hint,
            "sourceLine": item.source_line,
            "targets": target_urls,
            "status": "timeout",
            "confidence": 0.0,
            "finalLinks": [],
            "candidateLinks": [],
            "finalHit": False,
            "candidateHit": False,
            "warnings": ["scenario timeout after internal 55s / external 70s"],
        }
    final_urls = [canonicalize_url(link.url) for link in result.result.links]
    candidate_urls = [canonicalize_url(link.url) for link in result.link_candidates]
    final_hit = any(url in target_urls for url in final_urls if url)
    candidate_hit = any(url in target_urls for url in candidate_urls if url)
    return {
        "scenarioId": scenario.scenario_id,
        "recordingId": recording["id"],
        "workTypeHint": scenario.work_type_hint,
        "sourceLine": item.source_line,
        "targets": target_urls,
        "status": result.status,
        "confidence": result.confidence,
        "finalLinks": final_urls,
        "candidateLinks": candidate_urls,
        "finalHit": final_hit,
        "candidateHit": candidate_hit,
        "warnings": result.warnings,
    }


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


async def main(
    output_path: Path,
    only: set[str] | None = None,
    access_report_path: Path | None = None,
) -> None:
    recordings = recording_index()
    works = work_index()
    composers = composer_index()
    retriever = build_default_retriever()
    results: list[dict] = []
    scenario_access: dict[str, list[dict]] = {}
    try:
        for scenario in sample_scenarios():
            if only and scenario.scenario_id not in only:
                continue
            recording = recordings[scenario.recording_id]
            work = works[recording["workId"]]
            composer = composers[work["composerId"]]
            results.append(await run_scenario(retriever, recording, work, composer, scenario))
            scenario_access[scenario.scenario_id] = list(getattr(retriever, "consume_access_events", lambda: [])())

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        resolved_access_report = access_report_path or output_path.with_name(f"{output_path.stem}_access_report.json")
        resolved_access_report.parent.mkdir(parents=True, exist_ok=True)
        access_payload = build_access_report_payload(
            scenario_access=scenario_access,
            host_summary=dict(getattr(retriever, "get_access_summary", lambda: {})().get("hosts", {})),
        )
        resolved_access_report.write_text(json.dumps(access_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        summary = {
            "total": len(results),
            "finalHit": sum(1 for item in results if item["finalHit"]),
            "candidateHit": sum(1 for item in results if item["candidateHit"]),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(str(output_path))
        print(str(resolved_access_report))
    finally:
        close_retriever = getattr(retriever, "aclose", None)
        if callable(close_retriever):
            await close_retriever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "output" / "real_data_round10.json"),
    )
    parser.add_argument("--access-report", default="")
    parser.add_argument("--only", nargs="*", default=[])
    args = parser.parse_args()
    asyncio.run(
        main(
            Path(args.output),
            set(args.only) or None,
            Path(args.access_report) if args.access_report else None,
        )
    )
