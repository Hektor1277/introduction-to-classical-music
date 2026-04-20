from __future__ import annotations

import asyncio
import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from app.models.protocol import (
    EvidenceItem,
    ImageCandidate,
    LinkCandidate,
    LogEntry,
    ResultItemResponse,
    ResultPayload,
    RetrievalItem,
)
from app.services.source_profiles import PersonAliasLoader

LOW_CONFIDENCE_THRESHOLD = 0.45
FINAL_CONFIDENCE_THRESHOLD = 0.85
CORROBORATED_CONFIDENCE_THRESHOLD = 0.65
SAME_RECORDING_THRESHOLD = 0.75
FINAL_LINK_CONFIDENCE_THRESHOLD = 0.65
FINAL_IMAGE_CONFIDENCE_THRESHOLD = 0.65
PRIMARY_PLATFORM_COMPLETION_CONFIDENCE_THRESHOLD = 0.55
PRIMARY_PLATFORM_COMPLETION_CONFIDENCE_GAP = 0.28
PRIMARY_PLATFORM_COMPLETION_EXACTNESS_GAP = 0.02
PRIMARY_PLATFORM_COMPLETION_MIN_EXACTNESS = 0.04
PRIMARY_COMPLETION_PLATFORMS = {"bilibili", "youtube", "apple_music"}
CANDIDATE_GREEN_PER_PLATFORM_LIMIT = 3
CANDIDATE_YELLOW_PER_PLATFORM_LIMIT = 2
CANDIDATE_GREEN_EXACTNESS_THRESHOLD = 0.05
CANDIDATE_YELLOW_EXACTNESS_THRESHOLD = 0.0

FINALIZABLE_FIELDS = {
    "performanceDateText",
    "venueText",
    "albumTitle",
    "label",
    "releaseDate",
}

STAGE_WEIGHTS = {
    "existing-link": 0.8,
    "high-quality": 1.0,
    "streaming": 3.0,
    "fallback": 0.4,
    "llm": 0.5,
}


@dataclass(slots=True)
class RawInputEnvelope:
    item_id: str
    title: str
    source_line: str
    raw_text: str
    existing_links: list[dict[str, str]]


@dataclass(slots=True)
class DraftRecordingEntry:
    item_id: str
    title: str
    composer_name: str
    composer_name_latin: str
    work_title: str
    work_title_latin: str
    catalogue: str
    performance_date_text: str
    venue_text: str
    album_title: str
    label: str
    release_date: str
    notes: str
    source_line: str
    raw_text: str
    existing_links: list[dict[str, str]]
    primary_names: list[str] = field(default_factory=list)
    primary_names_latin: list[str] = field(default_factory=list)
    secondary_names: list[str] = field(default_factory=list)
    secondary_names_latin: list[str] = field(default_factory=list)
    query_lead_names: list[str] = field(default_factory=list)
    query_lead_names_latin: list[str] = field(default_factory=list)
    lead_names: list[str] = field(default_factory=list)
    lead_names_latin: list[str] = field(default_factory=list)
    ensemble_names: list[str] = field(default_factory=list)
    ensemble_names_latin: list[str] = field(default_factory=list)
    work_type_hint: str = "unknown"


@dataclass(slots=True)
class RetrievalProfile:
    category: str
    tags: list[str]
    queries: list[str]
    latin_queries: list[str] = field(default_factory=list)
    zh_queries: list[str] = field(default_factory=list)
    mixed_queries: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceRecord:
    url: str
    source_label: str
    source_kind: str
    title: str
    description: str
    platform: str
    weight: float
    same_recording_score: float
    duration_seconds: int = 0
    uploader: str = ""
    view_count: int = 0
    fields: dict[str, str] = field(default_factory=dict)
    images: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class FieldCandidate:
    field: str
    value: str
    confidence: float
    source_url: str
    source_label: str
    accepted: bool


class SourceProvider(Protocol):
    async def inspect_existing_links(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]: ...

    async def search_high_quality(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]: ...

    async def search_streaming(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]: ...

    async def search_fallback(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
    ) -> list[dict[str, Any]]: ...

    async def aclose(self) -> None: ...


class LlmClient(Protocol):
    async def synthesize(
        self,
        draft: DraftRecordingEntry,
        profile: RetrievalProfile,
        records: list[SourceRecord],
    ) -> dict[str, Any]: ...


def compact(value: Any) -> str:
    return str(value or "").strip()


def resolve_credit_bucket(*, work_type: str, role: str) -> str:
    role = compact(role).lower()
    work_type = compact(work_type).lower()
    if role in {"orchestra", "ensemble", "choir", "group"}:
        return "ensemble"
    if work_type == "concerto":
        if role == "soloist":
            return "primary"
        if role in {"conductor", "singer", "instrumentalist", "person"}:
            return "secondary"
    elif work_type == "opera_vocal":
        if role == "conductor":
            return "primary"
        if role in {"soloist", "singer", "instrumentalist", "person"}:
            return "secondary"
    elif work_type == "chamber_solo":
        if role in {"soloist", "instrumentalist", "person"}:
            return "primary"
        if role in {"conductor", "singer"}:
            return "secondary"
    else:
        if role == "conductor":
            return "primary"
        if role in {"soloist", "singer", "instrumentalist", "person"}:
            return "secondary"
    return "ignore"


def build_query_lead_terms(
    primary_values: list[str],
    secondary_values: list[str],
    *,
    prefer_collaboration: bool = False,
    max_primary_values: int = 2,
    max_secondary_values: int = 2,
) -> list[str]:
    if primary_values and secondary_values:
        combined: list[str] = []
        for primary_value in primary_values[:max_primary_values]:
            for secondary_value in secondary_values[:max_secondary_values]:
                combined.extend(
                    [
                        " ".join([primary_value, secondary_value]).strip(),
                        " / ".join([primary_value, secondary_value]).strip(),
                    ]
                )
        if prefer_collaboration:
            return dedupe_preserve_order([*combined, *primary_values, *secondary_values])
        return dedupe_preserve_order([*primary_values, combined[0], *secondary_values, combined[1]])
    return dedupe_preserve_order([*primary_values, *secondary_values])


class PersonNameLookup(Protocol):
    def resolve(self, person_id: str) -> dict[str, Any] | None: ...


def locate_parent_people_path() -> Path | None:
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "data" / "library" / "people.json"
        if candidate.is_file():
            return candidate
    return None


class LibraryPersonNameLookup:
    def __init__(self, people_path: Path | None = None) -> None:
        self._people_path = people_path or locate_parent_people_path()
        self._people_by_id: dict[str, dict[str, Any]] | None = None
        self._people_by_name: dict[str, dict[str, Any]] | None = None

    def resolve(self, person_id: str) -> dict[str, Any] | None:
        normalized_id = compact(person_id)
        if not normalized_id or self._people_path is None:
            return None
        self._ensure_loaded()
        return self._people_by_id.get(normalized_id)

    def resolve_name(self, value: str) -> dict[str, Any] | None:
        normalized = compact(value).lower()
        if not normalized or self._people_path is None:
            return None
        self._ensure_loaded()
        return self._people_by_name.get(normalized)

    def _ensure_loaded(self) -> None:
        if self._people_by_id is not None and self._people_by_name is not None:
            return
        if self._people_path is None:
            self._people_by_id = {}
            self._people_by_name = {}
            return
        try:
            payload = json.loads(self._people_path.read_text(encoding="utf-8"))
        except Exception:
            self._people_by_id = {}
            self._people_by_name = {}
            return

        self._people_by_id = {}
        self._people_by_name = {}
        for item in payload:
            person_id = compact(item.get("id"))
            if person_id:
                self._people_by_id[person_id] = item
            for value in [
                compact(item.get("name")),
                compact(item.get("nameLatin")),
                *[compact(alias) for alias in item.get("aliases") or []],
            ]:
                normalized = value.lower()
                if normalized and normalized not in self._people_by_name:
                    self._people_by_name[normalized] = item


class InputNormalizer:
    def __init__(
        self,
        person_name_lookup: PersonNameLookup | None = None,
        person_alias_loader: PersonAliasLoader | None = None,
    ) -> None:
        self._person_name_lookup = person_name_lookup or LibraryPersonNameLookup()
        self._person_alias_loader = person_alias_loader or PersonAliasLoader()

    def normalize(self, item: RetrievalItem) -> DraftRecordingEntry:
        primary_names: list[str] = []
        primary_names_latin: list[str] = []
        secondary_names: list[str] = []
        secondary_names_latin: list[str] = []
        ensembles: list[str] = []
        ensembles_latin: list[str] = []
        title_people, title_groups, title_date_hint = infer_title_entities(item.seed.title)
        work_type = compact(item.work_type_hint).lower()
        for credit in item.seed.credits:
            display_name = strip_alias_annotations(compact(credit.display_name))
            label = compact(credit.label)
            resolved_person = self._person_name_lookup.resolve(credit.person_id)
            resolved_name = compact((resolved_person or {}).get("name"))
            resolved_name_latin = compact((resolved_person or {}).get("nameLatin"))
            resolved_aliases = [compact(value) for value in (resolved_person or {}).get("aliases") or [] if compact(value)]
            primary_label = compact(display_name or resolved_name or label)
            role = compact(credit.role).lower()
            explicit_latin = extract_explicit_latin_alias(credit.display_name) or extract_explicit_latin_alias(credit.label)
            if not primary_label:
                continue
            latin_value = ""
            if explicit_latin:
                latin_value = explicit_latin
            elif label and looks_latin(label):
                latin_value = label
            elif looks_latin(primary_label):
                latin_value = primary_label
            elif looks_latin(resolved_name_latin):
                latin_value = resolved_name_latin
            alias_expansions = self._person_alias_loader.expand(primary_label, role=role)
            if resolved_name:
                alias_expansions.extend(self._person_alias_loader.expand(resolved_name, role=role))
            latin_variants = build_latin_credit_variants(latin_value, [*resolved_aliases, *alias_expansions])

            bucket = resolve_credit_bucket(work_type=work_type, role=role)
            if work_type == "chamber_solo" and role in {"soloist", "instrumentalist", "person"} and primary_names:
                bucket = "secondary"
            if work_type == "concerto" and role == "soloist" and primary_names:
                bucket = "secondary"
            if bucket == "primary":
                primary_names.append(primary_label)
                primary_names_latin.extend(latin_variants)
            elif bucket == "secondary":
                secondary_names.append(primary_label)
                secondary_names_latin.extend(latin_variants)
            elif bucket == "lead":
                primary_names.append(primary_label)
                primary_names_latin.extend(latin_variants)
            if role in {"orchestra", "ensemble", "choir", "group"}:
                ensembles.append(primary_label)
                ensembles_latin.extend(latin_variants)

        if primary_names and not secondary_names and work_type in {"concerto", "chamber_solo", "opera_vocal"}:
            for inferred_name in title_people:
                if any(person_variant_matches(inferred_name, existing) for existing in [*primary_names, *secondary_names]):
                    continue
                secondary_names.append(inferred_name)
                if looks_latin(inferred_name):
                    secondary_names_latin.append(inferred_name)
                    continue
                inferred_latin_variants = self._build_title_inferred_latin_variants(inferred_name, work_type=work_type)
                secondary_names_latin.extend(inferred_latin_variants)
        if not ensembles:
            for inferred_group in title_groups:
                if inferred_group in ensembles:
                    continue
                ensembles.append(inferred_group)
                if looks_latin(inferred_group):
                    ensembles_latin.append(inferred_group)

        title_performance_context = extract_title_performance_context(item.seed.title)
        performance_date_text = compact(item.seed.performance_date_text)
        if not performance_date_text:
            if work_type == "chamber_solo" and title_performance_context:
                performance_date_text = title_performance_context
            else:
                performance_date_text = title_date_hint
        if not performance_date_text:
            performance_date_text = extract_title_date_hint(item.item_id)

        leads = dedupe_preserve_order([*primary_names, *secondary_names])
        leads_latin = dedupe_preserve_order([*primary_names_latin, *secondary_names_latin])
        if not leads or not ensembles:
            inferred_leads, inferred_groups = infer_people_from_source_line(item.source_line)
            if not leads:
                leads = inferred_leads
                leads_latin = [value for value in inferred_leads if looks_latin(value)]
            if not ensembles:
                ensembles = inferred_groups
                ensembles_latin = [value for value in inferred_groups if looks_latin(value)]

        raw_text = " | ".join(
            value
            for value in [
                item.source_line,
                item.seed.title,
                item.seed.composer_name,
                item.seed.work_title,
                performance_date_text,
            ]
            if compact(value)
        )
        envelope = RawInputEnvelope(
            item_id=item.item_id,
            title=item.seed.title,
            source_line=item.source_line,
            raw_text=raw_text,
            existing_links=[
                {"platform": compact(link.platform), "url": compact(link.url), "title": compact(link.title)}
                for link in item.seed.links
                if compact(link.url)
            ],
        )
        prefer_collaboration = work_type in {"concerto", "chamber_solo", "opera_vocal"}
        query_variant_limit = 3 if work_type == "chamber_solo" else 2
        query_lead_names = build_query_lead_terms(
            primary_names,
            secondary_names,
            prefer_collaboration=prefer_collaboration,
            max_primary_values=query_variant_limit,
        )
        query_lead_names_latin = build_query_lead_terms(
            primary_names_latin,
            secondary_names_latin,
            prefer_collaboration=prefer_collaboration,
            max_primary_values=query_variant_limit,
        )
        if not query_lead_names:
            query_lead_names = leads
        if not query_lead_names_latin:
            query_lead_names_latin = leads_latin

        return DraftRecordingEntry(
            item_id=envelope.item_id,
            title=compact(item.seed.title),
            composer_name=compact(item.seed.composer_name),
            composer_name_latin=compact(item.seed.composer_name_latin),
            work_title=compact(item.seed.work_title),
            work_title_latin=compact(item.seed.work_title_latin),
            catalogue=compact(item.seed.catalogue),
            performance_date_text=performance_date_text,
            venue_text=compact(item.seed.venue_text),
            album_title=compact(item.seed.album_title),
            label=compact(item.seed.label),
            release_date=compact(item.seed.release_date),
            notes=compact(item.seed.notes),
            source_line=envelope.source_line,
            raw_text=envelope.raw_text,
            existing_links=envelope.existing_links,
            primary_names=dedupe_preserve_order(primary_names),
            primary_names_latin=dedupe_preserve_order(primary_names_latin),
            secondary_names=dedupe_preserve_order(secondary_names),
            secondary_names_latin=dedupe_preserve_order(secondary_names_latin),
            query_lead_names=dedupe_preserve_order(query_lead_names),
            query_lead_names_latin=dedupe_preserve_order(query_lead_names_latin),
            lead_names=dedupe_preserve_order(leads),
            lead_names_latin=dedupe_preserve_order(leads_latin),
            ensemble_names=dedupe_preserve_order(ensembles),
            ensemble_names_latin=dedupe_preserve_order(ensembles_latin),
            work_type_hint=work_type,
        )

    def _build_title_inferred_latin_variants(self, inferred_name: str, *, work_type: str) -> list[str]:
        variants: list[str] = []
        resolve_name = getattr(self._person_name_lookup, "resolve_name", None)
        if callable(resolve_name):
            resolved_person = resolve_name(inferred_name)
            if resolved_person:
                variants.extend(
                    build_latin_credit_variants(
                        compact((resolved_person or {}).get("nameLatin")),
                        [compact(value) for value in (resolved_person or {}).get("aliases") or [] if compact(value)],
                    )
                )
        normalized_work_type = compact(work_type).lower()
        inferred_role = None
        if normalized_work_type == "concerto":
            inferred_role = "conductor"
        elif normalized_work_type in {"chamber_solo", "opera_vocal"}:
            inferred_role = "soloist"
        variants.extend(build_latin_credit_variants("", self._person_alias_loader.expand(inferred_name, role=inferred_role)))
        return dedupe_preserve_order(variants)


class ProfileResolver:
    KEYWORD_MAP = {
        "piano": ("piano", "pianist", "钢琴"),
        "violin": ("violin", "violinist", "小提琴"),
        "vocal": ("soprano", "tenor", "baritone", "mezzo", "歌剧", "声乐", "女高音", "男高音"),
        "choral": ("choir", "chorus", "合唱"),
        "live": (" live ", "live in", "recorded live", "现场"),
        "studio": ("studio", "录音室"),
    }

    def __init__(self) -> None:
        self._normalizer = InputNormalizer()

    def resolve(self, item: RetrievalItem) -> RetrievalProfile:
        draft = self._normalizer.normalize(item)
        haystack = " ".join(
            [
                draft.title,
                draft.work_title,
                draft.work_title_latin,
                draft.source_line,
                " ".join(draft.lead_names),
                " ".join(draft.ensemble_names),
            ]
        ).lower()
        tags: list[str] = []
        padded = f" {haystack} "
        for tag, needles in self.KEYWORD_MAP.items():
            if any(needle.lower() in padded for needle in needles):
                tags.append(tag)

        latin_queries = build_queries(
            work_query=build_work_query(draft, prefer_latin=True),
            composer_query=compact(draft.composer_name_latin),
            lead_terms=draft.query_lead_names_latin or [value for value in draft.query_lead_names if looks_latin(value)],
            ensemble_terms=draft.ensemble_names_latin or [value for value in draft.ensemble_names if looks_latin(value)],
            title=draft.title,
            performance_date_text=draft.performance_date_text,
        )
        zh_queries = build_queries(
            work_query=build_work_query(draft, prefer_latin=False),
            composer_query=compact(draft.composer_name),
            lead_terms=[value for value in draft.query_lead_names if contains_cjk(value)],
            ensemble_terms=[value for value in draft.ensemble_names if contains_cjk(value)],
            title=draft.title,
            performance_date_text=draft.performance_date_text,
        )
        mixed_queries = build_queries(
            work_query=build_work_query(draft, prefer_latin=False) or build_work_query(draft, prefer_latin=True),
            composer_query=compact(draft.composer_name_latin or draft.composer_name),
            lead_terms=dedupe_preserve_order([*draft.query_lead_names_latin, *draft.query_lead_names]),
            ensemble_terms=dedupe_preserve_order([*draft.ensemble_names_latin, *draft.ensemble_names]),
            title=draft.title,
            performance_date_text=draft.performance_date_text,
        )
        queries = latin_queries or mixed_queries or zh_queries

        return RetrievalProfile(
            category=item.work_type_hint,
            tags=tags,
            queries=queries,
            latin_queries=latin_queries,
            zh_queries=zh_queries,
            mixed_queries=mixed_queries,
        )


def build_work_query(draft: DraftRecordingEntry, *, prefer_latin: bool) -> str:
    work = compact(draft.work_title_latin if prefer_latin else draft.work_title)
    if not work:
        if prefer_latin:
            work = compact(build_latin_work_alias(draft.work_title))
        if not work:
            work = compact(draft.work_title if prefer_latin else draft.work_title_latin)
    catalogue = compact(draft.catalogue)
    if work and catalogue and catalogue.lower() not in work.lower():
        return f"{work} {catalogue}"
    return work or draft.title


def build_queries(
    *,
    work_query: str,
    composer_query: str,
    lead_terms: list[str],
    ensemble_terms: list[str],
    title: str,
    performance_date_text: str,
) -> list[str]:
    queries: list[str] = []
    lead = dedupe_preserve_order(lead_terms)[:3] or [""]
    ensemble = dedupe_preserve_order(ensemble_terms)[:3] or [""]
    shapes = [
        lambda lead_term, ensemble_term: [work_query, lead_term, ensemble_term, performance_date_text],
        lambda lead_term, ensemble_term: [composer_query, work_query, lead_term, ensemble_term, performance_date_text],
        lambda lead_term, ensemble_term: [composer_query, work_query, lead_term, ensemble_term],
        lambda lead_term, ensemble_term: [work_query, lead_term, performance_date_text],
        lambda lead_term, ensemble_term: [lead_term, ensemble_term, performance_date_text],
        lambda lead_term, ensemble_term: [work_query, ensemble_term, performance_date_text],
        lambda lead_term, ensemble_term: [lead_term, performance_date_text],
        lambda lead_term, ensemble_term: [lead_term, ensemble_term],
        lambda lead_term, ensemble_term: [title, lead_term, ensemble_term],
        lambda lead_term, ensemble_term: [ensemble_term, performance_date_text],
    ]
    for shape in shapes:
        for lead_term in lead:
            for ensemble_term in ensemble:
                parts = shape(lead_term, ensemble_term)
                query = " ".join(compact(part) for part in parts if compact(part))
                if query and query not in queries:
                    queries.append(query)
    return queries


def looks_latin(value: str) -> bool:
    normalized = compact(value)
    return bool(normalized) and bool(re.search(r"[A-Za-z]", normalized)) and not contains_cjk(normalized)


def contains_cjk(value: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", value or ""))


def ascii_fold(value: str) -> str:
    normalized = compact(value)
    if not normalized:
        return ""
    folded = unicodedata.normalize("NFKD", normalized)
    return "".join(char for char in folded if not unicodedata.combining(char))


def build_condensed_person_latin_variants(value: str) -> list[str]:
    cleaned = compact(value)
    if not cleaned or not looks_latin(cleaned):
        return []
    parts = [part for part in cleaned.split() if part]
    if len(parts) < 3:
        return []
    surname_particles = {"da", "de", "del", "della", "der", "di", "du", "la", "le", "ten", "ter", "van", "von"}
    surname_parts = [parts[-1]]
    index = len(parts) - 2
    while index > 0 and parts[index].casefold() in surname_particles:
        surname_parts.insert(0, parts[index])
        index -= 1
    condensed = " ".join([parts[0], *surname_parts])
    if compact(condensed) == cleaned:
        return []
    variants = [condensed]
    folded = ascii_fold(condensed)
    if folded and looks_latin(folded) and folded != condensed:
        variants.append(folded)
    return dedupe_preserve_order(variants)


def build_latin_credit_variants(primary_value: str, aliases: list[str]) -> list[str]:
    candidates = [compact(primary_value), *[compact(alias) for alias in aliases]]
    variants: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        explicit_latin = extract_explicit_latin_alias(candidate)
        if explicit_latin:
            variants.extend(build_condensed_person_latin_variants(explicit_latin))
            variants.append(explicit_latin)
            folded_explicit = ascii_fold(explicit_latin)
            if folded_explicit and looks_latin(folded_explicit) and folded_explicit != explicit_latin:
                variants.extend(build_condensed_person_latin_variants(folded_explicit))
                variants.append(folded_explicit)
        cleaned = strip_alias_annotations(candidate)
        if not looks_latin(cleaned):
            continue
        variants.extend(build_condensed_person_latin_variants(cleaned))
        variants.append(cleaned)
        folded = ascii_fold(cleaned)
        if folded and looks_latin(folded) and folded != cleaned:
            variants.extend(build_condensed_person_latin_variants(folded))
            variants.append(folded)
    return dedupe_preserve_order(variants)


def build_latin_work_alias(value: str) -> str:
    text = compact(value)
    if not text:
        return ""
    patterns = [
        (r"第([\u4e00-\u9fff\d两零十百]+)(?:号)?交响曲", "Symphony"),
        (r"第([\u4e00-\u9fff\d两零十百]+)(?:号)?协奏曲", "Concerto"),
        (r"第([\u4e00-\u9fff\d两零十百]+)(?:号)?奏鸣曲", "Sonata"),
    ]
    for pattern, kind in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        number = normalize_cn_number(match.group(1))
        if number:
            return f"{kind} No. {number}"
    return ""


def strip_work_key_text(value: str) -> str:
    normalized = compact(value).lower()
    if not normalized:
        return ""
    stripped = re.sub(
        r"\bin\s+[a-g](?:[- ]?(?:sharp|flat))?\s+(?:major|minor)\b",
        "",
        normalized,
        flags=re.I,
    )
    stripped = re.sub(r"\s+", " ", stripped)
    return stripped.strip(" ,.;:-")


def build_generic_work_aliases(value: str) -> set[str]:
    normalized = compact(value)
    if not normalized:
        return set()
    lowered = normalized.lower()
    stripped = strip_work_key_text(lowered)
    stripped = re.sub(r"\b(?:op|k|bwv|hob|d|wab)\.?\s*\d+[a-z]?\b", "", stripped, flags=re.I)
    stripped = re.sub(r"\s+", " ", stripped).strip(" ,.;:-")
    aliases: set[str] = set()
    if stripped and stripped != lowered:
        aliases.add(stripped)
    if "piano concerto" in stripped:
        aliases.add("piano concerto")
    return aliases


def build_named_work_aliases(value: str) -> set[str]:
    text = compact(value)
    lowered = text.lower()
    if not text:
        return set()
    aliases: set[str] = set()
    if any(marker in lowered or marker in text for marker in ("appassionata", "热情")):
        aliases.update({"appassionata", "热情", "热情奏鸣曲"})
    if any(marker in lowered or marker in text for marker in ("spring", "春天")):
        aliases.update({"spring", "春天", "春天奏鸣曲"})
    if "piano sonata no.23" in lowered or "piano sonata no 23" in lowered or "op.57" in lowered or "op57" in lowered:
        aliases.update({"appassionata", "热情", "热情奏鸣曲"})
    if "violin sonata no.5" in lowered or "violin sonata no 5" in lowered or "op.24" in lowered or "op24" in lowered:
        aliases.update({"spring", "春天", "春天奏鸣曲"})
    return aliases


def normalize_cn_number(value: str) -> str:
    normalized = compact(value)
    if normalized.isdigit():
        return normalized
    digits = {
        "零": 0,
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    units = {"十": 10, "百": 100}
    total = 0
    current = 0
    for char in normalized:
        if char in digits:
            current = digits[char]
        elif char in units:
            total += (current or 1) * units[char]
            current = 0
    return str(total + current) if total or current else ""


def strip_alias_annotations(value: str) -> str:
    text = compact(value)
    if not text:
        return ""
    text = re.sub(r"\(\s*EN\s*[:：].*?\)", "", text, flags=re.I)
    text = re.sub(r"[,，]\s*EN\s*[:：].*$", "", text, flags=re.I)
    return compact(text)


def extract_explicit_latin_alias(value: str) -> str:
    text = compact(value)
    if not text:
        return ""
    patterns = [
        r"EN\s*[:：]\s*([^()]+?)(?=\s*(?:CHN|CN|中文)\s*[:：]|$)",
        r"\(\s*EN\s*[:：]\s*([^()]+?)(?=\s*(?:CHN|CN|中文)\s*[:：]|\))",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue
        alias = compact(match.group(1)).strip(" ,;|)")
        if alias and looks_latin(alias):
            return alias
    return ""


def normalize_source_record(payload: dict[str, Any]) -> SourceRecord:
    raw_weight = float(payload.get("weight", 0.0) or 0.0)
    weight = raw_weight / 100 if raw_weight > 1 else raw_weight
    return SourceRecord(
        url=compact(payload.get("url")),
        source_label=compact(payload.get("source_label") or payload.get("sourceLabel")),
        source_kind=compact(payload.get("source_kind") or payload.get("sourceKind")),
        title=compact(payload.get("title")),
        description=compact(payload.get("description")),
        platform=compact(payload.get("platform")) or "other",
        weight=max(0.0, min(1.0, weight if weight else 0.5)),
        same_recording_score=max(
            0.0,
            min(1.0, float(payload.get("same_recording_score", payload.get("sameRecordingScore", 0.0)) or 0.0)),
        ),
        duration_seconds=max(0, int(payload.get("duration_seconds", payload.get("durationSeconds", 0)) or 0)),
        uploader=compact(payload.get("uploader")),
        view_count=max(0, int(payload.get("view_count", payload.get("viewCount", 0)) or 0)),
        fields={key: compact(value) for key, value in dict(payload.get("fields") or {}).items() if compact(value)},
        images=[
            {
                "src": compact(image.get("src")),
                "sourceUrl": compact(image.get("sourceUrl") or payload.get("url")),
                "sourceKind": compact(image.get("sourceKind") or payload.get("source_kind") or payload.get("sourceKind")),
                "attribution": compact(image.get("attribution") or payload.get("source_label") or payload.get("sourceLabel")),
                "title": compact(image.get("title")),
            }
            for image in list(payload.get("images") or [])
            if compact(image.get("src"))
        ],
    )


class RetrievalPipeline:
    def __init__(
        self,
        *,
        source_provider: SourceProvider,
        llm_client: LlmClient | None = None,
    ) -> None:
        self._source_provider = source_provider
        self._llm_client = llm_client
        self._normalizer = InputNormalizer()
        self._profile_resolver = ProfileResolver()

    def consume_access_events(self) -> list[dict[str, Any]]:
        return list(getattr(self._source_provider, "consume_access_events", lambda: [])())

    def get_access_summary(self) -> dict[str, Any]:
        return dict(getattr(self._source_provider, "get_access_summary", lambda: {})())

    async def aclose(self) -> None:
        close_source_provider = getattr(self._source_provider, "aclose", None)
        if callable(close_source_provider):
            await close_source_provider()

    async def retrieve(
        self,
        item: RetrievalItem,
        *,
        cancel_event: Any | None = None,
        deadline: float | None = None,
    ) -> ResultItemResponse:
        start_request_scope = getattr(self._source_provider, "start_request_scope", None)
        if callable(start_request_scope):
            start_request_scope()
        draft = self._normalizer.normalize(item)
        profile = self._profile_resolver.resolve(item)
        logs = [LogEntry(message="draft entry initialized", itemId=item.item_id)]
        warnings: list[str] = []
        records: list[SourceRecord] = []
        deadline_exceeded = False

        stages = [
            ("existing-link", self._source_provider.inspect_existing_links),
            ("high-quality", self._source_provider.search_high_quality),
            ("streaming", self._source_provider.search_streaming),
            ("fallback", self._source_provider.search_fallback),
        ]
        remaining_stage_weights = [STAGE_WEIGHTS.get(label, 1.0) for label, _ in stages]
        if self._llm_client is not None:
            remaining_stage_weights.append(STAGE_WEIGHTS["llm"])

        for index, (label, loader) in enumerate(stages, start=1):
            if is_cancelled(cancel_event):
                return canceled_result(item.item_id, logs, warnings)
            if should_skip_stage(label=label, records=records):
                logs.append(
                    LogEntry(
                        message=f"{label} stage skipped: strong resource candidates already collected",
                        itemId=item.item_id,
                    )
                )
                continue
            stage_timeout = calculate_stage_timeout(deadline, remaining_stage_weights[index - 1 :])
            if stage_timeout is not None and stage_timeout <= 0:
                deadline_exceeded = True
                warnings.append("检索截止时间已到，来源收集提前结束。")
                logs.append(LogEntry(message=f"{label} stage skipped: deadline reached", itemId=item.item_id, level="warning"))
                break
            try:
                payloads = await run_with_optional_timeout(loader(draft, profile), stage_timeout)
                records.extend(normalize_source_record(payload) for payload in payloads)
                logs.append(LogEntry(message=f"{label} stage collected {len(payloads)} records", itemId=item.item_id))
                warnings.extend(getattr(self._source_provider, "consume_warnings", lambda: [])())
            except TimeoutError:
                warnings.append(f"{label} 阶段超时。")
                logs.append(LogEntry(message=f"{label} stage timed out", itemId=item.item_id, level="warning"))
            except Exception as error:
                warnings.append(f"{label} 阶段失败：{error}")
                logs.append(LogEntry(message=f"{label} stage failed: {error}", itemId=item.item_id, level="warning"))

        synthesis: dict[str, Any] = {}
        if self._llm_client is not None and records and not is_cancelled(cancel_event):
            stage_timeout = calculate_stage_timeout(deadline, [STAGE_WEIGHTS["llm"]])
            minimum_timeout = getattr(self._llm_client, "minimum_synthesis_timeout_seconds", 0.0)
            allow_realtime_synthesis = getattr(self._llm_client, "allow_realtime_synthesis", True)
            if not allow_realtime_synthesis:
                logs.append(
                    LogEntry(
                        message="llm synthesis skipped for this model; realtime retrieval uses rule-based assembly",
                        itemId=item.item_id,
                    )
                )
            elif should_skip_llm_synthesis(records):
                logs.append(
                    LogEntry(
                        message="llm synthesis skipped: top candidate is already unambiguous",
                        itemId=item.item_id,
                    )
                )
            elif stage_timeout is not None and stage_timeout < minimum_timeout:
                logs.append(
                    LogEntry(
                        message=f"llm synthesis skipped: budget {stage_timeout:.1f}s is below required {minimum_timeout:.1f}s",
                        itemId=item.item_id,
                    )
                )
            elif stage_timeout is None or stage_timeout > 0:
                try:
                    synthesis = await run_with_optional_timeout(
                        self._llm_client.synthesize(draft, profile, records),
                        stage_timeout,
                    )
                    logs.append(LogEntry(message="llm synthesis completed", itemId=item.item_id))
                except TimeoutError:
                    warnings.append("LLM 归并超时。")
                    logs.append(LogEntry(message="llm synthesis timed out", itemId=item.item_id, level="warning"))
                except Exception as error:
                    warnings.append(f"LLM 归并失败：{error}")
                    logs.append(LogEntry(message=f"llm synthesis failed: {error}", itemId=item.item_id, level="warning"))

        if is_cancelled(cancel_event):
            return canceled_result(item.item_id, logs, warnings)

        result, evidence, link_candidates, image_candidates, field_warnings, confidence = self._assemble_result(
            draft=draft,
            records=records,
            notes=compact(synthesis.get("notes")),
            accepted_urls=[compact(url) for url in synthesis.get("acceptedUrls", []) if compact(url)],
        )
        warnings.extend(field_warnings)
        warnings.extend(str(entry) for entry in synthesis.get("warnings", []) if compact(entry))

        status = self._resolve_status(item, records, result, evidence)
        if deadline_exceeded and status == "not_found":
            status = "failed"
        if deadline_exceeded:
            logs.append(LogEntry(message="retrieval deadline reached", itemId=item.item_id, level="warning"))
        if status == "not_found":
            warnings.append("未找到可确认属于同一版本的可信来源。")
        if not result.notes and compact(synthesis.get("summary")):
            result.notes = compact(synthesis.get("summary"))

        return ResultItemResponse(
            itemId=item.item_id,
            status=status,
            confidence=confidence,
            warnings=dedupe_preserve_order(warnings),
            result=result,
            evidence=evidence,
            linkCandidates=link_candidates,
            imageCandidates=image_candidates,
            logs=logs,
        )

    def _assemble_result(
        self,
        *,
        draft: DraftRecordingEntry,
        records: list[SourceRecord],
        notes: str,
        accepted_urls: list[str],
    ) -> tuple[ResultPayload, list[EvidenceItem], list[LinkCandidate], list[ImageCandidate], list[str], float]:
        result = ResultPayload()
        if notes:
            result.notes = notes
        warnings: list[str] = []
        evidence: list[EvidenceItem] = []
        record_map = {compact(record.url): record for record in records if compact(record.url)}
        raw_link_candidates = dedupe_link_candidates(
            sort_link_candidates(
                draft,
                [
                    LinkCandidate(
                        platform=record.platform or "other",
                        url=record.url,
                        title=record.title or draft.title,
                        sourceLabel=record.source_label,
                        confidence=round(record.same_recording_score, 2),
                    )
                    for record in records
                    if record.url and record.same_recording_score >= LOW_CONFIDENCE_THRESHOLD
                ],
                record_map,
            )
        )
        link_candidates = limit_link_candidates_per_platform(
            annotate_link_candidates(draft, raw_link_candidates),
            green_limit=CANDIDATE_GREEN_PER_PLATFORM_LIMIT,
            yellow_limit=CANDIDATE_YELLOW_PER_PLATFORM_LIMIT,
        )
        link_candidates = [
            candidate
            for candidate in link_candidates
            if classify_link_candidate_zone(draft, candidate)[0] != "red"
        ]

        candidate_map: dict[str, list[FieldCandidate]] = defaultdict(list)
        confidences: list[float] = []
        raw_images: list[tuple[SourceRecord, dict[str, Any], float]] = []

        for record in records:
            record_confidence = compute_record_confidence(record)
            if record_confidence >= LOW_CONFIDENCE_THRESHOLD:
                confidences.append(record_confidence)
            for field, value in record.fields.items():
                if not value:
                    continue
                candidate_map[field].append(
                    FieldCandidate(
                        field=field,
                        value=value,
                        confidence=record_confidence,
                        source_url=record.url,
                        source_label=record.source_label,
                        accepted=record.same_recording_score >= SAME_RECORDING_THRESHOLD,
                    )
                )
            for image in record.images:
                raw_images.append((record, image, record_confidence))

        field_warning_map: dict[str, str] = {}
        for field, candidates in candidate_map.items():
            for candidate in candidates:
                evidence.append(
                    EvidenceItem(
                        field=field,
                        sourceUrl=candidate.source_url,
                        sourceLabel=candidate.source_label,
                        confidence=round(candidate.confidence, 2),
                        note="final" if candidate.accepted else "candidate-only",
                    )
                )

            top = pick_final_candidate(candidates)
            if top is None:
                field_warning_map[field] = f"{field} 未达到最终采纳阈值。"
                continue
            if field == "performanceDateText":
                result.performance_date_text = top.value
            elif field == "venueText":
                result.venue_text = top.value
            elif field == "albumTitle":
                result.album_title = top.value
            elif field == "label":
                result.label = top.value
            elif field == "releaseDate":
                result.release_date = top.value

        image_candidates = dedupe_image_candidates(
            [
                ImageCandidate(
                    src=image["src"],
                    sourceUrl=image.get("sourceUrl") or record.url,
                    sourceKind=image.get("sourceKind") or record.source_kind,
                    attribution=image.get("attribution") or record.source_label,
                    title=image.get("title") or record.title or draft.title,
                )
                for record, image, confidence in raw_images
                if image.get("src") and confidence >= LOW_CONFIDENCE_THRESHOLD
            ]
        )
        result.images = [
            image
            for image in image_candidates
            if any(
                compact(image.src) == compact(source_image.get("src")) and confidence >= FINAL_IMAGE_CONFIDENCE_THRESHOLD
                for _, source_image, confidence in raw_images
            )
        ][:3]
        if not result.images and image_candidates and raw_link_candidates:
            winning_urls = {
                compact(candidate.url)
                for candidate in raw_link_candidates
                if (candidate.confidence or 0) >= FINAL_LINK_CONFIDENCE_THRESHOLD
            }
            result.images = [image for image in image_candidates if compact(image.source_url) in winning_urls][:3]
        if not result.images and image_candidates:
            warnings.append("找到封面候选，但尚未达到最终采纳阈值。")

        accepted_url_set = {compact(url) for url in accepted_urls if compact(url)}
        result.links = [
            candidate
            for candidate in raw_link_candidates
            if (candidate.confidence or 0) >= FINAL_LINK_CONFIDENCE_THRESHOLD or compact(candidate.url) in accepted_url_set
        ]
        ambiguous_upload_cluster = has_ambiguous_upload_cluster(draft, raw_link_candidates)
        if result.links:
            top_link_confidence = max((candidate.confidence or 0) for candidate in result.links)
            title_only_collaboration_hint = has_title_only_collaboration_hint(draft)
            if title_only_collaboration_hint and len(result.links) >= 5:
                floor_delta = 0.32
            elif ambiguous_upload_cluster and is_sparse_upload_query(draft):
                floor_delta = 0.26
            else:
                floor_delta = 0.18 if ambiguous_upload_cluster else 0.08
            floor = max(FINAL_LINK_CONFIDENCE_THRESHOLD, top_link_confidence - floor_delta)
            filtered_links = [
                candidate
                for candidate in raw_link_candidates
                if (candidate.confidence or 0) >= floor or compact(candidate.url) in accepted_url_set
            ]
            filtered_links = sort_link_candidates(
                draft,
                filtered_links,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            accepted_candidates = [
                candidate for candidate in raw_link_candidates if compact(candidate.url) in accepted_url_set
            ]
            accepted_candidate_platforms = {
                compact(candidate.platform) for candidate in accepted_candidates if compact(candidate.platform)
            }
            if accepted_url_set and len(filtered_links) < 3 and len(accepted_candidate_platforms) == 1 and accepted_candidates:
                target_platform = next(iter(accepted_candidate_platforms))
                best_accepted_exactness = max(
                    candidate_title_quality_score(draft, compact(candidate.title)) for candidate in accepted_candidates
                )
                rescue_same_platform_candidates = [
                    candidate
                    for candidate in raw_link_candidates
                    if compact(candidate.url) not in {compact(link.url) for link in filtered_links}
                    and compact(candidate.platform) == target_platform
                    and (candidate.confidence or 0.0) >= FINAL_LINK_CONFIDENCE_THRESHOLD
                    and candidate_title_quality_score(draft, compact(candidate.title)) >= best_accepted_exactness
                ]
                if rescue_same_platform_candidates:
                    filtered_links = sort_link_candidates(
                        draft,
                        dedupe_link_candidates([*filtered_links, *rescue_same_platform_candidates]),
                        record_map,
                        prefer_exactness=ambiguous_upload_cluster,
                    )
            reference_years = extract_reference_years(draft)
            if reference_years:
                year_matched_links = [
                    candidate
                    for candidate in filtered_links
                    if not extract_conflicting_years(compact(candidate.title).lower(), reference_years)
                    or compact(candidate.url) in accepted_url_set
                ]
                year_confirmed_links = [
                    candidate
                    for candidate in year_matched_links
                    if compact(candidate.url) in accepted_url_set
                    or any(year in compact(candidate.title).lower() for year in reference_years)
                ]
                if year_confirmed_links:
                    filtered_links = year_matched_links
            if ambiguous_upload_cluster and len(filtered_links) >= 3:
                best_exactness = max(candidate_title_quality_score(draft, compact(candidate.title)) for candidate in filtered_links)
                exactness_floor = max(0.03 if is_sparse_upload_query(draft) else 0.05, best_exactness - 0.05)
                filtered_links = [
                    candidate
                    for candidate in filtered_links
                    if candidate_title_quality_score(draft, compact(candidate.title)) >= exactness_floor - 1e-6
                    or compact(candidate.url) in accepted_url_set
                ]
            if not accepted_url_set and len(filtered_links) < 3 and has_cross_platform_exact_cluster(draft, raw_link_candidates):
                existing_urls = {compact(candidate.url) for candidate in filtered_links}
                top_platform = compact(filtered_links[0].platform) if filtered_links else ""
                top_exactness = (
                    candidate_title_quality_score(draft, compact(filtered_links[0].title)) if filtered_links else 0.0
                )
                rescue_floor = max(0.05, top_exactness - 0.06)
                cross_platform_rescues = [
                    candidate
                    for candidate in raw_link_candidates
                    if compact(candidate.url) not in existing_urls
                    and compact(candidate.platform) != top_platform
                    and (candidate.confidence or 0.0) >= FINAL_LINK_CONFIDENCE_THRESHOLD
                    and candidate_title_quality_score(draft, compact(candidate.title)) >= rescue_floor
                ]
                if cross_platform_rescues:
                    filtered_links = sort_link_candidates(
                        draft,
                        dedupe_link_candidates([*filtered_links, *cross_platform_rescues]),
                        record_map,
                        prefer_exactness=ambiguous_upload_cluster,
                    )
            accepted_links = [
                candidate for candidate in filtered_links if compact(candidate.url) in accepted_url_set
            ]
            accepted_platforms = {compact(candidate.platform) for candidate in accepted_links if compact(candidate.platform)}
            close_same_platform_alternate = False
            if accepted_url_set and accepted_links and len(accepted_platforms) == 1:
                target_platform = next(iter(accepted_platforms))
                if accepted_links:
                    best_accepted_exactness = max(
                        candidate_title_quality_score(draft, compact(candidate.title)) for candidate in accepted_links
                    )
                    best_accepted_confidence = max((candidate.confidence or 0.0) for candidate in accepted_links)
                    alternate_confidence_margin = 0.08
                    close_same_platform_alternate = any(
                        compact(candidate.url) not in accepted_url_set
                        and compact(candidate.platform) == target_platform
                        and candidate_title_quality_score(draft, compact(candidate.title))
                        >= best_accepted_exactness
                        and (candidate.confidence or 0.0) <= best_accepted_confidence + alternate_confidence_margin
                        for candidate in filtered_links
                    )
                    filtered_links = [
                        candidate
                        for candidate in filtered_links
                        if compact(candidate.url) in accepted_url_set
                        or compact(candidate.platform) != target_platform
                        or (
                            candidate_title_quality_score(draft, compact(candidate.title)) >= best_accepted_exactness
                            and (candidate.confidence or 0.0) <= best_accepted_confidence + alternate_confidence_margin
                        )
                    ]
            filtered_links = supplement_primary_platform_links(
                draft,
                filtered_links,
                raw_link_candidates,
                accepted_links,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links = supplement_complete_upload_rescue_links(
                draft,
                filtered_links,
                raw_link_candidates,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links, added_close_same_platform_complete_alternate = supplement_close_same_platform_complete_alternate_links(
                draft,
                filtered_links,
                raw_link_candidates,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links = supplement_independent_primary_platform_links(
                draft,
                filtered_links,
                raw_link_candidates,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links = supplement_apple_track_version_rescues(
                draft,
                filtered_links,
                raw_link_candidates,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links, added_longform_accepted_platform_alternate = supplement_accepted_platform_longform_alternate_links(
                draft,
                filtered_links,
                raw_link_candidates,
                accepted_links,
                record_map,
                prefer_exactness=ambiguous_upload_cluster,
            )
            filtered_links, added_chamber_cross_platform_low_confidence_rescue = (
                supplement_chamber_cross_platform_low_confidence_rescue_links(
                    draft,
                    filtered_links,
                    raw_link_candidates,
                    record_map,
                    prefer_exactness=ambiguous_upload_cluster,
                )
            )
            if filtered_links and max((candidate.confidence or 0.0) for candidate in filtered_links) < FINAL_LINK_CONFIDENCE_THRESHOLD:
                rescue_pool = [
                    candidate
                    for candidate in raw_link_candidates
                    if compact(candidate.url) not in {compact(link.url) for link in filtered_links}
                ]
                version_rescues = pick_version_rescue_links(draft, rescue_pool, record_map)
                if version_rescues:
                    filtered_links = sort_link_candidates(
                        draft,
                        dedupe_link_candidates([*filtered_links, *version_rescues]),
                        record_map,
                        prefer_exactness=True,
                    )
            final_link_limit = determine_final_link_limit(
                draft,
                filtered_links,
                accepted_url_count=len(accepted_url_set),
                ambiguous_upload_cluster=ambiguous_upload_cluster,
            )
            if close_same_platform_alternate:
                final_link_limit = max(final_link_limit, 3)
            if added_close_same_platform_complete_alternate:
                final_link_limit = max(final_link_limit, 3)
            if added_longform_accepted_platform_alternate:
                final_link_limit = max(final_link_limit, 3)
            if added_chamber_cross_platform_low_confidence_rescue:
                final_link_limit = max(final_link_limit, 3)
            filtered_links, champion_count = prioritize_primary_platform_champions(
                draft,
                filtered_links,
                record_map,
            )
            if champion_count:
                final_link_limit = max(final_link_limit, champion_count)
            filtered_links = prioritize_accepted_links(
                filtered_links,
                accepted_url_set,
                final_link_limit=final_link_limit,
            )
            result.links = filtered_links[:final_link_limit]
            result.links = supplement_chamber_post_final_same_platform_title_rescues(
                draft,
                result.links,
                raw_link_candidates,
                record_map,
                max_links=4,
            )
        if not result.links and accepted_url_set:
            result.links = sort_link_candidates(
                draft,
                [candidate for candidate in raw_link_candidates if compact(candidate.url) in accepted_url_set],
                record_map,
            )[:2]
        if not result.links and raw_link_candidates:
            version_rescues = pick_version_rescue_links(draft, raw_link_candidates, record_map)
            if version_rescues:
                result.links = version_rescues
            else:
                dominant = pick_dominant_link_candidate(
                    draft,
                    sort_link_candidates(draft, raw_link_candidates, record_map),
                    record_map,
                )
                if dominant is not None:
                    result.links = [dominant]
        if not result.images and image_candidates and accepted_url_set:
            result.images = [image for image in image_candidates if compact(image.source_url) in accepted_url_set][:3]
        if result.links and not result.images and image_candidates:
            winning_urls = {compact(candidate.url) for candidate in result.links}
            result.images = [image for image in image_candidates if compact(image.source_url) in winning_urls][:3]
        if result.links:
            result.links = annotate_link_candidates(draft, result.links)
            if result.links:
                first_link = result.links[0]
                if not compact(result.album_title) and compact(first_link.title):
                    result.album_title = compact(first_link.title)
                if not compact(result.label) and compact(first_link.source_label):
                    result.label = compact(first_link.source_label)
        strongest_match = max((record.same_recording_score for record in records), default=0.0)
        if strongest_match >= FINAL_LINK_CONFIDENCE_THRESHOLD or result.links or accepted_url_set:
            self._carry_forward_trusted_input_fields(draft, result)
        warnings.extend(
            warning
            for field, warning in field_warning_map.items()
            if not has_result_value(result, field)
        )
        return result, evidence, link_candidates, image_candidates, warnings, round(max(confidences or [0.0]), 2)

    def _carry_forward_trusted_input_fields(
        self,
        draft: DraftRecordingEntry,
        result: ResultPayload,
    ) -> None:
        trusted_values = {
            "performanceDateText": compact(draft.performance_date_text),
            "venueText": compact(draft.venue_text),
            "albumTitle": compact(draft.album_title),
            "label": compact(draft.label),
            "releaseDate": compact(draft.release_date),
        }
        for field, value in trusted_values.items():
            if not value or has_result_value(result, field):
                continue
            if field == "performanceDateText":
                result.performance_date_text = value
            elif field == "venueText":
                result.venue_text = value
            elif field == "albumTitle":
                result.album_title = value
            elif field == "label":
                result.label = value
            elif field == "releaseDate":
                result.release_date = value

    def _resolve_status(
        self,
        item: RetrievalItem,
        records: list[SourceRecord],
        result: ResultPayload,
        evidence: list[EvidenceItem],
    ) -> str:
        if not records or max((record.same_recording_score for record in records), default=0.0) < LOW_CONFIDENCE_THRESHOLD:
            return "not_found"

        finalized_scalar_values = [
            result.performance_date_text,
            result.venue_text,
            result.album_title,
            result.label,
            result.release_date,
        ]
        has_final_links = bool(result.links)
        has_final_images = bool(result.images)
        has_final_data = any(compact(value) for value in finalized_scalar_values) or has_final_links or has_final_images or compact(result.notes)
        if not has_final_data and evidence:
            return "partial"

        unresolved_fields = [
            field
            for field in item.requested_fields
            if field in FINALIZABLE_FIELDS and not has_result_value(result, field)
        ]
        if unresolved_fields:
            return "partial"
        return "succeeded"


async def run_with_optional_timeout(awaitable: Any, timeout_seconds: float | None) -> Any:
    if timeout_seconds is None:
        return await awaitable
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except asyncio.TimeoutError as error:
        raise TimeoutError from error


def calculate_stage_timeout(deadline: float | None, remaining_weights: list[float]) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return 0.0
    total_weight = max(0.5, sum(remaining_weights) or 1.0)
    current_weight = max(0.3, remaining_weights[0] if remaining_weights else 1.0)
    return max(0.4, remaining * (current_weight / total_weight))


def is_cancelled(cancel_event: Any | None) -> bool:
    return bool(cancel_event is not None and cancel_event.is_set())


def canceled_result(item_id: str, logs: list[LogEntry], warnings: list[str]) -> ResultItemResponse:
    return ResultItemResponse(
        itemId=item_id,
        status="failed",
        confidence=0.0,
        warnings=dedupe_preserve_order([*warnings, "任务已取消。"]),
        result=ResultPayload(notes="检索在完成前已取消。"),
        evidence=[],
        linkCandidates=[],
        imageCandidates=[],
        logs=[*logs, LogEntry(message="retrieval canceled", itemId=item_id, level="warning")],
    )


def compute_record_confidence(record: SourceRecord) -> float:
    kind_bonus = {
        "existing-link": 0.12,
        "high-quality": 0.08,
        "streaming": 0.02,
        "search": 0.0,
        "fallback": 0.0,
    }.get(record.source_kind, 0.0)
    return max(0.0, min(0.99, record.same_recording_score * 0.7 + record.weight * 0.2 + kind_bonus))


def pick_final_candidate(candidates: list[FieldCandidate]) -> FieldCandidate | None:
    if not candidates:
        return None
    grouped: dict[str, list[FieldCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.value].append(candidate)

    _, best_group = max(
        grouped.items(),
        key=lambda item: (
            len({candidate.source_label for candidate in item[1]}),
            max(candidate.confidence for candidate in item[1]),
        ),
    )
    top_confidence = max(candidate.confidence for candidate in best_group)
    corroborated = len({candidate.source_label for candidate in best_group}) >= 2
    if top_confidence >= FINAL_CONFIDENCE_THRESHOLD:
        return sorted(best_group, key=lambda candidate: candidate.confidence, reverse=True)[0]
    if corroborated:
        average_confidence = sum(candidate.confidence for candidate in best_group) / len(best_group)
        if average_confidence >= CORROBORATED_CONFIDENCE_THRESHOLD:
            return sorted(best_group, key=lambda candidate: candidate.confidence, reverse=True)[0]
    return None


def pick_dominant_link_candidate(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord] | None = None,
) -> LinkCandidate | None:
    if not candidates:
        return None
    record_map = record_map or {}
    ordered = sorted(candidates, key=lambda candidate: candidate.confidence or 0.0, reverse=True)
    top = ordered[0]
    top_confidence = top.confidence or 0.0
    runner_up_confidence = ordered[1].confidence or 0.0 if len(ordered) > 1 else 0.0
    is_known_platform = compact(top.platform) in {"youtube", "bilibili", "apple_music", "spotify", "qobuz"}
    if top_confidence >= 0.58:
        return top
    top_exactness = candidate_match_quality_score(draft, top, record_map.get(compact(top.url)))
    runner_up_exactness = (
        max(candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url))) for candidate in ordered[1:])
        if len(ordered) > 1
        else -0.05
    )
    if top_confidence >= 0.5 and is_known_platform and top_confidence - runner_up_confidence >= 0.08:
        return top
    if (
        top_confidence >= 0.5
        and is_known_platform
        and top_exactness >= 0.1
        and top_exactness - runner_up_exactness >= 0.03 - 1e-6
    ):
        return top
    if top_confidence >= LOW_CONFIDENCE_THRESHOLD and is_known_platform and len(ordered) == 1 and top_exactness >= 0.08:
        return top
    if (
        top_confidence >= LOW_CONFIDENCE_THRESHOLD
        and is_known_platform
        and top_exactness >= 0.08
        and top_exactness - runner_up_exactness >= 0.05 - 1e-6
    ):
        return top
    if top_confidence >= 0.5 and is_known_platform and len(ordered) == 1:
        return top
    return None


def sort_link_candidates(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> list[LinkCandidate]:
    return sorted(
        candidates,
        key=lambda candidate: (
            ambiguous_link_candidate_sort_key(draft, candidate, record_map.get(compact(candidate.url)))
            if prefer_exactness
            else link_candidate_sort_key(draft, candidate, record_map.get(compact(candidate.url)))
        ),
        reverse=True,
    )


def link_candidate_sort_key(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> tuple[float, float, int, int]:
    confidence = candidate.confidence or 0.0
    title = compact(candidate.title)
    lowered = title.lower()
    exactness = candidate_match_quality_score(draft, candidate, record)
    packaging = candidate_packaging_priority_score(candidate, record)
    if record is not None:
        if record.duration_seconds > 0:
            exactness += 0.03
        else:
            exactness -= 0.03
        if compact(record.uploader):
            exactness += 0.02
        else:
            exactness -= 0.02
        if record.view_count >= 8000:
            exactness += 0.06
        elif record.view_count >= 2000:
            exactness += 0.04
        elif record.view_count >= 500:
            exactness += 0.02
    if "new edition" in lowered or "remaster" in lowered or "restored" in lowered:
        exactness -= 0.08
    if "provided to youtube by" in lowered:
        exactness -= 0.08
    return (
        round(confidence + exactness + packaging, 4),
        round(packaging, 4),
        round(exactness, 4),
        record.view_count if record is not None else 0,
        -len(title),
    )


def ambiguous_link_candidate_sort_key(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> tuple[float, float, float, int]:
    title = compact(candidate.title)
    exactness = candidate_match_quality_score(draft, candidate, record)
    packaging = candidate_packaging_priority_score(candidate, record)
    metadata_support = 0.0
    if record is not None:
        if record.duration_seconds > 0:
            metadata_support += 0.06
        else:
            metadata_support -= 0.08
        if compact(record.uploader):
            metadata_support += 0.04
        else:
            metadata_support -= 0.05
        if record.view_count >= 8000:
            metadata_support += 0.06
        elif record.view_count >= 2000:
            metadata_support += 0.04
        elif record.view_count >= 500:
            metadata_support += 0.02
    confidence = min(candidate.confidence or 0.0, 0.88)
    return (
        round(exactness + metadata_support + packaging, 4),
        round(packaging, 4),
        round(confidence, 4),
        round((candidate.confidence or 0.0) + exactness, 4),
        record.view_count if record is not None else 0,
    )


def candidate_packaging_priority_score(candidate: LinkCandidate, record: SourceRecord | None) -> float:
    text = " ".join(
        part
        for part in [
            compact(candidate.title),
            compact(record.description) if record is not None else "",
            compact(candidate.url),
        ]
        if compact(part)
    )
    lowered = text.lower()
    score = 0.0
    is_apple_track = is_apple_track_url(candidate.url)
    if re.search(r"[?&]p=\d+", candidate.url, re.I):
        score -= 0.04
    if looks_like_title_single_movement(text):
        if is_apple_track and record is not None and (compact(record.description) or compact(record.uploader)):
            score -= 0.04 if looks_like_title_first_chapter(text) else 0.08
        elif looks_like_title_first_chapter(text):
            score -= 0.14
        else:
            score -= 0.28
    elif looks_like_title_multi_work_compilation(text):
        if is_apple_track and record is not None:
            score += 0.0
        else:
            score -= 0.06
    elif any(marker in lowered for marker in ("full", "complete", "full performance")):
        score += 0.04
    return score


def is_apple_track_url(url: str) -> bool:
    lowered = compact(url).lower()
    return "music.apple.com" in lowered and "?i=" in lowered


def looks_like_title_single_movement(value: str) -> bool:
    patterns = [
        r"(?:^|[\s(:\-–—])(i{1,3}|iv|v)\.\s",
        r"(?:^|[\s(:\-–—])([1-9])\.\s",
        r"\b(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+movement\b",
        r"\ballegro\b",
        r"\badagio\b",
        r"\bandante\b",
        r"\bscherzo\b",
        r"\brondo\b",
    ]
    return any(re.search(pattern, value or "", re.I) for pattern in patterns)


def looks_like_title_first_chapter(value: str) -> bool:
    patterns = [
        r"(?:^|[\s(:\-–—])(i|1)\.\s",
        r"\b1st movement\b",
        r"\bfirst movement\b",
        r"[?&]p=1\b",
    ]
    return any(re.search(pattern, value or "", re.I) for pattern in patterns)


def looks_like_title_multi_work_compilation(value: str) -> bool:
    patterns = [
        r"nos?\.\s*\d+\s*(?:and|&)\s*\d+",
        r"\bnos?\s*\d+\s*,\s*\d+",
        r"\bnos?\s*\d+\s*/\s*\d+",
        r"\bconcertos\b",
        r"\bsonatas\b",
        r" overture",
        r" overtures",
        r" works /",
        r" works by",
        r"\bbrahms\b.+\bschumann\b",
    ]
    return any(re.search(pattern, value or "", re.I) for pattern in patterns)


def looks_like_year_or_work(value: str) -> bool:
    lowered = compact(value).lower()
    work_markers = (
        "symphony",
        "concerto",
        "concertos",
        "sonata",
        "opera",
        "live",
        "festival",
        "交响曲",
        "协奏曲",
        "钢协",
        "奏鸣曲",
        "歌剧",
        "现场",
        "音乐节",
        "海顿",
        "布鲁克纳",
        "贝多芬",
        "马勒",
    )
    return bool(re.search(r"(19\d{2}|20\d{2})", lowered) or any(token in lowered for token in work_markers))


def candidate_title_quality_score(draft: DraftRecordingEntry, title: str) -> float:
    lowered = compact(title).lower()
    if not lowered:
        return -0.05
    score = 0.0
    reference_years = extract_reference_years(draft)
    if any(year in lowered for year in reference_years):
        score += 0.05
    elif reference_years and extract_conflicting_years(lowered, reference_years):
        score -= 0.05
    work_aliases = build_candidate_work_anchor_terms(draft)
    work_alias_hit = any(alias in lowered for alias in work_aliases)
    if work_alias_hit:
        score += 0.03
    if candidate_mentions_expected_composer(draft, lowered):
        score += 0.05
    if title_matches_catalogue(draft, lowered):
        score += 0.05
    elif should_require_catalogue_hint(draft, lowered):
        score -= 0.02
    if candidate_mentions_primary_and_secondary(draft, lowered):
        score += 0.04
    else:
        lead_hit = candidate_mentions_any_lead(draft, lowered)
        if lead_hit:
            score += 0.01
            solo_style_draft = not (
                draft.secondary_names
                or draft.secondary_names_latin
                or draft.ensemble_names
                or draft.ensemble_names_latin
            )
            if solo_style_draft and work_alias_hit:
                score += 0.03
    if any(marker in lowered for marker in ("new edition", "restored", "remaster", "reissue", "alt take")):
        score -= 0.08
    if any(marker in lowered for marker in ("blu-ray", "bluray", "bd版", "蓝光", "「bd」", "[bd]", "(bd)")):
        score -= 0.06
    return score


def candidate_description_support_score(draft: DraftRecordingEntry, description: str) -> float:
    lowered = compact(description).lower()
    if not lowered:
        return 0.0
    score = 0.0
    work_aliases = build_candidate_work_anchor_terms(draft)
    has_work_anchor = any(alias in lowered for alias in work_aliases)
    has_expected_composer = candidate_mentions_expected_composer(draft, lowered)
    reference_years = extract_reference_years(draft)
    if any(year in lowered for year in reference_years):
        score += 0.03
    elif reference_years and extract_conflicting_years(lowered, reference_years):
        score -= 0.04
    if has_work_anchor:
        score += 0.02
    if has_expected_composer:
        score += 0.03
    if candidate_mentions_primary_and_secondary(draft, lowered):
        score += 0.04
    elif candidate_mentions_any_lead(draft, lowered):
        score += 0.02
    context_hits = candidate_context_match_count(draft, lowered)
    if context_hits > 0:
        score += min(0.03, context_hits * 0.01)
    if score > 0 and not (has_work_anchor or has_expected_composer):
        score = min(score, 0.02)
    return max(-0.04, min(0.1, score))


def candidate_match_quality_score(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> float:
    title_score = candidate_title_quality_score(draft, compact(candidate.title))
    if record is None:
        return title_score
    description_score = candidate_description_support_score(draft, compact(record.description))
    if description_score <= 0:
        return title_score + description_score
    return title_score + min(0.1, description_score)


def title_matches_catalogue(draft: DraftRecordingEntry, lowered_title: str) -> bool:
    catalogue = compact(draft.catalogue).lower()
    if not catalogue:
        return False
    normalized_catalogue = re.sub(r"\s+", "", catalogue)
    normalized_title = re.sub(r"\s+", "", lowered_title)
    return catalogue in lowered_title or normalized_catalogue in normalized_title


def should_require_catalogue_hint(draft: DraftRecordingEntry, lowered_title: str) -> bool:
    if not compact(draft.catalogue):
        return False
    work_markers = ("concerto", "symphony", "sonata", "quartet", "trio", "op.")
    return any(marker in lowered_title for marker in work_markers)


def candidate_mentions_expected_composer(draft: DraftRecordingEntry, lowered_title: str) -> bool:
    composer_latin = compact(draft.composer_name_latin)
    if composer_latin:
        tokens = tokenize_person_name(composer_latin)
        if tokens and tokens[-1].lower() in {token.lower() for token in tokenize_person_name(lowered_title)}:
            return True
    composer_cjk = compact(draft.composer_name)
    if composer_cjk and composer_cjk in compact(lowered_title):
        return True
    if composer_cjk:
        composer_tokens = tokenize_person_name(composer_cjk)
        if any(len(token) >= 2 and token in compact(lowered_title) for token in composer_tokens):
            return True
    return False


def build_candidate_work_anchor_terms(draft: DraftRecordingEntry) -> set[str]:
    values: set[str] = set()
    for value in (compact(draft.work_title_latin), compact(draft.work_title), compact(draft.catalogue)):
        if not value:
            continue
        lowered = value.lower()
        values.add(lowered)
        values.update(build_generic_work_aliases(value))
        values.update(build_named_work_aliases(value))
        latin_alias = build_latin_work_alias(value)
        if latin_alias:
            values.add(latin_alias.lower())
        stripped = re.sub(r"\b(?:op|k|bwv|hob|d|wab)\.?\s*\d+[a-z]?\b", "", lowered, flags=re.I).strip(" ,.;:-")
        if stripped:
            values.add(stripped)
        if "钢琴协奏曲" in value:
            values.add("钢琴协奏曲")
            values.add("钢协")
        if "协奏曲" in value and "钢琴" in value:
            values.add("钢琴协奏曲")
            values.add("钢协")
    return {
        value
        for value in values
        if len(value) >= 4 or (contains_cjk(value) and len(value) >= 2)
    }


def candidate_mentions_primary_and_secondary(draft: DraftRecordingEntry, lowered_title: str) -> bool:
    primary = candidate_mentions_names(lowered_title, getattr(draft, "primary_names_latin", []) or getattr(draft, "primary_names", []))
    secondary = candidate_mentions_names(
        lowered_title,
        getattr(draft, "secondary_names_latin", []) or getattr(draft, "secondary_names", []),
    )
    return primary and secondary


def candidate_mentions_any_lead(draft: DraftRecordingEntry, lowered_title: str) -> bool:
    lead_values = [
        *getattr(draft, "primary_names_latin", []),
        *getattr(draft, "primary_names", []),
        *getattr(draft, "secondary_names_latin", []),
        *getattr(draft, "secondary_names", []),
        *draft.lead_names_latin,
        *draft.lead_names,
    ]
    return candidate_mentions_names(lowered_title, lead_values)


def candidate_mentions_names(lowered_title: str, values: list[str]) -> bool:
    title_tokens = {token.lower() for token in tokenize_person_name(lowered_title)}
    for value in values:
        tokens = tokenize_person_name(value)
        if not tokens:
            continue
        surname = tokens[-1].lower()
        if len(surname) >= 3 and surname.isascii():
            if surname in title_tokens:
                return True
        elif len(surname) >= 2 and surname in lowered_title:
            return True
    return False


def extract_reference_year(draft: DraftRecordingEntry) -> str:
    years = extract_reference_years(draft)
    return years[0] if years else ""


def extract_reference_years(draft: DraftRecordingEntry) -> list[str]:
    years: list[str] = []
    for value in (draft.performance_date_text, draft.title, draft.raw_text):
        years.extend(re.findall(r"((?:17|18|19|20)\d{2})", compact(value)))
    return dedupe_preserve_order(years)


def extract_conflicting_year(lowered_title: str, reference_year: str) -> bool:
    years = set(re.findall(r"(?:17|18|19|20)\d{2}", lowered_title))
    return bool(years and reference_year not in years)


def extract_conflicting_years(lowered_title: str, reference_years: list[str]) -> bool:
    years = set(re.findall(r"(?:17|18|19|20)\d{2}", lowered_title))
    return bool(years and reference_years and years.isdisjoint(set(reference_years)))


def extract_month_day_markers(value: str) -> set[str]:
    normalized = compact(value)
    if not normalized:
        return set()
    markers: set[str] = set()
    patterns = (
        r"(?:(?:17|18|19|20)\d{2})\s*[./-]\s*(\d{1,2})\s*[./-]\s*(\d{1,2})",
        r"(?:(?:17|18|19|20)\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?",
    )
    for pattern in patterns:
        for month, day in re.findall(pattern, normalized):
            markers.add(f"{int(month):02d}-{int(day):02d}")
    return markers


def is_tolerable_chamber_year_typo_candidate(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> bool:
    if compact(getattr(draft, "work_type_hint", "")) != "chamber_solo":
        return False
    if draft.secondary_names or draft.secondary_names_latin or draft.ensemble_names or draft.ensemble_names_latin:
        return False
    reference_markers = extract_month_day_markers(draft.performance_date_text)
    if not reference_markers:
        return False
    support_text = " ".join(
        part
        for part in [
            compact(candidate.title),
            compact(record.description) if record is not None else "",
            compact(record.uploader) if record is not None else "",
        ]
        if part
    ).lower()
    if not support_text:
        return False
    if not candidate_mentions_any_lead(draft, support_text):
        return False
    if candidate_conflicting_credit_tokens(draft, support_text):
        return False
    normalized_support_text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", support_text)
    work_supported = any(
        compact(alias).lower() in support_text
        or re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", compact(alias).lower()) in normalized_support_text
        for alias in build_candidate_work_anchor_terms(draft)
        if compact(alias)
    )
    if not work_supported:
        return False
    if not (reference_markers & extract_month_day_markers(support_text)):
        return False
    return candidate_match_quality_score(draft, candidate, record) >= 0.12


def has_ambiguous_upload_cluster(draft: DraftRecordingEntry, candidates: list[LinkCandidate]) -> bool:
    if len(candidates) < 3:
        return False
    top_confidence = candidates[0].confidence or 0.0
    top_platform = compact(candidates[0].platform)
    if top_platform not in {"youtube", "bilibili"}:
        return False
    sparse_query = is_sparse_upload_query(draft)
    confidence_window = 0.25 if sparse_query else 0.2
    reference_floor = 0.04 if sparse_query else 0.05
    near_top = [
        candidate
        for candidate in candidates
        if compact(candidate.platform) == top_platform
        and (candidate.confidence or 0.0) >= max(FINAL_LINK_CONFIDENCE_THRESHOLD, top_confidence - confidence_window)
    ]
    if len(near_top) < 3:
        return False
    exactness_scores = [candidate_title_quality_score(draft, compact(candidate.title)) for candidate in near_top]
    reference_hits = sum(1 for score in exactness_scores if score >= reference_floor)
    if len(near_top) >= 3 and reference_hits >= 2 and (max(exactness_scores, default=0.0) - min(exactness_scores, default=0.0)) >= 0.05:
        return True
    if len(near_top) >= 4 and reference_hits >= 2:
        return True
    return reference_hits >= 3


def has_cross_platform_exact_cluster(draft: DraftRecordingEntry, candidates: list[LinkCandidate]) -> bool:
    if len(candidates) < 3:
        return False
    top = candidates[0]
    top_platform = compact(top.platform)
    if top_platform not in {"youtube", "bilibili"}:
        return False
    top_exactness = candidate_title_quality_score(draft, compact(top.title))
    rescue_floor = max(0.05, top_exactness - 0.06)
    grouped_counts: dict[str, int] = defaultdict(int)
    for candidate in candidates[1:]:
        platform = compact(candidate.platform)
        if not platform or platform == top_platform:
            continue
        if (candidate.confidence or 0.0) < FINAL_LINK_CONFIDENCE_THRESHOLD:
            continue
        if candidate_title_quality_score(draft, compact(candidate.title)) < rescue_floor:
            continue
        grouped_counts[platform] += 1
    return any(count >= 2 for count in grouped_counts.values())


def build_performance_context_terms(draft: DraftRecordingEntry) -> set[str]:
    values: set[str] = set()
    for value in (compact(draft.performance_date_text), compact(draft.venue_text)):
        if not value:
            continue
        lowered = value.lower()
        values.update(re.findall(r"(?:17|18|19|20)\d{2}", lowered))
        values.update(token for token in re.findall(r"[a-z]{4,}", lowered) if token not in {"with", "from"})
        values.update(re.findall(r"[\u4e00-\u9fff]{2,}", value))
    return values


def candidate_context_match_count(draft: DraftRecordingEntry, title: str) -> int:
    lowered = compact(title).lower()
    if not lowered:
        return 0
    return sum(1 for term in build_performance_context_terms(draft) if term in lowered)


def draft_credit_values(draft: DraftRecordingEntry) -> list[str]:
    return [
        *getattr(draft, "primary_names_latin", []),
        *getattr(draft, "primary_names", []),
        *getattr(draft, "secondary_names_latin", []),
        *getattr(draft, "secondary_names", []),
        *draft.lead_names_latin,
        *draft.lead_names,
    ]


def candidate_inferred_people(draft: DraftRecordingEntry, title: str) -> list[str]:
    inferred = infer_people_from_title(title)
    if inferred:
        return inferred
    normalized = compact(title)
    lowered = normalized.lower()
    if not normalized:
        return []
    work_aliases = sorted(build_candidate_work_anchor_terms(draft), key=len, reverse=True)
    anchor_index = next((lowered.find(alias) for alias in work_aliases if alias and lowered.find(alias) > 0), -1)
    if anchor_index <= 0:
        return []
    prefix = compact(normalized[:anchor_index].strip(" -:|,;/"))
    if any(
        person_variant_matches(prefix, value)
        for value in [compact(draft.composer_name), compact(draft.composer_name_latin)]
        if compact(value)
    ):
        return []
    prefix_tokens = tokenize_person_name(prefix)
    if len(prefix_tokens) == 2 and looks_like_title_person(prefix):
        return [prefix]
    return []


def candidate_conflicting_credit_tokens(draft: DraftRecordingEntry, title: str) -> set[str]:
    inferred_people = [
        strip_title_credit_role_prefix(person)
        for person in candidate_inferred_people(draft, title)
    ]
    inferred_people = [
        person
        for person in inferred_people
        if compact(person) and not looks_like_year_or_work(person) and not looks_like_ensemble_name(person)
    ]
    if not inferred_people:
        return set()
    allowed_people = [value for value in draft_credit_values(draft) if compact(value)]
    if not allowed_people:
        return set()
    matched_people = [
        person for person in inferred_people if any(person_variant_matches(person, allowed) for allowed in allowed_people)
    ]
    unmatched_people = [person for person in inferred_people if person not in matched_people]
    lowered_title = compact(title).lower()
    work_anchor = any(alias in lowered_title for alias in build_candidate_work_anchor_terms(draft))
    expected_secondary_people = [
        *getattr(draft, "secondary_names_latin", []),
        *getattr(draft, "secondary_names", []),
    ]
    if matched_people and unmatched_people and has_collaboration_marker(title) and any(compact(value) for value in expected_secondary_people):
        return {token.lower() for person in unmatched_people for token in tokenize_person_name(person)}
    if not matched_people and len(unmatched_people) == 1 and work_anchor:
        person = unmatched_people[0]
        person_tokens = [token.lower() for token in tokenize_person_name(person)]
        composer_tokens = {
            token.lower()
            for value in [compact(draft.composer_name), compact(draft.composer_name_latin)]
            for token in tokenize_person_name(value)
            if compact(token)
        }
        work_like_tokens = {
            "piano",
            "violin",
            "cello",
            "concerto",
            "concertos",
            "sonata",
            "sonatas",
            "symphony",
            "symphonies",
            "op",
        }
        person_token_set = set(person_tokens)
        safe_token_set = composer_tokens | work_like_tokens
        is_composer_work_prefix = bool(person_token_set) and person_token_set.issubset(safe_token_set)
        # Avoid false red-zone conflicts for titles such as
        # "Beethoven: Piano Sonata No. 23 ...", where inferred prefix tokens are
        # fully explained by composer/work labels rather than a conflicting performer.
        if len(person_tokens) >= 2 and not is_composer_work_prefix:
            return {token.lower() for token in tokenize_person_name(person)}
    return set()


def strip_title_credit_role_prefix(value: str) -> str:
    normalized = compact(value)
    if not normalized:
        return ""
    return re.sub(
        (
            r"^(?:"
            r"演奏者|演奏|表演者|指挥|钢琴|钢琴家|小提琴|小提琴家|中提琴|大提琴|乐团|"
            r"performer|performed by|player|played by|conductor|piano|pianist|violin|violinist|"
            r"viola|violist|cello|cellist"
            r")\s*[:：-]\s*"
        ),
        "",
        normalized,
        flags=re.I,
    ).strip()


def looks_like_year_or_work(value: str) -> bool:
    lowered = compact(value).lower()
    work_markers = (
        "symphony",
        "concerto",
        "concertos",
        "sonata",
        "opera",
        "live",
        "festival",
        "交响曲",
        "协奏曲",
        "钢协",
        "奏鸣曲",
        "歌剧",
        "现场",
        "音乐节",
        "海顿",
        "布鲁克纳",
        "贝多芬",
        "马勒",
    )
    return bool(re.search(r"(19\d{2}|20\d{2})", lowered) or any(token in lowered for token in work_markers))


def classify_link_candidate_zone(draft: DraftRecordingEntry, candidate: LinkCandidate) -> tuple[str, str]:
    title = compact(candidate.title)
    lowered_title = title.lower()
    conflicts = candidate_conflicting_credit_tokens(draft, title)
    lead_hits = candidate_mentions_any_lead(draft, lowered_title)
    if conflicts and (has_collaboration_marker(title) or not lead_hits or len(conflicts) >= 2):
        return "red", f"conflicting-credit:{'/'.join(sorted(conflicts)[:3])}"

    exactness = candidate_title_quality_score(draft, title)
    confidence = candidate.confidence or 0.0
    if confidence >= FINAL_LINK_CONFIDENCE_THRESHOLD and exactness >= CANDIDATE_GREEN_EXACTNESS_THRESHOLD:
        return "green", "high-confidence"
    if exactness >= CANDIDATE_YELLOW_EXACTNESS_THRESHOLD:
        return "yellow", "review-needed"
    return "yellow", "low-evidence"


def is_independently_finalizable_primary_candidate(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> bool:
    platform = compact(candidate.platform)
    if platform not in PRIMARY_COMPLETION_PLATFORMS:
        return False
    zone, _ = classify_link_candidate_zone(draft, candidate)
    if zone == "red":
        return False

    confidence = candidate.confidence or 0.0
    exactness = candidate_match_quality_score(draft, candidate, record)
    if confidence >= FINAL_LINK_CONFIDENCE_THRESHOLD and exactness >= CANDIDATE_GREEN_EXACTNESS_THRESHOLD:
        return True

    if platform != "apple_music" or not is_apple_track_url(candidate.url) or record is None:
        return False
    if confidence < 0.54 or exactness < 0.03:
        return False

    support_text = " ".join(
        part for part in [compact(candidate.title), compact(record.description), compact(record.uploader)] if part
    ).lower()
    normalized_support_text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", support_text)
    work_supported = any(
        alias in support_text or re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", alias) in normalized_support_text
        for alias in build_candidate_work_anchor_terms(draft)
    )
    if not candidate_mentions_any_lead(draft, support_text):
        return False
    if not work_supported:
        return False
    if candidate_conflicting_credit_tokens(draft, support_text):
        return False
    reference_years = extract_reference_years(draft)
    year_haystack = compact(candidate.title).lower() if is_apple_track_url(candidate.url) else support_text
    if reference_years and extract_conflicting_years(year_haystack, reference_years):
        return False
    if (
        candidate_description_support_score(draft, compact(record.description)) <= 0
        and candidate_title_quality_score(draft, compact(candidate.title)) < 0.02
    ):
        return False
    return True


def is_version_rescue_candidate(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> bool:
    zone, _ = classify_link_candidate_zone(draft, candidate)
    if zone == "red":
        return False
    confidence = candidate.confidence or 0.0
    exactness = candidate_match_quality_score(draft, candidate, record)
    if confidence < LOW_CONFIDENCE_THRESHOLD or exactness < 0.08:
        return False
    if candidate_year_conflicts_reference(draft, candidate, record):
        return False
    support_text = " ".join(
        part
        for part in [
            compact(candidate.title),
            compact(record.description) if record is not None else "",
            compact(record.uploader) if record is not None else "",
        ]
        if part
    ).lower()
    if not candidate_mentions_any_lead(draft, support_text):
        return False
    if candidate_conflicting_credit_tokens(draft, support_text):
        return False
    return True


def candidate_year_conflicts_reference(
    draft: DraftRecordingEntry,
    candidate: LinkCandidate,
    record: SourceRecord | None,
) -> bool:
    reference_years = extract_reference_years(draft)
    if not reference_years:
        return False
    haystack = compact(candidate.title).lower()
    if record is not None and not is_apple_track_url(candidate.url):
        haystack = " ".join(part for part in [haystack, compact(record.description).lower()] if part)
    if not extract_conflicting_years(haystack, reference_years):
        return False
    if is_tolerable_chamber_year_typo_candidate(draft, candidate, record):
        return False
    return True


def annotate_link_candidates(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    *,
    filter_red: bool = True,
) -> list[LinkCandidate]:
    annotated: list[LinkCandidate] = []
    for candidate in candidates:
        zone, note = classify_link_candidate_zone(draft, candidate)
        if filter_red and zone == "red":
            continue
        annotated.append(candidate.model_copy(update={"zone": zone, "note": note}))
    return annotated


def supplement_primary_platform_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    accepted_links: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> list[LinkCandidate]:
    represented_platforms = {
        compact(candidate.platform) for candidate in filtered_links if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    }
    if len(represented_platforms) != 1:
        return filtered_links

    anchor_links = [
        candidate for candidate in accepted_links if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    ]
    if len({compact(candidate.platform) for candidate in anchor_links}) != 1:
        return filtered_links

    anchor_platform = compact(anchor_links[0].platform)
    if anchor_platform not in represented_platforms:
        return filtered_links

    context_terms = build_performance_context_terms(draft)
    if not context_terms:
        return filtered_links

    best_anchor_exactness = max(candidate_title_quality_score(draft, compact(candidate.title)) for candidate in anchor_links)
    best_anchor_confidence = max((candidate.confidence or 0.0) for candidate in anchor_links)
    best_anchor_context = max(candidate_context_match_count(draft, compact(candidate.title)) for candidate in anchor_links)
    exactness_floor = max(PRIMARY_PLATFORM_COMPLETION_MIN_EXACTNESS, best_anchor_exactness - PRIMARY_PLATFORM_COMPLETION_EXACTNESS_GAP)
    confidence_floor = max(
        PRIMARY_PLATFORM_COMPLETION_CONFIDENCE_THRESHOLD,
        best_anchor_confidence - PRIMARY_PLATFORM_COMPLETION_CONFIDENCE_GAP,
    )
    reference_years = extract_reference_years(draft)
    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    additions: list[LinkCandidate] = []

    for platform in PRIMARY_COMPLETION_PLATFORMS - represented_platforms:
        platform_candidates = [
            candidate
            for candidate in link_candidates
            if compact(candidate.url) not in existing_urls
            and compact(candidate.platform) == platform
            and (candidate.confidence or 0.0) >= confidence_floor
            and candidate_title_quality_score(draft, compact(candidate.title)) >= exactness_floor - 1e-6
            and candidate_context_match_count(draft, compact(candidate.title)) > best_anchor_context
            and (
                not reference_years
                or not extract_conflicting_years(compact(candidate.title).lower(), reference_years)
            )
        ]
        if not platform_candidates:
            continue
        best_candidate = sort_link_candidates(
            draft,
            platform_candidates,
            record_map,
            prefer_exactness=prefer_exactness,
        )[0]
        additions.append(best_candidate)
        existing_urls.add(compact(best_candidate.url))

    if not additions:
        return filtered_links
    return sort_link_candidates(
        draft,
        dedupe_link_candidates([*filtered_links, *additions]),
        record_map,
        prefer_exactness=prefer_exactness,
    )


def supplement_independent_primary_platform_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> list[LinkCandidate]:
    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    represented_platforms = {
        compact(candidate.platform)
        for candidate in filtered_links
        if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    }
    additions: list[LinkCandidate] = []

    for platform in PRIMARY_COMPLETION_PLATFORMS - represented_platforms:
        platform_candidates = [
            candidate
            for candidate in link_candidates
            if compact(candidate.url) not in existing_urls
            and compact(candidate.platform) == platform
            and not candidate_year_conflicts_reference(
                draft,
                candidate,
                record_map.get(compact(candidate.url)),
            )
            and is_independently_finalizable_primary_candidate(
                draft,
                candidate,
                record_map.get(compact(candidate.url)),
            )
        ]
        if not platform_candidates:
            continue
        best_candidate = sort_link_candidates(
            draft,
            platform_candidates,
            record_map,
            prefer_exactness=prefer_exactness,
        )[0]
        additions.append(best_candidate)
        existing_urls.add(compact(best_candidate.url))

    if not additions:
        return filtered_links
    return sort_link_candidates(
        draft,
        dedupe_link_candidates([*filtered_links, *additions]),
        record_map,
        prefer_exactness=prefer_exactness,
    )


def supplement_complete_upload_rescue_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> list[LinkCandidate]:
    if not filtered_links:
        return filtered_links

    top_candidate = filtered_links[0]
    top_record = record_map.get(compact(top_candidate.url))
    top_packaging = candidate_packaging_priority_score(top_candidate, top_record)
    if top_packaging >= -0.08 and not looks_like_title_single_movement(compact(top_candidate.title)):
        return filtered_links

    top_exactness = candidate_match_quality_score(draft, top_candidate, top_record)
    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    additions: list[LinkCandidate] = []

    for platform in PRIMARY_COMPLETION_PLATFORMS:
        platform_candidates = [
            candidate
            for candidate in link_candidates
            if compact(candidate.url) not in existing_urls
            and compact(candidate.platform) == platform
            and (candidate.confidence or 0.0) >= 0.5
            and not candidate_year_conflicts_reference(
                draft,
                candidate,
                record_map.get(compact(candidate.url)),
            )
            and classify_link_candidate_zone(draft, candidate)[0] != "red"
            and not looks_like_title_single_movement(compact(candidate.title))
            and candidate_packaging_priority_score(candidate, record_map.get(compact(candidate.url))) >= max(0.0, top_packaging + 0.08)
            and candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
            >= max(0.12, top_exactness - 0.03 - 1e-6)
        ]
        if not platform_candidates:
            continue
        best_candidate = sort_link_candidates(
            draft,
            platform_candidates,
            record_map,
            prefer_exactness=True,
        )[0]
        additions.append(best_candidate)
        existing_urls.add(compact(best_candidate.url))

    if not additions:
        return filtered_links
    return sort_link_candidates(
        draft,
        dedupe_link_candidates([*filtered_links, *additions]),
        record_map,
        prefer_exactness=prefer_exactness,
    )


def supplement_close_same_platform_complete_alternate_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> tuple[list[LinkCandidate], bool]:
    if len(filtered_links) < 2:
        return filtered_links, False
    chamber_work_text = " ".join(
        value.casefold()
        for value in [compact(draft.work_title_latin), compact(draft.work_title)]
        if compact(value)
    )
    if not any(token in chamber_work_text for token in ("sonata", "quartet", "trio", "奏鸣曲", "四重奏", "三重奏")):
        return filtered_links, False

    represented_platforms = {
        compact(candidate.platform)
        for candidate in filtered_links
        if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    }
    if len(represented_platforms) != 1:
        return filtered_links, False

    target_platform = next(iter(represented_platforms))
    anchor_links = [
        candidate
        for candidate in filtered_links
        if compact(candidate.platform) == target_platform
        and not looks_like_title_single_movement(compact(candidate.title))
        and candidate_packaging_priority_score(candidate, record_map.get(compact(candidate.url))) >= 0.0
    ]
    if len(anchor_links) < 2:
        return filtered_links, False

    anchor_exactness_scores = sorted(
        (
            candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
            for candidate in anchor_links
        ),
        reverse=True,
    )
    reference_anchor_exactness = anchor_exactness_scores[min(1, len(anchor_exactness_scores) - 1)]
    anchor_durations = [
        record_map.get(compact(candidate.url)).duration_seconds
        for candidate in anchor_links
        if record_map.get(compact(candidate.url)) is not None
        and record_map.get(compact(candidate.url)).duration_seconds > 0
    ]
    duration_floor = max(600, int(max(anchor_durations) * 0.75)) if anchor_durations else 0
    rescue_floor = max(0.08, reference_anchor_exactness - 0.06)
    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    rescue_candidates = [
        candidate
        for candidate in link_candidates
        if compact(candidate.url) not in existing_urls
        and compact(candidate.platform) == target_platform
        and (candidate.confidence or 0.0) >= LOW_CONFIDENCE_THRESHOLD
        and not candidate_year_conflicts_reference(
            draft,
            candidate,
            record_map.get(compact(candidate.url)),
        )
        and classify_link_candidate_zone(draft, candidate)[0] != "red"
        and not looks_like_title_single_movement(compact(candidate.title))
        and candidate_packaging_priority_score(candidate, record_map.get(compact(candidate.url))) >= 0.0
        and (
            duration_floor <= 0
            or record_map.get(compact(candidate.url)) is None
            or record_map.get(compact(candidate.url)).duration_seconds <= 0
            or record_map.get(compact(candidate.url)).duration_seconds >= duration_floor
        )
        and candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
        >= rescue_floor - 1e-6
    ]
    if not rescue_candidates:
        return filtered_links, False

    best_candidate = sort_link_candidates(
        draft,
        rescue_candidates,
        record_map,
        prefer_exactness=True,
    )[0]
    return (
        sort_link_candidates(
            draft,
            dedupe_link_candidates([*filtered_links, best_candidate]),
            record_map,
            prefer_exactness=prefer_exactness,
        ),
        True,
    )


def supplement_accepted_platform_longform_alternate_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    accepted_links: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> tuple[list[LinkCandidate], bool]:
    if not filtered_links or not accepted_links:
        return filtered_links, False

    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    additions: list[LinkCandidate] = []
    accepted_by_platform: dict[str, list[LinkCandidate]] = defaultdict(list)
    for candidate in accepted_links:
        platform = compact(candidate.platform)
        if platform in PRIMARY_COMPLETION_PLATFORMS:
            accepted_by_platform[platform].append(candidate)

    for platform, platform_accepted in accepted_by_platform.items():
        accepted_durations = [
            record_map.get(compact(candidate.url)).duration_seconds
            for candidate in platform_accepted
            if record_map.get(compact(candidate.url)) is not None
            and record_map.get(compact(candidate.url)).duration_seconds > 0
        ]
        if not accepted_durations:
            continue
        best_accepted_confidence = max((candidate.confidence or 0.0) for candidate in platform_accepted)
        best_accepted_exactness = max(
            candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
            for candidate in platform_accepted
        )
        longest_accepted_duration = max(accepted_durations)
        rescue_candidates = [
            candidate
            for candidate in link_candidates
            if compact(candidate.url) not in existing_urls
            and compact(candidate.platform) == platform
            and (candidate.confidence or 0.0) >= max(LOW_CONFIDENCE_THRESHOLD, best_accepted_confidence - 0.05)
            and not candidate_year_conflicts_reference(
                draft,
                candidate,
                record_map.get(compact(candidate.url)),
            )
            and classify_link_candidate_zone(draft, candidate)[0] != "red"
            and not looks_like_title_single_movement(compact(candidate.title))
            and (
                record_map.get(compact(candidate.url)) is not None
                and record_map.get(compact(candidate.url)).duration_seconds >= int(longest_accepted_duration * 1.35)
            )
            and candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
            >= max(0.0, best_accepted_exactness - 0.05 - 1e-6)
        ]
        if not rescue_candidates:
            continue
        best_candidate = max(
            rescue_candidates,
            key=lambda candidate: (
                record_map.get(compact(candidate.url)).duration_seconds if record_map.get(compact(candidate.url)) else 0,
                candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url))),
                candidate.confidence or 0.0,
            ),
        )
        additions.append(best_candidate)
        existing_urls.add(compact(best_candidate.url))

    if not additions:
        return filtered_links, False
    return (
        sort_link_candidates(
            draft,
            dedupe_link_candidates([*filtered_links, *additions]),
            record_map,
            prefer_exactness=prefer_exactness,
        ),
        True,
    )


def supplement_chamber_cross_platform_low_confidence_rescue_links(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> tuple[list[LinkCandidate], bool]:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return filtered_links, False
    if not filtered_links or len(filtered_links) >= 3:
        return filtered_links, False

    represented_platforms = {
        compact(candidate.platform)
        for candidate in filtered_links
        if compact(candidate.platform) in {"youtube", "bilibili"}
    }
    if len(represented_platforms) != 1:
        return filtered_links, False

    anchor = filtered_links[0]
    anchor_platform = compact(anchor.platform)
    if anchor_platform not in {"youtube", "bilibili"}:
        return filtered_links, False
    anchor_record = record_map.get(compact(anchor.url))
    anchor_title_exactness = candidate_title_quality_score(draft, compact(anchor.title))
    confidence_floor = max(0.58, (anchor.confidence or 0.0) - 0.16)
    title_exactness_floor = max(0.02, anchor_title_exactness - 0.08)

    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    rescue_pool = [
        candidate
        for candidate in link_candidates
        if compact(candidate.url) not in existing_urls
        and compact(candidate.platform) in {"youtube", "bilibili"}
        and compact(candidate.platform) != anchor_platform
        and (candidate.confidence or 0.0) >= confidence_floor
        and not candidate_year_conflicts_reference(
            draft,
            candidate,
            record_map.get(compact(candidate.url)),
        )
        and classify_link_candidate_zone(draft, candidate)[0] != "red"
        and candidate_title_quality_score(draft, compact(candidate.title)) >= title_exactness_floor - 1e-6
    ]
    if len(rescue_pool) < 2:
        return filtered_links, False

    grouped: dict[str, list[LinkCandidate]] = defaultdict(list)
    for candidate in rescue_pool:
        grouped[compact(candidate.platform)].append(candidate)
    if not grouped:
        return filtered_links, False

    target_platform = max(
        grouped.keys(),
        key=lambda platform: (
            len(grouped[platform]),
            max((candidate.confidence or 0.0) for candidate in grouped[platform]),
        ),
    )
    selected_rescues = sort_link_candidates(
        draft,
        grouped[target_platform],
        record_map,
        prefer_exactness=True,
    )[:2]
    if len(selected_rescues) < 2:
        return filtered_links, False

    return (
        sort_link_candidates(
            draft,
            dedupe_link_candidates([*filtered_links, *selected_rescues]),
            record_map,
            prefer_exactness=prefer_exactness,
        ),
        True,
    )


def supplement_chamber_post_final_same_platform_title_rescues(
    draft: DraftRecordingEntry,
    final_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    max_links: int = 4,
) -> list[LinkCandidate]:
    if compact(getattr(draft, "work_type_hint", "")).lower() != "chamber_solo":
        return final_links
    if len(final_links) >= max_links:
        return final_links

    primary_video_links = [
        candidate
        for candidate in final_links
        if compact(candidate.platform) in {"youtube", "bilibili"}
    ]
    if not primary_video_links:
        return final_links

    platform_counts = Counter(compact(candidate.platform) for candidate in primary_video_links)
    anchor_platform = max(
        platform_counts.keys(),
        key=lambda platform: (
            platform_counts[platform],
            max(
                (
                    candidate.confidence or 0.0
                    for candidate in primary_video_links
                    if compact(candidate.platform) == platform
                ),
                default=0.0,
            ),
        ),
    )
    anchor_title_exactness = max(
        candidate_title_quality_score(draft, compact(candidate.title))
        for candidate in primary_video_links
        if compact(candidate.platform) == anchor_platform
    )
    title_floor = max(0.08, anchor_title_exactness - 0.02)
    existing_urls = {compact(candidate.url) for candidate in final_links}
    rescue_candidates = [
        candidate
        for candidate in link_candidates
        if compact(candidate.url) not in existing_urls
        and compact(candidate.platform) == anchor_platform
        and LOW_CONFIDENCE_THRESHOLD <= (candidate.confidence or 0.0) < FINAL_LINK_CONFIDENCE_THRESHOLD
        and candidate_title_quality_score(draft, compact(candidate.title)) >= title_floor - 1e-6
        and classify_link_candidate_zone(draft, candidate)[0] != "red"
    ]
    if not rescue_candidates:
        return final_links

    selected_rescue = sort_link_candidates(
        draft,
        rescue_candidates,
        record_map,
        prefer_exactness=True,
    )[0]
    return dedupe_link_candidates([*final_links, selected_rescue])[:max_links]


def supplement_apple_track_version_rescues(
    draft: DraftRecordingEntry,
    filtered_links: list[LinkCandidate],
    link_candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
    *,
    prefer_exactness: bool = False,
) -> list[LinkCandidate]:
    represented_platforms = {
        compact(candidate.platform)
        for candidate in filtered_links
        if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    }
    apple_track_links = [
        candidate
        for candidate in filtered_links
        if compact(candidate.platform) == "apple_music" and is_apple_track_url(candidate.url)
    ]
    if not apple_track_links:
        return filtered_links
    if any(platform in represented_platforms for platform in {"youtube", "bilibili"}):
        return filtered_links

    existing_urls = {compact(candidate.url) for candidate in filtered_links}
    rescue_pool = [
        candidate
        for candidate in link_candidates
        if compact(candidate.url) not in existing_urls and compact(candidate.platform) in {"youtube", "bilibili"}
    ]
    rescue_links = pick_version_rescue_links(draft, rescue_pool, record_map)
    rescue_links = [
        candidate for candidate in rescue_links if compact(candidate.platform) in {"youtube", "bilibili"}
    ]
    if not rescue_links:
        return filtered_links
    return sort_link_candidates(
        draft,
        dedupe_link_candidates([*filtered_links, *rescue_links]),
        record_map,
        prefer_exactness=prefer_exactness or True,
    )


def prioritize_primary_platform_champions(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
) -> tuple[list[LinkCandidate], int]:
    champion_urls: list[str] = []
    for platform in PRIMARY_COMPLETION_PLATFORMS:
        platform_candidates = [
            candidate
            for candidate in candidates
            if compact(candidate.platform) == platform
            and is_independently_finalizable_primary_candidate(
                draft,
                candidate,
                record_map.get(compact(candidate.url)),
            )
        ]
        if platform_candidates:
            champion_urls.append(compact(platform_candidates[0].url))
    if not champion_urls:
        return candidates, 0
    champion_set = set(champion_urls)
    prioritized = [candidate for candidate in candidates if compact(candidate.url) in champion_set]
    prioritized.extend(candidate for candidate in candidates if compact(candidate.url) not in champion_set)
    return prioritized, len(champion_urls)


def prioritize_accepted_links(
    candidates: list[LinkCandidate],
    accepted_url_set: set[str],
    *,
    final_link_limit: int,
) -> list[LinkCandidate]:
    if not accepted_url_set:
        return candidates
    accepted_candidates = [
        candidate for candidate in candidates if compact(candidate.url) in accepted_url_set
    ]
    if not accepted_candidates or len(accepted_candidates) < max(1, final_link_limit):
        return candidates
    accepted_candidate_urls = {compact(candidate.url) for candidate in accepted_candidates}
    prioritized = list(accepted_candidates)
    prioritized.extend(
        candidate for candidate in candidates if compact(candidate.url) not in accepted_candidate_urls
    )
    return prioritized


def pick_version_rescue_links(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    record_map: dict[str, SourceRecord],
) -> list[LinkCandidate]:
    if not candidates:
        return []
    ordered = sort_link_candidates(draft, candidates, record_map, prefer_exactness=True)
    qualified = [
        candidate
        for candidate in ordered
        if is_version_rescue_candidate(draft, candidate, record_map.get(compact(candidate.url)))
    ]
    if not qualified:
        return []

    selected: list[LinkCandidate] = []
    seen_platforms: set[str] = set()
    for candidate in qualified:
        platform = compact(candidate.platform)
        if platform and platform not in seen_platforms and platform in PRIMARY_COMPLETION_PLATFORMS:
            selected.append(candidate)
            seen_platforms.add(platform)
        if len(selected) >= 3:
            break
    if selected:
        return selected

    primary = qualified[0]
    rescue_links = [primary]
    primary_exactness = candidate_match_quality_score(draft, primary, record_map.get(compact(primary.url)))
    for candidate in qualified[1:]:
        if len(rescue_links) >= 2:
            break
        exactness = candidate_match_quality_score(draft, candidate, record_map.get(compact(candidate.url)))
        if exactness >= primary_exactness - 0.03:
            rescue_links.append(candidate)
    return rescue_links


def determine_final_link_limit(
    draft: DraftRecordingEntry,
    candidates: list[LinkCandidate],
    *,
    accepted_url_count: int = 0,
    ambiguous_upload_cluster: bool = False,
) -> int:
    if not candidates:
        return 2
    collaboration_hint = has_title_only_collaboration_hint(draft)
    if accepted_url_count >= 2:
        if collaboration_hint and len(candidates) >= 5:
            return 5
        return min(4, max(2, accepted_url_count))
    if ambiguous_upload_cluster:
        if is_sparse_upload_query(draft):
            return 5
        return 4
    top_confidence = candidates[0].confidence or 0.0
    close_ties = [
        candidate
        for candidate in candidates
        if abs((candidate.confidence or 0.0) - top_confidence) <= 0.01
    ]
    if collaboration_hint and len(candidates) >= 5 and len(close_ties) >= 3:
        return 5
    if has_cross_platform_exact_cluster(draft, candidates):
        return 3
    primary_platform_counts = Counter(
        compact(candidate.platform)
        for candidate in candidates
        if compact(candidate.platform) in PRIMARY_COMPLETION_PLATFORMS
    )
    if (
        is_sparse_upload_query(draft)
        and len(candidates) >= 5
        and len(primary_platform_counts) >= 2
        and any(count >= 2 for count in primary_platform_counts.values())
    ):
        return 5
    if not has_explicit_year(draft.performance_date_text) and len(close_ties) > 1:
        return min(4, max(2, len(close_ties)))
    if len(close_ties) >= 3:
        return min(4, len(close_ties))
    return 2


def should_skip_stage(*, label: str, records: list[SourceRecord]) -> bool:
    if label != "fallback":
        return False
    strong_records = [
        record
        for record in records
        if compact(record.url)
        and record.platform in {"youtube", "bilibili", "apple_music", "spotify", "qobuz"}
        and record.same_recording_score >= 0.6
    ]
    if any(record.same_recording_score >= 0.72 for record in strong_records):
        return True
    return len(strong_records) >= 2


def should_skip_llm_synthesis(records: list[SourceRecord]) -> bool:
    scores = sorted((record.same_recording_score for record in records if compact(record.url)), reverse=True)
    if not scores:
        return True
    if scores[0] < 0.88:
        return False
    second_best = scores[1] if len(scores) > 1 else 0.0
    return scores[0] - second_best >= 0.12 and second_best <= 0.45


def has_explicit_year(value: str) -> bool:
    return bool(re.search(r"(17|18|19|20)\d{2}", compact(value)))


def is_sparse_upload_query(draft: DraftRecordingEntry) -> bool:
    if has_explicit_year(draft.source_line):
        return False
    query_leads = dedupe_preserve_order([*draft.query_lead_names_latin, *draft.query_lead_names])
    if len(query_leads) > 1:
        return False
    if draft.secondary_names or draft.secondary_names_latin:
        return False
    if draft.ensemble_names or draft.ensemble_names_latin:
        return False
    return True


def has_title_only_collaboration_hint(draft: DraftRecordingEntry) -> bool:
    if has_explicit_year(draft.source_line):
        return False
    if not (has_collaboration_marker(draft.title) or " - " in compact(draft.title)):
        return False
    return bool(draft.primary_names or draft.primary_names_latin or draft.lead_names or draft.lead_names_latin)


def has_result_value(result: ResultPayload, field: str) -> bool:
    mapping = {
        "performanceDateText": compact(result.performance_date_text),
        "venueText": compact(result.venue_text),
        "albumTitle": compact(result.album_title),
        "label": compact(result.label),
        "releaseDate": compact(result.release_date),
        "notes": compact(result.notes),
        "links": "yes" if result.links else "",
        "images": "yes" if result.images else "",
    }
    return bool(mapping.get(field, ""))


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values:
        normalized = compact(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def dedupe_link_candidates(items: list[LinkCandidate]) -> list[LinkCandidate]:
    seen: set[str] = set()
    unique: list[LinkCandidate] = []
    for item in items:
        url = compact(item.url)
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def limit_link_candidates_per_platform(
    items: list[LinkCandidate],
    *,
    green_limit: int,
    yellow_limit: int,
) -> list[LinkCandidate]:
    if green_limit <= 0 and yellow_limit <= 0:
        return []
    platform_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"green": 0, "yellow": 0})
    limited: list[LinkCandidate] = []
    for item in items:
        platform = compact(item.platform) or "other"
        zone = compact(item.zone or "yellow").lower()
        if zone == "green":
            if platform_counts[platform]["green"] >= green_limit:
                continue
            platform_counts[platform]["green"] += 1
        elif zone == "yellow":
            if platform_counts[platform]["yellow"] >= yellow_limit:
                continue
            platform_counts[platform]["yellow"] += 1
        else:
            continue
        limited.append(item)
    return limited


def dedupe_image_candidates(items: list[ImageCandidate]) -> list[ImageCandidate]:
    seen: set[str] = set()
    unique: list[ImageCandidate] = []
    for item in items:
        src = compact(item.src)
        if not src:
            continue
        key = src.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def infer_people_from_source_line(source_line: str) -> tuple[list[str], list[str]]:
    segments = [compact(part) for part in re.split(r"[|\n]", source_line or "") if compact(part)]
    if not segments:
        return [], []
    leads: list[str] = []
    groups: list[str] = []
    if segments and not looks_like_year_or_work(segments[0]):
        leads.append(segments[0])
    if len(segments) >= 2 and not looks_like_year_or_work(segments[1]):
        groups.append(segments[1])
    return leads, groups


def infer_people_from_title(title: str) -> list[str]:
    people, _, _ = infer_title_entities(title)
    return people


def infer_title_entities(title: str) -> tuple[list[str], list[str], str]:
    normalized = compact(title)
    if not normalized:
        return [], [], ""
    date_hint = extract_title_date_hint(normalized)
    people: list[str] = []
    groups: list[str] = []
    for part in split_title_segments(normalized):
        value = compact(part.strip(" ,;|"))
        if not value:
            continue
        if date_hint and date_hint in value:
            continue
        if looks_like_ensemble_name(value):
            groups.append(value)
            continue
        if looks_like_year_or_work(value):
            continue
        if looks_like_title_person(value):
            people.append(value)
    return dedupe_preserve_order(people[:3]), dedupe_preserve_order(groups[:2]), date_hint


def split_title_segments(value: str) -> list[str]:
    normalized = compact(value)
    if not normalized:
        return []
    return [
        part
        for part in re.split(r"\s*(?:\||、|&|/| and | with | feat\.?| - )\s*", normalized, flags=re.I)
        if compact(part)
    ]


def extract_title_date_hint(title: str) -> str:
    normalized = compact(title)
    if not normalized:
        return ""
    patterns = [
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+\d{4}\b",
        r"\b(?:early|mid|late)\s+'?\d{2}s\b",
        r"(?<!\d)(?:19|20)\d{2}(?!\d)",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.I)
        if match:
            return compact(match.group(0))
    return ""


def extract_title_performance_context(title: str) -> str:
    normalized = compact(title)
    if not normalized:
        return ""
    for part in split_title_segments(normalized):
        value = compact(part.strip(" ,;|"))
        if not value:
            continue
        year_hint = extract_title_date_hint(value)
        if not year_hint or compact(value) == year_hint:
            continue
        return value
    return ""


def looks_like_ensemble_name(value: str) -> bool:
    lowered = compact(value).lower()
    return any(
        token in lowered
        for token in (
            "orchestra",
            "philharmonic",
            "symphony orchestra",
            "ensemble",
            "choir",
            "chorus",
            "quartet",
            "trio",
            "乐团",
            "愛樂",
            "爱乐",
            "交响乐团",
            "管弦乐团",
            "合唱团",
            "四重奏",
            "三重奏",
        )
    )


def looks_like_title_person(value: str) -> bool:
    tokens = tokenize_person_name(value)
    if contains_cjk(value):
        return bool(tokens)
    if len(tokens) >= 2:
        return True
    return len(tokens) == 1 and len(tokens[0]) >= 4 and tokens[0][0].isalpha()


def person_variant_matches(left: str, right: str) -> bool:
    left_norm = re.sub(r"[^A-Za-z\u4e00-\u9fff]+", "", compact(left)).lower()
    right_norm = re.sub(r"[^A-Za-z\u4e00-\u9fff]+", "", compact(right)).lower()
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm or left_norm in right_norm or right_norm in left_norm:
        return True
    left_tokens = [token.lower() for token in tokenize_person_name(left)]
    right_tokens = [token.lower() for token in tokenize_person_name(right)]
    if len(left_tokens) < 2 or len(right_tokens) < 2:
        return False

    def ordered_subset(shorter: list[str], longer: list[str]) -> bool:
        cursor = 0
        for token in longer:
            if token == shorter[cursor]:
                cursor += 1
                if cursor == len(shorter):
                    return True
        return False

    return ordered_subset(left_tokens, right_tokens) or ordered_subset(right_tokens, left_tokens)


def tokenize_person_name(value: str) -> list[str]:
    return [token for token in re.split(r"[^A-Za-z\u4e00-\u9fff]+", compact(value)) if token]


def has_collaboration_marker(value: str) -> bool:
    normalized = compact(value).lower()
    return any(marker in normalized for marker in ("&", " / ", " and ", " with ", "、"))


def looks_like_year_or_work(value: str) -> bool:
    lowered = compact(value).lower()
    return bool(
        re.search(r"(19\d{2}|20\d{2})", lowered)
        or any(token in lowered for token in ("symphony", "concerto", "sonata", "opera", "交响曲", "协奏曲", "奏鸣曲", "歌剧"))
    )
def looks_like_year_or_work(value: str) -> bool:
    lowered = compact(value).lower()
    work_markers = (
        "symphony",
        "concerto",
        "concertos",
        "sonata",
        "opera",
        "live",
        "festival",
        "交响曲",
        "协奏曲",
        "钢协",
        "奏鸣曲",
        "歌剧",
        "现场",
        "音乐节",
        "海顿",
        "布鲁克纳",
        "贝多芬",
        "马勒",
    )
    return bool(re.search(r"(19\d{2}|20\d{2})", lowered) or any(token in lowered for token in work_markers))
