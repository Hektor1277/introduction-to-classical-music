from __future__ import annotations

import re
from typing import Any


PLACEHOLDER_VALUES = {
    "",
    "-",
    "—",
    "–",
    "未知",
    "none",
    "null",
    "n/a",
    "na",
}

WORK_TYPE_FIELDS = {
    "orchestral": ["composerName", "workTitle", "primaryPerson", "groupName", "performanceDateText"],
    "concerto": [
        "composerName",
        "workTitle",
        "primaryPerson",
        "secondaryPerson",
        "groupName",
        "performanceDateText",
    ],
    "opera_vocal": [
        "composerName",
        "workTitle",
        "primaryPerson",
        "secondaryPerson",
        "groupName",
        "performanceDateText",
    ],
    "chamber_solo": [
        "composerName",
        "workTitle",
        "primaryPerson",
        "secondaryPerson",
        "performanceDateText",
    ],
}

CHINESE_WORK_PATTERNS = [
    re.compile(r"(第[一二三四五六七八九十百零两\d]+(?:号)?交响曲)"),
    re.compile(r"([A-Ga-g](?:大调|小调)(?:钢琴|小提琴|大提琴)?协奏曲)"),
    re.compile(r"(第[一二三四五六七八九十百零两\d]+(?:号)?(?:钢琴|小提琴|大提琴)?协奏曲)"),
    re.compile(r"(第[一二三四五六七八九十百零两\d]+(?:号)?奏鸣曲)"),
    re.compile(r"(第[一二三四五六七八九十百零两\d]+(?:号)?(?:小提琴|钢琴|大提琴)?奏鸣曲)"),
    re.compile(r"(a小调钢琴协奏曲)"),
    re.compile(r"(A小调钢琴协奏曲)"),
]

LATIN_WORK_PATTERNS = [
    re.compile(r"(Symphony\s+No\.?\s*\d+(?:\s+in\s+[A-G][^|,\n]*)?)", re.I),
    re.compile(r"(Piano\s+Concerto[^|,\n]*)", re.I),
    re.compile(r"(Violin\s+Concerto[^|,\n]*)", re.I),
    re.compile(r"(Cello\s+Concerto[^|,\n]*)", re.I),
    re.compile(r"(Concerto\s+No\.?\s*\d+(?:\s+in\s+[A-G][^|,\n]*)?)", re.I),
    re.compile(r"(Sonata\s+No\.?\s*\d+(?:\s+in\s+[A-G][^|,\n]*)?)", re.I),
]

GROUP_SUFFIXES = (
    "orchestra",
    "philharmonic",
    "quartet",
    "trio",
    "ensemble",
    "choir",
    "chorus",
    "opera",
)
GROUP_ACRONYMS = {"LSO", "BSO", "BPO", "VPO", "CSO", "RCO", "NYP"}


def compact(value: Any) -> str:
    return str(value or "").strip()


def normalize_optional(value: str) -> str:
    normalized = compact(value)
    return "" if normalized.lower() in PLACEHOLDER_VALUES else normalized


def analyze_raw_text(raw_text: str, work_type_hint: str = "unknown") -> dict[str, str]:
    raw = compact(raw_text)
    result = empty_result()
    if not raw:
        return result

    pipe_segments = [normalize_optional(part) for part in re.split(r"\s*\|\s*", raw) if compact(part)]
    if len(pipe_segments) >= 4:
        apply_structured_segments(pipe_segments, result, work_type_hint)
    else:
        apply_free_text_fallback(raw, result, work_type_hint)

    fill_common_patterns(raw, result)
    fill_latin_companions(result)

    if not result["title"]:
        result["title"] = " - ".join(
            value
            for value in [
                result["primaryPerson"] or result["primaryPersonLatin"],
                result["secondaryPerson"] or result["secondaryPersonLatin"],
                result["groupName"] or result["groupNameLatin"],
                result["workTitle"] or result["workTitleLatin"],
                result["performanceDateText"],
            ]
            if compact(value)
        )

    return result


def empty_result() -> dict[str, str]:
    return {
        "title": "",
        "primaryPerson": "",
        "primaryPersonLatin": "",
        "secondaryPerson": "",
        "secondaryPersonLatin": "",
        "groupName": "",
        "groupNameLatin": "",
        "composerName": "",
        "composerNameLatin": "",
        "workTitle": "",
        "workTitleLatin": "",
        "catalogue": "",
        "performanceDateText": "",
    }


def apply_structured_segments(segments: list[str], result: dict[str, str], work_type_hint: str) -> None:
    schema = WORK_TYPE_FIELDS.get(work_type_hint, WORK_TYPE_FIELDS["orchestral"])
    tail_year = segments[-1] if segments and re.fullmatch(r"(19\d{2}|20\d{2})", compact(segments[-1])) else ""
    if tail_year:
        result["performanceDateText"] = tail_year
        segments = segments[:-1]

    for field, value in zip(schema, segments, strict=False):
        if field == "performanceDateText" and result["performanceDateText"]:
            continue
        result[field] = normalize_optional(value)


def apply_free_text_fallback(raw_text: str, result: dict[str, str], work_type_hint: str) -> None:
    lines = [compact(line) for line in raw_text.splitlines() if compact(line)]
    combined = " | ".join(lines) if lines else raw_text

    fill_common_patterns(combined, result)

    if lines:
        composer, work = split_composer_and_work(lines[0])
        if composer and not result["composerName"]:
            result["composerName"] = composer
        if work and not result["workTitle"]:
            result["workTitle"] = work

    parse_people_group_year(combined, result, work_type_hint)


def fill_common_patterns(text: str, result: dict[str, str]) -> None:
    year = extract_year(text)
    if year and not result["performanceDateText"]:
        result["performanceDateText"] = year

    catalogue = extract_catalogue(text)
    if catalogue and not result["catalogue"]:
        result["catalogue"] = catalogue

    work_title = extract_work_title(text)
    if work_title and not result["workTitle"]:
        result["workTitle"] = work_title

    if not result["composerName"] and result["workTitle"]:
        prefix = text.split(result["workTitle"], 1)[0].strip(" |,-:")
        if prefix and contains_cjk(prefix):
            result["composerName"] = prefix

    normalize_work_and_catalogue(result)


def split_composer_and_work(line: str) -> tuple[str, str]:
    work = extract_work_title(line)
    if not work:
        return "", ""
    head = line.split(work, 1)[0].strip(" |,-:")
    return normalize_optional(head), normalize_optional(work)


def parse_people_group_year(text: str, result: dict[str, str], work_type_hint: str) -> None:
    cleaned = compact(text)
    for value in (
        result["composerName"],
        result["composerNameLatin"],
        result["workTitle"],
        result["workTitleLatin"],
        result["catalogue"],
        result["performanceDateText"],
    ):
        if compact(value):
            cleaned = cleaned.replace(compact(value), " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" |,-:")
    if not cleaned:
        return

    group_name = extract_group_name(cleaned)
    if group_name and not result["groupName"]:
        result["groupName"] = group_name
        cleaned = cleaned[: cleaned.rfind(group_name)].strip(" |,-:")

    if not cleaned:
        return

    person_tokens = split_person_tokens(cleaned)
    if not person_tokens:
        return

    if work_type_hint == "concerto":
        assign_dual_people(result, person_tokens)
        return
    if work_type_hint == "opera_vocal":
        assign_dual_people(result, person_tokens)
        return
    if work_type_hint == "chamber_solo":
        assign_dual_people(result, person_tokens)
        return

    if not result["primaryPerson"]:
        result["primaryPerson"] = " ".join(person_tokens)


def extract_group_name(text: str) -> str:
    normalized = compact(text)
    if not normalized:
        return ""

    cn_match = re.search(r"([\u4e00-\u9fff]{2,}(?:乐团|愛樂樂團|爱乐乐团|爱乐|管弦乐团|交响乐团|四重奏|三重奏|合唱团|歌剧院))$", normalized)
    if cn_match:
        return cn_match.group(1)

    if re.search(r"\b(?:[A-Z]{2,6})\b$", normalized):
        tail = normalized.split()[-1]
        if tail.upper() in GROUP_ACRONYMS:
            return tail

    words = normalized.split()
    if not words:
        return ""
    last = words[-1]
    if last in {"Orchestra"}:
        if len(words) >= 3 and words[-2] in {"Philharmonic", "Symphony"}:
            return " ".join(words[-3:])
        if len(words) >= 2:
            return " ".join(words[-2:])
    if last in {"Philharmonic", "Quartet", "Trio", "Ensemble", "Choir", "Chorus", "Opera"}:
        if len(words) >= 2:
            return " ".join(words[-2:])
        return last
    return ""


def split_person_tokens(text: str) -> list[str]:
    normalized = compact(text)
    if not normalized:
        return []
    if contains_cjk(normalized):
        parts = [part for part in re.split(r"[、/&]| and | with |,|，", normalized, flags=re.I) if compact(part)]
        if len(parts) > 1:
            return [compact(part) for part in parts]
    return [token for token in normalized.split() if token]


def assign_dual_people(result: dict[str, str], tokens: list[str]) -> None:
    if not tokens:
        return
    if len(tokens) == 1:
        result["primaryPerson"] = result["primaryPerson"] or tokens[0]
        return
    if len(tokens) == 2:
        result["primaryPerson"] = result["primaryPerson"] or tokens[0]
        result["secondaryPerson"] = result["secondaryPerson"] or tokens[1]
        return
    if len(tokens) == 3:
        result["primaryPerson"] = result["primaryPerson"] or " ".join(tokens[:2])
        result["secondaryPerson"] = result["secondaryPerson"] or tokens[2]
        return
    result["primaryPerson"] = result["primaryPerson"] or " ".join(tokens[:2])
    result["secondaryPerson"] = result["secondaryPerson"] or " ".join(tokens[2:])


def extract_year(text: str) -> str:
    match = re.search(r"(19\d{2}|20\d{2})", text or "")
    return match.group(1) if match else ""


def extract_catalogue(text: str) -> str:
    match = re.search(r"\b(?:op|k|bwv|hob|d|wab)\.?\s*\d+[a-z]?\b", text or "", re.I)
    return match.group(0).replace(" ", "") if match else ""


def extract_work_title(text: str) -> str:
    for pattern in CHINESE_WORK_PATTERNS + LATIN_WORK_PATTERNS:
        match = pattern.search(text or "")
        if match:
            return normalize_optional(match.group(1))
    return ""


def normalize_work_and_catalogue(result: dict[str, str]) -> None:
    work_title = compact(result["workTitle"])
    if not work_title:
        return
    catalogue = extract_catalogue(work_title)
    if catalogue and not result["catalogue"]:
        result["catalogue"] = catalogue
    if catalogue:
        cleaned = re.sub(r"\b(?:op|k|bwv|hob|d|wab)\.?\s*\d+[a-z]?\b", "", work_title, flags=re.I).strip(" |,-:")
        if cleaned:
            result["workTitle"] = cleaned


def fill_latin_companions(result: dict[str, str]) -> None:
    pairs = [
        ("primaryPerson", "primaryPersonLatin"),
        ("secondaryPerson", "secondaryPersonLatin"),
        ("groupName", "groupNameLatin"),
        ("composerName", "composerNameLatin"),
        ("workTitle", "workTitleLatin"),
    ]
    for field, latin_field in pairs:
        if result[field] and looks_latin_text(result[field]) and not result[latin_field]:
            result[latin_field] = result[field]


def looks_latin_text(value: str) -> bool:
    return bool(re.search(r"[A-Za-z]", value or "")) and not contains_cjk(value)


def contains_cjk(value: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", value or ""))
