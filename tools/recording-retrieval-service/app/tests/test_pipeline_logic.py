from __future__ import annotations

import asyncio
import time

from app.models.protocol import CreateJobRequest
from app.services.http_sources import HttpSourceProvider
from app.services.pipeline import (
    build_candidate_work_anchor_terms,
    candidate_conflicting_credit_tokens,
    candidate_mentions_expected_composer,
    candidate_mentions_names,
    candidate_year_conflicts_reference,
    candidate_title_quality_score,
    classify_link_candidate_zone,
    DraftRecordingEntry,
    InputNormalizer,
    LinkCandidate,
    pick_dominant_link_candidate,
    person_variant_matches,
    ProfileResolver,
    RetrievalPipeline,
    SourceRecord,
    build_latin_credit_variants,
    build_latin_work_alias,
    build_queries,
    sort_link_candidates,
)
from tests.fixtures import sample_request


class FakeSourceProvider:
    async def inspect_existing_links(self, draft, profile):
        return [
            {
                "url": "https://archive.example/kleiber-1975",
                "source_label": "Existing Link",
                "source_kind": "existing-link",
                "title": "Beethoven Symphony No. 5 - Kleiber Vienna 1975",
                "description": "Live recording in Vienna 1975. Deutsche Grammophon release 1976.",
                "platform": "archive",
                "weight": 1.0,
                "same_recording_score": 0.95,
                "fields": {
                    "performanceDateText": "1975",
                    "venueText": "Vienna",
                    "label": "Deutsche Grammophon",
                    "releaseDate": "1976",
                    "albumTitle": "Beethoven: Symphony No. 5",
                },
                "images": [
                    {
                        "src": "https://archive.example/images/kleiber-1975.jpg",
                        "title": "Cover",
                        "sourceUrl": "https://archive.example/kleiber-1975",
                        "sourceKind": "existing-link",
                    }
                ],
            }
        ]

    async def search_high_quality(self, draft, profile):
        return [
            {
                "url": "https://catalog.example/kleiber-1975",
                "source_label": "Catalog",
                "source_kind": "high-quality",
                "title": "Beethoven: Symphony No. 5 / Kleiber / Vienna Philharmonic",
                "description": "Recorded live in Vienna, 1975.",
                "platform": "other",
                "weight": 0.9,
                "same_recording_score": 0.92,
                "fields": {
                    "performanceDateText": "1975",
                    "venueText": "Vienna",
                    "label": "Deutsche Grammophon",
                },
            }
        ]

    async def search_streaming(self, draft, profile):
        return [
            {
                "url": "https://stream.example/kleiber-1975",
                "source_label": "Streaming",
                "source_kind": "streaming",
                "title": "Kleiber Beethoven 5 Vienna 1975",
                "description": "Live in Vienna 1975.",
                "platform": "youtube",
                "weight": 0.75,
                "same_recording_score": 0.88,
                "fields": {
                    "albumTitle": "Beethoven: Symphony No. 5",
                },
            }
        ]

    async def search_fallback(self, draft, profile):
        return []


class FakeLlm:
    async def synthesize(self, draft, profile, records):
        return {
            "notes": "已确认属于同一版本，发行信息由已有链接与目录页交叉支持。",
            "warnings": ["封面仍待进一步核对"],
            "summary": "关键字段较完整，但封面仍建议人工复核。",
        }


class WeakSourceProvider:
    async def inspect_existing_links(self, draft, profile):
        return []

    async def search_high_quality(self, draft, profile):
        return [
            {
                "url": "https://blog.example/speculative",
                "source_label": "Speculative Blog",
                "source_kind": "high-quality",
                "title": "Possibly the same recording",
                "description": "Maybe 1974 or 1975.",
                "platform": "other",
                "weight": 0.35,
                "same_recording_score": 0.55,
                "fields": {
                    "venueText": "Vienna",
                },
            }
        ]

    async def search_streaming(self, draft, profile):
        return []

    async def search_fallback(self, draft, profile):
        return []


class SlowSourceProvider:
    async def inspect_existing_links(self, draft, profile):
        await asyncio.sleep(0.4)
        return []

    async def search_high_quality(self, draft, profile):
        await asyncio.sleep(0.4)
        return []

    async def search_streaming(self, draft, profile):
        await asyncio.sleep(0.4)
        return []

    async def search_fallback(self, draft, profile):
        await asyncio.sleep(0.4)
        return []


class ClosableSourceProvider(FakeSourceProvider):
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def test_pipeline_writes_high_confidence_fields_to_result_and_keeps_candidates() -> None:
    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=FakeSourceProvider(), llm_client=FakeLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.status == "succeeded"
    assert result.result.performance_date_text == "1975"
    assert result.result.venue_text == "Vienna"
    assert result.result.label == "Deutsche Grammophon"
    assert result.result.release_date == "1976"
    assert result.result.notes == "已确认属于同一版本，发行信息由已有链接与目录页交叉支持。"
    assert result.result.images
    assert result.result.images[0].src == "https://archive.example/images/kleiber-1975.jpg"
    assert any(candidate.url == "https://stream.example/kleiber-1975" for candidate in result.link_candidates)
    assert any(evidence.field == "label" for evidence in result.evidence)
    assert "封面仍待进一步核对" in result.warnings


def test_pipeline_keeps_low_confidence_field_as_candidate_only() -> None:
    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=WeakSourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.status == "partial"
    assert result.result.venue_text is None
    assert any(candidate.url == "https://blog.example/speculative" for candidate in result.link_candidates)
    assert result.result.links == []
    assert any(evidence.field == "venueText" for evidence in result.evidence)
    assert any("未达到最终采纳阈值" in warning for warning in result.warnings)


def test_pipeline_respects_deadline_and_returns_without_hanging() -> None:
    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=SlowSourceProvider(), llm_client=None)
    deadline = time.monotonic() + 0.2

    result = asyncio.run(pipeline.retrieve(request.items[0], deadline=deadline))

    assert result.status in {"failed", "not_found"}
    assert any("截止时间" in warning or "超时" in warning for warning in result.warnings)


def test_profile_resolver_prioritizes_work_person_group_and_year_queries() -> None:
    payload = sample_request()
    payload["items"][0]["seed"]["workTitleLatin"] = "Symphony No. 1 in C Minor"
    payload["items"][0]["seed"]["catalogue"] = "Op.68"
    request = CreateJobRequest.model_validate(payload)

    profile = ProfileResolver().resolve(request.items[0])

    assert profile.queries
    assert any("Symphony No. 1 in C Minor Op.68" in query for query in profile.queries)
    assert any("Conductor 1" in query for query in profile.queries)
    assert any("Orchestra 1" in query for query in profile.queries)


def test_profile_resolver_adds_people_and_year_fallback_queries_for_non_latin_input() -> None:
    payload = sample_request()
    payload["items"][0]["sourceLine"] = "柴可夫斯基 | 第五交响曲 | Rudolf Kempe | London Symphony Orchestra | 1964"
    payload["items"][0]["seed"]["title"] = "Rudolf Kempe - London Symphony Orchestra - 第五交响曲 - 1964"
    payload["items"][0]["seed"]["composerName"] = "柴可夫斯基"
    payload["items"][0]["seed"]["composerNameLatin"] = ""
    payload["items"][0]["seed"]["workTitle"] = "第五交响曲"
    payload["items"][0]["seed"]["workTitleLatin"] = ""
    payload["items"][0]["seed"]["catalogue"] = ""
    payload["items"][0]["seed"]["performanceDateText"] = "1964"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "conductor", "displayName": "Rudolf Kempe", "label": "Rudolf Kempe"},
        {"role": "orchestra", "displayName": "London Symphony Orchestra", "label": "London Symphony Orchestra"},
    ]
    request = CreateJobRequest.model_validate(payload)

    profile = ProfileResolver().resolve(request.items[0])

    assert "Rudolf Kempe London Symphony Orchestra 1964" in profile.queries
    assert "Rudolf Kempe 1964" in profile.queries


def test_profile_resolver_adds_people_only_query_when_group_missing() -> None:
    payload = sample_request()
    payload["items"][0]["sourceLine"] = "柴可夫斯基 | 第五交响曲 | Albert Coates | - | 1922"
    payload["items"][0]["seed"]["title"] = "Albert Coates - 第五交响曲 - 1922"
    payload["items"][0]["seed"]["composerName"] = "柴可夫斯基"
    payload["items"][0]["seed"]["composerNameLatin"] = ""
    payload["items"][0]["seed"]["workTitle"] = "第五交响曲"
    payload["items"][0]["seed"]["workTitleLatin"] = ""
    payload["items"][0]["seed"]["catalogue"] = ""
    payload["items"][0]["seed"]["performanceDateText"] = "1922"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "conductor", "displayName": "Albert Coates", "label": "Albert Coates"},
    ]
    request = CreateJobRequest.model_validate(payload)

    profile = ProfileResolver().resolve(request.items[0])

    assert "Albert Coates 1922" in profile.queries


def test_build_latin_work_alias_generates_english_alias_from_chinese_work_title() -> None:
    assert build_latin_work_alias("第五交响曲") == "Symphony No. 5"
    assert build_latin_work_alias("第五协奏曲") == "Concerto No. 5"
    assert build_latin_work_alias("第一奏鸣曲") == "Sonata No. 1"


def test_profile_resolver_builds_latin_queries_for_sparse_ui_style_input() -> None:
    payload = sample_request()
    payload["items"][0]["sourceLine"] = "柴可夫斯基 | 第五交响曲 op.64 | monteux | BSO | -"
    payload["items"][0]["seed"]["title"] = "monteux - BSO - 第五交响曲"
    payload["items"][0]["seed"]["composerName"] = "柴可夫斯基"
    payload["items"][0]["seed"]["composerNameLatin"] = ""
    payload["items"][0]["seed"]["workTitle"] = "第五交响曲"
    payload["items"][0]["seed"]["workTitleLatin"] = ""
    payload["items"][0]["seed"]["catalogue"] = "op.64"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "conductor", "displayName": "monteux", "label": "monteux"},
        {"role": "orchestra", "displayName": "BSO", "label": "BSO"},
    ]
    request = CreateJobRequest.model_validate(payload)

    profile = ProfileResolver().resolve(request.items[0])

    assert "Symphony No. 5 op.64 monteux BSO" in profile.latin_queries


def test_profile_resolver_builds_role_aware_concerto_queries() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Schumann | Piano Concerto, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | 1960"
    payload["items"][0]["seed"]["title"] = "Annie Fischer & Kletzki"
    payload["items"][0]["seed"]["composerName"] = "舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1960"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "安妮·费舍尔", "label": "Annie Fischer"},
        {"role": "conductor", "displayName": "克列茨基", "label": "Kletzki"},
        {"role": "orchestra", "displayName": "布达佩斯爱乐乐团", "label": "Budapest Philharmonic Orchestra"},
    ]
    request = CreateJobRequest.model_validate(payload)

    profile = ProfileResolver().resolve(request.items[0])

    assert any("Annie Fischer Kletzki" in query for query in profile.latin_queries)
    assert any("Budapest Philharmonic Orchestra" in query for query in profile.latin_queries)
    assert "Annie Fischer Kletzki" in profile.latin_queries[0]


def test_input_normalizer_assigns_second_chamber_soloist_to_secondary_slot() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Beethoven | Violin Sonata No.5, Op.24 | Jean Fournier | Ginette Doyen | -"
    payload["items"][0]["seed"]["title"] = "Jean Fournier & Ginette Doyen"
    payload["items"][0]["seed"]["composerName"] = "贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "第5号小提琴奏鸣曲“春天”"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Sonata No.5, Op.24"
    payload["items"][0]["seed"]["catalogue"] = "Op.24"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "让·富尼埃", "label": "Jean Fournier"},
        {"role": "soloist", "displayName": "吉内特·多延", "label": "Ginette Doyen"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert draft.primary_names == ["让·富尼埃"]
    assert draft.secondary_names == ["吉内特·多延"]
    assert "Jean Fournier Ginette Doyen" in draft.query_lead_names_latin


def test_input_normalizer_extracts_embedded_english_aliases_from_credit_display_name() -> None:
    payload = sample_request()
    payload["items"][0]["seed"]["credits"] = [
        {
            "role": "soloist",
            "displayName": "Александр Яковлевич Могилевский, EN: Alexander Yakovlevich Mogilevsky",
            "label": "小提琴",
        },
        {
            "role": "orchestra",
            "displayName": "Budapesti Filharmóniai Társaság Zenekara (EN: Budapest Philharmonic Orchestra CHN: 布达佩斯爱乐乐团)",
            "label": "乐团",
        },
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert "Alexander Yakovlevich Mogilevsky" in draft.lead_names_latin
    assert all("EN:" not in value for value in draft.lead_names)
    assert "Budapest Philharmonic Orchestra" in draft.ensemble_names_latin
    assert all("EN:" not in value for value in draft.ensemble_names)


def test_input_normalizer_expands_transliterated_credit_aliases_from_person_alias_table() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "亚历山大·莫吉列夫斯基", "label": "亚历山大·莫吉列夫斯基"},
        {"role": "soloist", "displayName": "列奥尼德·克鲁策", "label": "列奥尼德·克鲁策"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert "Alexander Mogilevsky" in draft.primary_names_latin
    assert "Alexander Yakovlevich Mogilevsky" in draft.primary_names_latin
    assert "Alexandre Moguilewsky" in draft.primary_names_latin


def test_input_normalizer_infers_missing_chamber_collaborator_from_title() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Beethoven | Violin Sonata No.5, Op.24 | Jean Fournier | - | -"
    payload["items"][0]["seed"]["title"] = "Jean Fournier & Ginette Doyen"
    payload["items"][0]["seed"]["composerName"] = "Beethoven"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "Violin Sonata No.5, Op.24"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Sonata No.5, Op.24"
    payload["items"][0]["seed"]["catalogue"] = "Op.24"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jean Fournier", "label": "Jean Fournier"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert "Ginette Doyen" in draft.secondary_names_latin
    assert "Jean Fournier Ginette Doyen" in draft.query_lead_names_latin


def test_input_normalizer_uses_soloist_aliases_for_title_inferred_chamber_collaborator() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Sonata No.5, Op.24 | 亚历山大·莫吉列夫斯基 | -"
    payload["items"][0]["seed"]["title"] = "亚历山大·莫吉列夫斯基 - 列奥尼德·克鲁策"
    payload["items"][0]["seed"]["composerName"] = "贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "第5号小提琴奏鸣曲, “春天”"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Sonata No.5, Op.24"
    payload["items"][0]["seed"]["catalogue"] = ""
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "亚历山大·莫吉列夫斯基", "label": "亚历山大·莫吉列夫斯基"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert "Alexandre Moguilewsky" in draft.primary_names_latin
    assert "Leonid Kreutzer" in draft.secondary_names_latin
    assert "Alexandre Moguilewsky Leonid Kreutzer" in draft.query_lead_names_latin


def test_input_normalizer_uses_person_id_lookup_for_missing_latin_credit_names() -> None:
    class FakePersonNameLookup:
        def resolve(self, person_id: str):
            mapping = {
                "person-solo": {"name": "安妮·费舍尔", "nameLatin": "Annie Fischer"},
                "person-cond": {"name": "保罗·克列茨基", "nameLatin": "Paul Kletzki"},
                "person-orch": {"name": "布达佩斯爱乐乐团", "nameLatin": "Budapest Philharmonic Orchestra"},
            }
            return mapping.get(person_id)

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "舒曼 | a小调钢琴协奏曲 | 安妮·费舍尔 | 保罗·克列茨基 | 布达佩斯爱乐乐团 | -"
    payload["items"][0]["seed"]["title"] = "克列茨基 - 安妮 - 布达佩斯爱乐乐团"
    payload["items"][0]["seed"]["composerName"] = "罗伯特·舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "personId": "person-solo", "displayName": "安妮·费舍尔", "label": ""},
        {"role": "conductor", "personId": "person-cond", "displayName": "保罗·克列茨基", "label": ""},
        {"role": "orchestra", "personId": "person-orch", "displayName": "布达佩斯爱乐乐团", "label": ""},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer(person_name_lookup=FakePersonNameLookup()).normalize(request.items[0])

    assert draft.primary_names_latin[0] == "Annie Fischer"
    assert draft.secondary_names_latin[0] == "Paul Kletzki"
    assert "Budapest Philharmonic Orchestra" in draft.ensemble_names_latin
    assert "Annie Fischer Paul Kletzki" in draft.query_lead_names_latin


def test_input_normalizer_adds_person_lookup_latin_aliases_for_query_generation() -> None:
    class FakePersonNameLookup:
        def resolve(self, person_id: str):
            mapping = {
                "person-solo": {
                    "name": "瓦尔特·吉泽金",
                    "nameLatin": "Walter Gieseking",
                    "aliases": ["吉泽金"],
                },
                "person-cond": {
                    "name": "威尔海姆·富特文格勒",
                    "nameLatin": "Wilhelm Furtwängler",
                    "aliases": ["Wilhelm Furtwangler", "富特文格勒"],
                },
                "person-orch": {
                    "name": "柏林爱乐乐团",
                    "nameLatin": "Berliner Philharmoniker",
                    "aliases": ["Berlin Philharmonic Orchestra", "BPO"],
                },
            }
            return mapping.get(person_id)

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["seed"]["composerName"] = "罗伯特·舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "personId": "person-solo", "displayName": "瓦尔特·吉泽金", "label": ""},
        {"role": "conductor", "personId": "person-cond", "displayName": "威尔海姆·富特文格勒", "label": ""},
        {"role": "orchestra", "personId": "person-orch", "displayName": "柏林爱乐乐团", "label": ""},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer(person_name_lookup=FakePersonNameLookup()).normalize(request.items[0])

    assert "Wilhelm Furtwangler" in draft.secondary_names_latin
    assert "Berlin Philharmonic Orchestra" in draft.ensemble_names_latin
    assert any("Walter Gieseking Wilhelm Furtwangler" in query for query in draft.query_lead_names_latin)


def test_input_normalizer_enriches_title_inferred_secondary_latin_aliases_from_person_alias_loader() -> None:
    class FakePersonAliasLoader:
        def expand(self, value: str, *, role: str | None = None):
            mapping = {
                ("conductor", "富特文格勒"): ["富特文格勒", "Wilhelm Furtwangler"],
            }
            return mapping.get((role, value), [value])

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["seed"]["title"] = "富特文格勒 - 吉泽金 - 柏林爱乐乐团 - March 3, 1942 Berlin"
    payload["items"][0]["seed"]["composerName"] = "舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Walter Gieseking", "label": "Walter Gieseking"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer(person_alias_loader=FakePersonAliasLoader()).normalize(request.items[0])

    assert "富特文格勒" in draft.secondary_names
    assert "Wilhelm Furtwangler" in draft.secondary_names_latin
    assert any("Walter Gieseking Wilhelm Furtwangler" in query for query in draft.query_lead_names_latin)


def test_input_normalizer_enriches_credit_person_latin_aliases_from_person_alias_loader() -> None:
    class FakePersonAliasLoader:
        def expand(self, value: str, *, role: str | None = None):
            mapping = {
                ("soloist", "山根弥生子"): ["山根弥生子", "山根美代子", "Miyoko Yamane"],
            }
            return mapping.get((role, value), [value])

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["seed"]["title"] = "席蒙·戈尔德贝格 - 山根美代子 - June, 1961"
    payload["items"][0]["seed"]["composerName"] = "路德维希·凡·贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "第5号小提琴奏鸣曲, “春天”"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Sonata No.5, Op.24"
    payload["items"][0]["seed"]["catalogue"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "席蒙·戈尔德贝格", "label": "小提琴"},
        {"role": "soloist", "displayName": "山根弥生子", "label": "钢琴"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer(person_alias_loader=FakePersonAliasLoader()).normalize(request.items[0])

    assert "Miyoko Yamane" in draft.secondary_names_latin
    assert any("Miyoko Yamane" in query for query in draft.query_lead_names_latin)


def test_input_normalizer_extracts_english_alias_from_person_lookup_name_latin_payload() -> None:
    class FakePersonNameLookup:
        def resolve(self, person_id: str):
            mapping = {
                "person-solo": {
                    "name": "斯维亚托斯拉夫·特奥菲洛维奇·里赫特",
                    "nameLatin": "Святослав Теофилович Рихтер, EN: Sviatoslav Teofilovich Richter",
                    "aliases": ["里赫特", "斯维亚托斯拉夫·里赫特"],
                },
            }
            return mapping.get(person_id)

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["seed"]["composerName"] = "罗伯特·舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {
            "role": "soloist",
            "personId": "person-solo",
            "displayName": "斯维亚托斯拉夫·特奥菲洛维奇·里赫特",
            "label": "",
        },
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer(person_name_lookup=FakePersonNameLookup()).normalize(request.items[0])

    assert "Sviatoslav Teofilovich Richter" in draft.primary_names_latin
    assert any("Sviatoslav Teofilovich Richter" in query for query in draft.query_lead_names_latin)


def test_person_variant_matches_when_middle_name_is_omitted() -> None:
    assert person_variant_matches("玛丽亚·格林伯格", "玛丽亚·伊斯拉列夫娜·格林伯格")
    assert person_variant_matches("Maria Grinberg", "Maria Israilevna Grinberg")


def test_candidate_conflicting_credit_tokens_ignores_shorter_same_person_variant() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-grinberg-full",
        title="埃利亚斯伯格 - 格林伯格 - 苏联国家交响乐团 - 1958",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1958",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="罗伯特·舒曼 | a小调钢琴协奏曲 | 玛丽亚·伊斯拉列夫娜·格林伯格 | 卡尔·埃利亚斯伯格 | 苏联国家交响乐团 | 1958",
        raw_text="罗伯特·舒曼 | a小调钢琴协奏曲 | 玛丽亚·伊斯拉列夫娜·格林伯格 | 卡尔·埃利亚斯伯格 | 苏联国家交响乐团 | 1958",
        existing_links=[],
        primary_names=["玛丽亚·伊斯拉列夫娜·格林伯格"],
        primary_names_latin=["Maria Grinberg", "Maria Israilevna Grinberg"],
        secondary_names=["卡尔·埃利亚斯伯格"],
        secondary_names_latin=["Carl Eliasberg"],
        query_lead_names=["玛丽亚·伊斯拉列夫娜·格林伯格", "卡尔·埃利亚斯伯格"],
        query_lead_names_latin=["Maria Grinberg Carl Eliasberg", "Maria Grinberg / Carl Eliasberg"],
        lead_names=["玛丽亚·伊斯拉列夫娜·格林伯格", "卡尔·埃利亚斯伯格"],
        lead_names_latin=["Maria Grinberg", "Maria Israilevna Grinberg", "Carl Eliasberg"],
        ensemble_names=["苏联国家交响乐团"],
        ensemble_names_latin=["USSR State Symphony Orchestra"],
    )

    title = "【玛丽亚·格林伯格 | 舒曼钢协】Maria Grinberg plays Schumann Piano Concerto Op. 54"

    assert candidate_conflicting_credit_tokens(draft, title) == set()


def test_candidate_conflicting_credit_tokens_ignores_matching_performer_role_prefix_for_duo_title() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-goldberg-yamane-full",
        title="席蒙·戈尔德贝格 - 山根美代子 - June, 1991",
        composer_name="贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第5号小提琴奏鸣曲“春天”",
        work_title_latin="Violin Sonata No.5, Op.24",
        catalogue="Op.24",
        performance_date_text="June, 1991",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Violin Sonata No.5, Op.24 | Szymon Goldberg | Miyoko Yamane | June, 1991",
        raw_text="Ludwig van Beethoven | Violin Sonata No.5, Op.24 | Szymon Goldberg | Miyoko Yamane | June, 1991",
        existing_links=[],
        primary_names=["席蒙·戈尔德贝格"],
        primary_names_latin=["Szymon Goldberg"],
        secondary_names=["山根美代子"],
        secondary_names_latin=["Miyoko Yamane"],
        query_lead_names=["席蒙·戈尔德贝格", "山根美代子"],
        query_lead_names_latin=["Szymon Goldberg Miyoko Yamane", "Szymon Goldberg / Miyoko Yamane"],
        lead_names=["席蒙·戈尔德贝格", "山根美代子"],
        lead_names_latin=["Szymon Goldberg", "Miyoko Yamane"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )
    title = "贝多芬：第五号小提琴奏鸣曲“春天”/ 演奏者：戈尔德贝格 & 山根美代子（Szymon Goldberg, Miyoko Yamane）"
    candidate = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1tS411P7xf/",
        title=title,
        sourceLabel="Bilibili Search Browser Search",
        confidence=0.97,
    )

    assert candidate_conflicting_credit_tokens(draft, title) == set()
    assert classify_link_candidate_zone(draft, candidate)[0] == "green"


def test_candidate_conflicting_credit_tokens_ignores_composer_work_prefix_for_chamber_solo_track_title() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-egon-petri-conflict-guard",
        title="Egon Petri 1956",
        composer_name="Ludwig van Beethoven",
        composer_name_latin="Ludwig van Beethoven",
        work_title="Piano Sonata No.23, Op.57 Appassionata",
        work_title_latin="Piano Sonata No.23, Op.57 Appassionata",
        catalogue="Op.57",
        performance_date_text="1956",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        raw_text="Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956",
        existing_links=[],
        primary_names=["Egon Petri"],
        primary_names_latin=["Egon Petri"],
        secondary_names=[],
        secondary_names_latin=[],
        query_lead_names=["Egon Petri"],
        query_lead_names_latin=["Egon Petri"],
        lead_names=["Egon Petri"],
        lead_names_latin=["Egon Petri"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )
    title = 'Beethoven: Piano Sonata No. 23 in F Minor, Op. 57 "Appassionata": I. Allegro assai'
    candidate = LinkCandidate(
        platform="youtube",
        url="https://www.youtube.com/watch?v=3oo93lgs15Y",
        title=title,
        sourceLabel="YouTube Search",
        confidence=0.61,
    )

    assert candidate_conflicting_credit_tokens(draft, title) == set()
    assert classify_link_candidate_zone(draft, candidate)[0] != "red"


def test_build_latin_credit_variants_prefers_condensed_alias_for_long_person_name() -> None:
    variants = build_latin_credit_variants("Wilhelm Walter Friedrich Kempff", [])

    assert variants[0] == "Wilhelm Kempff"
    assert "Wilhelm Walter Friedrich Kempff" in variants


def test_input_normalizer_falls_back_to_year_hint_from_item_id_when_date_missing() -> None:
    payload = sample_request()
    payload["items"][0]["itemId"] = "recording-demo-1954-full"
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["seed"]["title"] = "Richter Budapest Academy"
    payload["items"][0]["seed"]["composerName"] = "Robert Schumann"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "Piano Concerto in A minor"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["sourceLine"] = "Robert Schumann | Piano Concerto in A minor | Sviatoslav Richter | Janos Ferencsik | Hungarian National Philharmonic Orchestra | -"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": ""},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": ""},
        {"role": "orchestra", "displayName": "Hungarian National Philharmonic Orchestra", "label": ""},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert draft.performance_date_text == "1954"


def test_input_normalizer_extracts_year_hint_from_production_style_item_id() -> None:
    payload = sample_request()
    payload["items"][0]["itemId"] = "recording-a小调钢琴协奏曲-里赫特-and-费伦奇克1954-full"
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["seed"]["title"] = "费伦奇克 - 里赫特 - 匈牙利国家爱乐乐团 - 布达佩斯音乐学院"
    payload["items"][0]["seed"]["composerName"] = "罗伯特·舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["sourceLine"] = "罗伯特·舒曼 | a小调钢琴协奏曲 | 斯维亚托斯拉夫·特奥菲洛维奇·里赫特 | 费伦奇克·亚诺什 | 匈牙利国家爱乐乐团 | -"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "斯维亚托斯拉夫·特奥菲洛维奇·里赫特", "label": ""},
        {"role": "conductor", "displayName": "费伦奇克·亚诺什", "label": ""},
        {"role": "orchestra", "displayName": "匈牙利国家爱乐乐团", "label": ""},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert draft.performance_date_text == "1954"


def test_input_normalizer_recovers_concerto_collaborator_group_and_date_from_title() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op.61 | Jascha Heifetz | -"
    payload["items"][0]["seed"]["title"] = "Toscanini - Heifetz - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City"
    payload["items"][0]["seed"]["composerName"] = "贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "D大调小提琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op.61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "亚莎·海菲兹", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert "Toscanini" in draft.secondary_names
    assert "NBC Symphony Orchestra" in draft.ensemble_names_latin
    assert draft.performance_date_text == "March 11, 1940"


def test_input_normalizer_keeps_title_performance_context_for_sparse_chamber_solo_title() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Beethoven | Piano Sonata No.23, Op.57 | Claudio Arrau | -"
    payload["items"][0]["seed"]["title"] = "阿劳 - Beethovenfest Bonn 1970"
    payload["items"][0]["seed"]["composerName"] = "贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "第二十三号奏鸣曲，热情"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Sonata No.23, Op.57"
    payload["items"][0]["seed"]["catalogue"] = "Op.57"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Claudio Arrau", "label": "Claudio Arrau"},
    ]
    request = CreateJobRequest.model_validate(payload)

    draft = InputNormalizer().normalize(request.items[0])

    assert draft.performance_date_text == "Beethovenfest Bonn 1970"


def test_build_queries_keeps_composer_work_lead_date_combo_for_sparse_html_search() -> None:
    queries = build_queries(
        work_query="spring",
        composer_query="Ludwig van Beethoven",
        lead_terms=["Jean Fournier"],
        ensemble_terms=[],
        title="Jean Fournier - early '50s",
        performance_date_text="early '50s",
    )

    assert "Ludwig van Beethoven spring Jean Fournier early '50s" in queries


def test_pipeline_promotes_confident_links_and_associated_images_to_result() -> None:
    class LinkOnlySourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/right",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Exact Recording",
                    "description": "Exact same recording",
                    "platform": "youtube",
                    "weight": 0.9,
                    "same_recording_score": 0.92,
                    "fields": {},
                    "images": [
                        {
                            "src": "https://stream.example/right.jpg",
                            "sourceUrl": "https://stream.example/right",
                            "sourceKind": "streaming",
                            "title": "Exact Recording",
                        }
                    ],
                }
            ]

        async def search_fallback(self, draft, profile):
            return []

    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=LinkOnlySourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.status == "partial"
    assert result.result.links
    assert result.result.links[0].url == "https://stream.example/right"
    assert result.result.images
    assert result.result.images[0].src == "https://stream.example/right.jpg"


def test_pipeline_keeps_trusted_input_performance_date_when_search_confirms_recording() -> None:
    class MatchingSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/right",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Exact Recording",
                    "description": "Same performers, no explicit year.",
                    "platform": "youtube",
                    "weight": 0.8,
                    "same_recording_score": 0.9,
                    "fields": {},
                    "images": [],
                }
            ]

        async def search_fallback(self, draft, profile):
            return []

    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=MatchingSourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.result.performance_date_text == "1975"
    assert not any("performanceDateText" in warning for warning in result.warnings)


def test_pipeline_only_promotes_top_tier_links_into_final_result() -> None:
    class MixedConfidenceSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/right",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Exact Recording (1955)",
                    "description": "Glenn Gould Goldberg Variations 1955",
                    "platform": "youtube",
                    "weight": 0.9,
                    "same_recording_score": 0.94,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://stream.example/wrong-year",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Same work, wrong year (1981)",
                    "description": "Glenn Gould Goldberg Variations 1981",
                    "platform": "youtube",
                    "weight": 0.82,
                    "same_recording_score": 0.78,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            return []

    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=MixedConfidenceSourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == ["https://stream.example/right"]
    assert any(candidate.url == "https://stream.example/wrong-year" for candidate in result.link_candidates)


def test_pipeline_promotes_llm_accepted_candidate_into_final_result() -> None:
    class BorderlineSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/borderline",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Kempe / LSO 1964",
                    "description": "Tchaikovsky Symphony No.5 Proms live",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.52,
                    "fields": {},
                    "images": [
                        {
                            "src": "https://stream.example/borderline.jpg",
                            "sourceUrl": "https://stream.example/borderline",
                            "sourceKind": "streaming",
                            "title": "Kempe / LSO 1964",
                        }
                    ],
                }
            ]

        async def search_fallback(self, draft, profile):
            return []

    class AcceptingLlm:
        async def synthesize(self, draft, profile, records):
            return {
                "summary": "LLM confirmed the borderline candidate as the same recording.",
                "acceptedUrls": ["https://stream.example/borderline"],
            }

    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=BorderlineSourceProvider(), llm_client=AcceptingLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.result.links
    assert result.result.links[0].url == "https://stream.example/borderline"
    assert result.result.images


def test_pipeline_promotes_single_exact_low_confidence_platform_candidate() -> None:
    class SingleExactSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=exactlow001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann - Piano Concerto, op.54 Eliso Virsaladze",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1920,
                    "uploader": "Archive",
                    "view_count": 2400,
                    "fields": {},
                    "images": [],
                }
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | "
        "Alexander Rudin | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Eliso Virsaladze"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Eliso Virsaladze", "label": "Eliso Virsaladze"},
        {"role": "conductor", "displayName": "Alexander Rudin", "label": "Alexander Rudin"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SingleExactSourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == ["https://www.youtube.com/watch?v=exactlow001"]


def test_pipeline_promotes_exact_low_confidence_candidate_over_same_score_noise() -> None:
    class SparseVirsaladzeProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=tDxa2aOQ0w0",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann - Piano Concerto, op.54 Eliso Virsaladze",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1920,
                    "uploader": "Archive",
                    "view_count": 2400,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=pzS7WUTh8Sc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Eliso Virsaladze plays Rachmaninov Concerto No.2",
                    "description": "Wrong work upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2010,
                    "uploader": "Archive",
                    "view_count": 5100,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1TJ411a7E8/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Elisso Virsaladze & Moscow Chamber Orchestra",
                    "description": "Wrong repertoire upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2050,
                    "uploader": "Archive",
                    "view_count": 1800,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | "
        "Alexander Rudin | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Eliso Virsaladze"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Eliso Virsaladze", "label": "Eliso Virsaladze"},
        {"role": "conductor", "displayName": "Alexander Rudin", "label": "Alexander Rudin"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseVirsaladzeProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == ["https://www.youtube.com/watch?v=tDxa2aOQ0w0"]


def test_pipeline_keeps_best_low_confidence_candidate_when_sparse_exact_cluster_has_no_clear_gap() -> None:
    class SparseExactClusterProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=tDxa2aOQ0w0",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann - Piano Concerto, op.54 Eliso Virsaladze",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1920,
                    "uploader": "Archive",
                    "view_count": 2400,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=CLElMqoOT6I",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Eliso Virsaladze - Schumann Piano Concerto in A minor",
                    "description": "Alternate upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1935,
                    "uploader": "Collector",
                    "view_count": 1800,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1h7411z71D/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "【钢琴】Eliso Virsaladze演奏 舒曼 钢琴协奏曲Op.54",
                    "description": "Mirror upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1910,
                    "uploader": "Archive",
                    "view_count": 900,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | "
        "Alexander Rudin | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Eliso Virsaladze"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Eliso Virsaladze", "label": "Eliso Virsaladze"},
        {"role": "conductor", "displayName": "Alexander Rudin", "label": "Alexander Rudin"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseExactClusterProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    candidate_urls = [link.url for link in result.link_candidates]
    assert "https://www.youtube.com/watch?v=tDxa2aOQ0w0" in final_urls
    assert "https://www.bilibili.com/video/BV1h7411z71D/" in candidate_urls
    assert "https://www.youtube.com/watch?v=CLElMqoOT6I" not in final_urls


def test_pipeline_uses_description_context_to_promote_generic_title_candidate() -> None:
    class DescriptionAwareProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=descgood001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann Piano Concerto",
                    "description": "Wilhelm Kempff Antal Dorati Concertgebouw Orchestra Amsterdam 1959 live full performance",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1910,
                    "uploader": "Archive",
                    "view_count": 2100,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=descbad001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann Piano Concerto",
                    "description": "Historic concerto upload Clara Haskil 1960 studio recording",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1910,
                    "uploader": "Archive",
                    "view_count": 2100,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | "
        "Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959"
    )
    payload["items"][0]["seed"]["title"] = "Wilhelm Kempff 1959"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1959"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Wilhelm Kempff", "label": "Wilhelm Kempff"},
        {"role": "conductor", "displayName": "Antal Dorati", "label": "Antal Dorati"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=DescriptionAwareProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == ["https://www.youtube.com/watch?v=descgood001"]


def test_pipeline_description_support_does_not_promote_wrong_work_candidate() -> None:
    class DescriptionGuardProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=descgood002",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Historic concerto performance",
                    "description": "Sviatoslav Richter Janos Ferencsik Budapest 1954 Schumann Piano Concerto",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2400,
                    "uploader": "Archive",
                    "view_count": 5200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=descbad002",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Historic concerto performance",
                    "description": "Sviatoslav Richter Budapest 1954 Rachmaninov Concerto No.2",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2400,
                    "uploader": "Archive",
                    "view_count": 5200,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Budapest | 1954"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1954 Budapest"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=DescriptionGuardProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == ["https://www.youtube.com/watch?v=descgood002"]


def test_pipeline_keeps_multiple_equivalent_upload_links_when_llm_confirms_same_version() -> None:
    class MultiUploadSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/upload-a",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Heifetz / Toscanini 1940",
                    "description": "Same recording upload A",
                    "platform": "youtube",
                    "weight": 0.72,
                    "same_recording_score": 0.6,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://stream.example/upload-b",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Heifetz / Toscanini 1940 new edition",
                    "description": "Same recording upload B",
                    "platform": "youtube",
                    "weight": 0.72,
                    "same_recording_score": 0.6,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://stream.example/upload-c",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Heifetz / Toscanini 1940 restored",
                    "description": "Same recording upload C",
                    "platform": "youtube",
                    "weight": 0.72,
                    "same_recording_score": 0.59,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            return []

    class MultiAcceptingLlm:
        async def synthesize(self, draft, profile, records):
            return {
                "summary": "These uploads point to the same historical recording.",
                "acceptedUrls": [
                    "https://stream.example/upload-a",
                    "https://stream.example/upload-b",
                    "https://stream.example/upload-c",
                ],
            }

    request = CreateJobRequest.model_validate(sample_request())
    pipeline = RetrievalPipeline(source_provider=MultiUploadSourceProvider(), llm_client=MultiAcceptingLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [link.url for link in result.result.links] == [
        "https://stream.example/upload-a",
        "https://stream.example/upload-b",
        "https://stream.example/upload-c",
    ]


def test_pipeline_promotes_clean_same_recording_upload_variant_into_final_links() -> None:
    class AmbiguousUploadSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "ベートーヴェン：ヴァイオリン協奏曲 ニ長調 作品61 ハイフェッツ, トスカニーニ 1940",
                    "description": "Jascha Heifetz violin Arturo Toscanini NBC Symphony Orchestra 11 March 1940",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Toscanini NBC Symphony Jascha Heifetz violin 11 Mar 1940",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=IFBQqw_-W5A",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "L. V. Beethoven, Violin Concerto - J. Heifetz (Vn) - A. Toscanini (C) - NBC Symphony Orch (1940)",
                    "description": "Full concerto upload with movements and 1940 date",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": "Canonical upload title without edition tag",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=-rUNkiGgJx8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini NEW EDITION",
                    "description": "Variant upload with edition tag",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | Arturo Toscanini | March 11, 1940"
    payload["items"][0]["seed"]["title"] = "Heifetz / Toscanini 1940"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = "March 11, 1940"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
        {"role": "conductor", "displayName": "Arturo Toscanini", "label": "Arturo Toscanini"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=AmbiguousUploadSourceProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls
    assert "https://www.youtube.com/watch?v=-rUNkiGgJx8" not in final_urls


def test_sort_link_candidates_prefers_standalone_then_collection_then_first_movement() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-larrocha-priority",
        title="萨瓦利施 - 拉罗查 - 瑞士罗曼德管弦乐团 - January 12, 1977",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="January 12, 1977",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Alicia de Larrocha | Wolfgang Sawallisch | Orchestre de la Suisse Romande | January 12, 1977",
        raw_text="",
        existing_links=[],
        primary_names=["阿利西亚·德·拉罗查"],
        primary_names_latin=["Alicia de Larrocha"],
        secondary_names=["沃尔夫冈·萨瓦利施"],
        secondary_names_latin=["Wolfgang Sawallisch"],
        lead_names=["阿利西亚·德·拉罗查", "沃尔夫冈·萨瓦利施"],
        lead_names_latin=["Alicia de Larrocha", "Wolfgang Sawallisch"],
        ensemble_names=["瑞士罗曼德管弦乐团"],
        ensemble_names_latin=["Orchestre de la Suisse Romande"],
    )
    record_map = {
        "https://www.youtube.com/watch?v=standalone": SourceRecord(
            url="https://www.youtube.com/watch?v=standalone",
            source_label="YouTube Search",
            source_kind="streaming",
            title="Schumann Piano Concerto in A minor, Op.54 Alicia de Larrocha Sawallisch complete live",
            description="Full standalone performance 1977",
            platform="youtube",
            weight=0.68,
            same_recording_score=0.78,
            duration_seconds=1880,
            uploader="Official Archive",
            view_count=1000,
        ),
        "https://www.bilibili.com/video/BV1collection?p=12": SourceRecord(
            url="https://www.bilibili.com/video/BV1collection?p=12",
            source_label="Bilibili Search",
            source_kind="streaming",
            title="Alicia de Larrocha live 1977 Brahms and Schumann concertos",
            description="Collection upload including Schumann Piano Concerto Op.54 with Sawallisch",
            platform="bilibili",
            weight=0.68,
            same_recording_score=0.78,
            duration_seconds=5400,
            uploader="Archive Channel",
            view_count=1000,
        ),
        "https://www.youtube.com/watch?v=movement1": SourceRecord(
            url="https://www.youtube.com/watch?v=movement1",
            source_label="YouTube Search",
            source_kind="streaming",
            title="Schumann Piano Concerto in A minor, Op.54 I. Allegro affettuoso Alicia de Larrocha",
            description="Single movement upload from the same concert",
            platform="youtube",
            weight=0.68,
            same_recording_score=0.78,
            duration_seconds=620,
            uploader="Archive Channel",
            view_count=1000,
        ),
    }
    candidates = [
        LinkCandidate(
            platform=record.platform,
            url=record.url,
            title=record.title,
            sourceLabel=record.source_label,
            confidence=round(record.same_recording_score, 2),
        )
        for record in record_map.values()
    ]

    ordered = sort_link_candidates(draft, candidates, record_map)

    assert [candidate.url for candidate in ordered] == [
        "https://www.youtube.com/watch?v=standalone",
        "https://www.bilibili.com/video/BV1collection?p=12",
        "https://www.youtube.com/watch?v=movement1",
    ]


def test_pipeline_skips_llm_synthesis_for_unambiguous_top_candidate() -> None:
    class UnambiguousSourceProvider:
        async def inspect_existing_links(self, draft, profile):
            return []

        async def search_high_quality(self, draft, profile):
            return []

        async def search_streaming(self, draft, profile):
            return [
                {
                    "url": "https://stream.example/exact",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Exact Recording",
                    "description": "Exact same recording with matching performers and year.",
                    "platform": "youtube",
                    "weight": 0.95,
                    "same_recording_score": 0.93,
                    "fields": {"albumTitle": "Exact Recording"},
                    "images": [],
                },
                {
                    "url": "https://stream.example/distant-second",
                    "source_label": "Streaming",
                    "source_kind": "streaming",
                    "title": "Similar but lower confidence",
                    "description": "Same work, weaker evidence.",
                    "platform": "youtube",
                    "weight": 0.6,
                    "same_recording_score": 0.33,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            return []

    class CountingLlm:
        def __init__(self) -> None:
            self.calls = 0

        async def synthesize(self, draft, profile, records):
            self.calls += 1
            return {"summary": "should not be called"}

    request = CreateJobRequest.model_validate(sample_request())
    llm = CountingLlm()
    pipeline = RetrievalPipeline(source_provider=UnambiguousSourceProvider(), llm_client=llm)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.result.links
    assert result.result.links[0].url == "https://stream.example/exact"
    assert llm.calls == 0


def test_sort_link_candidates_prefers_exact_collaboration_credit_over_compilation_packaging() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-kempff-dorati-ordering",
        title="Wilhelm Kempff & Antal Dorati",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="1959",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | Concertgebouw Orchestra Amsterdam | 1959",
        existing_links=[],
        primary_names=["肯普夫"],
        primary_names_latin=["Wilhelm Kempff"],
        secondary_names=["多拉蒂"],
        secondary_names_latin=["Antal Dorati"],
        lead_names=["肯普夫", "多拉蒂"],
        lead_names_latin=["Wilhelm Kempff", "Antal Dorati"],
        ensemble_names=["阿姆斯特丹皇家音乐厅管弦乐团"],
        ensemble_names_latin=["Concertgebouw Orchestra Amsterdam", "Royal Concertgebouw Orchestra"],
    )
    record_map = {
        "https://www.bilibili.com/video/BV1exact/": SourceRecord(
            url="https://www.bilibili.com/video/BV1exact/",
            source_label="Bilibili Search Browser Search",
            source_kind="streaming",
            title="Wilhelm Kempff Antal Dorati Schumann Piano Concerto Op.54 1959 complete",
            description="Concertgebouw Orchestra Amsterdam 1959 live full performance",
            platform="bilibili",
            weight=0.68,
            same_recording_score=0.79,
            duration_seconds=1880,
            uploader="Classical Vault",
            view_count=2200,
        ),
        "https://www.bilibili.com/video/BV1compilation/": SourceRecord(
            url="https://www.bilibili.com/video/BV1compilation/",
            source_label="Bilibili Search Browser Search",
            source_kind="streaming",
            title="Wilhelm Kempff plays Schumann Piano Concertos 1950s",
            description="Compilation upload with multiple concerto performances",
            platform="bilibili",
            weight=0.68,
            same_recording_score=0.79,
            duration_seconds=5400,
            uploader="Archive Channel",
            view_count=2200,
        ),
    }
    candidates = [
        LinkCandidate(
            platform=record.platform,
            url=record.url,
            title=record.title,
            sourceLabel=record.source_label,
            confidence=round(record.same_recording_score, 2),
        )
        for record in record_map.values()
    ]

    ordered = sort_link_candidates(draft, candidates, record_map, prefer_exactness=True)

    assert [candidate.url for candidate in ordered] == [
        "https://www.bilibili.com/video/BV1exact/",
        "https://www.bilibili.com/video/BV1compilation/",
    ]


def test_candidate_mentions_names_does_not_mistake_clara_for_lara() -> None:
    assert candidate_mentions_names("clara schumann piano concerto in a minor", ["Adelina de Lara"]) is False
    assert candidate_mentions_names("adelina de lara 舒曼钢协", ["Adelina de Lara"]) is True


def test_candidate_work_anchor_terms_include_generic_piano_concerto() -> None:
    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | "
        "BBC Scottish Symphony Orchestra | May 29, 1951"
    )
    payload["items"][0]["seed"]["title"] = "Adelina de Lara & Ian Whyte"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Adelina de Lara", "label": "Adelina de Lara"},
        {"role": "conductor", "displayName": "Ian Whyte", "label": "Ian Whyte"},
    ]
    request = CreateJobRequest.model_validate(payload)
    draft = InputNormalizer().normalize(request.items[0])

    assert "piano concerto" in build_candidate_work_anchor_terms(draft)


def test_candidate_title_quality_score_recognizes_delara_cjk_work_shorthand() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-delara-title-quality",
        title="Adelina de Lara & Ian Whyte",
        composer_name="舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="Op.54",
        performance_date_text="May 29, 1951",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | BBC Scottish Symphony Orchestra | May 29, 1951",
        existing_links=[],
        primary_names=["阿德利纳·德·劳拉"],
        primary_names_latin=["Adelina de Lara"],
        secondary_names=["伊恩·怀特"],
        secondary_names_latin=["Ian Whyte"],
        lead_names=["阿德利纳·德·劳拉", "伊恩·怀特"],
        lead_names_latin=["Adelina de Lara", "Ian Whyte"],
        ensemble_names=["英国广播公司苏格兰交响乐团"],
        ensemble_names_latin=["BBC Scottish Symphony Orchestra"],
    )

    actual_title = "【Adelina de Lara】克拉拉的爱徒会如何演奏舒曼钢协？"
    wrong_title = "Clara Schumann Piano Concerto in A minor"

    assert candidate_title_quality_score(draft, actual_title) > candidate_title_quality_score(draft, wrong_title)


def test_candidate_title_quality_score_rewards_expected_composer_for_sparse_virsaladze_titles() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-virsaladze-title-quality",
        title="亚历山大·鲁丁 - 维尔萨拉泽 - 莫斯科音乐学院大音乐厅",
        composer_name="罗伯特·舒曼",
        composer_name_latin="Robert Schumann",
        work_title="a小调钢琴协奏曲",
        work_title_latin="Piano Concerto, Op.54",
        catalogue="",
        performance_date_text="",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="罗伯特·舒曼 | a小调钢琴协奏曲 | 埃莉索·维尔萨拉泽 | 亚历山大·鲁丁 | -",
        raw_text="罗伯特·舒曼 | a小调钢琴协奏曲 | 埃莉索·维尔萨拉泽 | 亚历山大·鲁丁 | -",
        existing_links=[],
        primary_names=["埃莉索·维尔萨拉泽"],
        primary_names_latin=[],
        secondary_names=["亚历山大·鲁丁"],
        secondary_names_latin=[],
        lead_names=["埃莉索·维尔萨拉泽", "亚历山大·鲁丁"],
        lead_names_latin=[],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    correct_title = "Schumann - Piano Concerto, op.54 Eliso Virsaladze"
    wrong_title = "Eliso Virsaladze plays Rachmaninov Concerto No.2"

    assert candidate_title_quality_score(draft, correct_title) >= 0.08
    assert candidate_title_quality_score(draft, correct_title) > candidate_title_quality_score(draft, wrong_title)


def test_candidate_mentions_expected_composer_accepts_cjk_surname_fragment() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-sonoda-composer-fragment",
        title="園田高弘 - 2023.10.31",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="",
        performance_date_text="2023.10.31",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        raw_text="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        existing_links=[],
        primary_names=["園田高弘"],
        primary_names_latin=["Takahiro Sonoda"],
        secondary_names=[],
        secondary_names_latin=[],
        lead_names=["園田高弘"],
        lead_names_latin=["Takahiro Sonoda"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    assert candidate_mentions_expected_composer(
        draft,
        "贝多芬-第二十三钢琴奏鸣曲-热情op.57（園田高弘）".lower(),
    )


def test_candidate_title_quality_score_rewards_actual_appassionata_cjk_title() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-sonoda-title-quality",
        title="園田高弘 - 2023.10.31",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="",
        performance_date_text="2023.10.31",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        raw_text="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        existing_links=[],
        primary_names=["園田高弘"],
        primary_names_latin=["Takahiro Sonoda"],
        secondary_names=[],
        secondary_names_latin=[],
        lead_names=["園田高弘"],
        lead_names_latin=["Takahiro Sonoda"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )

    actual_title = "贝多芬-第二十三钢琴奏鸣曲-热情op.57（園田高弘）"
    wrong_title = '贝多芬热情奏鸣曲 Performed by Emil Gilels'

    assert candidate_title_quality_score(draft, actual_title) >= 0.08
    assert candidate_title_quality_score(draft, actual_title) > candidate_title_quality_score(draft, wrong_title)


def test_pick_dominant_link_candidate_prefers_actual_appassionata_cjk_candidate() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-sonoda-dominant",
        title="園田高弘 - 2023.10.31",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="",
        performance_date_text="2023.10.31",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        raw_text="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        existing_links=[],
        primary_names=["園田高弘"],
        primary_names_latin=["Takahiro Sonoda"],
        secondary_names=[],
        secondary_names_latin=[],
        lead_names=["園田高弘"],
        lead_names_latin=["Takahiro Sonoda"],
        ensemble_names=[],
        ensemble_names_latin=[],
    )
    target = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1ouNbzqEQt/",
        title="贝多芬-第二十三钢琴奏鸣曲-热情op.57（園田高弘）",
        source_label="Bilibili Search",
        confidence=0.51,
    )
    runner_up = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1Rt411J7nt/",
        title="Piano Sonata No 23 in F flat minor Appassionata, Op.57 Ludwig van Beethoven-e",
        source_label="Bilibili Search",
        confidence=0.45,
    )
    record_map = {
        target.url: SourceRecord(
            url=target.url,
            source_label="Bilibili Search",
            source_kind="streaming",
            title=target.title,
            description=(
                "Piano Sonata No. 23 in f minor, op.57 Composer：Ludwig van Beethoven "
                "Piano/Editor：Takahiro Sonoda 園田高弘 2003年10月31日75岁现场"
            ),
            platform="bilibili",
            weight=0.68,
            same_recording_score=0.51,
            duration_seconds=1447,
            uploader="挽晚风儿儿儿",
            view_count=2497,
            fields={},
            images=[],
        ),
        runner_up.url: SourceRecord(
            url=runner_up.url,
            source_label="Bilibili Search",
            source_kind="streaming",
            title=runner_up.title,
            description="youtube eoIkpHCDlkg",
            platform="bilibili",
            weight=0.68,
            same_recording_score=0.45,
            duration_seconds=1648,
            uploader="muzikair",
            view_count=345,
            fields={},
            images=[],
        ),
    }

    dominant = pick_dominant_link_candidate(draft, [target, runner_up], record_map)

    assert dominant is not None
    assert dominant.url == target.url


def test_candidate_year_conflicts_reference_tolerates_chamber_single_lead_same_month_day_typo() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-sonoda-year-typo",
        title="園田高弘 - 2023.10.31",
        composer_name="路德维希·凡·贝多芬",
        composer_name_latin="Ludwig van Beethoven",
        work_title="第二十三号奏鸣曲，热情",
        work_title_latin="Piano Sonata No.23, Op.57",
        catalogue="Op.57",
        performance_date_text="2023.10.31",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        raw_text="路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31",
        existing_links=[],
        primary_names=["園田高弘"],
        primary_names_latin=["Takahiro Sonoda"],
        secondary_names=[],
        secondary_names_latin=[],
        lead_names=["園田高弘"],
        lead_names_latin=["Takahiro Sonoda"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="chamber_solo",
    )
    candidate = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1ouNbzqEQt/",
        title="贝多芬-第二十三钢琴奏鸣曲-热情op.57（園田高弘）",
        sourceLabel="Bilibili Search",
        confidence=0.51,
    )
    record = SourceRecord(
        url=candidate.url,
        source_label="Bilibili Search",
        source_kind="streaming",
        title=candidate.title,
        description=(
            "Piano Sonata No. 23 in f minor, op.57 Composer：Ludwig van Beethoven "
            "路德维希·凡·贝多芬 Piano/Editor：Takahiro Sonoda 園田高弘 2003年10月31日75岁现场"
        ),
        platform="bilibili",
        weight=0.68,
        same_recording_score=0.51,
        duration_seconds=1447,
        uploader="挽晚风儿儿儿",
        view_count=2497,
        fields={},
        images=[],
    )

    assert candidate_year_conflicts_reference(draft, candidate, record) is False


def test_candidate_year_conflicts_reference_keeps_concerto_year_conflict_strict() -> None:
    draft = DraftRecordingEntry(
        item_id="recording-cortot-year-conflict",
        title="Alfred Cortot 1951",
        composer_name="Robert Schumann",
        composer_name_latin="Robert Schumann",
        work_title="Piano Concerto in A minor, Op.54",
        work_title_latin="Piano Concerto in A minor, Op.54",
        catalogue="Op.54",
        performance_date_text="1951.10.31",
        venue_text="",
        album_title="",
        label="",
        release_date="",
        notes="",
        source_line="Robert Schumann | Piano Concerto in A minor, Op.54 | Alfred Cortot | 1951.10.31",
        raw_text="Robert Schumann | Piano Concerto in A minor, Op.54 | Alfred Cortot | 1951.10.31",
        existing_links=[],
        primary_names=["Alfred Cortot"],
        primary_names_latin=["Alfred Cortot"],
        secondary_names=[],
        secondary_names_latin=[],
        lead_names=["Alfred Cortot"],
        lead_names_latin=["Alfred Cortot"],
        ensemble_names=[],
        ensemble_names_latin=[],
        work_type_hint="concerto",
    )
    candidate = LinkCandidate(
        platform="bilibili",
        url="https://www.bilibili.com/video/BV1fakeconcerto/",
        title="Schumann Piano Concerto Alfred Cortot",
        sourceLabel="Bilibili Search",
        confidence=0.51,
    )
    record = SourceRecord(
        url=candidate.url,
        source_label="Bilibili Search",
        source_kind="streaming",
        title=candidate.title,
        description="Schumann Piano Concerto Alfred Cortot 1931年10月31日 live",
        platform="bilibili",
        weight=0.68,
        same_recording_score=0.51,
        duration_seconds=1880,
        uploader="Archive",
        view_count=2497,
        fields={},
        images=[],
    )

    assert candidate_year_conflicts_reference(draft, candidate, record) is True


def test_pipeline_rescues_sonoda_appassionata_candidate_despite_same_month_day_year_typo() -> None:
    class SonodaProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1ouNbzqEQt/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "贝多芬-第二十三钢琴奏鸣曲-热情op.57（園田高弘）",
                    "description": (
                        "Piano Sonata No. 23 in f minor, op.57 Composer：Ludwig van Beethoven "
                        "路德维希·凡·贝多芬 Piano/Editor：Takahiro Sonoda 園田高弘 2003年10月31日75岁现场"
                    ),
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.51,
                    "duration_seconds": 1447,
                    "uploader": "挽晚风儿儿儿",
                    "view_count": 2497,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1PJ411B7xk/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": '贝多芬热情奏鸣曲Piano Sonata No. 23 "Appassionata", Op. 57 Performed by Emil Gilels',
                    "description": "https://www.youtube.com/watch?v=QImFm4Y_QPM Ludwig van Beethoven Piano Sonata No. 23 Appassionata",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 1528,
                    "uploader": "Ruijerd39",
                    "view_count": 3900,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1oZ421u7M6/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "贝多芬Op.57《f小调第23号钢琴奏鸣曲-热情》园田高弘 DENON全集版 Beethoven Piano Sonata No.23",
                    "description": "",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1460,
                    "uploader": "贝多芬圣殿",
                    "view_count": 22,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class EmptyAcceptedUrlsLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {"summary": "", "notes": "", "warnings": [], "acceptedUrls": []}

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "路德维希·凡·贝多芬 | 第二十三号奏鸣曲，热情 | 園田高弘 | 2023.10.31"
    payload["items"][0]["seed"]["title"] = "園田高弘 - 2023.10.31"
    payload["items"][0]["seed"]["composerName"] = "路德维希·凡·贝多芬"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitle"] = "第二十三号奏鸣曲，热情"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Sonata No.23, Op.57"
    payload["items"][0]["seed"]["catalogue"] = "Op.57"
    payload["items"][0]["seed"]["performanceDateText"] = "2023.10.31"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "園田高弘", "label": "钢琴"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SonodaProvider(), llm_client=EmptyAcceptedUrlsLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.bilibili.com/video/BV1ouNbzqEQt/" in final_urls
    assert "https://www.bilibili.com/video/BV1PJ411B7xk/" not in final_urls


def test_pipeline_isolates_access_events_between_concurrent_retrievals() -> None:
    class IsolatedAccessProvider(HttpSourceProvider):
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del profile
            self._record_access_event(
                url=f"https://catalog.example/{draft.item_id}",
                operation="test-search",
                ok=True,
                duration_ms=1.0,
                source_kind="high-quality",
                source_label=draft.item_id,
                query=draft.item_id,
            )
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return []

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    async def run_test() -> tuple[list[dict], list[dict]]:
        payload = sample_request(item_count=2)
        payload["items"][0]["itemId"] = "item-a"
        payload["items"][0]["seed"]["title"] = "Title A"
        payload["items"][1]["itemId"] = "item-b"
        payload["items"][1]["seed"]["title"] = "Title B"
        request = CreateJobRequest.model_validate(payload)
        pipeline = RetrievalPipeline(source_provider=IsolatedAccessProvider(), llm_client=None)
        ready = asyncio.Event()
        completed = 0

        async def run_item(item):
            nonlocal completed
            await pipeline.retrieve(item)
            completed += 1
            if completed == 2:
                ready.set()
            await ready.wait()
            return pipeline.consume_access_events()

        return await asyncio.gather(*(run_item(item) for item in request.items))

    first_events, second_events = asyncio.run(run_test())

    assert [event["query"] for event in first_events] == ["item-a"]
    assert [event["query"] for event in second_events] == ["item-b"]


def test_pipeline_prefers_canonical_exact_upload_for_sparse_heifetz_query() -> None:
    class SparseHeifetzProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven Violin Concerto - Heifetz",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 0,
                    "uploader": "",
                    "view_count": 180,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=IFBQqw_-W5A",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "L. V. Beethoven, Violin Concerto, Op. 61 - J. Heifetz - NBC Symphony Orch. (1940)",
                    "description": "Complete concerto upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.9,
                    "duration_seconds": 2610,
                    "uploader": "Historic Vault",
                    "view_count": 14500,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto, Op.61 - Heifetz / Toscanini",
                    "description": "Canonical upload title",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "duration_seconds": 2664,
                    "uploader": "Classical Archive",
                    "view_count": 32200,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "Heifetz"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseHeifetzProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert final_urls[0] == "https://www.youtube.com/watch?v=9YWr1UcbZE8"
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls
    assert "https://www.youtube.com/watch?v=XazjX-k2aco" not in final_urls


def test_pipeline_keeps_wide_heifetz_upload_cluster_when_exact_titles_remain_close() -> None:
    class WideHeifetzProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2307,
                    "uploader": "Collector A",
                    "view_count": 1291,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "ベートーヴェン：ヴァイオリン協奏曲 ニ長調 作品61 ハイフェッツ, トスカニーニ 1940",
                    "description": "Japanese upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2326,
                    "uploader": "Collector B",
                    "view_count": 1467,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": "Canonical upload title",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.73,
                    "duration_seconds": 2315,
                    "uploader": "Classical Archive",
                    "view_count": 9645,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=-rUNkiGgJx8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini NEW EDITION",
                    "description": "Remaster upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.73,
                    "duration_seconds": 2316,
                    "uploader": "Archive C",
                    "view_count": 357,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "Heifetz 1940"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=WideHeifetzProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls
    assert "https://www.youtube.com/watch?v=-rUNkiGgJx8" not in final_urls


def test_pipeline_keeps_sparse_heifetz_canonical_alternate_upload_when_cluster_stays_ambiguous() -> None:
    class SparseHeifetzAltProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1vp421U7kw/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Beethoven Violin Concerto in D Major Heifetz Toscanini NBC 1940",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2312,
                    "uploader": "Uploader A",
                    "view_count": 2000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1QF411s79n/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Heifetz Toscanini Beethoven Violin Concerto Op.61",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2316,
                    "uploader": "Uploader B",
                    "view_count": 1800,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven Violin Concerto Heifetz Toscanini 1940",
                    "description": "Japanese exact upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2326,
                    "uploader": "Uploader C",
                    "view_count": 2200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.84,
                    "duration_seconds": 2307,
                    "uploader": "Collector A",
                    "view_count": 1291,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": (
                        "Ludwig van Beethoven Violin Concerto in D, Op. 61 "
                        "1. Allegro ma non troppo 2. Larghetto 3. Rondo "
                        "Jascha Heifetz violin Arturo Toscanini conductor"
                    ),
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.67,
                    "duration_seconds": 2315,
                    "uploader": "Private Reserve",
                    "view_count": 1600,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "托斯卡尼尼 - 海菲兹 - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseHeifetzAltProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls


def test_pipeline_keeps_cross_platform_exact_heifetz_links_when_llm_is_missing() -> None:
    class CrossPlatformHeifetzProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1vp421U7kw/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "贝多芬op.61《D大调小提琴协奏曲》海菲兹+托斯卡尼尼1940+NBC交响乐团 Beethoven Violin Concerto in D Major",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.96,
                    "duration_seconds": 2308,
                    "uploader": "Uploader A",
                    "view_count": 84,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.78,
                    "duration_seconds": 2307,
                    "uploader": "Collector A",
                    "view_count": 1292,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": "Canonical upload title",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.71,
                    "duration_seconds": 2315,
                    "uploader": "Classical Archive",
                    "view_count": 9696,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Fm4y1G7NA/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "[骑熊净谱对照]贝多芬D大调小提琴协奏曲｜Jascha Heifetz演奏",
                    "description": "Score-following upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2088,
                    "uploader": "Uploader B",
                    "view_count": 1622,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=_N15_3_TP7I",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Jascha Heifetz \"Violin Concerto\" Beethoven",
                    "description": "Generic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.59,
                    "duration_seconds": 2343,
                    "uploader": "Uploader C",
                    "view_count": 205590,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=-rUNkiGgJx8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini NEW EDITION",
                    "description": "Remaster upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.59,
                    "duration_seconds": 2316,
                    "uploader": "Archive C",
                    "view_count": 361,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "Toscanini - Heifetz - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=CrossPlatformHeifetzProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=8Aclk_O4bSc" in final_urls
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls
    assert "https://www.youtube.com/watch?v=_N15_3_TP7I" not in final_urls
    assert "https://www.youtube.com/watch?v=-rUNkiGgJx8" not in final_urls


def test_pipeline_adds_missing_youtube_link_when_cross_platform_candidate_is_more_specific() -> None:
    class RichterProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1HP411m7JC/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Sviatoslav Richter/里赫特在匈牙利（1954.3.8）：舒曼钢协/勃拉姆斯间奏曲",
                    "description": "Historic Bilibili upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "duration_seconds": 2410,
                    "uploader": "Uploader A",
                    "view_count": 1300,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=OoHkA74RPLU",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Sviatoslav Richter in Budapest, 1954 - Schumann Piano Concerto",
                    "description": "Cross-platform upload with exact year and city",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.55,
                    "duration_seconds": 2402,
                    "uploader": "Archive YT",
                    "view_count": 5200,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class SingleBilibiliLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": ["https://www.bilibili.com/video/BV1HP411m7JC/"],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Hungarian State Philharmonic Orchestra | March 8, 1954 Budapest"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "March 8, 1954 Budapest"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=RichterProvider(), llm_client=SingleBilibiliLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.bilibili.com/video/BV1HP411m7JC/" in final_urls
    assert "https://www.youtube.com/watch?v=OoHkA74RPLU" in final_urls


def test_pipeline_does_not_add_wrong_youtube_platform_completion_when_specificity_is_not_better() -> None:
    class AnnieProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1TE411f7uh/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "【安妮·费舍尔】舒曼钢协现场视频 Annie Fischer plays Schumann Piano Concerto Op. 54",
                    "description": "Bilibili target upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.65,
                    "duration_seconds": 1905,
                    "uploader": "Uploader A",
                    "view_count": 4000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=R4YZRoHbrCw",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann, Piano Concerto in A Minor, Op.54 / Fischer & Giulini",
                    "description": "Wrong conductor upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 1910,
                    "uploader": "Uploader YT",
                    "view_count": 6200,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class SingleBilibiliLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": ["https://www.bilibili.com/video/BV1TE411f7uh/"],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | "
        "Paul Kletzki | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Annie Fischer"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Annie Fischer", "label": "Annie Fischer"},
        {"role": "conductor", "displayName": "Paul Kletzki", "label": "Paul Kletzki"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=AnnieProvider(), llm_client=SingleBilibiliLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert final_urls == ["https://www.bilibili.com/video/BV1TE411f7uh/"]


def test_pipeline_adds_missing_apple_music_link_when_cross_platform_candidate_is_more_specific() -> None:
    class RichterAppleProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1HP411m7JC/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "里赫特 1954 舒曼钢协",
                    "description": "Sviatoslav Richter 1954",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.93,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://music.apple.com/us/album/schumann-piano-concerto/123456789?i=987654321",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Piano Concerto in A Minor, Op. 54",
                    "description": "Sviatoslav Richter | Hungarian State Orchestra | Janos Ferencsik | 1954",
                    "platform": "apple_music",
                    "weight": 0.68,
                    "same_recording_score": 0.89,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class SingleBilibiliLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": ["https://www.bilibili.com/video/BV1HP411m7JC/"],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Hungarian State Orchestra | 1954"
    )
    payload["items"][0]["seed"]["title"] = "Richter 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1954"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=RichterAppleProvider(), llm_client=SingleBilibiliLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.bilibili.com/video/BV1HP411m7JC/" in final_urls
    assert "https://music.apple.com/us/album/schumann-piano-concerto/123456789?i=987654321" in final_urls


def test_pipeline_keeps_high_evidence_apple_track_in_final_links_as_independent_primary_platform() -> None:
    class KleiberAppleProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=lsLNUwLLNq8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "L. van Beethoven: Symphony No. 7 / Carlos Kleiber (Vienna, 1976)",
                    "description": "Carlos Kleiber Vienna Philharmonic 1976",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2312,
                    "uploader": "Archive A",
                    "view_count": 34000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1PG4y1L7ir/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "贝多芬 第七交响曲 Op.92 卡洛斯 克莱伯 维也纳爱乐乐团 1976",
                    "description": "Carlos Kleiber Vienna Philharmonic 1976",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2310,
                    "uploader": "Uploader Bili",
                    "view_count": 5400,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-i-poco-sostenuto-vivace/1644892939?i=1644892962",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Symphony No. 7 in A Major, Op. 92: I. Poco sostenuto - Vivace",
                    "description": (
                        "Vienna Philharmonic & Carlos Kleiber | Beethoven: Symphonies Nos. 5 & 7 | "
                        "Classical | 1995-02-20T12:00:00Z"
                    ),
                    "platform": "apple_music",
                    "weight": 0.68,
                    "same_recording_score": 0.56,
                    "duration_seconds": 814,
                    "uploader": "Vienna Philharmonic & Carlos Kleiber",
                    "view_count": 1200,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "orchestral"
    payload["items"][0]["sourceLine"] = (
        "Ludwig van Beethoven | Symphony No.7 in A major,Op.92 | Carlos Kleiber | "
        "Vienna Philharmonic | 1976"
    )
    payload["items"][0]["seed"]["title"] = "Carlos Kleiber 1976"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Symphony No.7 in A major,Op.92"
    payload["items"][0]["seed"]["catalogue"] = "Op.92"
    payload["items"][0]["seed"]["performanceDateText"] = "1976"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "conductor", "displayName": "Carlos Kleiber", "label": "Carlos Kleiber"},
        {"role": "orchestra", "displayName": "Vienna Philharmonic", "label": "Vienna Philharmonic"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=KleiberAppleProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=lsLNUwLLNq8" in final_urls
    assert "https://www.bilibili.com/video/BV1PG4y1L7ir/" in final_urls
    assert (
        "https://music.apple.com/us/album/symphony-no-7-in-a-major-op-92-i-poco-sostenuto-vivace/1644892939?i=1644892962"
        in final_urls
    )


def test_pipeline_adds_version_rescue_candidate_when_low_confidence_accepted_apple_track_would_hide_strict_version_hit() -> None:
    class VirsaladzeAppleProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54-i-allegro-affettuoso-live/1563042200?i=1563042205",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Piano Concerto in A Minor, Op. 54: I. Allegro affettuoso (Live)",
                    "description": "Eliso Virsaladze | Schumann | Live | 2019-01-01T00:00:00Z",
                    "platform": "apple_music",
                    "weight": 0.68,
                    "same_recording_score": 0.56,
                    "duration_seconds": 871,
                    "uploader": "Eliso Virsaladze",
                    "view_count": 0,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=tDxa2aOQ0w0",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann - Piano Concerto, op.54 Eliso Virsaladze",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1920,
                    "uploader": "Archive",
                    "view_count": 2400,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV18Sc6eREgV/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "【钢琴】Eliso Virsaladze演奏 舒曼 钢琴协奏曲Op.54",
                    "description": "Mirror upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1910,
                    "uploader": "Archive",
                    "view_count": 900,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class AcceptedAppleOnlyLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": [
                    "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54-i-allegro-affettuoso-live/1563042200?i=1563042205"
                ],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | "
        "Alexander Rudin | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Alexander Rudin - Eliso Virsaladze"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Eliso Virsaladze", "label": "Eliso Virsaladze"},
        {"role": "conductor", "displayName": "Alexander Rudin", "label": "Alexander Rudin"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=VirsaladzeAppleProvider(), llm_client=AcceptedAppleOnlyLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert (
        "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54-i-allegro-affettuoso-live/1563042200?i=1563042205"
        in final_urls
    )
    assert "https://www.youtube.com/watch?v=tDxa2aOQ0w0" in final_urls


def test_pipeline_keeps_youtube_full_version_alongside_independently_finalizable_apple_first_movement() -> None:
    class VirsaladzeAppleMovementProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54-i-allegro-affettuoso-live/1563042200?i=1563042205",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Piano Concerto in A Minor, Op. 54: I. Allegro affettuoso (Live)",
                    "description": "Eliso Virsaladze | Schumann | Live performance",
                    "platform": "apple_music",
                    "weight": 0.68,
                    "same_recording_score": 0.71,
                    "duration_seconds": 871,
                    "uploader": "Eliso Virsaladze",
                    "view_count": 0,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=tDxa2aOQ0w0",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann - Piano Concerto, op.54 Eliso Virsaladze",
                    "description": "Historic full concerto upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1920,
                    "uploader": "Archive",
                    "view_count": 2400,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Eliso Virsaladze | "
        "Alexander Rudin | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Alexander Rudin - Eliso Virsaladze"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Eliso Virsaladze", "label": "Eliso Virsaladze"},
        {"role": "conductor", "displayName": "Alexander Rudin", "label": "Alexander Rudin"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=VirsaladzeAppleMovementProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert (
        "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54-i-allegro-affettuoso-live/1563042200?i=1563042205"
        in final_urls
    )
    assert "https://www.youtube.com/watch?v=tDxa2aOQ0w0" in final_urls


def test_pipeline_keeps_sparse_heifetz_alternate_upload_even_when_llm_accepts_only_top_four() -> None:
    class SparseHeifetzAltProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1vp421U7kw/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Beethoven Violin Concerto in D Major Heifetz Toscanini NBC 1940",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2312,
                    "uploader": "Uploader A",
                    "view_count": 2000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1QF411s79n/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Heifetz Toscanini Beethoven Violin Concerto Op.61",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2316,
                    "uploader": "Uploader B",
                    "view_count": 1800,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven Violin Concerto Heifetz Toscanini 1940",
                    "description": "Japanese exact upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2326,
                    "uploader": "Uploader C",
                    "view_count": 2200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.84,
                    "duration_seconds": 2307,
                    "uploader": "Collector A",
                    "view_count": 1291,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": (
                        "Ludwig van Beethoven Violin Concerto in D, Op. 61 "
                        "1. Allegro ma non troppo 2. Larghetto 3. Rondo "
                        "Jascha Heifetz violin Arturo Toscanini conductor"
                    ),
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.67,
                    "duration_seconds": 2315,
                    "uploader": "Private Reserve",
                    "view_count": 1600,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class FourUrlLlm:
        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": [
                    "https://www.bilibili.com/video/BV1vp421U7kw/",
                    "https://www.bilibili.com/video/BV1QF411s79n/",
                    "https://www.youtube.com/watch?v=XazjX-k2aco",
                    "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                ],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "Toscanini - Heifetz - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseHeifetzAltProvider(), llm_client=FourUrlLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls


def test_pipeline_keeps_close_same_platform_heifetz_alternate_when_fast_llm_accepts_single_youtube() -> None:
    class SparseHeifetzProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1vp421U7kw/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "贝多芬op.61《D大调小提琴协奏曲》海菲兹+托斯卡尼尼1940+NBC交响乐团 Beethoven Violin Concerto in D Major",
                    "description": "Chinese exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.96,
                    "duration_seconds": 2308,
                    "uploader": "Uploader A",
                    "view_count": 84,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=8Aclk_O4bSc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (Heifetz/Toscanini 1940)",
                    "description": "Historic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.78,
                    "duration_seconds": 2307,
                    "uploader": "Collector A",
                    "view_count": 1292,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=9YWr1UcbZE8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini",
                    "description": "Canonical upload title",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.71,
                    "duration_seconds": 2315,
                    "uploader": "Classical Archive",
                    "view_count": 9696,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Fm4y1G7NA/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "[骑熊净谱对照]贝多芬D大调小提琴协奏曲｜Jascha Heifetz演奏",
                    "description": "Score-following upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 2088,
                    "uploader": "Uploader B",
                    "view_count": 1622,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=_N15_3_TP7I",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Jascha Heifetz \"Violin Concerto\" Beethoven",
                    "description": "Generic upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.59,
                    "duration_seconds": 2343,
                    "uploader": "Uploader C",
                    "view_count": 205590,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=-rUNkiGgJx8",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven: Violin Concerto (1940) Heifetz/Toscanini NEW EDITION",
                    "description": "Remaster upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.59,
                    "duration_seconds": 2316,
                    "uploader": "Archive C",
                    "view_count": 361,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class SingleYoutubeFastLlm:
        minimum_synthesis_timeout_seconds = 4.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "保留明确接受的一条YouTube上传。",
                "notes": "",
                "warnings": [],
                "acceptedUrls": ["https://www.youtube.com/watch?v=8Aclk_O4bSc"],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Violin Concerto in D major, Op. 61 | Jascha Heifetz | - | -"
    payload["items"][0]["seed"]["title"] = "Toscanini - Heifetz - NBC Symphony Orchestra - March 11, 1940, in Studio 8H, Radio City"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Violin Concerto in D major, Op. 61"
    payload["items"][0]["seed"]["catalogue"] = "Op.61"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Jascha Heifetz", "label": "Jascha Heifetz"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SparseHeifetzProvider(), llm_client=SingleYoutubeFastLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=8Aclk_O4bSc" in final_urls
    assert "https://www.youtube.com/watch?v=9YWr1UcbZE8" in final_urls
    assert "https://www.youtube.com/watch?v=_N15_3_TP7I" not in final_urls
    assert "https://www.youtube.com/watch?v=-rUNkiGgJx8" not in final_urls


def test_pipeline_keeps_close_same_platform_complete_alternate_without_accepted_urls() -> None:
    class SpringAlternateProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=topspring001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Mogilevsky & Kreutzer - Beethoven : Spring Sonata in F Op.24 (1931) 再復刻",
                    "description": "High-confidence alternate upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 1462,
                    "uploader": "Archive A",
                    "view_count": 12000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=topspring002",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": 'Alexander Mogilevsky and Leonid Kreutzer - Violin Sonata No. 5 "Spring" (Beethoven) 1931',
                    "description": "Another high-confidence alternate upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 1458,
                    "uploader": "Archive B",
                    "view_count": 11000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=canonicalspring",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Alexandre Moguilewsky & Leonid Kreutzer: Beethoven: Violin Sonata No. 5 (R. ca 1931)",
                    "description": "Canonical upload with transliterated spelling",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.47,
                    "duration_seconds": 1449,
                    "uploader": "Archive C",
                    "view_count": 4200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=spring-part2",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": 'Alexandre Moguilewsky & Leonid Kreutzer play Beethoven Sonata No. 5 in F major Op. 24 "Spring" (2/2)',
                    "description": "Split upload second half",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 690,
                    "uploader": "Archive D",
                    "view_count": 2300,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = (
        'Ludwig van Beethoven | Violin Sonata No.5, Op.24 "Spring" | '
        "Alexander Mogilevsky | Leonid Kreutzer | 1931"
    )
    payload["items"][0]["seed"]["title"] = "Mogilevsky & Kreutzer"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = 'Violin Sonata No.5, Op.24 "Spring"'
    payload["items"][0]["seed"]["catalogue"] = "Op.24"
    payload["items"][0]["seed"]["performanceDateText"] = "1931"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Alexander Mogilevsky", "label": "Alexander Mogilevsky"},
        {"role": "soloist", "displayName": "Leonid Kreutzer", "label": "Leonid Kreutzer"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=SpringAlternateProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=topspring001" in final_urls
    assert "https://www.youtube.com/watch?v=topspring002" in final_urls
    assert "https://www.youtube.com/watch?v=canonicalspring" in final_urls
    assert "https://www.youtube.com/watch?v=spring-part2" not in final_urls


def test_pipeline_keeps_close_same_platform_longform_alternate_when_llm_accepts_other_platform_winners() -> None:
    class CortotProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=IqqhlFh0_rI",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Alfred Cortot plays Schumann Piano Concerto in A minor, op. 54 (1951)",
                    "description": "Back Blue Classics upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "duration_seconds": 1998,
                    "uploader": "Back Blue Classics",
                    "view_count": 438,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Pr4y1Q74W/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "舒曼钢协和诗人之恋，Cortot钢琴，Souzay男中，Fricsay指挥",
                    "description": "Long-form upload containing Schumann concerto and Dichterliebe",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 3686,
                    "uploader": "_HideousLight_",
                    "view_count": 2348,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Y34y1p7e7/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "科尔托五十年代舒曼钢协录音 弗里乔伊指挥 Alfred Cortot plays Schuman PC Conducted by Ferenc Fricsay",
                    "description": "Shorter exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 1932,
                    "uploader": "赫尔曼-阿本德罗特",
                    "view_count": 422,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=rpOF3eNNaxE",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Alfred Cortot Schumann Piano Concerto in A-min. Op. 54",
                    "description": "Alternate YouTube upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.47,
                    "duration_seconds": 1787,
                    "uploader": "rimb68",
                    "view_count": 1167,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class CortotFullLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": [
                    "https://www.youtube.com/watch?v=IqqhlFh0_rI",
                    "https://www.bilibili.com/video/BV1Y34y1p7e7/",
                ],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Alfred Cortot | "
        "Ferenc Fricsay | Berliner Philharmoniker | 1951"
    )
    payload["items"][0]["seed"]["title"] = "Alfred Cortot 1951"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1951"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Alfred Cortot", "label": "Alfred Cortot"},
        {"role": "conductor", "displayName": "Ferenc Fricsay", "label": "Ferenc Fricsay"},
        {"role": "orchestra", "displayName": "Berliner Philharmoniker", "label": "Berliner Philharmoniker"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=CortotProvider(), llm_client=CortotFullLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=IqqhlFh0_rI" in final_urls
    assert "https://www.bilibili.com/video/BV1Y34y1p7e7/" in final_urls
    assert "https://www.bilibili.com/video/BV1Pr4y1Q74W/" in final_urls


def test_pipeline_keeps_accepted_longform_same_platform_alternate_through_final_limit() -> None:
    class CortotPartialProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1vL5ozzEyp/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "弗里乔伊（Ferenc Fricsay），科尔托（Alfred Cortot）/ 柴可夫斯基：第5交响曲；舒曼：钢琴协奏曲",
                    "description": "Accepted long compilation upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "duration_seconds": 4681,
                    "uploader": "virgo_bottle",
                    "view_count": 270,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=IqqhlFh0_rI",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Alfred Cortot plays Schumann Piano Concerto in A minor, op. 54 (1951)",
                    "description": "Strong YouTube upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.81,
                    "duration_seconds": 1998,
                    "uploader": "Back Blue Classics",
                    "view_count": 438,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Pr4y1Q74W/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "舒曼钢协和诗人之恋，Cortot钢琴，Souzay男中，Fricsay指挥",
                    "description": "Accepted long-form alternate upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 3686,
                    "uploader": "_HideousLight_",
                    "view_count": 2348,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1Y34y1p7e7/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "科尔托五十年代舒曼钢协录音 弗里乔伊指挥 Alfred Cortot plays Schuman PC Conducted by Ferenc Fricsay",
                    "description": "Accepted shorter exact upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 1932,
                    "uploader": "赫尔曼-阿本德罗特",
                    "view_count": 422,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class CortotPartialLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": [
                    "https://www.bilibili.com/video/BV1vL5ozzEyp/",
                    "https://www.bilibili.com/video/BV1Pr4y1Q74W/",
                    "https://www.bilibili.com/video/BV1Y34y1p7e7/",
                ],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = "Robert Schumann | Piano Concerto in A minor, Op.54 | Alfred Cortot | - | -"
    payload["items"][0]["seed"]["title"] = "Alfred Cortot"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1951"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Alfred Cortot", "label": "Alfred Cortot"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=CortotPartialProvider(), llm_client=CortotPartialLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.bilibili.com/video/BV1vL5ozzEyp/" in final_urls
    assert "https://www.bilibili.com/video/BV1Pr4y1Q74W/" in final_urls
    assert "https://www.bilibili.com/video/BV1Y34y1p7e7/" in final_urls


def test_pipeline_aclose_closes_source_provider() -> None:
    provider = ClosableSourceProvider()
    pipeline = RetrievalPipeline(source_provider=provider, llm_client=None)

    asyncio.run(pipeline.aclose())

    assert provider.closed is True


def test_pipeline_keeps_exact_karajan_upload_among_close_year_matched_ties() -> None:
    class KarajanProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=iPQWH7rKlaM",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Richard Strauss - Eine Alpensinfonie / Karajan - Berliner Philharmoniker / Live Recording 1982",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 3060,
                    "uploader": "Vault",
                    "view_count": 21000,
                    "fields": {},
                    "images": [],
                    "description": "",
                },
                {
                    "url": "https://www.bilibili.com/video/BV1WV4y1h799/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "卡拉扬《理查·施特劳斯：阿尔卑斯山交响曲》柏林爱乐「BD」_哔哩哔哩_bilibili",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 3050,
                    "uploader": "Uploader",
                    "view_count": 15000,
                    "fields": {},
                    "images": [],
                    "description": "",
                },
                {
                    "url": "https://www.youtube.com/watch?v=fDi1PSz8mRE",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "STRAUSS: ALPINE SYMPHONY / BERLIN  PO / KARAJAN  (1982 live)",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 3058,
                    "uploader": "Archive",
                    "view_count": 18800,
                    "fields": {},
                    "images": [],
                    "description": "",
                },
                {
                    "url": "https://www.youtube.com/watch?v=oPpGxrUHLO4",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Richard Strauss – Eine Alpensinfonie – Herbert von Karajan, Berliner Philharmoniker, 1981",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.97,
                    "duration_seconds": 3042,
                    "uploader": "Archive",
                    "view_count": 17000,
                    "fields": {},
                    "images": [],
                    "description": "",
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "orchestral"
    payload["items"][0]["sourceLine"] = "Richard Strauss | Eine Alpensinfonie, Op.64 | Herbert von Karajan | Berlin Philharmonic Orchestra | August 28, 1982 Salzburg"
    payload["items"][0]["seed"]["title"] = "Karajan Alpine 1982"
    payload["items"][0]["seed"]["composerName"] = "理查·施特劳斯"
    payload["items"][0]["seed"]["composerNameLatin"] = "Richard Strauss"
    payload["items"][0]["seed"]["workTitle"] = "阿尔卑斯山交响曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Eine Alpensinfonie, Op.64"
    payload["items"][0]["seed"]["catalogue"] = "Op.64"
    payload["items"][0]["seed"]["performanceDateText"] = "August 28, 1982 Salzburg"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "conductor", "displayName": "Herbert von Karajan", "label": "Herbert von Karajan"},
        {"role": "orchestra", "displayName": "Berlin Philharmonic Orchestra", "label": "Berlin Philharmonic Orchestra"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=KarajanProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    assert "https://www.youtube.com/watch?v=fDi1PSz8mRE" in final_urls
    assert "https://www.youtube.com/watch?v=oPpGxrUHLO4" not in final_urls


def test_pipeline_promotes_exact_annie_upload_from_candidate_only_tie() -> None:
    class AnnieTieProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=wkMQ1q4V4Vs",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Robert Schumann Piano Concerto in A minor, Op.54 Annie Fischer Paul Kletzki Budapest Philharmonic Orchestra",
                    "description": "Exact collaborator set.",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.51,
                    "duration_seconds": 1904,
                    "uploader": "Archive",
                    "view_count": 4200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=crIta1ClQeo",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann: Piano Concerto in A minor, Op.54 Annie Fischer / Otto Klemperer",
                    "description": "Wrong collaborator but similar confidence.",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 1876,
                    "uploader": "Archive",
                    "view_count": 3900,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | Kletzki | Budapest Philharmonic Orchestra | -"
    )
    payload["items"][0]["seed"]["title"] = "Annie Fischer"
    payload["items"][0]["seed"]["composerName"] = "舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Annie Fischer", "label": "Annie Fischer"},
        {"role": "conductor", "displayName": "Kletzki", "label": "Kletzki"},
        {
            "role": "orchestra",
            "displayName": "Budapest Philharmonic Orchestra",
            "label": "Budapest Philharmonic Orchestra",
        },
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=AnnieTieProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert result.result.links
    assert result.result.links[0].url == "https://www.youtube.com/watch?v=wkMQ1q4V4Vs"


def test_pipeline_keeps_kempff_exact_upload_in_final_links_but_leaves_compilation_as_candidate_only() -> None:
    class KempffProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1NY411y7Wc/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Wilhelm Kempff Antal Dorati Schumann Piano Concerto Op.54 1959 complete",
                    "description": "Concertgebouw Orchestra Amsterdam 1959 live full performance",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.79,
                    "duration_seconds": 1880,
                    "uploader": "Classical Vault",
                    "view_count": 2200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV135411e7JL/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Wilhelm Kempff Antal Dorati Schumann Piano Concertos 1950s",
                    "description": "Compilation upload with multiple concerto performances",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.78,
                    "duration_seconds": 5400,
                    "uploader": "Archive Channel",
                    "view_count": 2200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1FW4y1s7e8/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Schumann Piano Concerto Op.54 Wilhelm Kempff Antal Dorati 1959 Amsterdam",
                    "description": "Alternate upload of the same live performance",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.77,
                    "duration_seconds": 1874,
                    "uploader": "Historic Archive",
                    "view_count": 2100,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Wilhelm Kempff | Antal Dorati | "
        "Concertgebouw Orchestra Amsterdam | 1959"
    )
    payload["items"][0]["seed"]["title"] = "Wilhelm Kempff & Antal Dorati"
    payload["items"][0]["seed"]["composerName"] = "舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "1959"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Wilhelm Kempff", "label": "Wilhelm Kempff"},
        {"role": "conductor", "displayName": "Antal Dorati", "label": "Antal Dorati"},
        {
            "role": "orchestra",
            "displayName": "Concertgebouw Orchestra Amsterdam",
            "label": "Concertgebouw Orchestra Amsterdam",
        },
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=KempffProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [link.url for link in result.result.links]
    candidate_urls = [link.url for link in result.link_candidates]

    assert final_urls == [
        "https://www.bilibili.com/video/BV1NY411y7Wc/",
        "https://www.bilibili.com/video/BV1FW4y1s7e8/",
    ]
    assert "https://www.bilibili.com/video/BV135411e7JL/" in candidate_urls
    assert "https://www.bilibili.com/video/BV135411e7JL/" not in final_urls


def test_pipeline_prefers_llm_accepted_delara_target_over_wrong_higher_confidence_upload() -> None:
    class DeLaraProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1Za9QYnE9D/",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                    "title": "Clara Schumann Piano Concerto in A minor",
                    "description": "Piano: Michal Tal Conductor: Keren Kagarlitsky Israel Camerata Jerusalem Orchestra",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.65,
                    "duration_seconds": 1378,
                    "uploader": "Amy-yui",
                    "view_count": 68,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1CWb7eHENQ/",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                    "title": "【Adelina de Lara】克拉拉的爱徒会如何演奏舒曼钢协？",
                    "description": "BBC broadcast; 29 May 1951",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.45,
                    "duration_seconds": 1991,
                    "uploader": "_HideousLight_",
                    "view_count": 2018,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1qc41157iE/",
                    "source_label": "Bilibili Search Browser Search",
                    "source_kind": "streaming",
                    "title": "深沉而隽永 爱德琳娜·黛·劳拉夫人五十年代的录音室风貌 Adelina de Lara plays Beethoven, Brahms and Schumann",
                    "description": "1951-1952 London Musical Club recordings",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 18552,
                    "uploader": "_HideousLight_",
                    "view_count": 3856,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class AcceptedTargetLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": ["https://www.bilibili.com/video/BV1CWb7eHENQ/"],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | "
        "BBC Scottish Symphony Orchestra | May 29, 1951"
    )
    payload["items"][0]["seed"]["title"] = "Adelina de Lara & Ian Whyte"
    payload["items"][0]["seed"]["composerName"] = "舒曼"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitle"] = "a小调钢琴协奏曲"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "May 29, 1951"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Adelina de Lara", "label": "Adelina de Lara"},
        {"role": "conductor", "displayName": "Ian Whyte", "label": "Ian Whyte"},
        {
            "role": "orchestra",
            "displayName": "BBC Scottish Symphony Orchestra",
            "label": "BBC Scottish Symphony Orchestra",
        },
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=DeLaraProvider(), llm_client=AcceptedTargetLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [candidate.url for candidate in result.result.links]
    candidate_urls = [candidate.url for candidate in result.link_candidates]

    assert "https://www.bilibili.com/video/BV1CWb7eHENQ/" in candidate_urls
    assert final_urls == ["https://www.bilibili.com/video/BV1CWb7eHENQ/"]


def test_pipeline_excludes_conflicting_clara_candidate_from_candidate_links() -> None:
    class DeLaraProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1Za9QYnE9D/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Clara Schumann Piano Concerto in A minor",
                    "description": "Wrong pianist and conductor",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.65,
                    "duration_seconds": 1378,
                    "uploader": "Amy-yui",
                    "view_count": 68,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1CWb7eHENQ/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Adelina de Lara plays Schumann Piano Concerto (BBC, 1951)",
                    "description": "BBC broadcast; 29 May 1951",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.52,
                    "duration_seconds": 1991,
                    "uploader": "_HideousLight_",
                    "view_count": 2018,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | "
        "BBC Scottish Symphony Orchestra | May 29, 1951"
    )
    payload["items"][0]["seed"]["title"] = "Adelina de Lara & Ian Whyte"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "May 29, 1951"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Adelina de Lara", "label": "Adelina de Lara"},
        {"role": "conductor", "displayName": "Ian Whyte", "label": "Ian Whyte"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=DeLaraProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    candidate_urls = [candidate.url for candidate in result.link_candidates]
    assert "https://www.bilibili.com/video/BV1CWb7eHENQ/" in candidate_urls
    assert "https://www.bilibili.com/video/BV1Za9QYnE9D/" not in candidate_urls


def test_pipeline_excludes_conflicting_clara_candidate_from_final_links() -> None:
    class DeLaraProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1Za9QYnE9D/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Clara Schumann Piano Concerto in A minor",
                    "description": "Wrong pianist and conductor",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.65,
                    "duration_seconds": 1378,
                    "uploader": "Amy-yui",
                    "view_count": 68,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Adelina de Lara | Ian Whyte | "
        "BBC Scottish Symphony Orchestra | May 29, 1951"
    )
    payload["items"][0]["seed"]["title"] = "Adelina de Lara & Ian Whyte"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "May 29, 1951"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Adelina de Lara", "label": "Adelina de Lara"},
        {"role": "conductor", "displayName": "Ian Whyte", "label": "Ian Whyte"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=DeLaraProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [candidate.url for candidate in result.result.links] == []


def test_pipeline_excludes_conflicting_giulini_candidate_but_keeps_same_platform_alternate() -> None:
    class AnnieProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1TE411f7uh/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Annie Fischer plays Schumann Piano Concerto Op. 54",
                    "description": "Target upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.65,
                    "duration_seconds": 1905,
                    "uploader": "Uploader A",
                    "view_count": 4000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=R4YZRoHbrCw",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann, Piano Concerto in A Minor, Op.54 / Fischer & Giulini",
                    "description": "Wrong conductor upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 1910,
                    "uploader": "Uploader YT",
                    "view_count": 6200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1altkletzki/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Annie Fischer / Kletzki - Schumann Piano Concerto",
                    "description": "Same recording alternate upload",
                    "platform": "bilibili",
                    "weight": 0.68,
                    "same_recording_score": 0.58,
                    "duration_seconds": 1908,
                    "uploader": "Uploader B",
                    "view_count": 2100,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Annie Fischer | "
        "Paul Kletzki | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Annie Fischer"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = ""
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Annie Fischer", "label": "Annie Fischer"},
        {"role": "conductor", "displayName": "Paul Kletzki", "label": "Paul Kletzki"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=AnnieProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    candidate_urls = [candidate.url for candidate in result.link_candidates]
    assert "https://www.bilibili.com/video/BV1TE411f7uh/" in candidate_urls
    assert "https://www.bilibili.com/video/BV1altkletzki/" in candidate_urls
    assert "https://www.youtube.com/watch?v=R4YZRoHbrCw" not in candidate_urls


def test_pipeline_caps_candidate_links_to_three_high_confidence_and_two_review_needed_per_platform() -> None:
    class MultiPlatformCandidateProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                *[
                    {
                        "url": f"https://www.youtube.com/watch?v=ytcand{i}",
                        "source_label": "YouTube Search",
                        "source_kind": "streaming",
                        "title": f"Richter Schumann 1954 candidate {i}",
                        "description": "YouTube candidate",
                        "platform": "youtube",
                        "weight": 0.7 if i < 3 else 0.52,
                        "same_recording_score": 0.78 - i * 0.04,
                        "duration_seconds": 2400,
                        "uploader": "Uploader YT",
                        "view_count": 1000 + i,
                        "fields": {},
                        "images": [],
                    }
                    for i in range(6)
                ],
                *[
                    {
                        "url": f"https://www.bilibili.com/video/BV1cap{i}/",
                        "source_label": "Bilibili Search",
                        "source_kind": "streaming",
                        "title": f"Richter Schumann 1954 bilibili {i}",
                        "description": "Bilibili candidate",
                        "platform": "bilibili",
                        "weight": 0.7 if i < 3 else 0.52,
                        "same_recording_score": 0.77 - i * 0.04,
                        "duration_seconds": 2400,
                        "uploader": "Uploader Bili",
                        "view_count": 900 + i,
                        "fields": {},
                        "images": [],
                    }
                    for i in range(6)
                ],
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Hungarian State Philharmonic Orchestra | March 8, 1954 Budapest"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "March 8, 1954 Budapest"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=MultiPlatformCandidateProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    candidate_urls = [candidate.url for candidate in result.link_candidates]
    youtube_candidates = [url for url in candidate_urls if "youtube.com" in url]
    bilibili_candidates = [url for url in candidate_urls if "bilibili.com" in url]
    youtube_zones = [candidate.zone for candidate in result.link_candidates if "youtube.com" in candidate.url]
    bilibili_zones = [candidate.zone for candidate in result.link_candidates if "bilibili.com" in candidate.url]

    assert len(youtube_candidates) == 5
    assert len(bilibili_candidates) == 5
    assert youtube_candidates[:3] == [
        "https://www.youtube.com/watch?v=ytcand0",
        "https://www.youtube.com/watch?v=ytcand1",
        "https://www.youtube.com/watch?v=ytcand2",
    ]
    assert bilibili_candidates[:3] == [
        "https://www.bilibili.com/video/BV1cap0/",
        "https://www.bilibili.com/video/BV1cap1/",
        "https://www.bilibili.com/video/BV1cap2/",
    ]
    assert set(youtube_candidates[3:]).issubset(
        {
            "https://www.youtube.com/watch?v=ytcand3",
            "https://www.youtube.com/watch?v=ytcand4",
            "https://www.youtube.com/watch?v=ytcand5",
        }
    )
    assert set(bilibili_candidates[3:]).issubset(
        {
            "https://www.bilibili.com/video/BV1cap3/",
            "https://www.bilibili.com/video/BV1cap4/",
            "https://www.bilibili.com/video/BV1cap5/",
        }
    )
    assert youtube_zones == ["green", "green", "green", "yellow", "yellow"]
    assert bilibili_zones == ["green", "green", "green", "yellow", "yellow"]


def test_pipeline_final_links_select_one_independent_winner_per_primary_platform() -> None:
    class ThreePlatformProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1bestbili/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Sviatoslav Richter Janos Ferencsik Schumann Piano Concerto 1954",
                    "description": "Budapest 1954 exact upload",
                    "platform": "bilibili",
                    "weight": 0.7,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2400,
                    "uploader": "Uploader Bili A",
                    "view_count": 5000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1altbili/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Richter Schumann Piano Concerto 1954",
                    "description": "Same platform alternate upload",
                    "platform": "bilibili",
                    "weight": 0.7,
                    "same_recording_score": 0.96,
                    "duration_seconds": 2390,
                    "uploader": "Uploader Bili B",
                    "view_count": 4200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=ytbest001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Richter Ferencsik Schumann Piano Concerto 1954 Budapest",
                    "description": "Hungarian State Philharmonic Orchestra",
                    "platform": "youtube",
                    "weight": 0.7,
                    "same_recording_score": 0.86,
                    "duration_seconds": 2410,
                    "uploader": "Uploader YT",
                    "view_count": 8200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54/123456789?i=987654321",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Piano Concerto in A Minor, Op. 54",
                    "description": "Sviatoslav Richter | Janos Ferencsik | Budapest 1954",
                    "platform": "apple_music",
                    "weight": 0.7,
                    "same_recording_score": 0.83,
                    "duration_seconds": 2405,
                    "uploader": "Sviatoslav Richter",
                    "view_count": 0,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Hungarian State Philharmonic Orchestra | March 8, 1954 Budapest"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "March 8, 1954 Budapest"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=ThreePlatformProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [candidate.url for candidate in result.result.links]
    assert final_urls == [
        "https://www.bilibili.com/video/BV1bestbili/",
        "https://www.youtube.com/watch?v=ytbest001",
        "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54/123456789?i=987654321",
    ]


def test_pipeline_final_links_do_not_keep_same_platform_alternates_once_primary_platforms_are_covered() -> None:
    class ThreePlatformAltProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1bestbili/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Richter Ferencsik Schumann Piano Concerto",
                    "description": "Budapest exact upload",
                    "platform": "bilibili",
                    "weight": 0.7,
                    "same_recording_score": 0.97,
                    "duration_seconds": 2400,
                    "uploader": "Uploader Bili A",
                    "view_count": 5000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1altbili/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Richter / Ferencsik Schumann archive upload",
                    "description": "Same platform alternate upload",
                    "platform": "bilibili",
                    "weight": 0.7,
                    "same_recording_score": 0.96,
                    "duration_seconds": 2390,
                    "uploader": "Uploader Bili B",
                    "view_count": 4200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=ytbest001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Richter Ferencsik Schumann Piano Concerto",
                    "description": "Budapest exact upload",
                    "platform": "youtube",
                    "weight": 0.7,
                    "same_recording_score": 0.86,
                    "duration_seconds": 2410,
                    "uploader": "Uploader YT",
                    "view_count": 8200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54/123456789?i=987654321",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": "Piano Concerto in A Minor, Op. 54",
                    "description": "Sviatoslav Richter | Janos Ferencsik | Budapest exact upload",
                    "platform": "apple_music",
                    "weight": 0.7,
                    "same_recording_score": 0.83,
                    "duration_seconds": 2405,
                    "uploader": "Sviatoslav Richter",
                    "view_count": 0,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    class AcceptedBilibiliAlternatesLlm:
        minimum_synthesis_timeout_seconds = 0.0
        allow_realtime_synthesis = True

        async def synthesize(self, draft, profile, records):
            del draft, profile, records
            return {
                "summary": "",
                "notes": "",
                "warnings": [],
                "acceptedUrls": [
                    "https://www.bilibili.com/video/BV1bestbili/",
                    "https://www.bilibili.com/video/BV1altbili/",
                ],
            }

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | - | -"
    )
    payload["items"][0]["seed"]["title"] = "Sviatoslav Richter / Janos Ferencsik"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=ThreePlatformAltProvider(), llm_client=AcceptedBilibiliAlternatesLlm())

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [candidate.url for candidate in result.result.links] == [
        "https://www.bilibili.com/video/BV1bestbili/",
        "https://www.youtube.com/watch?v=ytbest001",
        "https://music.apple.com/us/album/piano-concerto-in-a-minor-op-54/123456789?i=987654321",
    ]


def test_pipeline_chamber_solo_adds_low_confidence_cross_platform_rescues_for_appassionata_cluster() -> None:
    class ChamberSoloCrossPlatformProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1JV411o7WK/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Egon Petri plays Beethoven Piano Sonata No.23 Appassionata Op.57",
                    "description": "Historic upload. Egon Petri 1956.",
                    "platform": "bilibili",
                    "weight": 0.72,
                    "same_recording_score": 0.73,
                    "duration_seconds": 1370,
                    "uploader": "Archive A",
                    "view_count": 6200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.bilibili.com/video/BV1WnkiYWErt/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "所罗门《贝多芬：第二十三钢琴奏鸣曲“热情”》",
                    "description": "Wrong soloist",
                    "platform": "bilibili",
                    "weight": 0.7,
                    "same_recording_score": 0.53,
                    "duration_seconds": 1315,
                    "uploader": "Archive B",
                    "view_count": 2300,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=jEVt42o8UEQ",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": 'Beethoven: Piano Sonata No. 23 in F Minor, Op. 57 "Appassionata": I. Allegro assai',
                    "description": "Egon Petri 1956 complete recording",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 1360,
                    "uploader": "Archive YT 1",
                    "view_count": 4100,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=3oo93lgs15Y",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": 'Beethoven: Piano Sonata No. 23 in F Minor, Op. 57 "Appassionata": I. Allegro assai',
                    "description": "Egon Petri 1956 complete recording",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 1362,
                    "uploader": "Archive YT 2",
                    "view_count": 3800,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Egon Petri | 1956"
    payload["items"][0]["seed"]["title"] = "Egon Petri 1956"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Sonata No.23, Op.57"
    payload["items"][0]["seed"]["workTitle"] = "第二十三号奏鸣曲，热情"
    payload["items"][0]["seed"]["catalogue"] = "Op.57"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Egon Petri", "label": "Egon Petri"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=ChamberSoloCrossPlatformProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [candidate.url for candidate in result.result.links]
    assert "https://www.bilibili.com/video/BV1JV411o7WK/" in final_urls
    assert "https://www.youtube.com/watch?v=jEVt42o8UEQ" in final_urls
    assert "https://www.youtube.com/watch?v=3oo93lgs15Y" in final_urls
    assert len(final_urls) >= 3


def test_pipeline_chamber_solo_adds_same_platform_low_confidence_exact_title_rescue() -> None:
    class ChamberSoloSamePlatformProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://music.apple.com/us/album/sonata-no-23-in-f-minor-op-57-appassionata-i-allegro-assai/589787212?i=589787324&uo=4",
                    "source_label": "Apple Music Search",
                    "source_kind": "streaming",
                    "title": 'Sonata No. 23 in F Minor, Op. 57 "Appassionata": I. Allegro assai',
                    "description": "Walter Gieseking",
                    "platform": "apple_music",
                    "weight": 0.7,
                    "same_recording_score": 0.69,
                    "duration_seconds": 620,
                    "uploader": "Walter Gieseking",
                    "view_count": 0,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=A-LWnNJ1Yhc",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Walter Gieseking, piano - Beethoven - Sonata in F minor, Op. 57 ('Appassionata') (1949 - complete)",
                    "description": "Complete recording",
                    "platform": "youtube",
                    "weight": 0.72,
                    "same_recording_score": 0.8,
                    "duration_seconds": 1365,
                    "uploader": "Uploader YT A",
                    "view_count": 5200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=bETptkdwA5U",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Beethoven - Piano sonata n°23 - Gieseking 1939",
                    "description": "Historic recording",
                    "platform": "youtube",
                    "weight": 0.71,
                    "same_recording_score": 0.77,
                    "duration_seconds": 1320,
                    "uploader": "Uploader YT B",
                    "view_count": 4100,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=eMi5KZ4lDCA",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": 'Walter Gieseking plays Beethoven Piano Sonata No.23 "Appassionata" (1947)',
                    "description": "Target exact upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.49,
                    "duration_seconds": 1342,
                    "uploader": "Uploader YT C",
                    "view_count": 1800,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "chamber_solo"
    payload["items"][0]["sourceLine"] = "Ludwig van Beethoven | Piano Sonata No.23, Op.57 | Walter Gieseking | 1947"
    payload["items"][0]["seed"]["title"] = "Walter Gieseking 1947"
    payload["items"][0]["seed"]["composerNameLatin"] = "Ludwig van Beethoven"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Sonata No.23, Op.57"
    payload["items"][0]["seed"]["workTitle"] = "第二十三号奏鸣曲，热情"
    payload["items"][0]["seed"]["catalogue"] = "Op.57"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Walter Gieseking", "label": "Walter Gieseking"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=ChamberSoloSamePlatformProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    final_urls = [candidate.url for candidate in result.result.links]
    assert "https://www.youtube.com/watch?v=A-LWnNJ1Yhc" in final_urls
    assert "https://www.youtube.com/watch?v=eMi5KZ4lDCA" in final_urls


def test_pipeline_concerto_does_not_enable_chamber_low_confidence_cross_platform_rescue() -> None:
    class ConcertoCrossPlatformProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.bilibili.com/video/BV1bestconcerto/",
                    "source_label": "Bilibili Search",
                    "source_kind": "streaming",
                    "title": "Richter Ferencsik Schumann Piano Concerto Op.54 complete",
                    "description": "Budapest 1954 full performance",
                    "platform": "bilibili",
                    "weight": 0.72,
                    "same_recording_score": 0.73,
                    "duration_seconds": 2430,
                    "uploader": "Archive Bili",
                    "view_count": 6200,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=ytalt001",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann Piano Concerto Op.54 Richter",
                    "description": "Alternative upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 2400,
                    "uploader": "Archive YT 1",
                    "view_count": 4100,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=ytalt002",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Schumann Piano Concerto Op.54 Richter live",
                    "description": "Alternative upload",
                    "platform": "youtube",
                    "weight": 0.68,
                    "same_recording_score": 0.61,
                    "duration_seconds": 2395,
                    "uploader": "Archive YT 2",
                    "view_count": 3900,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Budapest | 1954"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=ConcertoCrossPlatformProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    assert [candidate.url for candidate in result.result.links] == [
        "https://www.bilibili.com/video/BV1bestconcerto/",
    ]


def test_pipeline_candidate_links_still_filters_red_zone_even_with_review_slots() -> None:
    class RedZoneProvider:
        async def inspect_existing_links(self, draft, profile):
            del draft, profile
            return []

        async def search_high_quality(self, draft, profile):
            del draft, profile
            return []

        async def search_streaming(self, draft, profile):
            del draft, profile
            return [
                {
                    "url": "https://www.youtube.com/watch?v=good1",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Sviatoslav Richter Schumann Piano Concerto 1954",
                    "description": "Good candidate",
                    "platform": "youtube",
                    "weight": 0.72,
                    "same_recording_score": 0.82,
                    "duration_seconds": 2400,
                    "uploader": "Uploader YT",
                    "view_count": 1000,
                    "fields": {},
                    "images": [],
                },
                {
                    "url": "https://www.youtube.com/watch?v=wrongclara",
                    "source_label": "YouTube Search",
                    "source_kind": "streaming",
                    "title": "Clara Schumann Piano Concerto 1835",
                    "description": "Wrong composer family candidate",
                    "platform": "youtube",
                    "weight": 0.75,
                    "same_recording_score": 0.84,
                    "duration_seconds": 2400,
                    "uploader": "Uploader Wrong",
                    "view_count": 900,
                    "fields": {},
                    "images": [],
                },
            ]

        async def search_fallback(self, draft, profile):
            del draft, profile
            return []

    payload = sample_request()
    payload["items"][0]["workTypeHint"] = "concerto"
    payload["items"][0]["sourceLine"] = (
        "Robert Schumann | Piano Concerto in A minor, Op.54 | Sviatoslav Richter | "
        "Janos Ferencsik | Hungarian State Philharmonic Orchestra | March 8, 1954 Budapest"
    )
    payload["items"][0]["seed"]["title"] = "Richter Budapest 1954"
    payload["items"][0]["seed"]["composerNameLatin"] = "Robert Schumann"
    payload["items"][0]["seed"]["workTitleLatin"] = "Piano Concerto in A minor, Op.54"
    payload["items"][0]["seed"]["catalogue"] = "Op.54"
    payload["items"][0]["seed"]["performanceDateText"] = "March 8, 1954 Budapest"
    payload["items"][0]["seed"]["credits"] = [
        {"role": "soloist", "displayName": "Sviatoslav Richter", "label": "Sviatoslav Richter"},
        {"role": "conductor", "displayName": "Janos Ferencsik", "label": "Janos Ferencsik"},
    ]
    request = CreateJobRequest.model_validate(payload)
    pipeline = RetrievalPipeline(source_provider=RedZoneProvider(), llm_client=None)

    result = asyncio.run(pipeline.retrieve(request.items[0]))

    candidate_urls = [candidate.url for candidate in result.link_candidates]
    assert "https://www.youtube.com/watch?v=good1" in candidate_urls
    assert "https://www.youtube.com/watch?v=wrongclara" not in candidate_urls
