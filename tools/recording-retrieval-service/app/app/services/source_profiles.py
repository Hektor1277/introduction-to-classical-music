from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class SourceProfileEntry:
    url: str
    is_chinese: bool = False


@dataclass(slots=True)
class SourceProfileSet:
    high_quality: list[SourceProfileEntry]
    streaming: list[SourceProfileEntry]


def materials_root() -> Path:
    return Path(__file__).resolve().parents[2] / "materials" / "source-profiles"


def default_orchestra_alias_path() -> Path:
    return materials_root() / "orchestra-abbreviations.txt"


def legacy_orchestra_alias_path() -> Path:
    return materials_root() / "Orchestra Abbreviation Comparison.txt"


def default_person_alias_path() -> Path:
    return materials_root() / "person-name-aliases.txt"


def ensure_orchestra_alias_file() -> Path:
    runtime_path = default_orchestra_alias_path()
    if runtime_path.is_file():
        return runtime_path

    legacy_path = legacy_orchestra_alias_path()
    if legacy_path.is_file():
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(legacy_path, runtime_path)
        return runtime_path

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text("", encoding="utf-8")
    return runtime_path


def ensure_person_alias_file() -> Path:
    runtime_path = default_person_alias_path()
    if runtime_path.is_file():
        return runtime_path

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        "\n".join(
            [
                "# 人物姓名映射文档",
                "# 用法：",
                "# 1. 使用 #section-name 定义角色分组，例如 #global、#conductor、#soloist、#composer、#pianist。",
                "# 2. 每行一个映射组，使用 = 连接不同语言、不同译名、不同写法。",
                "# 3. 建议按“中文常用名 = 中文别名 = Latin/原文短名 = Latin/原文全名”填写。",
                "# 4. 系统会双向读取：输入中文可展开 Latin/原文，输入 Latin/原文也可回查中文或缩写。",
                "# 5. #global 中的映射适用于所有角色；角色分组中的映射会在对应角色里优先使用。",
                "",
                "#global",
                "# 例：西贝柳斯 = 西贝留士 = Sibelius = Jean Sibelius",
                "",
                "#conductor",
                "# 例：蒙都 = 蒙特 = Monteux = Pierre Monteux",
                "",
                "#soloist",
                "# 例：安妮·费舍尔 = Annie Fischer",
                "",
                "#composer",
                "# 例：舒曼 = Robert Schumann",
                "",
                "#pianist",
                "",
                "#violinist",
                "",
                "#soprano",
                "",
                "#tenor",
                "",
                "#baritone",
                "",
                "#ensemble",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return runtime_path


class OrchestraAliasLoader:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or ensure_orchestra_alias_file()
        self._abbreviation_map: dict[str, list[str]] = {}
        self._reverse_map: dict[str, list[str]] = {}
        self._load()

    def expand(self, value: str) -> list[str]:
        normalized = compact(value)
        if not normalized:
            return []

        results = [normalized]
        key_upper = normalized.upper()
        for item in self._abbreviation_map.get(key_upper, []):
            if item not in results:
                results.append(item)

        key_lower = normalized.lower()
        for item in self._reverse_map.get(key_lower, []):
            if item not in results:
                results.append(item)
        return results

    def _load(self) -> None:
        if not self._path.is_file():
            return
        for raw_line in self._path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            left, right = [part.strip() for part in line.split("=", 1)]
            if not left or not right:
                continue
            abbreviation = left
            expansions = [compact(part) for part in right.split("=") if compact(part)]
            if not expansions:
                continue
            bucket = self._abbreviation_map.setdefault(abbreviation.upper(), [])
            for expansion in expansions:
                if expansion not in bucket:
                    bucket.append(expansion)
                reverse_bucket = self._reverse_map.setdefault(expansion.lower(), [])
                if abbreviation not in reverse_bucket:
                    reverse_bucket.append(abbreviation)


class PersonAliasLoader:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or ensure_person_alias_file()
        self._section_map: dict[str, dict[str, list[str]]] = {}
        self._load()

    def expand(self, value: str, *, role: str | None = None) -> list[str]:
        normalized = compact(value)
        if not normalized:
            return []

        results = [normalized]
        seen = {normalized.lower()}
        roles = []
        if compact(role):
            roles.append(compact(role).lower())
        if "global" not in roles:
            roles.append("global")

        for role_name in roles:
            section = self._section_map.get(role_name, {})
            for alias in section.get(normalized.lower(), []):
                lowered = alias.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                results.append(alias)
        return results

    def remember(self, *, role: str | None = None, values: list[str]) -> None:
        cleaned = dedupe_alias_values(values)
        if len(cleaned) < 2:
            return

        role_name = compact(role).lower() or "global"
        path = self._path
        if not path.is_file():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        raw_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

        groups = self._iter_section_groups(raw_lines, role_name)
        lowered_cleaned = {value.lower() for value in cleaned}
        for existing in groups:
            lowered_existing = {value.lower() for value in existing}
            if lowered_existing.intersection(lowered_cleaned):
                merged = dedupe_alias_values([*existing, *cleaned])
                self._replace_group(raw_lines, role_name, existing, merged)
                path.write_text("\n".join(raw_lines).rstrip() + "\n", encoding="utf-8")
                self._load()
                return

        header = f"#{role_name}"
        if header not in raw_lines:
            if raw_lines and raw_lines[-1].strip():
                raw_lines.append("")
            raw_lines.append(header)
        insert_at = self._section_insert_index(raw_lines, role_name)
        raw_lines.insert(insert_at, " = ".join(cleaned))
        path.write_text("\n".join(raw_lines).rstrip() + "\n", encoding="utf-8")
        self._load()

    def _load(self) -> None:
        self._section_map = {}
        if not self._path.is_file():
            return

        current_section = "global"
        for raw_line in self._path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if is_section_header_line(line):
                section_name = line[1:].strip().lower()
                if section_name:
                    current_section = section_name
                continue
            if line.startswith("#"):
                continue
            values = dedupe_alias_values([part.strip() for part in line.split("=")])
            if len(values) < 2:
                continue
            section = self._section_map.setdefault(current_section, {})
            for value in values:
                bucket = section.setdefault(value.lower(), [])
                for candidate in values:
                    if candidate.lower() == value.lower() or candidate in bucket:
                        continue
                    bucket.append(candidate)

    def _iter_section_groups(self, lines: list[str], role_name: str) -> list[list[str]]:
        groups: list[list[str]] = []
        current_section = "global"
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            if is_section_header_line(line):
                current_section = line[1:].strip().lower() or current_section
                continue
            if current_section != role_name or line.startswith("#"):
                continue
            values = dedupe_alias_values([part.strip() for part in line.split("=")])
            if len(values) >= 2:
                groups.append(values)
        return groups

    def _section_insert_index(self, lines: list[str], role_name: str) -> int:
        current_section = "global"
        section_start = len(lines)
        for index, raw_line in enumerate(lines):
            line = raw_line.strip()
            if is_section_header_line(line):
                current_section = line[1:].strip().lower() or current_section
                if current_section == role_name:
                    section_start = index + 1
                    break
        insert_at = len(lines)
        for index in range(section_start, len(lines)):
            line = lines[index].strip()
            if is_section_header_line(line):
                insert_at = index
                break
        return insert_at

    def _replace_group(
        self,
        lines: list[str],
        role_name: str,
        existing_group: list[str],
        merged_group: list[str],
    ) -> None:
        current_section = "global"
        for index, raw_line in enumerate(lines):
            line = raw_line.strip()
            if is_section_header_line(line):
                current_section = line[1:].strip().lower() or current_section
                continue
            if current_section != role_name or line.startswith("#"):
                continue
            values = dedupe_alias_values([part.strip() for part in line.split("=")])
            if values == existing_group:
                lines[index] = " = ".join(merged_group)
                return


class SourceProfileLoader:
    def __init__(self, root: Path) -> None:
        self._root = root

    def load(self, *, category: str, tags: list[str]) -> SourceProfileSet:
        return SourceProfileSet(
            high_quality=self._load_group("high-quality", category=category, tags=tags),
            streaming=self._load_group("streaming", category=category, tags=tags),
        )

    def _load_group(self, group: str, *, category: str, tags: list[str]) -> list[SourceProfileEntry]:
        ordered_names = ["global", category, *tags]
        merged: list[SourceProfileEntry] = []
        seen: set[str] = set()

        for item in self._read_single_file_sections(group, ordered_names):
            key = item.url.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)

        if merged:
            return merged

        for name in ordered_names:
            if not name:
                continue
            path = self._root / group / f"{name}.txt"
            if not path.is_file():
                continue
            for item in self._read_legacy_profile(path):
                key = item.url.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
        return merged

    def _read_single_file_sections(self, group: str, ordered_names: list[str]) -> list[SourceProfileEntry]:
        path = self._root / f"{group}.txt"
        if not path.is_file():
            return []

        section_map: dict[str, list[SourceProfileEntry]] = {}
        current_section = "global"
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if is_section_header_line(line):
                section_name = line[1:].strip().lower()
                if section_name:
                    current_section = section_name
                    section_map.setdefault(current_section, [])
                continue
            if line.startswith("#"):
                continue
            entry = parse_source_profile_entry(line)
            if entry is None:
                continue
            section_map.setdefault(current_section, []).append(entry)

        merged: list[SourceProfileEntry] = []
        for name in ordered_names:
            merged.extend(section_map.get(name.lower(), []))
        return merged

    def _read_legacy_profile(self, path: Path) -> list[SourceProfileEntry]:
        entries: list[SourceProfileEntry] = []
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            entry = parse_source_profile_entry(line)
            if entry is not None:
                entries.append(entry)
        return entries


def parse_source_profile_entry(line: str) -> SourceProfileEntry | None:
    normalized = compact(line)
    if not normalized or normalized.startswith("#"):
        return None

    is_chinese = False
    if normalized.startswith("[") and "]" in normalized:
        marker, normalized = normalized.split("]", 1)
        flags = {flag.strip().lower() for flag in marker[1:].split(",")}
        is_chinese = "zh" in flags or "cn" in flags or "chinese" in flags
        normalized = compact(normalized)

    if not normalized:
        return None
    return SourceProfileEntry(url=normalized, is_chinese=is_chinese)


def compact(value: str | None) -> str:
    return str(value or "").strip()


def is_section_header_line(value: str) -> bool:
    return value.startswith("#") and not value.startswith("##") and not value.startswith("# ")


def dedupe_alias_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values:
        normalized = compact(re.sub(r"\s+", " ", value))
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(normalized)
    return items
