from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import webbrowser
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from app.models.protocol import CreateJobRequest, HealthResponse
from app.services.http_sources import materials_root
from app.services.input_analysis import analyze_raw_text
from app.services.llm_client import (
    DualModelLlmClient,
    default_llm_config_path,
    ensure_runtime_llm_config,
    is_llm_configured,
    load_llm_config,
)
from app.services.orchestrator import JobOrchestrator, TERMINAL_JOB_STATUSES
from app.services.retrieval import StubRetriever
from app.services.source_profiles import PersonAliasLoader, ensure_orchestra_alias_file, ensure_person_alias_file

APP_VERSION = "0.1.0"
SERVICE_NAME = "recording-retrieval-service"


def profile_group_paths() -> dict[str, Path]:
    return {
        "high-quality": materials_root() / "high-quality.txt",
        "streaming": materials_root() / "streaming.txt",
        "llm-config": default_llm_config_path(),
        "orchestra-aliases": ensure_orchestra_alias_file(),
        "person-aliases": ensure_person_alias_file(),
    }


@dataclass(slots=True)
class ServiceProbe:
    occupied: bool
    is_ours: bool
    health_ok: bool
    ui_ok: bool


def iter_ui_root_candidates() -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    def add(path: Path) -> None:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            candidates.append(resolved)

    if getattr(sys, "frozen", False):
        if hasattr(sys, "_MEIPASS"):
            add(Path(sys._MEIPASS) / "app" / "ui")
        executable = getattr(sys, "executable", None)
        if executable:
            exe_dir = Path(executable).resolve().parent
            add(exe_dir / "_internal" / "app" / "ui")
            add(exe_dir / "app" / "ui")

    add(Path(str(files("app").joinpath("ui"))))
    return candidates


def resolve_ui_root() -> Path:
    checked: list[str] = []
    for candidate in iter_ui_root_candidates():
        checked.append(str(candidate))
        if (candidate / "index.html").is_file() and (candidate / "assets").is_dir():
            return candidate
    checked_paths = ", ".join(checked) if checked else "<none>"
    raise RuntimeError(f"UI assets not found. Checked: {checked_paths}")


def launch_path(path: Path) -> None:
    target = str(path.resolve())
    if os.name == "nt":
        os.startfile(target)  # type: ignore[attr-defined]
        return
    if sys.platform == "darwin":
        subprocess.Popen(["open", target])
        return
    subprocess.Popen(["xdg-open", target])


def profile_targets_payload() -> dict[str, dict[str, str]]:
    payload: dict[str, dict[str, str]] = {}
    key_map = {
        "high-quality": "highQuality",
        "streaming": "streaming",
        "llm-config": "llmConfig",
        "orchestra-aliases": "orchestraAliases",
        "person-aliases": "personAliases",
    }
    for group, path in profile_group_paths().items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if group == "llm-config":
            ensure_runtime_llm_config(path)
        elif not path.exists():
            path.write_text("", encoding="utf-8")
        payload[key_map[group]] = {
            "group": group,
            "path": str(path.resolve()),
            "directory": str(path.parent.resolve()),
        }
    return payload


def remember_person_aliases(loader: PersonAliasLoader, payload: dict[str, str], work_type_hint: str) -> None:
    role_map = {
        "orchestral": ("conductor", "ensemble"),
        "concerto": ("soloist", "conductor"),
        "opera_vocal": ("conductor", "singer"),
        "chamber_solo": ("performer", "ensemble"),
        "unknown": ("person", "ensemble"),
    }
    primary_role, secondary_role = role_map.get(work_type_hint, ("person", "ensemble"))
    pairs = [
        (primary_role, payload.get("primaryPerson", ""), payload.get("primaryPersonLatin", "")),
        (secondary_role, payload.get("secondaryPerson", ""), payload.get("secondaryPersonLatin", "")),
        ("composer", payload.get("composerName", ""), payload.get("composerNameLatin", "")),
        ("ensemble", payload.get("groupName", ""), payload.get("groupNameLatin", "")),
    ]
    for role, local_name, latin_name in pairs:
        local_value = str(local_name or "").strip()
        latin_value = str(latin_name or "").strip()
        if not local_value or not latin_value:
            continue
        if local_value == latin_value:
            continue
        loader.remember(role=role, values=[local_value, latin_value])


def apply_person_aliases(loader: PersonAliasLoader, payload: dict[str, str], work_type_hint: str) -> None:
    role_map = {
        "orchestral": ("conductor", "ensemble"),
        "concerto": ("soloist", "conductor"),
        "opera_vocal": ("conductor", "singer"),
        "chamber_solo": ("performer", "ensemble"),
        "unknown": ("person", "ensemble"),
    }
    primary_role, secondary_role = role_map.get(work_type_hint, ("person", "ensemble"))
    mappings = [
        ("primaryPerson", "primaryPersonLatin", primary_role),
        ("secondaryPerson", "secondaryPersonLatin", secondary_role),
        ("composerName", "composerNameLatin", "composer"),
    ]
    for local_key, latin_key, role in mappings:
        local_value = str(payload.get(local_key, "")).strip()
        latin_value = str(payload.get(latin_key, "")).strip()
        if local_value and not latin_value:
            expansion = next((item for item in loader.expand(local_value, role=role) if any("A" <= ch <= "z" for ch in item)), "")
            if expansion:
                payload[latin_key] = expansion
        if latin_value and not local_value:
            expansion = next((item for item in loader.expand(latin_value, role=role) if any("\u4e00" <= ch <= "\u9fff" for ch in item)), "")
            if expansion:
                payload[local_key] = expansion


def _is_port_occupied(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex((host, port)) == 0


def _fetch_text(url: str) -> tuple[int | None, str]:
    try:
        with urlopen(url, timeout=0.5) as response:
            payload = response.read().decode("utf-8", errors="replace")
            return response.status, payload
    except HTTPError as error:
        payload = error.read().decode("utf-8", errors="replace")
        return error.code, payload
    except URLError:
        return None, ""


def probe_existing_service(host: str, port: int) -> ServiceProbe:
    if not _is_port_occupied(host, port):
        return ServiceProbe(occupied=False, is_ours=False, health_ok=False, ui_ok=False)

    health_status, health_payload = _fetch_text(f"http://{host}:{port}/health")
    ui_status, _ = _fetch_text(f"http://{host}:{port}/")
    is_ours = False
    health_ok = False
    if health_status == 200:
        try:
            document = json.loads(health_payload)
        except json.JSONDecodeError:
            document = {}
        is_ours = document.get("service") == SERVICE_NAME and document.get("protocolVersion") == "v1"
        health_ok = is_ours

    return ServiceProbe(
        occupied=True,
        is_ours=is_ours,
        health_ok=health_ok,
        ui_ok=ui_status == 200,
    )


def handle_existing_service(mode: str, host: str, port: int) -> bool:
    probe = probe_existing_service(host, port)
    if not probe.occupied:
        return False

    url = f"http://{host}:{port}/"
    if mode == "service" and probe.is_ours and probe.health_ok:
        print(f"{SERVICE_NAME} is already running at {url}")
        return True

    if mode == "ui" and probe.is_ours and probe.ui_ok:
        print(f"{SERVICE_NAME} is already running at {url}. Opening the existing UI.")
        webbrowser.open(url)
        return True

    if probe.is_ours and probe.health_ok:
        raise RuntimeError(
            f"An existing {SERVICE_NAME} instance is running on {host}:{port}, but its UI is unavailable. "
            "Stop that process and retry."
        )

    raise RuntimeError(
        f"Port {host}:{port} is already in use by another process. "
        "Stop the existing process or choose another port."
    )


def create_app(*, retriever: StubRetriever | None = None) -> FastAPI:
    app = FastAPI(title="Recording Retrieval Service", version=APP_VERSION)
    orchestrator = JobOrchestrator(log_dir=Path("logs"), retriever=retriever)
    app.state.orchestrator = orchestrator
    llm_config = load_llm_config()
    llm_client = DualModelLlmClient(llm_config) if is_llm_configured(llm_config) else None
    app.state.llm_client = llm_client
    person_alias_loader = PersonAliasLoader()
    app.state.person_alias_loader = person_alias_loader

    ui_root = resolve_ui_root()
    asset_root = ui_root / "assets"
    app.mount("/assets", StaticFiles(directory=str(asset_root)), name="assets")

    @app.get("/", response_class=FileResponse)
    async def index() -> FileResponse:
        return FileResponse(ui_root / "index.html")

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse()

    @app.get("/favicon.ico")
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/ui/profile-targets")
    async def ui_profile_targets() -> dict[str, dict[str, str]]:
        return profile_targets_payload()

    @app.post("/ui/analyze-text")
    async def ui_analyze_text(payload: dict) -> dict[str, str]:
        raw_text = str(payload.get("rawText", ""))
        work_type_hint = str(payload.get("workTypeHint", "unknown"))
        result = analyze_raw_text(
            raw_text=raw_text,
            work_type_hint=work_type_hint,
        )
        apply_person_aliases(person_alias_loader, result, work_type_hint)
        if llm_client is not None and raw_text.strip():
            allow_realtime_analysis = getattr(llm_client, "allow_realtime_analysis", True)
            if allow_realtime_analysis:
                try:
                    llm_result = await llm_client.analyze_input(raw_text, work_type_hint)
                except Exception:
                    llm_result = {}
                for key, value in llm_result.items():
                    if key in result and not str(result.get(key, "")).strip() and str(value or "").strip():
                        result[key] = str(value).strip()
        apply_person_aliases(person_alias_loader, result, work_type_hint)
        remember_person_aliases(person_alias_loader, result, work_type_hint)
        return result

    @app.post("/ui/open-profile/{group}")
    async def ui_open_profile(group: str) -> dict[str, str]:
        path = profile_group_paths().get(group)
        if path is None:
            raise HTTPException(status_code=404, detail="Unknown profile group")
        launch_path(path)
        return {"opened": str(path.resolve())}

    @app.post("/v1/jobs", status_code=202)
    async def create_job(request: CreateJobRequest) -> dict:
        accepted = orchestrator.create_job(request)
        return accepted.model_dump(by_alias=True)

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str) -> dict:
        status = orchestrator.get_status(job_id)
        if status is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return status.model_dump(by_alias=True)

    @app.get("/v1/jobs/{job_id}/results")
    async def get_results(job_id: str) -> dict:
        status = orchestrator.get_status(job_id)
        if status is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if status.status not in TERMINAL_JOB_STATUSES:
            raise HTTPException(status_code=409, detail="Job is not finished yet")
        results = orchestrator.get_results(job_id)
        if results is None:
            raise HTTPException(status_code=409, detail="Job results are not ready")
        return results.model_dump(by_alias=True)

    @app.post("/v1/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str) -> dict:
        status = orchestrator.cancel_job(job_id)
        if status is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return status.model_dump(by_alias=True)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Recording Retrieval Service")
    parser.add_argument("--mode", choices=["service", "ui"], default="service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4780)
    args = parser.parse_args()

    try:
        if handle_existing_service(args.mode, args.host, args.port):
            return

        if args.mode == "ui":
            url = f"http://{args.host}:{args.port}/"
            threading.Timer(0.8, lambda: webbrowser.open(url)).start()

        uvicorn.run(create_app(), host=args.host, port=args.port)
    except Exception as error:
        print(f"[{SERVICE_NAME}] startup failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
