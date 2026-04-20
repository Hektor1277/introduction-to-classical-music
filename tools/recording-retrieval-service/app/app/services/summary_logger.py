from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SummaryLogger:
    def __init__(self, log_dir: Path | None) -> None:
        self.log_dir = log_dir
        if self.log_dir is not None:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    def log(self, job_id: str, **payload: Any) -> None:
        if self.log_dir is None:
            return
        target = self.log_dir / f"{job_id}.log"
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
