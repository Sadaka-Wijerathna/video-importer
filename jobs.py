# jobs.py — In-memory job state manager
import asyncio
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ImportJob:
    job_id: str
    admin_id: str
    status: str = "running"       # running | stopped | completed | failed
    progress: int = 0
    total: int = 0
    message: str = "Initializing..."
    logs: list[str] = field(default_factory=list)
    stop_flag: bool = False
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    # Composite key fields — set after creation for multi-job support
    source_chat: str = ""
    target_bot: str = ""

    def add_log(self, msg: str):
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        self.logs.append(entry)
        if len(self.logs) > 1000:
            self.logs = self.logs[-1000:]
        print(entry, flush=True)


# Global job store.
# Key = admin_id for backward-compat with /stop-job and /job-status (uses admin_id only).
# Multiple concurrent jobs per admin are allowed when source+target differ.
# Composite key: "{admin_id}::{source_chat}::{target_bot}"
_jobs: dict[str, ImportJob] = {}


def _composite_key(admin_id: str, source_chat: str = "", target_bot: str = "") -> str:
    """Return composite key for multi-job lookup."""
    if source_chat and target_bot:
        return f"{admin_id}::{source_chat}::{target_bot}"
    return admin_id


def get_job(admin_id: str, source_chat: str = "", target_bot: str = "") -> Optional[ImportJob]:
    """Look up by composite key first, fall back to admin_id for legacy callers."""
    if source_chat and target_bot:
        key = _composite_key(admin_id, source_chat, target_bot)
        if key in _jobs:
            return _jobs[key]
    # Fallback: return any running job for this admin (legacy /stop-job behavior)
    return _jobs.get(admin_id) or next(
        (j for j in _jobs.values() if j.admin_id == admin_id), None
    )


def create_job(job_id: str, admin_id: str, source_chat: str = "", target_bot: str = "") -> ImportJob:
    job = ImportJob(job_id=job_id, admin_id=admin_id, source_chat=source_chat, target_bot=target_bot)
    key = _composite_key(admin_id, source_chat, target_bot)
    _jobs[key] = job
    return job


def get_running_job(admin_id: str, source_chat: str, target_bot: str) -> Optional[ImportJob]:
    """Find a running job for the exact same source→target pipeline."""
    key = _composite_key(admin_id, source_chat, target_bot)
    job = _jobs.get(key)
    return job if job and job.status == "running" else None


def get_all_jobs() -> list[ImportJob]:
    return list(_jobs.values())


def remove_job(admin_id: str, source_chat: str = "", target_bot: str = ""):
    key = _composite_key(admin_id, source_chat, target_bot)
    _jobs.pop(key, None)
    # Also clean up legacy key
    _jobs.pop(admin_id, None)

