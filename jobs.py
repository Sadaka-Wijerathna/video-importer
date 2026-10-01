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

    def add_log(self, msg: str):
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        self.logs.append(entry)
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]
        print(entry, flush=True)


# Global job store — one job per admin_id at a time
_jobs: dict[str, ImportJob] = {}


def get_job(admin_id: str) -> Optional[ImportJob]:
    return _jobs.get(admin_id)


def create_job(job_id: str, admin_id: str) -> ImportJob:
    job = ImportJob(job_id=job_id, admin_id=admin_id)
    _jobs[admin_id] = job
    return job


def remove_job(admin_id: str):
    _jobs.pop(admin_id, None)
