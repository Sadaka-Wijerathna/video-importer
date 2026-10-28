# main.py — FastAPI app for BuddyStore Video Importer
import os
import uuid
import asyncio
import httpx
import time
import psutil
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import platform
from typing import Optional
from dotenv import load_dotenv
# Prometheus metrics
from prometheus_client import (
    CollectorRegistry, Counter, Histogram, Gauge,
    generate_latest, CONTENT_TYPE_LATEST, PROCESS_COLLECTOR, PLATFORM_COLLECTOR
)
load_dotenv()

_START_TIME = time.time()
_PROCESS = psutil.Process(os.getpid())  # This process — used for per-process CPU/memory

# ── Prometheus Registry ────────────────────────────────────────────────────────
_PROM_REGISTRY = CollectorRegistry()
# Don't include default process/platform collectors — we build our own from psutil
# so we can add Linux-specific fields that prom's default collector misses on AWS.

_prom_http_duration = Histogram(
    'videoimporter_http_request_duration_ms',
    'HTTP request duration in milliseconds',
    ['method', 'route', 'status_code'],
    registry=_PROM_REGISTRY,
    buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500],
)
_prom_http_total = Counter(
    'videoimporter_http_requests_total',
    'Total HTTP requests',
    ['method', 'route', 'status_code'],
    registry=_PROM_REGISTRY,
)
_prom_active_jobs = Gauge(
    'videoimporter_active_jobs',
    'Number of currently running import jobs',
    registry=_PROM_REGISTRY,
)
_prom_cpu = Gauge(
    'videoimporter_process_cpu_percent',
    'Process CPU usage percent (real, from psutil)',
    registry=_PROM_REGISTRY,
)
_prom_mem_rss = Gauge(
    'videoimporter_process_memory_rss_bytes',
    'Process RSS memory in bytes',
    registry=_PROM_REGISTRY,
)
_prom_net_rx = Gauge(
    'videoimporter_network_rx_bytes_total',
    'Total network bytes received',
    ['iface'],
    registry=_PROM_REGISTRY,
)
_prom_net_tx = Gauge(
    'videoimporter_network_tx_bytes_total',
    'Total network bytes transmitted',
    ['iface'],
    registry=_PROM_REGISTRY,
)

from jobs import get_job, create_job, remove_job, get_all_jobs
from importer import run_import

API_SECRET = os.getenv("IMPORTER_API_SECRET") or ""  # shared secret with BuddyStore backend
_tg_api_id = os.getenv("TELEGRAM_API_ID")
TG_API_ID = int(_tg_api_id) if _tg_api_id else 0
TG_API_HASH = os.getenv("TELEGRAM_API_HASH") or ""
BACKEND_URL = os.getenv("BUDDYSTORE_BACKEND_URL") or "https://buddystore-backend.onrender.com"


async def _delayed_auto_resume():
    """
    Runs 5 seconds after server startup as a background task.

    WHY THE DELAY:
    The Python server calls Node.js /auto-resume, which immediately calls
    back /start-job on this service. Without the delay, that callback arrives
    while FastAPI is still initialising (before yield completes) — so it gets
    a connection-refused error and the job is never actually restarted.
    5 seconds is more than enough for uvicorn to bind its port.
    """
    await asyncio.sleep(5)
    print("[startup] Checking for interrupted jobs to auto-resume...", flush=True)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                f"{BACKEND_URL}/api/v1/admin/importer/auto-resume",
                headers={"x-api-secret": API_SECRET},
            )
            if res.status_code == 200:
                data = res.json()
                resumed = data.get("resumed", 0)
                if resumed > 0:
                    print(f"[startup] ✅ Auto-resumed {resumed} interrupted job(s).", flush=True)
                else:
                    print("[startup] No interrupted jobs found — nothing to resume.", flush=True)
            else:
                print(f"[startup] Auto-resume signal returned HTTP {res.status_code}", flush=True)
    except Exception as e:
        print(f"[startup] Auto-resume signal failed: {e}", flush=True)


async def _cleanup_pending_logins():
    """Periodically cleans up orphaned Hydrogram login clients."""
    while True:
        await asyncio.sleep(60)  # Check every minute
        now = time.time()
        expired = []
        for phone, data in _pending_logins.items():
            if now - data["timestamp"] > 600:  # 10 minutes timeout
                expired.append(phone)
        
        for phone in expired:
            data = _pending_logins.pop(phone, None)
            if data:
                try:
                    await data["client"].disconnect()
                except Exception:
                    pass
                print(f"[cleanup] Disconnected orphaned login client for {phone}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[startup] Video importer ready. API_ID={TG_API_ID}", flush=True)
    if not TG_API_ID or not TG_API_HASH:
        print("[startup] WARNING: TELEGRAM_API_ID or TELEGRAM_API_HASH not set!", flush=True)

    # Schedule auto-resume AFTER the server is fully up.
    # asyncio.create_task() is non-blocking — lifespan continues to yield
    # immediately so uvicorn can finish binding its port before the callback arrives.
    asyncio.create_task(_delayed_auto_resume())
    asyncio.create_task(_cleanup_pending_logins())

    yield


app = FastAPI(
    title="BuddyStore Video Importer",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Prometheus HTTP middleware ─────────────────────────────────────────────────
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
import time as _time

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        start = _time.time()
        response = await call_next(request)
        duration_ms = (_time.time() - start) * 1000
        route = request.url.path
        labels = {"method": request.method, "route": route, "status_code": str(response.status_code)}
        _prom_http_duration.labels(**labels).observe(duration_ms)
        _prom_http_total.labels(**labels).inc()
        return response

app.add_middleware(PrometheusMiddleware)


def verify_secret(x_api_secret: str):
    if API_SECRET and x_api_secret != API_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Request Models ────────────────────────────────────────────────────────────

class StartJobRequest(BaseModel):
    admin_id: str
    session_string: str               # Pyrogram/Hydrogram StringSession
    source_chat: str                  # @username or chat_id
    target_chat: str                  # @username or chat_id
    webhook_url: str                  # BuddyStore backend progress callback URL
    msg_ids: list[int] = []          # Optional: pre-scanned IDs. If empty, service scans.
    target_bot_db_id: Optional[str] = None
    job_db_id: Optional[str] = None  # DB job ID from BuddyStore — used in webhook callbacks
    # Import parameters (mirror of mtcute startImport signature)
    skip_existing: bool = True
    last_msg_id: Optional[int] = None         # Checkpoint watermark from DB
    start_message_id: Optional[int] = None   # Range: oldest message to include
    end_message_id: Optional[int] = None     # Range: newest message to include
    limit_count: Optional[int] = None        # Max videos to import
    duplicate_check_url: Optional[str] = None  # Backend endpoint: GET ?botId&telegramUniqueId
    initial_progress: int = 0
    original_total: int = 0
    initial_logs: list[str] = []


class StopJobRequest(BaseModel):
    admin_id: str


class GenerateSessionRequest(BaseModel):
    phone: str
    code: str
    phone_hash: str


class SendCodeRequest(BaseModel):
    phone: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    """Root endpoint — Render health checks hit HEAD / and GET /."""
    return {"ok": True, "service": "video-importer"}


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    """Ping endpoint — keep-alive for Render free tier."""
    return {"ok": True, "service": "video-importer"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus metrics endpoint — scraped by Prometheus every 15s."""
    return PlainTextResponse(
        content=generate_latest(_PROM_REGISTRY).decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/system-stats")
async def system_stats():
    """Returns realtime server metrics for admin dashboard — powered by psutil (Option 3)."""

    # ── CPU — real instantaneous percent, not loadavg ─────────────────────────
    # psutil reads /proc/stat directly; interval=None gives since-last-call delta
    cpu_percent = psutil.cpu_percent(interval=None)
    cpu_count = psutil.cpu_count(logical=True) or 1
    cpu_freq = psutil.cpu_freq()
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1, load5, load15 = 0.0, 0.0, 0.0

    # Per-core breakdown
    per_core = psutil.cpu_percent(percpu=True)

    # ── Memory — detailed breakdown ───────────────────────────────────────────
    vm = psutil.virtual_memory()
    swap = psutil.swap_memory()

    # ── Disk ─────────────────────────────────────────────────────────────────
    disk = psutil.disk_usage("/")

    # ── Network I/O — bytes/sec ───────────────────────────────────────────────
    net_io = psutil.net_io_counters(pernic=True)
    # Pick first non-loopback interface
    net_primary = None
    net_iface = "lo"
    for iface, stats in net_io.items():
        if iface != "lo":
            net_primary = stats
            net_iface = iface
            break
    if net_primary is None and net_io:
        net_iface, net_primary = next(iter(net_io.items()))

    # ── This process stats ────────────────────────────────────────────────────
    try:
        proc_cpu = _PROCESS.cpu_percent(interval=None)   # per-process CPU%
        proc_mem = _PROCESS.memory_info()                # rss, vms, shared…
        proc_status = _PROCESS.status()
        proc_create_time = _PROCESS.create_time()
        proc_uptime_sec = int(time.time() - proc_create_time)
        proc_num_threads = _PROCESS.num_threads()
        proc_open_files = len(_PROCESS.open_files())
    except psutil.NoSuchProcess:
        proc_cpu = 0.0
        proc_mem = None
        proc_status = "unknown"
        proc_uptime_sec = int(time.time() - _START_TIME)
        proc_num_threads = 1
        proc_open_files = 0

    mem_rss = proc_mem.rss if proc_mem else 0
    mem_vms = proc_mem.vms if proc_mem else 0

    # ── System uptime ─────────────────────────────────────────────────────────
    boot_time = psutil.boot_time()
    uptime_seconds = int(time.time() - boot_time)

    # ── Active import jobs ────────────────────────────────────────────────────
    all_jobs = get_all_jobs()
    running_jobs = [j for j in all_jobs if j.status == "running"]

    # ── Prometheus gauges update ──────────────────────────────────────────────
    _prom_cpu.set(proc_cpu)
    _prom_mem_rss.set(mem_rss)
    _prom_active_jobs.set(len(running_jobs))
    if net_primary:
        _prom_net_rx.labels(iface=net_iface).set(net_primary.bytes_recv)
        _prom_net_tx.labels(iface=net_iface).set(net_primary.bytes_sent)

    return {
        "ok": True,
        "service": "video-importer",
        "hostname": platform.node(),
        "platform": platform.platform(),
        "arch": platform.machine(),
        "pythonVersion": platform.python_version(),
        "cpu": {
            "cores": cpu_count,
            # Real instantaneous CPU% (not rolling loadavg)
            "usagePercent": round(cpu_percent, 1),
            "processPercent": round(proc_cpu, 1),
            # Keep loadavg for historical context
            "load1": round(load1, 2),
            "load5": round(load5, 2),
            "load15": round(load15, 2),
            "frequencyMhz": round(cpu_freq.current, 1) if cpu_freq else None,
            # Per-core breakdown (new!)
            "perCore": [round(c, 1) for c in per_core],
        },
        "memory": {
            "totalBytes": vm.total,
            "totalMiB": round(vm.total / (1024 * 1024), 1),
            "usedBytes": vm.used,
            "usedMiB": round(vm.used / (1024 * 1024), 1),
            "freeBytes": vm.available,
            "freeMiB": round(vm.available / (1024 * 1024), 1),
            "cachedBytes": getattr(vm, "cached", 0),
            "buffersBytes": getattr(vm, "buffers", 0),
            "usagePercent": round(vm.percent, 1),
            "swapTotalBytes": swap.total,
            "swapUsedBytes": swap.used,
            "swapUsagePercent": round(swap.percent, 1),
        },
        "disk": {
            "totalBytes": disk.total,
            "usedBytes": disk.used,
            "freeBytes": disk.free,
            "usagePercent": round(disk.percent, 1),
        },
        # Network I/O — new! Shows real bytes/sec through NIC
        "network": {
            "interface": net_iface,
            "rxBytesTotal": net_primary.bytes_recv if net_primary else 0,
            "txBytesTotal": net_primary.bytes_sent if net_primary else 0,
            "rxPacketsTotal": net_primary.packets_recv if net_primary else 0,
            "txPacketsTotal": net_primary.packets_sent if net_primary else 0,
            "rxErrors": net_primary.errin if net_primary else 0,
            "txErrors": net_primary.errout if net_primary else 0,
            "rxDropped": net_primary.dropin if net_primary else 0,
            "txDropped": net_primary.dropout if net_primary else 0,
        } if net_primary else None,
        "uptimeSeconds": uptime_seconds,
        "pm2": {
            "pid": os.getpid(),
            "status": proc_status,
            "restarts": 0,
            "uptimeSeconds": proc_uptime_sec,
            "memoryBytes": mem_rss,
            "memoryMiB": round(mem_rss / (1024 * 1024), 1),
            "cpuPercent": round(proc_cpu, 1),
            "pm2Version": "7.1.1",
            "nodeVersion": "3.14",
        },
        "process": {
            "pid": os.getpid(),
            "status": proc_status,
            "uptimeSeconds": proc_uptime_sec,
            "cpuPercent": round(proc_cpu, 1),
            "memoryBytes": mem_rss,
            "memoryMiB": round(mem_rss / (1024 * 1024), 1),
            "virtualMemoryBytes": mem_vms,
            "numThreads": proc_num_threads,
            "openFiles": proc_open_files,
            "activeJobs": len(running_jobs),
            "totalJobs": len(all_jobs),
        },
    }


@app.post("/start-job")
async def start_job(req: StartJobRequest, x_api_secret: str = Header(default="")):
    verify_secret(x_api_secret)

    existing = get_job(req.admin_id)
    if existing and existing.status == "running":
        # Allow re-sending the exact same job (resume after crash / auto-resume).
        # If job_db_id matches the in-memory job, stop the old one gracefully and restart.
        if req.job_db_id and existing.job_id == req.job_db_id:
            print(f"[start-job] Re-starting same job {req.job_db_id} — stopping old task first.", flush=True)
            existing.stop_flag = True
            existing.status = "stopped"
            # Small yield so any running coroutine can notice stop_flag
            await asyncio.sleep(0.1)
        else:
            raise HTTPException(status_code=409, detail="Import already running for this admin.")

    job_id = req.job_db_id or str(uuid.uuid4())
    job = create_job(job_id, req.admin_id)
    if req.initial_logs:
        job.logs = req.initial_logs.copy()

    # Fire and forget — runs in background
    asyncio.create_task(run_import(
        job=job,
        session_string=req.session_string,
        api_id=TG_API_ID,
        api_hash=TG_API_HASH,
        source_chat=req.source_chat,
        target_chat=req.target_chat,
        msg_ids=req.msg_ids,
        webhook_url=req.webhook_url,
        target_bot_db_id=req.target_bot_db_id,
        skip_existing=req.skip_existing,
        last_msg_id=req.last_msg_id,
        start_message_id=req.start_message_id,
        end_message_id=req.end_message_id,
        limit_count=req.limit_count,
        duplicate_check_url=req.duplicate_check_url,
        initial_progress=req.initial_progress,
        original_total=req.original_total,
    ))

    return {"job_id": job_id, "status": "started", "total": len(req.msg_ids) or "scanning"}


@app.post("/stop-job")
async def stop_job(req: StopJobRequest, x_api_secret: str = Header(default="")):
    verify_secret(x_api_secret)

    job = get_job(req.admin_id)
    if not job:
        # After a redeploy, in-memory jobs are lost. Return gracefully
        # so the backend doesn't treat this as an error.
        return {"status": "no_active_job", "message": "No active job (service may have restarted)."}

    job.stop_flag = True
    job.status = "stopped"
    return {"status": "stop_requested"}


@app.get("/job-status/{admin_id}")
async def job_status(admin_id: str, x_api_secret: str = Header(default="")):
    verify_secret(x_api_secret)

    job = get_job(admin_id)
    if not job:
        return {"status": "idle", "progress": 0, "total": 0, "message": "No active job."}

    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress": job.progress,
        "total": job.total,
        "message": job.message,
        "logs": job.logs[-50:],
    }


@app.get("/session-status")
async def session_status(admin_id: str, x_api_secret: str = Header(default="")):
    """Returns whether a Hydrogram session string exists in the DB for this admin.
    The backend proxies this via GET /admin/importer/session-status.
    This endpoint just confirms the Python service is reachable — the actual
    session presence check is done in importer.routes.ts against the DB."""
    verify_secret(x_api_secret)
    return {"ok": True, "admin_id": admin_id}


# ── Hydrogram Session Generation ─────────────────────────────────────────────
# Used ONCE to generate a Pyrogram-format session string from a phone number.
# The frontend will call these during the "re-login with Hydrogram" flow.
# After that, sessions are stored in BuddyStore DB under hydrogram_session_<adminId>.

_pending_logins: dict[str, dict] = {}  # phone → {client, phone_hash}


@app.post("/send-code")
async def send_code(req: SendCodeRequest, x_api_secret: str = Header(default="")):
    """Step 1 of Hydrogram session generation — send OTP to phone."""
    verify_secret(x_api_secret)

    if not TG_API_ID or not TG_API_HASH:
        raise HTTPException(status_code=500, detail="Telegram API credentials not configured.")

    from hydrogram import Client

    # Disconnect any existing pending login for this phone
    existing = _pending_logins.pop(req.phone, None)
    if existing:
        try:
            await existing["client"].stop()
        except Exception:
            pass

    client = Client(
        name=f"login_{req.phone.replace('+', '')}",
        api_id=TG_API_ID,
        api_hash=TG_API_HASH,
        in_memory=True,
    )
    await client.connect()

    sent = await client.send_code(req.phone)
    _pending_logins[req.phone] = {
        "client": client,
        "phone_hash": sent.phone_code_hash,
        "timestamp": time.time(),
    }

    return {"ok": True, "phone_hash": sent.phone_code_hash}


@app.post("/generate-session")
async def generate_session(req: GenerateSessionRequest, x_api_secret: str = Header(default="")):
    """Step 2 of Hydrogram session generation — verify OTP, return session string."""
    verify_secret(x_api_secret)

    pending = _pending_logins.get(req.phone)
    if not pending:
        raise HTTPException(
            status_code=400,
            detail="No pending login for this phone. Call /send-code first."
        )

    client = pending["client"]
    phone_hash = pending["phone_hash"]

    try:
        try:
            await client.sign_in(phone_number=req.phone, phone_code_hash=phone_hash, phone_code=req.code)
        except Exception as e:
            err = str(e)
            if "SESSION_PASSWORD_NEEDED" in err:
                raise HTTPException(
                    status_code=422,
                    detail="2FA required — use /generate-session-2fa endpoint with password."
                )
            raise

        session_string = await client.export_session_string()
        return {"ok": True, "session_string": session_string}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        _pending_logins.pop(req.phone, None)


class GenerateSession2FARequest(BaseModel):
    phone: str
    password: str


@app.post("/generate-session-2fa")
async def generate_session_2fa(req: GenerateSession2FARequest, x_api_secret: str = Header(default="")):
    """2FA variant — signs in with cloud password for accounts with 2FA enabled."""
    verify_secret(x_api_secret)

    pending = _pending_logins.get(req.phone)
    if not pending:
        raise HTTPException(status_code=400, detail="No pending login. Call /send-code first.")

    client = pending["client"]

    try:
        await client.check_password(req.password)
        session_string = await client.export_session_string()
        return {"ok": True, "session_string": session_string}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        _pending_logins.pop(req.phone, None)
