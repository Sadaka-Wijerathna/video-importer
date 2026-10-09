# main.py — FastAPI app for BuddyStore Video Importer
import os
import uuid
import asyncio
import httpx
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from jobs import get_job, create_job, remove_job
from importer import run_import

load_dotenv()

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

@app.get("/")
async def root():
    """Root endpoint — Render health checks hit HEAD / and GET /."""
    return {"ok": True, "service": "video-importer"}


@app.get("/health")
async def health():
    """Ping endpoint — keep-alive for Render free tier."""
    return {"ok": True, "service": "video-importer"}


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
