# main.py — FastAPI app for BuddyStore Video Importer
import os
import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from jobs import get_job, create_job, remove_job
from importer import run_import

load_dotenv()

API_SECRET = os.getenv("IMPORTER_API_SECRET", "")  # shared secret with BuddyStore backend
TG_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TG_API_HASH = os.getenv("TELEGRAM_API_HASH", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[startup] Video importer ready. API_ID={TG_API_ID}", flush=True)
    if not TG_API_ID or not TG_API_HASH:
        print("[startup] WARNING: TELEGRAM_API_ID or TELEGRAM_API_HASH not set!", flush=True)
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
    # Import parameters (mirror of mtcute startImport signature)
    skip_existing: bool = True
    last_msg_id: Optional[int] = None         # Checkpoint watermark from DB
    start_message_id: Optional[int] = None   # Range: oldest message to include
    end_message_id: Optional[int] = None     # Range: newest message to include
    limit_count: Optional[int] = None        # Max videos to import
    duplicate_check_url: Optional[str] = None  # Backend endpoint: GET ?botId&telegramUniqueId


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
        raise HTTPException(status_code=409, detail="Import already running for this admin.")

    job_id = str(uuid.uuid4())
    job = create_job(job_id, req.admin_id)

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
