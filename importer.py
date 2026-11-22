# importer.py — Full Hydrogram import loop
# Features: channel scan, startMsgId/endMsgId range, skipExisting watermark,
#           limitCount, duplicate check, thumbnail upload, flood wait,
#           EOF retry, token-bucket rate limiter, webhook progress callbacks,
#           scan result cache, bounded task queue, adaptive concurrency,
#           drop_author server-side copy, Premium detection, DC logging,
#           local checkpoint persistence,
#           ffmpeg video enhancement (denoise + sharpen + 2× upscale)
#           on CHAT_FORWARDS_RESTRICTED reupload path.

import os
import json
import time
import asyncio
import subprocess
import random
import httpx
from hydrogram import Client
import hydrogram.errors
from hydrogram.errors import FloodWait, FileReferenceExpired
from hydrogram.raw import functions, types as raw_types
from jobs import ImportJob

# --- MONKEYPATCH HYDROGRAM FLOOD PREMIUM BUG ---
# Hydrogram maps error 'FLOOD_PREMIUM_WAIT' to 'FloodPremiumWait' in its rpc_error.py,
# but forgot to actually define it in `hydrogram.errors`.
# We define it here at runtime so that `getattr` succeeds and we can catch it as a normal FloodWait.
if not hasattr(hydrogram.errors, "FloodPremiumWait"):
    class FloodPremiumWait(FloodWait):
        ID = "FLOOD_PREMIUM_WAIT"
    
    setattr(hydrogram.errors, "FloodPremiumWait", FloodPremiumWait)
# -----------------------------------------------

DOWNLOADS_DIR = os.getenv(
    "DOWNLOADS_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads"),
)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

SCAN_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan_cache")
os.makedirs(SCAN_CACHE_DIR, exist_ok=True)
SCAN_CACHE_TTL = 3600  # seconds — cache is valid for 1 hour

CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

DC_NAMES = {1: "DC1 (Miami)", 2: "DC2 (Amsterdam)", 4: "DC4 (Amsterdam)", 5: "DC5 (Singapore)"}


# ── Token Bucket Rate Limiter ─────────────────────────────────────────────────
# Replaces fixed per-video delay with an adaptive system:
#   • Premium sessions: larger bucket (240 tokens, 4 tokens/s refill = 26× quota)
#   • Free sessions:    normal bucket (60 tokens,  1 token/s refill)
#   • Cost per video:   size-aware — larger files drain more tokens
#   • FloodWait penalty: drains the bucket proportionally
#   • When bucket is full → near-zero delay; when empty → natural backpressure

class TokenBucket:
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, amount: float = 1.0) -> float:
        """Returns seconds to wait (0.0 if bucket had enough tokens)."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now
            if self.tokens >= amount:
                self.tokens -= amount
                return 0.0
            wait = (amount - self.tokens) / self.refill_rate
            self.tokens = 0.0
            return wait

    async def penalize(self, seconds: float) -> None:
        """Drain bucket after a FloodWait — back-pressure proportional to wait time."""
        async with self._lock:
            drain = seconds * self.refill_rate * 0.5  # drain 50% of FloodWait equivalent
            self.tokens = max(0.0, self.tokens - drain)

    @property
    def level_pct(self) -> float:
        return round((self.tokens / self.capacity) * 100, 1)


def _token_cost(file_size_mb: float) -> float:
    """Size-aware token cost so large files naturally pace themselves."""
    if file_size_mb <= 5:
        return 1.0
    elif file_size_mb <= 50:
        return 1.0 + file_size_mb / 25.0
    else:
        return 3.0 + file_size_mb / 50.0


# ── Local checkpoint persistence ──────────────────────────────────────────────
# Saves checkpoint every 10 videos to disk so a PM2 restart can be fast-resumed
# without hitting the backend webhook (which may be temporarily unreachable).

def _save_local_checkpoint(job_id: str, msg_id: int, processed: int, total: int) -> None:
    path = os.path.join(CHECKPOINT_DIR, f"{job_id}.json")
    try:
        with open(path, "w") as f:
            json.dump({"msg_id": msg_id, "processed": processed, "total": total, "ts": time.time()}, f)
    except Exception as e:
        print(f"[checkpoint] Save failed: {e}", flush=True)


def _clear_local_checkpoint(job_id: str) -> None:
    path = os.path.join(CHECKPOINT_DIR, f"{job_id}.json")
    try:
        if os.path.exists(path):
            os.unlink(path)
    except Exception:
        pass


def _scan_cache_key(source_chat: str | int, start_id: int | None, end_id: int | None) -> str:
    return f"{source_chat}_{start_id or 0}_{end_id or 0}"


def _load_scan_cache(key: str) -> list[int] | None:
    path = os.path.join(SCAN_CACHE_DIR, f"{key}.json")
    try:
        if not os.path.exists(path):
            return None
        age = time.time() - os.path.getmtime(path)
        if age > SCAN_CACHE_TTL:
            os.unlink(path)
            return None
        with open(path) as f:
            data = json.load(f)
        return data.get("ids") if isinstance(data.get("ids"), list) else None
    except Exception:
        return None


def _save_scan_cache(key: str, ids: list[int]) -> None:
    path = os.path.join(SCAN_CACHE_DIR, f"{key}.json")
    try:
        with open(path, "w") as f:
            json.dump({"ids": ids, "saved_at": time.time()}, f)
    except Exception as e:
        print(f"[scan_cache] Failed to save: {e}", flush=True)


# ── Webhook helper ─────────────────────────────────────────────────────────────

def _get_api_secret() -> str:
    return os.getenv("IMPORTER_API_SECRET", "")

async def notify_buddystore(webhook_url: str, payload: dict):
    """Fire-and-forget progress callback to BuddyStore backend."""
    try:
        secret = _get_api_secret()
        headers = {"x-api-secret": secret} if secret else {}
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(webhook_url, json=payload, headers=headers)
            if r.status_code >= 400:
                print(f"[webhook] To {webhook_url} Non-OK response {r.status_code}: {r.text[:200]}", flush=True)
            else:
                print(f"[webhook] ✅ Progress updated: {payload.get('progress')}/{payload.get('total')}", flush=True)
    except Exception as e:
        print(f"[webhook] Failed to {webhook_url}: {e}", flush=True)


# ── Duplicate check helper ─────────────────────────────────────────────────────

async def is_duplicate(duplicate_check_url: str | None, bot_db_id: str | None,
                       unique_id: str | None, file_size: int, duration: int) -> bool:
    """Ask BuddyStore backend if this video is already stored."""
    if not duplicate_check_url or not bot_db_id:
        return False
    try:
        params: dict = {"botId": bot_db_id}
        if unique_id:
            params["telegramUniqueId"] = unique_id
        else:
            params["fileSize"] = str(file_size)
            params["duration"] = str(duration)
        secret = _get_api_secret()
        headers = {"x-api-secret": secret} if secret else {}
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(duplicate_check_url, params=params, headers=headers)
            return r.json().get("isDuplicate", False)
    except Exception as e:
        print(f"[dedup] check failed: {e}", flush=True)
        return False


# ── Channel scanner ────────────────────────────────────────────────────────────

async def scan_channel(
    tg: Client,
    job: ImportJob,
    source_chat: str | int,
    start_message_id: int | None,
    end_message_id: int | None,
    last_msg_id: int | None,
    webhook_url: str,
) -> list[int]:
    """
    Iterate messages.getHistory newest→oldest and collect video message IDs.

    Strategy:
      • offset_id = end_message_id + 1  (ceiling, 0 = newest)
      • floor_id  = max(last_msg_id, start_message_id - 1)  (floor)
      • Telegram API returns up to 100 messages per getHistory call.
      • Results are cached to disk for 1 hour to speed up auto-resume rescans.
    """
    job.add_log("Scanning channel for videos...")
    job.message = "Scanning..."

    # ── Scan cache (only for full-range scans, not checkpoint scans) ─────────
    # A checkpoint scan uses last_msg_id as floor → different result set each time.
    # Only cache when last_msg_id is None (first ever run for this range).
    cache_key = _scan_cache_key(source_chat, start_message_id, end_message_id)
    if not last_msg_id:
        cached = _load_scan_cache(cache_key)
        if cached is not None:
            job.add_log(f"✅ Scan cache hit — {len(cached)} videos loaded from cache (< 1h old).")
            return cached
    # ─────────────────────────────────────────────────────────────────────────

    video_ids: list[int] = []
    offset_id = (end_message_id + 1) if end_message_id else 0
    floor_id = 0
    if start_message_id:
        floor_id = start_message_id - 1
    if last_msg_id and last_msg_id > floor_id:
        floor_id = last_msg_id

    # Resolve peer once before loop
    try:
        peer = await tg.resolve_peer(source_chat)
    except Exception as e:
        job.add_log(f"Warning: resolve_peer failed ({e}), will retry in loop")
        peer = source_chat

    has_more = True

    while has_more:
        if job.stop_flag:
            break

        try:
            # Re-resolve if needed
            if isinstance(peer, (str, int)):
                peer = await tg.resolve_peer(source_chat)

            result = await tg.invoke(
                functions.messages.GetHistory(
                    peer=peer,
                    offset_id=offset_id,
                    offset_date=0,
                    add_offset=0,
                    limit=100,
                    max_id=0,
                    min_id=floor_id,
                    hash=0,
                )
            )
        except FloodWait as fw:
            wait = fw.value + 3
            job.add_log(f"[Scan Flood Wait] {wait}s...")
            await asyncio.sleep(wait)
            continue
        except Exception as e:
            job.add_log(f"[Scan Error] {e} — stopping scan.")
            break

        messages = result.messages
        if not messages:
            break

        reached_floor = False
        for msg in messages:
            if job.stop_flag:
                break
            if not hasattr(msg, "id"):
                continue
            if msg.id <= floor_id:
                reached_floor = True
                break
            if end_message_id and msg.id > end_message_id:
                continue

            # Check for video media
            if not hasattr(msg, "media") or not msg.media:
                continue

            media = msg.media
            if isinstance(media, raw_types.MessageMediaDocument):
                doc = media.document
                if not isinstance(doc, raw_types.Document):
                    continue
                mime = doc.mime_type or ""
                is_video = mime.startswith("video/") or any(
                    isinstance(a, (raw_types.DocumentAttributeVideo, raw_types.DocumentAttributeAnimated))
                    for a in doc.attributes
                )
                if is_video:
                    video_ids.append(msg.id)

        if reached_floor or job.stop_flag:
            break

        oldest_msg_id = messages[-1].id
        # Prevent infinite loop if offset does not advance
        if offset_id > 0 and oldest_msg_id >= offset_id:
            break

        offset_id = oldest_msg_id

        # Update progress on every page
        job.message = f"Scanning... {len(video_ids)} videos found"
        job.total = len(video_ids)
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": "running", "progress": 0, "total": len(video_ids),
            "message": f"Scanning... {len(video_ids)} videos found",
            "logs": job.logs[-100:],
        })

        # Telegram getHistory limit is 100.
        # If fewer than 100 messages returned, channel beginning is reached.
        # If the oldest message on this page is <= floor_id, floor is reached.
        if len(messages) < 100 or oldest_msg_id <= floor_id:
            has_more = False

    # Return in chronological order (oldest first, like original)
    video_ids.reverse()
    job.add_log(f"Scan complete: {len(video_ids)} videos found.")
    # Save to cache only for full-range scans (no checkpoint floor)
    if not last_msg_id and video_ids:
        _save_scan_cache(cache_key, video_ids)
        job.add_log(f"💾 Scan result cached ({len(video_ids)} IDs) — fast resume available for 1h.")
    return video_ids


# ── Thumbnail helper ───────────────────────────────────────────────────────────

async def download_thumbnail(tg: Client, doc: raw_types.Document, tmp_prefix: str) -> str | None:
    """Download the best available thumbnail to a temp file. Returns path or None."""
    try:
        thumbs = [t for t in doc.thumbs if isinstance(t, raw_types.PhotoSize)]
        if not thumbs:
            return None
        # Pick the largest JPEG thumbnail (excludes video previews)
        best = max(thumbs, key=lambda t: (getattr(t, "w", 0) or 0))
        thumb_path = f"{tmp_prefix}_thumb.jpg"
        await tg.download_media(
            raw_types.InputDocumentFileLocation(
                id=doc.id,
                access_hash=doc.access_hash,
                file_reference=doc.file_reference,
                thumb_size=best.type,
            ),
            file_name=thumb_path,
        )
        # Telegram rejects thumbnails > 200 KB
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) <= 200 * 1024:
            return thumb_path
        if os.path.exists(thumb_path):
            os.unlink(thumb_path)
        return None
    except Exception as e:
        print(f"[thumb] Telegram thumb failed: {e}", flush=True)
        return None


def generate_thumbnail_ffmpeg(video_path: str, thumb_path: str) -> str | None:
    """Extract a frame from the video at ~1s using ffmpeg. Returns path or None."""
    # Skip ffmpeg thumbnail generation for very large videos to avoid OOM
    # Raised from 50MB → 150MB now that we have 1GB RAM (AWS t2.micro vs Render 512MB)
    try:
        if os.path.exists(video_path) and os.path.getsize(video_path) > 150 * 1024 * 1024:
            print("[thumb] Video too large (>150MB), skipping ffmpeg to avoid OOM", flush=True)
            return None
    except Exception:
        pass

    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", video_path,
                "-ss", "1",           # seek to 1 second
                "-frames:v", "1",     # grab 1 frame
                "-q:v", "5",          # JPEG quality (lower = better, 2-5 is good)
                "-vf", "scale='min(320,iw)':-1",  # max 320px wide, keep aspect ratio
                thumb_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) <= 200 * 1024:
            return thumb_path
        # If too large, try again at lower quality
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 200 * 1024:
            subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-i", video_path,
                    "-ss", "1",
                    "-frames:v", "1",
                    "-q:v", "10",
                    "-vf", "scale='min(200,iw)':-1",
                    thumb_path,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) <= 200 * 1024:
                return thumb_path
        if os.path.exists(thumb_path):
            os.unlink(thumb_path)
        return None
    except FileNotFoundError:
        print("[thumb] ffmpeg not installed — skipping thumbnail generation", flush=True)
        return None
    except Exception as e:
        print(f"[thumb] ffmpeg failed: {e}", flush=True)
        if os.path.exists(thumb_path):
            try:
                os.unlink(thumb_path)
            except Exception:
                pass
        return None


# ── Video enhancer (ffmpeg CPU filters) ───────────────────────────────────────
# Applied ONLY on the CHAT_FORWARDS_RESTRICTED reupload path (file is on disk).
# Server-side copies bypass this — Telegram handles those bytes, we never touch them.
#
# Filter chain (color-enhanced — no upscale, safe on t2.micro):
#   hqdn3d=2:1:3:3        — light denoiser (removes compression grain without blurring detail)
#   eq=contrast/sat/gamma — boosts contrast 1.08×, saturation 1.25×, slight brightness lift
#   unsharp=5:5:1.2       — stronger edge sharpening for a crisper look
#
# Why no upscale:
#   2× Lanczos on t2.micro takes 1-5 min per video, balloons file size 5-10×,
#   and Telegram re-compresses the upload anyway — zero visible gain.
#   Color/contrast enhancement survives Telegram's re-encode and is visually noticeable.
#
# Limits:
#   • Skipped if video > ENHANCE_MAX_SIZE_MB (default 80 MB) — prevents OOM/timeout
#   • Falls back silently to original file on any ffmpeg error
#   • Uses -preset fast and -crf 20 for good quality / fast encode

ENHANCE_MAX_SIZE_MB = 80  # skip enhancement for files larger than this

def enhance_video_ffmpeg(
    video_path: str,
    orig_width: int,
    orig_height: int,
    job_add_log=None,
    tag: str = "",
) -> tuple[str | None, int, int]:
    """
    Run ffmpeg denoise + sharpen on video_path (no upscale — too slow on t2.micro).
    Returns (enhanced_path, orig_w, orig_h) if successful, (None, orig_w, orig_h) on skip/fail.
    Caller is responsible for deleting enhanced_path when done.
    """
    try:
        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
    except Exception:
        return None, orig_width, orig_height

    # ── Size gate ────────────────────────────────────────────────────────────
    if file_size_mb > ENHANCE_MAX_SIZE_MB:
        if job_add_log:
            job_add_log(
                f"{tag} 🎬 Enhancement skipped — file too large "
                f"({file_size_mb:.1f} MB > {ENHANCE_MAX_SIZE_MB} MB limit)"
            )
        return None, orig_width, orig_height

    if job_add_log:
        job_add_log(
            f"{tag} 🎬 Enhancing ({orig_width}×{orig_height}, {file_size_mb:.1f} MB) "
            f"— denoise + color boost + sharpen"
        )

    enhanced_path = video_path.replace(".tmp", "_enhanced.mp4")
    if enhanced_path == video_path:
        enhanced_path = video_path + "_enhanced.mp4"

    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", video_path,
                "-vf", "hqdn3d=2:1:3:3,eq=contrast=1.08:brightness=0.015:saturation=1.25:gamma=1.05,unsharp=5:5:1.2:3:3:0.5",
                "-c:v", "libx264",
                "-crf", "20",
                "-preset", "fast",
                "-c:a", "copy",             # audio passthrough
                "-movflags", "+faststart",   # web-optimized MP4
                enhanced_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=300,  # 5 min max (denoise+sharpen on 80MB is well within this)
        )
        if result.returncode != 0:
            err = result.stderr[-300:].decode(errors="ignore") if result.stderr else "unknown"
            if job_add_log:
                job_add_log(
                    f"{tag} ⚠️ ffmpeg enhancement failed (rc={result.returncode}) "
                    f"— using original. {err}"
                )
            if os.path.exists(enhanced_path):
                os.unlink(enhanced_path)
            return None, orig_width, orig_height

        new_size_mb = os.path.getsize(enhanced_path) / (1024 * 1024)
        if job_add_log:
            job_add_log(
                f"{tag} ✨ Enhanced: {file_size_mb:.1f} MB → {new_size_mb:.1f} MB "
                f"({orig_width}×{orig_height}, denoise+color boost+sharpen)"
            )
        return enhanced_path, orig_width, orig_height

    except FileNotFoundError:
        if job_add_log:
            job_add_log(f"{tag} ⚠️ ffmpeg not installed — enhancement skipped")
        return None, orig_width, orig_height
    except subprocess.TimeoutExpired:
        if job_add_log:
            job_add_log(f"{tag} ⚠️ ffmpeg timed out (>5 min) — using original file")
        if os.path.exists(enhanced_path):
            try:
                os.unlink(enhanced_path)
            except Exception:
                pass
        return None, orig_width, orig_height
    except Exception as e:
        if job_add_log:
            job_add_log(f"{tag} ⚠️ Enhancement error: {e} — using original")
        if os.path.exists(enhanced_path):
            try:
                os.unlink(enhanced_path)
            except Exception:
                pass
        return None, orig_width, orig_height


# ── Main import loop ───────────────────────────────────────────────────────────

async def _run_import_inner(
    job: ImportJob,
    session_string: str,
    api_id: int,
    api_hash: str,
    source_chat: str,
    target_chat: str,
    msg_ids: list[int],
    webhook_url: str,
    target_bot_db_id: str | None,
    skip_existing: bool = True,
    last_msg_id: int | None = None,
    start_message_id: int | None = None,
    end_message_id: int | None = None,
    limit_count: int | None = None,
    duplicate_check_url: str | None = None,
    initial_progress: int = 0,
    original_total: int = 0,
):
    # Parse chat IDs to int if they are numeric (Hydrogram requires int for IDs, str for usernames)
    try:
        src_peer = int(source_chat)
    except ValueError:
        src_peer = source_chat

    try:
        tgt_peer = int(target_chat)
    except ValueError:
        tgt_peer = target_chat

    async with Client(
        name=f"session_{job.admin_id}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    ) as tg:
        
        # ── Peer hydration ────────────────────────────────────────────────────
        # Hydrogram (like Pyrogram) maintains an internal peer cache. It MUST
        # "see" a chat before it can resolve its access_hash. For channels the
        # user has already joined, loading dialogs populates this cache.
        # With limit=5 only the 5 most recent chats were loaded, causing
        # PEER_ID_INVALID for any channel not in that tiny window.
        # ── Premium detection ──────────────────────────────────────────────────
        # Check if the session account has Telegram Premium.
        # Premium gives 26× upload quota (4GB/min vs 150MB/min), dramatically
        # reducing FloodWaits. We tune concurrency and token bucket accordingly.
        is_premium = False
        try:
            me = await tg.get_me()
            is_premium = bool(getattr(me, "is_premium", False))
            if is_premium:
                job.add_log("⚡ PREMIUM SESSION DETECTED — running at full speed! (26× upload quota)")
                job.message = "⚡ Premium session — maximum throughput mode"
            else:
                job.add_log("ℹ️ Free session — rate limits apply. Consider Telegram Premium for 26× speed.")
        except Exception as e:
            job.add_log(f"⚠️ Could not detect Premium status: {e}")

        job.add_log("🔄 Loading Telegram dialogs...")
        job.message = "🔄 Loading Telegram session dialogs (~30s)..."
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": "running", "progress": 0, "total": 0,
            "message": job.message, "logs": job.logs[-100:],
            "isPremium": is_premium,
        })
        try:
            dialog_count = 0
            async for _ in tg.get_dialogs(limit=500):
                dialog_count += 1
                # Webhook every 50 dialogs so the UI shows live progress
                if dialog_count % 50 == 0:
                    job.message = f"🔄 Loading dialogs... {dialog_count}/500"
                    await notify_buddystore(webhook_url, {
                        "jobId": job.job_id, "adminId": job.admin_id,
                        "status": "running", "progress": 0, "total": 0,
                        "message": job.message, "logs": job.logs[-100:],
                        "isPremium": is_premium,
                    })
            job.add_log(f"✅ Loaded {dialog_count} dialogs")
        except Exception as e:
            job.add_log(f"⚠️ Dialog load partial: {e}")

        # For numeric channel IDs (e.g. -1001234567890), try to resolve
        # directly via get_chat() which works even if not in dialogs.
        for peer_label, peer_val in [("source", src_peer), ("target", tgt_peer)]:
            if isinstance(peer_val, int):
                try:
                    chat = await tg.get_chat(peer_val)
                    job.add_log(f"Resolved {peer_label} peer: {getattr(chat, 'title', peer_val)}")
                except Exception as e:
                    job.add_log(f"Warning: could not pre-resolve {peer_label} ({peer_val}): {e}")

        # ── Step 1: Scan or use pre-supplied msg_ids ──────────────────────────
        if msg_ids:
            final_ids = msg_ids
            job.add_log(f"Using {len(final_ids)} pre-supplied message IDs.")
        else:
            final_ids = await scan_channel(
                tg, job, src_peer,
                start_message_id, end_message_id,
                last_msg_id if skip_existing else None,
                webhook_url,
            )

        if job.stop_flag:
            job.status = "stopped"
            await notify_buddystore(webhook_url, {
                "jobId": job.job_id, "adminId": job.admin_id,
                "status": "stopped", "progress": 0, "total": 0,
                "message": "Stopped during scan.",
            })
            return

        if limit_count and len(final_ids) > limit_count:
            final_ids = final_ids[:limit_count]
            job.add_log(f"Capped to {limit_count} videos (limitCount).")

        num_remaining = len(final_ids)
        if original_total > 0 and initial_progress > 0 and msg_ids:
            # Only use checkpoint offset if pre-supplied msg_ids (no fresh scan).
            # A fresh scan always resets processed to 0 to avoid overcounting.
            total = original_total
            processed = initial_progress
        else:
            # Fresh scan (or resume with empty msg_ids) — always start from 0.
            # IMPORTANT: also reset initial_progress to 0 so the tag formula
            # [initial_progress + list_idx + 1 / total] doesn't overcounts on
            # auto-resume where initial_progress is set but msg_ids is empty.
            total = num_remaining
            processed = 0
            initial_progress = 0  # ← fix: reset so tag formula stays [1/N]..[N/N]

        job.total = total

        if num_remaining == 0:
            job.status = "completed"
            if total == 0:
                job.message = "No videos found to import."
            else:
                job.message = f"✅ Import complete — {processed}/{total} videos imported."
                
            await notify_buddystore(webhook_url, {
                "jobId": job.job_id, "adminId": job.admin_id,
                "status": "completed", "progress": processed, "total": total,
                "message": job.message,
                "logs": job.logs[-100:],
            })
            return

        job.add_log(f"Starting import: {total} videos.")
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": "running", "progress": 0, "total": total,
            "message": f"Starting import of {total} videos...",
            "logs": job.logs[-100:],
            "isPremium": is_premium,
        })

        # ── Step 2: Concurrent download + serialized paced upload loop ──────────────
        # CONCURRENCY is dynamic:
        #   • Premium: 4 workers (26× quota), free: 2 workers
        #   • Drops to 1 worker after 10 FloodWaits regardless of session type
        # Token Bucket replaces fixed delays — adapts in real-time to quota state.
        BASE_CONCURRENCY = 4 if is_premium else 2
        download_semaphore = asyncio.Semaphore(BASE_CONCURRENCY)
        upload_lock = asyncio.Lock()
        checkpoint_lock = asyncio.Lock()

        # Token Bucket: Premium = 4GB/min quota (~26× free), Free = 150MB/min
        # capacity=60/240 represents ~1 min of budget; refill_rate = tokens/second
        bucket = TokenBucket(
            capacity=240.0 if is_premium else 60.0,
            refill_rate=4.0 if is_premium else 1.0,
        )
        if is_premium:
            job.add_log("⚡ Token bucket: Premium mode (240 cap, 4.0/s refill)")
        else:
            job.add_log("ℹ️ Token bucket: Free mode (60 cap, 1.0/s refill)")

        # Checkpoint saved locally every 10 successful videos (crash resilience)
        _local_checkpoint_counter = 0

        # processed is initialized above
        latest_checkpoint_msg_id: int | None = None
        flood_wait_count = 0          # total FloodWait exceptions hit this session
        flood_wait_total_seconds = 0  # cumulative seconds spent waiting on FloodWaits
        channel_forwards_restricted = False
        source_dc_id: int | None = None  # detected DC of source channel files

        # ── Dynamic concurrency: drop to 1 worker if FloodWaits spike ────────
        _concurrency_reduced = False

        completed_flags: list[bool] = [False] * num_remaining
        safe_checkpoint_ptr = -1  # highest index where ALL 0..index are complete

        async def advance_checkpoint(list_idx: int) -> int | None:
            nonlocal safe_checkpoint_ptr, latest_checkpoint_msg_id
            async with checkpoint_lock:
                completed_flags[list_idx] = True
                while (
                    safe_checkpoint_ptr + 1 < num_remaining
                    and completed_flags[safe_checkpoint_ptr + 1]
                ):
                    safe_checkpoint_ptr += 1
                if safe_checkpoint_ptr >= 0:
                    latest_checkpoint_msg_id = final_ids[safe_checkpoint_ptr]
            return latest_checkpoint_msg_id

        async def handle_skip(list_idx: int, reason: str):
            nonlocal processed
            tag = f"[{initial_progress + list_idx + 1}/{total}]"
            job.add_log(f"{tag} {reason}")
            chk = await advance_checkpoint(list_idx)
            async with checkpoint_lock:
                processed += 1
                job.progress = processed
                cur_processed = processed
            await notify_buddystore(webhook_url, {
                "jobId": job.job_id, "adminId": job.admin_id,
                "status": "running",
                "progress": cur_processed, "total": total,
                "message": f"Processing {cur_processed}/{total} videos...",
                "logs": job.logs[-100:],
                "checkpointMsgId": chk,
            })

        async def process_video(list_idx: int, msg_id: int) -> None:
            nonlocal processed, latest_checkpoint_msg_id, flood_wait_count, flood_wait_total_seconds, channel_forwards_restricted, download_semaphore, _concurrency_reduced, source_dc_id, _local_checkpoint_counter

            if job.stop_flag:
                return

            tag = f"[{initial_progress + list_idx + 1}/{total}]"
            success = False
            attempts = 0
            max_attempts = 5
            backoff_schedule = [5, 15, 30, 60, 120]

            while not success and attempts < max_attempts and not job.stop_flag:
                attempts += 1
                tmp_file: str | None = None
                thumb_file: str | None = None
                enhanced_file: str | None = None

                try:
                    # ── Phase 1: Download & prepare media (Parallel between workers) ──
                    async with download_semaphore:
                        if job.stop_flag:
                            return

                        msg = await tg.get_messages(src_peer, msg_id)
                        if not msg or (not msg.video and not msg.document):
                            await handle_skip(list_idx, "Skipped — not a video")
                            success = True
                            return

                        media = msg.video or msg.document
                        unique_id: str | None = getattr(media, "file_unique_id", None)
                        file_size: int = getattr(media, "file_size", 0) or 0
                        duration: int = getattr(media, "duration", 0) or 0
                        width: int = getattr(media, "width", 0) or 0
                        height: int = getattr(media, "height", 0) or 0
                        caption: str = msg.caption or ""

                        # ── DC-aware routing logging ───────────────────────────────
                        raw_doc = getattr(media, "_raw", None) or getattr(msg, "_raw", None)
                        if raw_doc and hasattr(raw_doc, "dc_id") and source_dc_id is None:
                            source_dc_id = raw_doc.dc_id
                            dc_name = DC_NAMES.get(source_dc_id, f"DC{source_dc_id}")
                            job.add_log(f"📡 Source files are on {dc_name} (detected from first video)")

                        if skip_existing and target_bot_db_id:
                            dup = await is_duplicate(
                                duplicate_check_url, target_bot_db_id,
                                unique_id, file_size, duration
                            )
                            if dup:
                                await handle_skip(list_idx, "Skipped — duplicate already in DB")
                                success = True
                                return

                        file_size_mb = file_size / (1024 * 1024)

                        # Check if fast-forward is possible (only if channel allows it)
                        can_try_forward = not channel_forwards_restricted
                        if not can_try_forward:
                            # Stream chunked to disk
                            job.message = f"{tag} Downloading ({file_size_mb:.1f} MB)..."
                            tmp_prefix = os.path.join(DOWNLOADS_DIR, f"{job.admin_id}_{msg_id}")
                            tmp_file = f"{tmp_prefix}.tmp"
                            await tg.download_media(msg, file_name=tmp_file)

                            if raw_doc and hasattr(raw_doc, "thumbs"):
                                thumb_file = await download_thumbnail(tg, raw_doc, tmp_prefix)
                            if not thumb_file and tmp_file and os.path.exists(tmp_file):
                                thumb_file = generate_thumbnail_ffmpeg(tmp_file, f"{tmp_prefix}_thumb.jpg")

                    # ── Phase 2: Upload to Telegram (Strictly serialized & paced) ──
                    async with upload_lock:
                        if job.stop_flag:
                            return

                        forward_done = False
                        if can_try_forward:
                            try:
                                # ── drop_author=True: server-side copy, no "Forwarded from" label ──
                                # Uses raw MTProto ForwardMessages with drop_author=True, which is
                                # equivalent to Bot API copyMessage — no bandwidth used, <1s per video.
                                await tg.invoke(
                                    functions.messages.ForwardMessages(
                                        from_peer=await tg.resolve_peer(src_peer),
                                        to_peer=await tg.resolve_peer(tgt_peer),
                                        id=[msg_id],
                                        random_id=[random.randint(0, 2**63)],
                                        drop_author=True,          # no "Forwarded from X" banner
                                        drop_media_captions=False,
                                    )
                                )
                                job.add_log(f"{tag} ✅ Imported ({file_size_mb:.1f} MB) — server-side copy")
                                forward_done = True
                                success = True
                            except Exception as fwd_err:
                                fwd_msg = str(fwd_err)
                                if "CHAT_FORWARDS_RESTRICTED" in fwd_msg or "you can't forward" in fwd_msg.lower() or "forbidden" in fwd_msg.lower():
                                    channel_forwards_restricted = True
                                    job.add_log(f"{tag} 🔒 Channel restricts forwarding — switching to download+reupload mode")
                                else:
                                    raise

                        # If forward was not possible or channel was restricted, upload from disk
                        if not forward_done:
                            if not tmp_file or not os.path.exists(tmp_file):
                                job.message = f"{tag} Downloading ({file_size_mb:.1f} MB)..."
                                tmp_prefix = os.path.join(DOWNLOADS_DIR, f"{job.admin_id}_{msg_id}")
                                tmp_file = f"{tmp_prefix}.tmp"
                                await tg.download_media(msg, file_name=tmp_file)
                                raw_doc2 = getattr(media, "_raw", None) or getattr(msg, "_raw", None)
                                if raw_doc2 and hasattr(raw_doc2, "thumbs"):
                                    thumb_file = await download_thumbnail(tg, raw_doc2, tmp_prefix)
                                if not thumb_file and tmp_file and os.path.exists(tmp_file):
                                    thumb_file = generate_thumbnail_ffmpeg(tmp_file, f"{tmp_prefix}_thumb.jpg")

                            # ── ffmpeg enhancement (denoise + sharpen + 2× upscale) ───────
                            # Only on the restricted-channel reupload path.
                            # Runs in a thread executor so asyncio event loop stays unblocked.
                            # Falls back silently to original if ffmpeg fails/times out.
                            job.message = f"{tag} Enhancing ({file_size_mb:.1f} MB)..."
                            loop = asyncio.get_event_loop()
                            enhanced_path, upload_w, upload_h = await loop.run_in_executor(
                                None,
                                enhance_video_ffmpeg,
                                tmp_file, width, height, job.add_log, tag,
                            )
                            if enhanced_path and os.path.exists(enhanced_path):
                                enhanced_file = enhanced_path   # track for finally cleanup
                                upload_video = enhanced_file
                                enhanced_mb = os.path.getsize(enhanced_file) / (1024 * 1024)
                            else:
                                upload_video = tmp_file
                                upload_w, upload_h = width, height
                                enhanced_mb = file_size_mb
                            # ─────────────────────────────────────────────────────────────

                            job.message = f"{tag} Uploading ({enhanced_mb:.1f} MB)..."
                            send_kwargs: dict = dict(
                                chat_id=tgt_peer,
                                video=upload_video,
                                caption=caption,
                                duration=duration,
                                width=upload_w,
                                height=upload_h,
                                supports_streaming=True,
                            )
                            if thumb_file and os.path.exists(thumb_file):
                                send_kwargs["thumb"] = thumb_file

                            await tg.send_video(**send_kwargs)
                            enhanced_label = " (✨ enhanced)" if enhanced_file else ""
                            job.add_log(f"{tag} ✅ Imported ({enhanced_mb:.1f} MB){enhanced_label}")
                            success = True

                        # ── Post-Upload: Token Bucket pacing + checkpoint + webhook ──
                        processed += 1
                        job.progress = processed
                        local_processed = processed
                        local_chk = await advance_checkpoint(list_idx)

                        # Local checkpoint every 10 successful videos (crash resilience)
                        _local_checkpoint_counter += 1
                        if _local_checkpoint_counter % 10 == 0 and local_chk:
                            _save_local_checkpoint(job.job_id, local_chk, local_processed, total)

                        fw_suffix = (
                            f" | ⚠️ FloodWaits: {flood_wait_count} "
                            f"({flood_wait_total_seconds // 60}m {flood_wait_total_seconds % 60}s total)"
                            if flood_wait_count > 0 else ""
                        )

                        dc_suffix = f" | 📡 {DC_NAMES.get(source_dc_id, f'DC{source_dc_id}')}" if source_dc_id else ""
                        premium_flag = " | ⚡ Premium" if is_premium else ""

                        # ── Token Bucket delay ────────────────────────────────────────
                        # Consume tokens proportional to file size; wait if bucket is low.
                        # This replaces fixed delays and batch cooldowns with adaptive pacing.
                        token_cost = _token_cost(file_size_mb)
                        bucket_wait = await bucket.consume(token_cost)
                        if bucket_wait > 0:
                            job.add_log(
                                f"⏳ Token bucket empty — pacing {bucket_wait:.1f}s "
                                f"(cost={token_cost:.1f} for {file_size_mb:.1f}MB)"
                            )
                            await asyncio.sleep(bucket_wait)

                        await notify_buddystore(webhook_url, {
                            "jobId": job.job_id, "adminId": job.admin_id,
                            "status": "running",
                            "progress": local_processed, "total": total,
                            "message": f"Imported {local_processed}/{total} videos{fw_suffix}{dc_suffix}{premium_flag}",
                            "logs": job.logs[-100:],
                            "checkpointMsgId": local_chk,
                            "floodWaitCount": flood_wait_count,
                            "floodWaitTotalSeconds": flood_wait_total_seconds,
                            "isPremium": is_premium,
                            "sourceDcId": source_dc_id,
                            "sourceDcName": DC_NAMES.get(source_dc_id, None) if source_dc_id else None,
                            "bucketLevel": bucket.level_pct,
                        })

                except FloodWait as fw:
                    wait = fw.value + 3
                    flood_wait_count += 1
                    flood_wait_total_seconds += fw.value
                    fw_mins = flood_wait_total_seconds // 60
                    fw_secs = flood_wait_total_seconds % 60
                    job.add_log(
                        f"{tag} ⏳ Telegram rate limited — waiting {wait}s... "
                        f"(FloodWait #{flood_wait_count}, {fw_mins}m {fw_secs}s total this session)"
                    )

                    # ── Penalize token bucket ─────────────────────────────────────
                    await bucket.penalize(fw.value)
                    job.add_log(f"{tag} 💧 Token bucket penalized by FloodWait — bucket at {bucket.level_pct}%")

                    # ── Dynamic concurrency reduction ─────────────────────────────
                    # After 10 FloodWaits, drop to 1 concurrent download to relieve
                    # API pressure. Rebuild the semaphore with the new limit.
                    if flood_wait_count >= 10 and not _concurrency_reduced:
                        _concurrency_reduced = True
                        download_semaphore = asyncio.Semaphore(1)
                        job.add_log(
                            f"⚠️ High FloodWait rate ({flood_wait_count} hits) — "
                            f"reducing concurrency to 1 worker to relieve Telegram pressure."
                        )
                    if fw.value > 3600:
                        job.status = "stopped"
                        job.message = f"Telegram rate limit: wait {fw.value // 60} min. Resume later."
                        job.add_log(f"{tag} ⏸ Rate limit too long ({fw.value // 60} min) — paused. Resume later.")
                        await notify_buddystore(webhook_url, {
                            "jobId": job.job_id, "adminId": job.admin_id,
                            "status": "stopped",
                            "progress": processed, "total": total,
                            "message": job.message,
                            "logs": job.logs[-100:],
                            "checkpointMsgId": latest_checkpoint_msg_id,
                            "floodWaitCount": flood_wait_count,
                            "floodWaitTotalSeconds": flood_wait_total_seconds,
                            "isPremium": is_premium,
                            "sourceDcId": source_dc_id,
                        })
                        return

                    # Immediate rate-limit status update to BuddyStore
                    rem_m = wait // 60
                    rem_s = wait % 60
                    rem_str = f"{rem_m}m {rem_s}s" if rem_m > 0 else f"{rem_s}s"
                    job.message = f"⏳ Telegram rate limit — resting {rem_str}. Resumes automatically."
                    await notify_buddystore(webhook_url, {
                        "jobId": job.job_id, "adminId": job.admin_id,
                        "status": "running",
                        "progress": processed, "total": total,
                        "message": job.message,
                        "logs": job.logs[-100:],
                        "checkpointMsgId": latest_checkpoint_msg_id,
                        "floodWaitCount": flood_wait_count,
                        "floodWaitTotalSeconds": flood_wait_total_seconds,
                    })

                    # Active countdown with 15s heartbeats
                    remaining = wait
                    while remaining > 0 and not job.stop_flag:
                        step = min(remaining, 5)
                        await asyncio.sleep(step)
                        remaining -= step
                        if remaining > 0 and remaining % 15 == 0:
                            cur_m = remaining // 60
                            cur_s = remaining % 60
                            cur_str = f"{cur_m}m {cur_s}s" if cur_m > 0 else f"{cur_s}s"
                            job.message = f"⏳ Telegram rate limit — {cur_str} remaining. Resumes automatically."
                            await notify_buddystore(webhook_url, {
                                "jobId": job.job_id, "adminId": job.admin_id,
                                "status": "running",
                                "progress": processed, "total": total,
                                "message": job.message,
                                "logs": job.logs[-100:],
                                "checkpointMsgId": latest_checkpoint_msg_id,
                                "floodWaitCount": flood_wait_count,
                                "floodWaitTotalSeconds": flood_wait_total_seconds,
                            })

                    # Extra recovery buffer to avoid re-triggering quota
                    recovery = min(30, max(5, fw.value // 10))
                    job.add_log(f"{tag} 🔄 Recovery buffer +{recovery}s to restore Telegram quota...")
                    job.message = f"⏳ Stabilizing connection (+{recovery}s)..."
                    await asyncio.sleep(recovery)
                    job.add_log(f"{tag} ▶️ Rate limit lifted! Resuming import...")
                    job.message = f"Resuming import ({processed}/{total} done)..."
                    await notify_buddystore(webhook_url, {
                        "jobId": job.job_id, "adminId": job.admin_id,
                        "status": "running",
                        "progress": processed, "total": total,
                        "message": job.message,
                        "logs": job.logs[-100:],
                        "checkpointMsgId": latest_checkpoint_msg_id,
                    })

                except Exception as e:
                    err_msg = str(e)
                    is_retryable = any(x in err_msg.lower() for x in [
                        "file reference", "unexpected eof", "stream reading",
                        "connection", "timeout", "reset", "filelocat"
                    ])
                    if is_retryable and attempts < max_attempts:
                        backoff = backoff_schedule[attempts - 1]
                        job.add_log(
                            f"{tag} 🔄 Retry {attempts}/{max_attempts}: {err_msg[:60]} — waiting {backoff}s..."
                        )
                        await asyncio.sleep(backoff)
                    else:
                        job.add_log(f"{tag} ❌ Failed — {err_msg[:100]}")
                        success = True
                        await advance_checkpoint(list_idx)

                finally:
                    for f in [tmp_file, thumb_file, enhanced_file]:
                        if f and os.path.exists(f):
                            try:
                                os.unlink(f)
                            except Exception:
                                pass

        # ── Bounded task queue (#3) — avoids creating 1000+ coroutines at once ──
        # Instead of asyncio.gather(*all_tasks), feed a queue and run N workers.
        # Workers pull tasks one-at-a-time, so only BASE_CONCURRENCY tasks are
        # live in memory at any moment regardless of how large the channel is.
        job.add_log(f"🚀 Starting bounded import queue: {total} videos, {BASE_CONCURRENCY} workers.")

        task_queue: asyncio.Queue = asyncio.Queue()
        for i, mid in enumerate(final_ids):
            await task_queue.put((i, mid))

        async def queue_worker():
            while not task_queue.empty() and not job.stop_flag:
                try:
                    i, mid = task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await process_video(i, mid)
                task_queue.task_done()

        await asyncio.gather(
            *[queue_worker() for _ in range(BASE_CONCURRENCY)],
            return_exceptions=True,
        )

        # ── Job finished ──────────────────────────────────────────────────────
        if not job.stop_flag:
            job.status = "completed"
            job.message = f"✅ Import complete — {processed}/{total} videos imported."
            _clear_local_checkpoint(job.job_id)

        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": job.status,
            "progress": processed, "total": total,
            "message": job.message,
            "logs": job.logs[-200:],
            "checkpointMsgId": latest_checkpoint_msg_id,
            "isPremium": is_premium,
            "sourceDcId": source_dc_id,
            "sourceDcName": DC_NAMES.get(source_dc_id, None) if source_dc_id else None,
        })

async def run_import(
    job: ImportJob,
    session_string: str,
    api_id: int,
    api_hash: str,
    source_chat: str,
    target_chat: str,
    msg_ids: list[int],
    webhook_url: str,
    target_bot_db_id: str | None,
    skip_existing: bool = True,
    last_msg_id: int | None = None,
    start_message_id: int | None = None,
    end_message_id: int | None = None,
    limit_count: int | None = None,
    duplicate_check_url: str | None = None,
    initial_progress: int = 0,
    original_total: int = 0,
):
    import traceback
    try:
        await _run_import_inner(
            job, session_string, api_id, api_hash, source_chat, target_chat, msg_ids,
            webhook_url, target_bot_db_id, skip_existing, last_msg_id, start_message_id,
            end_message_id, limit_count, duplicate_check_url, initial_progress, original_total
        )
    except Exception as e:
        err_msg = traceback.format_exc()
        job.add_log(f"💥 FATAL ERROR: {e}")
        print(f"[{job.job_id}] FATAL ERROR:\n{err_msg}", flush=True)
        job.status = "failed"
        job.message = f"Failed: {e}"
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": "failed", "progress": initial_progress, "total": original_total,
            "message": job.message,
            "logs": job.logs[-200:],
        })
