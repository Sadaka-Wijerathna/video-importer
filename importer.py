# importer.py — Hydrogram download/upload loop
import os
import asyncio
import httpx
from hydrogram import Client
from hydrogram.errors import FloodWait, FileReferenceExpired
from jobs import ImportJob

DOWNLOADS_DIR = "/tmp/tg_imports"
os.makedirs(DOWNLOADS_DIR, exist_ok=True)


async def notify_buddystore(webhook_url: str, payload: dict):
    """Send progress update back to BuddyStore backend."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(webhook_url, json=payload)
    except Exception as e:
        print(f"[webhook] Failed to notify BuddyStore: {e}", flush=True)


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
):
    """Main import loop — downloads from source, uploads to target."""

    async with Client(
        name=f"session_{job.admin_id}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,        # no session file on disk
        no_updates=True,       # we only need file downloads, not updates
    ) as tg:

        total = len(msg_ids)
        job.total = total
        job.add_log(f"Starting. {total} videos to import.")

        await notify_buddystore(webhook_url, {
            "jobId": job.job_id,
            "adminId": job.admin_id,
            "status": "running",
            "progress": 0,
            "total": total,
            "message": f"Starting import of {total} videos...",
        })

        processed = 0
        videos_since_cooldown = 0

        for msg_id in msg_ids:
            if job.stop_flag:
                job.status = "stopped"
                job.add_log("Stopped by admin.")
                break

            success = False
            attempts = 0
            max_attempts = 5
            backoff_schedule = [5, 15, 30, 60, 120]  # seconds

            while not success and attempts < max_attempts and not job.stop_flag:
                attempts += 1
                tmp_file = None

                try:
                    # ── Step 1: Fetch message (fresh file reference) ─────────
                    msg = await tg.get_messages(source_chat, msg_id)
                    if not msg or (not msg.video and not msg.document):
                        job.add_log(f"Skipped msg {msg_id}: no video media.")
                        success = True
                        break

                    media = msg.video or msg.document

                    # ── Step 2: Try fast forward first ──────────────────────
                    try:
                        await tg.forward_messages(target_chat, source_chat, msg_id)
                        job.add_log(f"✓ msg {msg_id} forwarded (fast path)")
                        success = True
                        break
                    except Exception as fwd_err:
                        fwd_msg = str(fwd_err)
                        if "CHAT_FORWARDS_RESTRICTED" in fwd_msg or "you can't" in fwd_msg.lower():
                            pass  # expected for restricted channels, fall through
                        else:
                            raise  # unexpected error, re-raise

                    # ── Step 3: Download to disk ─────────────────────────────
                    file_size_bytes = media.file_size or 0
                    file_size_mb = file_size_bytes / (1024 * 1024)
                    job.add_log(f"↓ Downloading msg {msg_id} ({file_size_mb:.1f} MB)...")

                    tmp_file = os.path.join(DOWNLOADS_DIR, f"{job.admin_id}_{msg_id}.tmp")

                    # Hydrogram streams to disk — no full-RAM buffering
                    await tg.download_media(msg, file_name=tmp_file)

                    # ── Step 4: Upload from disk ─────────────────────────────
                    job.add_log(f"↑ Uploading msg {msg_id} to {target_chat}...")

                    caption = msg.caption or ""
                    duration = getattr(media, "duration", 0) or 0
                    width = getattr(media, "width", 0) or 0
                    height = getattr(media, "height", 0) or 0

                    await tg.send_video(
                        chat_id=target_chat,
                        video=tmp_file,
                        caption=caption,
                        duration=duration,
                        width=width,
                        height=height,
                        supports_streaming=True,
                    )

                    job.add_log(f"✓ msg {msg_id} imported ({file_size_mb:.1f} MB)")
                    success = True

                except FloodWait as fw:
                    wait = fw.value + 3
                    job.add_log(f"[Flood Wait] msg {msg_id}: waiting {wait}s...")
                    # If flood wait is absurdly long (>1 hr), pause the job gracefully
                    if fw.value > 3600:
                        job.status = "stopped"
                        job.message = f"Telegram rate limit: wait {fw.value // 60} min. Resume later."
                        job.add_log(f"[Flood Wait] Extremely long wait ({fw.value}s). Job paused. Resume manually.")
                        await notify_buddystore(webhook_url, {
                            "jobId": job.job_id,
                            "adminId": job.admin_id,
                            "status": "stopped",
                            "progress": processed,
                            "total": total,
                            "message": job.message,
                            "logs": job.logs[-20:],
                        })
                        return
                    await asyncio.sleep(wait)
                    # Don't increment attempts — flood wait is not a failure

                except (FileReferenceExpired, Exception) as e:
                    err_msg = str(e)
                    is_retryable = any(x in err_msg.lower() for x in [
                        "file reference", "unexpected eof", "stream reading",
                        "connection", "timeout", "reset"
                    ])

                    if is_retryable and attempts < max_attempts:
                        backoff = backoff_schedule[attempts - 1]
                        job.add_log(
                            f"[Retry {attempts}/{max_attempts}] msg {msg_id}: "
                            f"{err_msg[:80]} — retrying in {backoff}s..."
                        )
                        await asyncio.sleep(backoff)
                        # Loop continues — tg.get_messages() gives fresh file reference
                    else:
                        job.add_log(f"✗ msg {msg_id} skipped: {err_msg[:120]}")
                        success = True  # skip this video, move on

                finally:
                    # Always clean up temp file
                    if tmp_file and os.path.exists(tmp_file):
                        try:
                            os.unlink(tmp_file)
                        except Exception:
                            pass

            # ── Progress update ──────────────────────────────────────────────
            processed += 1
            videos_since_cooldown += 1
            job.progress = processed

            should_notify = processed % 5 == 0 or processed >= total
            if should_notify:
                await notify_buddystore(webhook_url, {
                    "jobId": job.job_id,
                    "adminId": job.admin_id,
                    "status": "running",
                    "progress": processed,
                    "total": total,
                    "message": f"Imported {processed}/{total} videos",
                    "logs": job.logs[-20:],  # last 20 log lines
                })

            # ── Batch cooldown every 40 videos ──────────────────────────────
            if videos_since_cooldown >= 40 and processed < total:
                videos_since_cooldown = 0
                job.add_log("[Cooldown] 40 videos done. Resting 25s...")
                await notify_buddystore(webhook_url, {
                    "jobId": job.job_id, "adminId": job.admin_id,
                    "status": "running", "progress": processed, "total": total,
                    "message": "Batch cooldown: resting 25s...",
                })
                await asyncio.sleep(25)
            else:
                await asyncio.sleep(1.5)  # polite breathing delay

        # ── Job finished ─────────────────────────────────────────────────────
        if not job.stop_flag:
            job.status = "completed"
            job.message = f"Import completed. {processed}/{total} videos processed."

        final_status = job.status
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id,
            "adminId": job.admin_id,
            "status": final_status,
            "progress": processed,
            "total": total,
            "message": job.message,
            "logs": job.logs[-50:],
        })
