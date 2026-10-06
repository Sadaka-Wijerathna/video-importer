# importer.py — Full Hydrogram import loop
# Features: channel scan, startMsgId/endMsgId range, skipExisting watermark,
#           limitCount, duplicate check, thumbnail upload, flood wait,
#           EOF retry, batch cooldown, webhook progress callbacks.

import os
import asyncio
import subprocess
import httpx
from hydrogram import Client
from hydrogram.errors import FloodWait, FileReferenceExpired
from hydrogram.raw import functions, types as raw_types
from jobs import ImportJob

DOWNLOADS_DIR = "/tmp/tg_imports"
os.makedirs(DOWNLOADS_DIR, exist_ok=True)


# ── Webhook helper ─────────────────────────────────────────────────────────────

API_SECRET = os.getenv("IMPORTER_API_SECRET", "")

async def notify_buddystore(webhook_url: str, payload: dict):
    """Fire-and-forget progress callback to BuddyStore backend."""
    try:
        headers = {"x-api-secret": API_SECRET} if API_SECRET else {}
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(webhook_url, json=payload, headers=headers)
            if r.status_code >= 400:
                print(f"[webhook] Non-OK response {r.status_code}: {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"[webhook] Failed: {e}", flush=True)


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
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(duplicate_check_url, params=params)
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

    Strategy mirrors the original mtcute implementation exactly:
      • offsetId = end_message_id + 1  (ceiling, 0 = newest)
      • minId    = max(last_msg_id, start_message_id - 1)  (floor)
    """
    job.add_log("Scanning channel for videos...")
    job.message = "Scanning..."

    video_ids: list[int] = []
    offset_id = (end_message_id + 1) if end_message_id else 0
    # Use the highest floor available so we don't re-process videos
    # we already completed (last_msg_id) or before the user's requested start.
    floor_id = 0
    if start_message_id:
        floor_id = start_message_id - 1
    if last_msg_id and last_msg_id > floor_id:
        floor_id = last_msg_id

    scan_tick = 0
    has_more = True

    while has_more:
        if job.stop_flag:
            break

        try:
            # Resolve peer fresh each page (safe for usernames and IDs)
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

        page_had_new = False
        for msg in messages:
            if job.stop_flag:
                break
            if not hasattr(msg, "id"):
                continue
            if msg.id <= floor_id:
                has_more = False
                break
            if end_message_id and msg.id > end_message_id:
                continue

            page_had_new = True

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
                    scan_tick += 1

        # Advance offset to the oldest message in this page
        if messages:
            offset_id = messages[-1].id

        if scan_tick % 200 == 0 and scan_tick > 0:
            job.message = f"Scanning... {scan_tick} found"
            job.total = scan_tick
            await notify_buddystore(webhook_url, {
                "jobId": job.job_id, "adminId": job.admin_id,
                "status": "running", "progress": 0, "total": scan_tick,
                "message": f"Scanning... {scan_tick} videos found",
                "logs": job.logs[-20:],
            })

        if len(messages) < 100 or not page_had_new:
            has_more = False

    # Return in chronological order (oldest first, like original)
    video_ids.reverse()
    job.add_log(f"Scan complete: {len(video_ids)} videos found.")
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
    # To prevent OOM on free tier, skip ffmpeg for large videos (> 50MB)
    try:
        if os.path.exists(video_path) and os.path.getsize(video_path) > 50 * 1024 * 1024:
            print("[thumb] Video too large (>50MB), skipping ffmpeg to avoid OOM", flush=True)
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


# ── Main import loop ───────────────────────────────────────────────────────────

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
        job.add_log("🔄 Loading Telegram dialogs...")
        try:
            dialog_count = 0
            async for _ in tg.get_dialogs(limit=200):
                dialog_count += 1
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
        if original_total > 0:
            total = original_total
            processed = initial_progress
        else:
            total = num_remaining
            processed = 0

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
                "logs": job.logs[-20:],
            })
            return

        job.add_log(f"Starting import: {total} videos.")
        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": "running", "progress": 0, "total": total,
            "message": f"Starting import of {total} videos...",
            "logs": job.logs[-20:],
        })

        # ── Step 2: Concurrent import loop (1 parallel worker to save memory on free tier) ──────────────
        CONCURRENCY = 1
        semaphore = asyncio.Semaphore(CONCURRENCY)
        progress_lock = asyncio.Lock()

        # processed is initialized above
        videos_since_cooldown = 0
        latest_checkpoint_msg_id: int | None = None

        # Safe checkpoint tracking: record which indices in final_ids are done.
        # We only advance the checkpoint past a contiguous block of completions —
        # so if worker B finishes final_ids[1] before worker A finishes final_ids[0],
        # the checkpoint stays at whatever it was until final_ids[0] is also done.
        completed_flags: list[bool] = [False] * num_remaining
        safe_checkpoint_ptr = -1  # highest index where ALL 0..index are complete

        async def process_video(list_idx: int, msg_id: int) -> None:
            nonlocal processed, videos_since_cooldown, latest_checkpoint_msg_id, safe_checkpoint_ptr

            async with semaphore:
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

                    try:
                        # Fresh message fetch on every attempt → fresh file reference
                        msg = await tg.get_messages(src_peer, msg_id)
                        if not msg or (not msg.video and not msg.document):
                            job.add_log(f"{tag} Skipped — not a video")
                            success = True
                            break

                        media = msg.video or msg.document
                        unique_id: str | None = getattr(media, "file_unique_id", None)
                        file_size: int = getattr(media, "file_size", 0) or 0
                        duration: int = getattr(media, "duration", 0) or 0
                        width: int = getattr(media, "width", 0) or 0
                        height: int = getattr(media, "height", 0) or 0
                        caption: str = msg.caption or ""

                        # ── Duplicate check ───────────────────────────────────────
                        if skip_existing and target_bot_db_id:
                            dup = await is_duplicate(
                                duplicate_check_url, target_bot_db_id,
                                unique_id, file_size, duration
                            )
                            if dup:
                                job.add_log(f"{tag} Skipped — duplicate already in DB")
                                success = True
                                break

                        # ── Fast forward (works for non-restricted channels) ───────
                        try:
                            await tg.forward_messages(tgt_peer, src_peer, msg_id)
                            file_size_mb = file_size / (1024 * 1024)
                            job.add_log(f"{tag} ✅ Imported ({file_size_mb:.1f} MB) — forwarded")
                            success = True
                            break
                        except Exception as fwd_err:
                            fwd_msg = str(fwd_err)
                            if "CHAT_FORWARDS_RESTRICTED" in fwd_msg or \
                               "you can't forward" in fwd_msg.lower() or \
                               "forbidden" in fwd_msg.lower():
                                pass  # expected — fall through to download+upload
                            else:
                                raise  # re-raise unexpected errors

                        # ── Download to disk ───────────────────────────────────────
                        file_size_mb = file_size / (1024 * 1024)
                        job.message = f"{tag} Downloading ({file_size_mb:.1f} MB)..."
                        tmp_prefix = os.path.join(
                            DOWNLOADS_DIR, f"{job.admin_id}_{msg_id}"
                        )
                        tmp_file = f"{tmp_prefix}.tmp"

                        # Hydrogram streams directly to disk — no full-RAM buffering
                        await tg.download_media(msg, file_name=tmp_file)

                        # ── Thumbnail ─────────────────────────────────────────────
                        raw_doc = getattr(media, "_raw", None) or getattr(msg, "_raw", None)
                        if raw_doc and hasattr(raw_doc, "thumbs"):
                            thumb_file = await download_thumbnail(tg, raw_doc, tmp_prefix)

                        # Fallback: generate thumbnail from the video itself via ffmpeg
                        if not thumb_file and tmp_file and os.path.exists(tmp_file):
                            thumb_file = generate_thumbnail_ffmpeg(
                                tmp_file, f"{tmp_prefix}_thumb.jpg"
                            )

                        # ── Upload from disk ───────────────────────────────────────
                        job.message = f"{tag} Uploading ({file_size_mb:.1f} MB)..."
                        send_kwargs: dict = dict(
                            chat_id=tgt_peer,
                            video=tmp_file,
                            caption=caption,
                            duration=duration,
                            width=width,
                            height=height,
                            supports_streaming=True,
                        )
                        if thumb_file and os.path.exists(thumb_file):
                            send_kwargs["thumb"] = thumb_file

                        await tg.send_video(**send_kwargs)
                        job.add_log(f"{tag} ✅ Imported ({file_size_mb:.1f} MB)")
                        success = True

                    except FloodWait as fw:
                        wait = fw.value + 3
                        job.add_log(f"{tag} ⏳ Rate limited — waiting {wait}s...")
                        if fw.value > 3600:
                            job.status = "stopped"
                            job.message = (
                                f"Telegram rate limit: wait "
                                f"{fw.value // 60} min. Resume later."
                            )
                            job.add_log(
                                f"{tag} ⏸ Rate limit too long ({fw.value // 60} min) — paused. Resume later."
                            )
                            await notify_buddystore(webhook_url, {
                                "jobId": job.job_id, "adminId": job.admin_id,
                                "status": "stopped",
                                "progress": processed, "total": total,
                                "message": job.message,
                                "logs": job.logs[-20:],
                                "checkpointMsgId": latest_checkpoint_msg_id,
                            })
                            return
                        await asyncio.sleep(wait)
                        # Don't increment attempts — flood wait is not a failure

                    except Exception as e:
                        err_msg = str(e)
                        is_retryable = any(x in err_msg.lower() for x in [
                            "file reference", "unexpected eof", "stream reading",
                            "connection", "timeout", "reset", "filelocat"
                        ])
                        if is_retryable and attempts < max_attempts:
                            backoff = backoff_schedule[attempts - 1]
                            job.add_log(
                                f"{tag} 🔄 Retry {attempts}/{max_attempts}: "
                                f"{err_msg[:60]} — waiting {backoff}s..."
                            )
                            await asyncio.sleep(backoff)
                        else:
                            job.add_log(f"{tag} ❌ Failed — {err_msg[:100]}")
                            success = True  # skip this video, move on

                    finally:
                        for f in [tmp_file, thumb_file]:
                            if f and os.path.exists(f):
                                try:
                                    os.unlink(f)
                                except Exception:
                                    pass

            # ── Update shared progress (guarded by lock) ──────────────────────
            # Note: asyncio is single-threaded so the lock prevents logical races
            # between the two coroutines at their respective await boundaries.
            local_should_cooldown = False
            async with progress_lock:
                processed += 1
                videos_since_cooldown += 1
                job.progress = processed

                # Advance safe checkpoint pointer: walk forward from the last safe
                # position as long as the next index is also marked complete.
                completed_flags[list_idx] = True
                while (
                    safe_checkpoint_ptr + 1 < num_remaining
                    and completed_flags[safe_checkpoint_ptr + 1]
                ):
                    safe_checkpoint_ptr += 1
                if safe_checkpoint_ptr >= 0:
                    latest_checkpoint_msg_id = final_ids[safe_checkpoint_ptr]

                local_processed = processed
                local_checkpoint = latest_checkpoint_msg_id

                # Cooldown every 20 completed videos (was 40 — halved for 2× throughput)
                if videos_since_cooldown >= 20 and local_processed < total:
                    videos_since_cooldown = 0
                    local_should_cooldown = True

            # Progress callback and cooldown outside the lock
            await notify_buddystore(webhook_url, {
                "jobId": job.job_id, "adminId": job.admin_id,
                "status": "running",
                "progress": local_processed, "total": total,
                "message": f"Imported {local_processed}/{total} videos",
                "logs": job.logs[-20:],
                "checkpointMsgId": local_checkpoint,
            })

            if local_should_cooldown and local_processed < total:
                job.add_log(f"⏳ Batch cooldown — resting 15s ({local_processed}/{total} done)")
                await asyncio.sleep(15)   # was 25s every 40 — now 15s every 20
            else:
                await asyncio.sleep(0.8)  # was 1.5s — reduced for 2× throughput

        # Launch all tasks; semaphore keeps exactly CONCURRENCY running at once
        job.add_log(f"🚀 Starting concurrent import: {total} videos, {CONCURRENCY} workers.")
        await asyncio.gather(
            *[process_video(i, mid) for i, mid in enumerate(final_ids)],
            return_exceptions=True,
        )

        # ── Job finished ──────────────────────────────────────────────────────
        if not job.stop_flag:
            job.status = "completed"
            job.message = f"✅ Import complete — {processed}/{total} videos imported."

        await notify_buddystore(webhook_url, {
            "jobId": job.job_id, "adminId": job.admin_id,
            "status": job.status,
            "progress": processed, "total": total,
            "message": job.message,
            "logs": job.logs[-50:],
            "checkpointMsgId": latest_checkpoint_msg_id,
        })
