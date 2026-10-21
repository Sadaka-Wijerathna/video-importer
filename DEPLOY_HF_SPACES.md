# BuddyStore Video Importer — Step-by-step HuggingFace Spaces Deployment
# 
# Prerequisites: git installed, HuggingFace account at huggingface.co (free, no card)

## Step 1 — Create the Space

Go to: https://huggingface.co/new-space

Fill in:
  - Owner: your HF username
  - Space name: buddystore-video-importer   (or any name)
  - License: MIT
  - SDK: Docker                             ← IMPORTANT: must be Docker, not Gradio
  - Visibility: Private                     ← keep private (contains API endpoints)

Click "Create Space"

---

## Step 2 — Set Secrets (in HF Space Settings)

Go to your Space → Settings → Repository secrets → New secret

Add each of these (copy from your Render env vars or .env file):

  Name: TELEGRAM_API_ID
  Value: (your Telegram API ID number)

  Name: TELEGRAM_API_HASH
  Value: (your Telegram API hash)

  Name: IMPORTER_API_SECRET
  Value: (shared secret — same value as in buddystore-backend)

  Name: BUDDYSTORE_BACKEND_URL
  Value: https://buddystore-backend.onrender.com  (or wherever your backend is)

  Name: PYTHON_VERSION
  Value: 3.12.0

---

## Step 3 — Push code to the Space

Run these commands in your video-importer directory:

  # Add the HF Space as a remote
  git remote add hf-space https://huggingface.co/spaces/YOUR_HF_USERNAME/buddystore-video-importer

  # Push (you'll be asked for HF username + password or token)
  # Use a write-scoped token from: https://huggingface.co/settings/tokens
  git push hf-space main

---

## Step 4 — Wait for build

HF Spaces will:
  1. Clone your repo
  2. Build the Dockerfile (installs Python deps + ffmpeg)
  3. Start uvicorn on port 7860
  4. Show "Running" status

Build takes ~2-3 minutes. Check logs at:
  https://huggingface.co/spaces/YOUR_HF_USERNAME/buddystore-video-importer/logs

---

## Step 5 — Get your Space URL

Your service URL will be:
  https://YOUR_HF_USERNAME-buddystore-video-importer.hf.space

Example:  https://sadak-buddystore-video-importer.hf.space

---

## Step 6 — Update buddystore-backend env var

In your Render buddystore-backend service, update:

  VIDEO_IMPORTER_URL = https://YOUR_HF_USERNAME-buddystore-video-importer.hf.space

The backend will now route all import jobs to HF Spaces instead of Render.

---

## Step 7 — Set up UptimeRobot (prevent 48hr sleep)

HF Spaces free tier sleeps after 48 hours of no traffic.
The video-importer only gets traffic when a job runs, so we need a keep-alive.

1. Go to: https://uptimerobot.com  (free, no credit card)
2. Sign up → Add New Monitor
3. Monitor Type: HTTP(s)
4. Friendly Name: BuddyStore Video Importer
5. URL: https://YOUR_HF_USERNAME-buddystore-video-importer.hf.space/health
6. Monitoring Interval: Every 30 minutes
7. Save

This pings /health every 30 min → Space never sleeps.

---

## Troubleshooting

Build fails with "permission denied":
  → The Dockerfile creates user 1000. Make sure /tmp/tg_imports is chowned in the Dockerfile (already done).

Space shows "Building" forever:
  → Check build logs. Usually a pip install failure. Try: pip install hydrogram==0.2.0 manually to test.

"Port 7860 not responding":
  → The CMD in Dockerfile must use --port 7860. Already set correctly.

Backend gets 502/504 calling importer:
  → The Space is sleeping. UptimeRobot ping should prevent this.
  → Or Space is restarting — check HF Spaces logs.

Jobs don't auto-resume after Space restart:
  → Expected behavior. The backend's /auto-resume route will re-trigger jobs.
  → The 5-second startup delay in main.py handles this automatically.
