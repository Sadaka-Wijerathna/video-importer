#!/usr/bin/env python3
"""Quick test: generate a 640x360 dummy thumbnail and apply the watermark."""
import subprocess, os, sys

# Step 1: Generate a dark test image
test_img = "/tmp/test_thumb_orig.jpg"
result = subprocess.run(
    ["ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=0x1e293b:s=640x360:d=1",
     "-frames:v", "1", test_img],
    capture_output=True, text=True
)
if result.returncode != 0:
    print("FAIL step1:", result.stderr[-300:])
    sys.exit(1)
print("Step 1 OK — test image created:", os.path.getsize(test_img), "bytes")

# Step 2: Apply dual watermark
wm_img = "/tmp/test_thumb_wm.jpg"
vf = (
    "drawtext=text='@BuddySeller'"
    ":fontsize=22:fontcolor=white@0.92"
    ":x=(W-tw)/2:y=(H-th)/2"
    ":box=1:boxcolor=0x000000@0.42:boxborderw=12"
    ",drawtext=text='I N T E R N A L   U S E   O N L Y'"
    ":fontsize=9:fontcolor=white@0.65"
    ":x=(W-tw)/2:y=H-th-12"
)
result = subprocess.run(
    ["ffmpeg", "-y", "-i", test_img, "-vf", vf, "-q:v", "3", wm_img],
    capture_output=True, text=True
)
if result.returncode != 0:
    print("FAIL step2:", result.stderr[-500:])
    sys.exit(1)
print("Step 2 OK — watermarked image:", os.path.getsize(wm_img), "bytes")
print("WATERMARK_TEST_PASSED")
