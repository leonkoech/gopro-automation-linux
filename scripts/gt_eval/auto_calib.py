"""Per-game court calibration: Gemini repaints the 3PT/4PT lines in flat
marker colors, cv2 extracts the masks, and we vectorize to the same polyline
json the classifier already consumes.

Usage: python3 auto_calib.py <frame.jpg> <angle FL|FR> <out.json> [overlay.jpg]
"""
import json
import sys
from datetime import datetime, timezone

import cv2
import numpy as np
from google import genai
from google.genai import types

W, H = 1920, 1080
MODEL = "gemini-3-pro-image-preview"
PROMPT = (
    "Repaint this basketball court photo with two edits and NOTHING else "
    "changed: (1) trace the WHITE painted 3-point arc line on the wooden "
    "floor in pure cyan (#00FFFF), about 8 pixels wide, along its entire "
    "visible length including the corners; (2) trace the thin RED painted "
    "4-point line (the outermost arc on the wood, beyond the 3-point line) "
    "in pure yellow (#FFFF00), same width, entire visible length. Keep every "
    "other pixel identical to the input. Do not trace the paint inside the "
    "key or the center circle."
)


def load_key():
    for line in open("/home/dev/shot_typing/.env"):
        if line.startswith("GOOGLE_API_KEY="):
            return line.strip().split("=", 1)[1]
    raise SystemExit("no GOOGLE_API_KEY")


def vectorize(mask, n_bins=40):
    ys, xs = np.nonzero(mask)
    if len(xs) < 500:
        return []
    x0, x1 = xs.min(), xs.max()
    pts = []
    for b in range(n_bins):
        lo = x0 + (x1 - x0) * b / n_bins
        hi = x0 + (x1 - x0) * (b + 1) / n_bins
        sel = (xs >= lo) & (xs < hi)
        if sel.sum() < 10:
            continue
        pts.append([float(xs[sel].mean()), float(np.median(ys[sel]))])
    # Outlier rejection: an occluder (person/logo) can bend a bin far off the
    # arc. Drop points whose y deviates >45px from the median of their
    # neighborhood, then keep the longest consistent run.
    if len(pts) >= 7:
        cleaned = []
        for i, (x, y) in enumerate(pts):
            nb = [pts[j][1] for j in range(max(0, i - 3), min(len(pts), i + 4)) if j != i]
            if abs(y - float(np.median(nb))) <= 45:
                cleaned.append([x, y])
        if len(cleaned) >= max(10, len(pts) // 2):
            pts = cleaned
    return pts


def main():
    frame_path, angle, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    overlay_path = sys.argv[4] if len(sys.argv) > 4 else None
    client = genai.Client(api_key=load_key())
    img_bytes = open(frame_path, "rb").read()
    resp = client.models.generate_content(
        model=MODEL,
        contents=[types.Part.from_bytes(data=img_bytes, mime_type="image/jpeg"), PROMPT],
    )
    painted = None
    for part in resp.candidates[0].content.parts:
        if getattr(part, "inline_data", None) and part.inline_data.data:
            painted = np.frombuffer(part.inline_data.data, dtype=np.uint8)
            painted = cv2.imdecode(painted, cv2.IMREAD_COLOR)
    if painted is None:
        raise SystemExit("no image returned")
    ph, pw = painted.shape[:2]
    # masks in the painted image's own resolution, then scale coords to 1920x1080
    b, g, r = painted[:, :, 0].astype(int), painted[:, :, 1].astype(int), painted[:, :, 2].astype(int)
    cyan = ((b > 180) & (g > 180) & (r < 120)).astype(np.uint8)
    yellow = ((r > 180) & (g > 180) & (b < 120)).astype(np.uint8)
    sx, sy = W / pw, H / ph
    three = [[x * sx, y * sy] for x, y in vectorize(cyan)]
    four = [[x * sx, y * sy] for x, y in vectorize(yellow, n_bins=35)]
    if len(three) < 15 or len(four) < 12:
        raise SystemExit(f"trace too sparse: three={len(three)} four={len(four)}")

    def close_region(pts):
        """The classifier treats the polyline as a CLOSED polygon (implicit
        first-last edge). An open arc closes across the court interior and
        everything near the basket tests outside (the round-3 4PT explosion).
        Close along the baseline instead: run back above the arc's top edge."""
        y_top = min(p[1] for p in pts) - 120
        return pts + [[pts[-1][0], y_top], [pts[0][0], y_top]]

    three = close_region(three)
    four = close_region(four)
    out = {"angle": angle, "w": W, "h": H, "model": MODEL,
           "three_pt_white": three, "four_pt_red": four,
           "notes": f"auto_calib {datetime.now(timezone.utc).isoformat()} "
                    f"cyan={int(cyan.sum())}px yellow={int(yellow.sum())}px src={frame_path}"}
    json.dump(out, open(out_path, "w"))
    print(f"CALIB_OK three={len(three)} four={len(four)} painted={pw}x{ph}")
    if overlay_path:
        img = cv2.imread(frame_path)
        img = cv2.resize(img, (W, H))
        for pts, col in ((three, (255, 255, 0)), (four, (255, 0, 255))):
            arr = np.array(pts, dtype=np.int32)
            cv2.polylines(img, [arr], False, col, 3)
        cv2.imwrite(overlay_path, img)
        print("overlay written", overlay_path)


if __name__ == "__main__":
    main()
