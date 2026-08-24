# Shot-Detection Backtest — how to run

Offline validation of the SL/SR detector against a fully human-annotated past game.
Full design: `docs/SHOT_DETECTION_BACKTEST_PLAN.md`. **Runs offline — does not touch
`SHOT_VALIDATION_ENABLED` or the live service.** Run on the **box** (GPU); the Mac OOMs.

## Frozen ground truth
`data/<short-id>_gt.json` (committed). Regenerate from the annotation DB:
```bash
python3 -m agx_pipeline.shot_detect.backtest.gt <game_id> --freeze
# needs annotation Supabase creds: NEXT_PUBLIC_SUPABASE_URL / _ANON_KEY /
# _SYNC_EMAIL / _SYNC_PASSWORD (env, or the wb repo .env auto-read on the Mac)
```

## Full run (box)
```bash
cd /home/dev/gopro-automation-linux
python3 -m agx_pipeline.shot_detect.backtest.run \
  --game   fdcd9bd4-3615-4b4a-911f-3a5242c561ac \
  --weight agx_pipeline/shot_detect/weights/ball_yolo26s_gray_hifps_v3_best.pt \
  --footage-dir /home/dev/backtest/fdcd9bd4 \
  --out         /home/dev/backtest/out \
  --s3-prefix   court-a/2026-07-28/fdcd9bd4-3615-4b4a-911f
# add --subset 20 for a setup-1 smoke; --confirm-setup2 to refine the auto list at full fps
```
Stages: stage footage from S3 → per-cam rim estimate + coarse 30fps scan → δ calibrate →
setup-1 windowed replay → setup-2 detected list → score → write
`out/<short>_backtest.json` (+ Firebase `shot-backtests/<game_id>` for the frontend card).

Cost: coarse scan ≈ ~53 min/cam; setup-1 windowed ≈ tens of min. Run as a background job
(`nohup … &`) in a no-live-games window — it shares the GPU with recording/transcode.

## Reading results
`out/<short>_backtest.json`:
- `calibration.{SL,SR}` — δ, matched/n_gt, `peakness` (want ~1; low = footage/GT don't line up → stop).
- `setup1_manual` — coverage + make/miss confusion (this is the Phase-1 shadow accuracy).
- `setup2_automated` — detection precision/recall + make/miss accuracy on matched pairs.
