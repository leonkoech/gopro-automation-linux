"""Offline shot-detection backtest harness.

Replays a fully human-annotated past game (ground truth = the annotation `plays`
table) against the SL/SR high-fps detector two ways — a manual/trigger replay
(the deployed validate node) and an automated end-to-end scan — and compares
both to GT. See docs/SHOT_DETECTION_BACKTEST_PLAN.md.

This subpackage is OFFLINE ONLY: nothing in agx_pipeline.service imports it, and
running it does NOT enable the live node (SHOT_VALIDATION_ENABLED is untouched).
"""
