"""
Jetson & GoPro Health Monitor

Uses the Tailscale API to check if Jetsons are online, then queries each
Jetson directly (via Tailscale IP) for GoPro status.
Sends email alerts via SMTP when issues are detected.

Designed to run as a Northflank cron job every 10-20 minutes.

Environment variables:
    TAILSCALE_API_KEY       — Tailscale API key (tskey-api-...)
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM_EMAIL
    ALERT_TO_EMAIL          — recipient (default: courtside@uai.tech)
    EXPECTED_GOPROS_PER_JETSON (default: 2)
    CHECK_TIMEOUT           — seconds per HTTP attempt (default: 20)
    CHECK_RETRIES           — extra retries after first failure (default: 2)
    CHECK_RETRY_BACKOFF_SEC — seconds between retries (default: 3)
    STALE_THRESHOLD_MIN     — minutes before a device is considered offline (default: 5)
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger(__name__)

# --- Configuration ---

TAILSCALE_API_KEY = os.environ.get("TAILSCALE_API_KEY", "")

# Jetson devices to monitor — hostname must match Tailscale device hostname.
# Treat empty string the same as unset so callers (e.g. GitHub Actions) can
# pass JETSON_DEVICES="" without crashing on json.loads.
_jetson_devices_raw = os.getenv("JETSON_DEVICES", "").strip()
JETSON_DEVICES = json.loads(_jetson_devices_raw) if _jetson_devices_raw else [
    {"name": "Jetson Nano 1", "tailscale_hostname": "jetson-nano-002", "tailscale_ip": "100.87.190.71"},
    {"name": "Jetson Nano 2", "tailscale_hostname": "JETSON-NANO-001", "tailscale_ip": "100.106.30.98"},
]

SMTP_HOST = os.environ.get("SMTP_HOST", "smtpout.secureserver.net")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.environ.get("SMTP_FROM_EMAIL", "Courtside@uai.tech")
ALERT_TO_EMAIL = os.environ.get("ALERT_TO_EMAIL", "courtside@uai.tech")

EXPECTED_GOPROS = int(os.environ.get("EXPECTED_GOPROS_PER_JETSON", "2"))
CHECK_TIMEOUT = int(os.environ.get("CHECK_TIMEOUT", "20"))
CHECK_RETRIES = int(os.environ.get("CHECK_RETRIES", "2"))
CHECK_RETRY_BACKOFF_SEC = float(os.environ.get("CHECK_RETRY_BACKOFF_SEC", "3"))
STALE_THRESHOLD_MIN = int(os.environ.get("STALE_THRESHOLD_MIN", "5"))

# Alert dedup — suppresses repeat emails for the same ongoing issue.
# State is persisted between cron runs (e.g. via GitHub Actions cache) so the
# same problem only emails once per UTC day; the next day it re-alerts once.
ALERT_STATE_PATH = os.environ.get("ALERT_STATE_PATH", ".monitor-alert-state.json")
FORCE_ALERT = os.environ.get("FORCE_ALERT", "").strip().lower() in ("1", "true", "yes")


# --- Data structures ---

@dataclass(frozen=True)
class JetsonStatus:
    name: str
    tailscale_hostname: str
    tailscale_online: bool
    last_seen: Optional[str] = None
    last_seen_ago_min: Optional[float] = None
    disk_free_gb: Optional[float] = None
    gopro_count: int = 0
    gopro_names: tuple[str, ...] = field(default_factory=tuple)
    gopro_error: Optional[str] = None
    error: Optional[str] = None


# --- Tailscale API ---

def get_tailscale_devices() -> dict[str, dict]:
    """Fetch all devices from Tailscale API, keyed by lowercase hostname."""
    if not TAILSCALE_API_KEY:
        log.error("TAILSCALE_API_KEY not set")
        return {}

    req = Request(
        "https://api.tailscale.com/api/v2/tailnet/-/devices?fields=all",
        headers={"Authorization": f"Bearer {TAILSCALE_API_KEY}"},
    )
    try:
        with urlopen(req, timeout=CHECK_TIMEOUT) as resp:
            data = json.loads(resp.read())
    except (URLError, OSError, json.JSONDecodeError) as exc:
        log.error("Tailscale API error: %s", exc)
        return {}

    return {
        d["hostname"].lower(): d
        for d in data.get("devices", [])
        if "hostname" in d
    }


# --- GoPro check via direct Tailscale IP ---

def _fetch_json(url: str, attempts: int) -> tuple[Optional[dict], Optional[Exception]]:
    """GET a JSON endpoint with retries. Returns (data, last_error)."""
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=CHECK_TIMEOUT) as resp:
                return json.loads(resp.read()), None
        except (URLError, OSError, json.JSONDecodeError, TimeoutError) as exc:
            last_exc = exc
            if i < attempts - 1:
                log.warning("Retry %d/%d for %s: %s", i + 1, attempts - 1, url, exc)
                time.sleep(CHECK_RETRY_BACKOFF_SEC)
    return None, last_exc


def check_gopros(tailscale_ip: str) -> tuple[int, tuple[str, ...], Optional[float], Optional[str]]:
    """Query Jetson's /api/gopros and /api/system/info via Tailscale IP.

    Retries transient failures so a single slow/busy response doesn't trigger
    a false alert. Returns (gopro_count, gopro_names, disk_free_gb, error).
    When ``error`` is set, ``gopro_count`` and ``gopro_names`` are unknown —
    callers must not treat them as "zero GoPros connected".
    """
    base = f"http://{tailscale_ip}:5000"
    attempts = max(1, 1 + CHECK_RETRIES)

    data, exc = _fetch_json(f"{base}/api/gopros", attempts)
    if data is None:
        return 0, (), None, f"GoPro check failed: {exc}"

    gopros = data.get("gopros", [])
    gopro_count = len(gopros)
    gopro_names = tuple(g.get("name", g.get("id", "unknown")) for g in gopros)

    # Disk space is non-critical — single attempt, swallow errors.
    info, _ = _fetch_json(f"{base}/api/system/info", 1)
    disk_free_gb = info.get("system", {}).get("disk_free_gb") if info else None

    return gopro_count, gopro_names, disk_free_gb, None


# --- Main check ---

def check_all_jetsons() -> list[JetsonStatus]:
    """Check all configured Jetsons via Tailscale API + direct GoPro query."""
    ts_devices = get_tailscale_devices()
    now = datetime.now(timezone.utc)
    statuses: list[JetsonStatus] = []

    for cfg in JETSON_DEVICES:
        name = cfg["name"]
        ts_hostname = cfg["tailscale_hostname"]
        ts_ip = cfg["tailscale_ip"]

        device = ts_devices.get(ts_hostname.lower())

        if not device:
            statuses.append(JetsonStatus(
                name=name, tailscale_hostname=ts_hostname,
                tailscale_online=False,
                error=f"Device '{ts_hostname}' not found in Tailscale",
            ))
            continue

        # Parse lastSeen
        last_seen_str = device.get("lastSeen", "")
        last_seen_ago: Optional[float] = None
        is_online = False
        if last_seen_str:
            try:
                last_seen_dt = datetime.fromisoformat(last_seen_str.replace("Z", "+00:00"))
                last_seen_ago = (now - last_seen_dt).total_seconds() / 60.0
                is_online = last_seen_ago < STALE_THRESHOLD_MIN
            except ValueError:
                pass

        # If online, query GoPros directly
        gopro_count = 0
        gopro_names: tuple[str, ...] = ()
        disk_free_gb: Optional[float] = None
        gopro_error: Optional[str] = None

        if is_online:
            gopro_count, gopro_names, disk_free_gb, gopro_error = check_gopros(ts_ip)

        statuses.append(JetsonStatus(
            name=name, tailscale_hostname=ts_hostname,
            tailscale_online=is_online,
            last_seen=last_seen_str, last_seen_ago_min=last_seen_ago,
            disk_free_gb=disk_free_gb,
            gopro_count=gopro_count, gopro_names=gopro_names,
            gopro_error=gopro_error,
        ))

    return statuses


# --- Alert logic ---

def build_alert(statuses: list[JetsonStatus]) -> Optional[str]:
    """Build alert message if issues found. Returns None if all healthy."""
    issues: list[str] = []

    for s in statuses:
        if not s.tailscale_online:
            ago = f" (last seen {s.last_seen_ago_min:.0f} min ago)" if s.last_seen_ago_min else ""
            issues.append(
                f"CRITICAL — {s.name} ({s.tailscale_hostname}) is OFFLINE{ago}"
                + (f"\n   {s.error}" if s.error else "")
            )
        else:
            if s.gopro_error:
                issues.append(f"WARNING — {s.name}: {s.gopro_error}")
            elif s.gopro_count < EXPECTED_GOPROS:
                issues.append(
                    f"WARNING — {s.name}: Only {s.gopro_count}/{EXPECTED_GOPROS} GoPros connected"
                    f"\n   Connected: {', '.join(s.gopro_names) or 'none'}"
                )
            if s.disk_free_gb is not None and s.disk_free_gb < 5.0:
                issues.append(
                    f"WARNING — {s.name}: Low disk space ({s.disk_free_gb:.1f} GB free)"
                )

    if not issues:
        return None

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = f"UBALL System Alert — {now}\n{'=' * 50}\n\n"
    body += "\n\n".join(issues)
    body += "\n\n" + "-" * 50
    body += "\nAll Jetson Status:\n"
    for s in statuses:
        status_str = "ONLINE" if s.tailscale_online else "OFFLINE"
        if not s.tailscale_online:
            gopro_info = "N/A"
        elif s.gopro_error:
            gopro_info = "GoPro check unreachable"
        else:
            gopro_info = f"{s.gopro_count} GoPros"
        disk_info = f"{s.disk_free_gb:.1f}GB free" if s.disk_free_gb else "N/A"
        body += f"  {s.name}: {status_str} | {gopro_info} | {disk_info}\n"

    return body


def compute_alert_signature(statuses: list[JetsonStatus]) -> str:
    """Stable signature of current issues — used to dedup repeat alerts.

    Two runs that surface the same set of problems on the same devices produce
    the same signature, so we can suppress the second email. If the problem
    set changes (e.g. a second Jetson goes offline), the signature changes
    and we re-alert immediately.
    """
    parts: list[str] = []
    for s in sorted(statuses, key=lambda x: x.name):
        if not s.tailscale_online:
            parts.append(f"{s.name}:offline")
            continue
        if s.gopro_error:
            parts.append(f"{s.name}:gopro_api_unreachable")
        elif s.gopro_count < EXPECTED_GOPROS:
            parts.append(f"{s.name}:gopros={s.gopro_count}")
        if s.disk_free_gb is not None and s.disk_free_gb < 5.0:
            parts.append(f"{s.name}:low_disk")
    return "|".join(parts)


def load_alert_state() -> dict:
    """Load previous alert state; returns {} if missing or unreadable."""
    try:
        with open(ALERT_STATE_PATH) as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_alert_state(state: dict) -> None:
    """Persist alert state so the next cron run can dedup against it."""
    try:
        with open(ALERT_STATE_PATH, "w") as f:
            json.dump(state, f)
    except OSError as exc:
        log.warning("Failed to write alert state to %s: %s", ALERT_STATE_PATH, exc)


def should_send_alert(signature: str, today_utc: str) -> bool:
    """Suppress repeat alerts with the same signature on the same UTC day.

    Alert is sent when:
      - FORCE_ALERT env var is truthy (for manual testing), OR
      - no prior state exists (first alert after recovery), OR
      - the issue signature changed (problem set escalated), OR
      - the UTC date rolled over (send one reminder the next day).
    """
    if FORCE_ALERT:
        return True
    state = load_alert_state()
    return not (
        state.get("signature") == signature and state.get("date") == today_utc
    )


def send_email(subject: str, body: str) -> None:
    """Send alert email via SMTP."""
    if not SMTP_USER or not SMTP_PASSWORD:
        log.error("SMTP credentials not configured — printing alert instead")
        log.info("Subject: %s\n%s", subject, body)
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM_EMAIL
    msg["To"] = ALERT_TO_EMAIL
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM_EMAIL, [ALERT_TO_EMAIL], msg.as_string())
        log.info("Alert email sent to %s", ALERT_TO_EMAIL)
    except Exception as exc:
        log.error("Failed to send alert email: %s", exc)
        raise


# --- Main ---

def main() -> None:
    log.info("Starting Jetson health check (%d devices)", len(JETSON_DEVICES))

    statuses = check_all_jetsons()

    for s in statuses:
        if not s.tailscale_online:
            log.warning("%s: OFFLINE — last seen %.0f min ago",
                        s.name, s.last_seen_ago_min or -1)
        elif s.gopro_error:
            log.warning("%s: ONLINE but GoPro API unreachable — %s",
                        s.name, s.gopro_error)
        else:
            log.info(
                "%s: ONLINE | %d GoPros (%s) | %s",
                s.name, s.gopro_count,
                ", ".join(s.gopro_names) or "none",
                f"{s.disk_free_gb:.1f} GB free" if s.disk_free_gb else "disk N/A",
            )

    alert_body = build_alert(statuses)
    now = datetime.now(timezone.utc)
    today_utc = now.strftime("%Y-%m-%d")

    if not alert_body:
        # All healthy — clear dedup state so the next issue alerts immediately.
        if os.path.exists(ALERT_STATE_PATH):
            try:
                os.remove(ALERT_STATE_PATH)
            except OSError as exc:
                log.warning("Failed to clear alert state: %s", exc)
        log.info("All systems healthy — no alert needed")
        return

    offline = [s.name for s in statuses if not s.tailscale_online]
    unreachable = [s.name for s in statuses if s.tailscale_online and s.gopro_error]
    low_gopros = [
        s.name for s in statuses
        if s.tailscale_online and not s.gopro_error and s.gopro_count < EXPECTED_GOPROS
    ]

    if offline:
        subject = f"UBALL Alert: {', '.join(offline)} OFFLINE"
    elif low_gopros:
        subject = f"UBALL Alert: GoPro disconnected on {', '.join(low_gopros)}"
    elif unreachable:
        subject = f"UBALL Alert: {', '.join(unreachable)} API unreachable"
    else:
        subject = "UBALL Alert: Jetson/GoPro Issue Detected"

    signature = compute_alert_signature(statuses)
    if not should_send_alert(signature, today_utc):
        log.info(
            "Issues detected but alert suppressed — same signature already "
            "emailed today (signature=%r, date=%s). Will re-alert tomorrow "
            "if still present.",
            signature, today_utc,
        )
        return

    log.warning("Issues detected — sending alert")
    send_email(subject, alert_body)
    save_alert_state({
        "signature": signature,
        "date": today_utc,
        "last_sent_at": now.isoformat(),
        "subject": subject,
    })


if __name__ == "__main__":
    main()
