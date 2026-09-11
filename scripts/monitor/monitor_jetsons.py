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
    STALE_THRESHOLD_MIN     — fallback: minutes before a device with no
                              Tailscale "online" flag is considered offline
                              (default: 5). The API's own "online" flag is
                              preferred when present.
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
from urllib.error import HTTPError, URLError

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
# Per-device keys:
#   name, tailscale_hostname, tailscale_ip  — required
#   check_service (default True)            — also query the device's
#       :5000 API for recorder count + disk free. Set False to only check
#       Tailscale online/offline (e.g. the AGX, which doesn't serve that API;
#       its recorder health is covered by the camrec exporter + Prometheus).
_jetson_devices_raw = os.getenv("JETSON_DEVICES", "").strip()
JETSON_DEVICES = json.loads(_jetson_devices_raw) if _jetson_devices_raw else [
    {"name": "AGX Orin (agx-1)", "tailscale_hostname": "agxorin001",
     "tailscale_ip": "100.116.99.109", "check_service": False},
    # The two Jetson Nanos are decommissioned — intentionally off, so we don't
    # monitor them (they would just alert as permanently OFFLINE). Add the next
    # AGX Orin(s) here, or override the whole list via the JETSON_DEVICES env.
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
    service_checked: bool = True  # False => only Tailscale online/offline was checked
    error: Optional[str] = None


# --- Tailscale API ---

class TailscaleAPIError(RuntimeError):
    """The Tailscale API itself could not be queried.

    Raised for an expired/revoked key, a network failure, or a malformed
    response — i.e. cases where we simply do not know any device's state.
    Distinct from a successful call that happens to return no matching
    devices, so callers can avoid the false "everything is OFFLINE" alarm.
    """


def get_tailscale_devices() -> dict[str, dict]:
    """Fetch all devices from Tailscale API, keyed by lowercase hostname.

    Raises TailscaleAPIError if the API cannot be queried.
    """
    if not TAILSCALE_API_KEY:
        raise TailscaleAPIError("TAILSCALE_API_KEY is not set")

    req = Request(
        "https://api.tailscale.com/api/v2/tailnet/-/devices?fields=all",
        headers={"Authorization": f"Bearer {TAILSCALE_API_KEY}"},
    )
    try:
        with urlopen(req, timeout=CHECK_TIMEOUT) as resp:
            data = json.loads(resp.read())
    except HTTPError as exc:
        if exc.code in (401, 403):
            detail = (
                f"HTTP {exc.code} - the TAILSCALE_API_KEY has expired or been "
                f"revoked (Tailscale API keys expire, 90 days by default)"
            )
        else:
            detail = f"HTTP {exc.code} {exc.reason}"
        raise TailscaleAPIError(detail) from exc
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise TailscaleAPIError(f"{type(exc).__name__}: {exc}") from exc

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
    """Check all configured Jetsons via Tailscale API + direct GoPro query.

    Propagates TailscaleAPIError so main() can distinguish "the monitor
    couldn't check" from "the devices are down".
    """
    ts_devices = get_tailscale_devices()
    now = datetime.now(timezone.utc)
    statuses: list[JetsonStatus] = []

    for cfg in JETSON_DEVICES:
        name = cfg["name"]
        ts_hostname = cfg["tailscale_hostname"]
        ts_ip = cfg["tailscale_ip"]
        check_service = cfg.get("check_service", True)

        device = ts_devices.get(ts_hostname.lower())

        if not device:
            statuses.append(JetsonStatus(
                name=name, tailscale_hostname=ts_hostname,
                tailscale_online=False,
                error=f"Device '{ts_hostname}' not found in Tailscale",
            ))
            continue

        # Parse lastSeen (used for the "X min ago" display and as a fallback).
        last_seen_str = device.get("lastSeen", "")
        last_seen_ago: Optional[float] = None
        if last_seen_str:
            try:
                last_seen_dt = datetime.fromisoformat(last_seen_str.replace("Z", "+00:00"))
                last_seen_ago = (now - last_seen_dt).total_seconds() / 60.0
            except ValueError:
                pass

        # Prefer Tailscale's own "online" flag — it reflects the live control-
        # plane connection. lastSeen can lag several minutes on a device that
        # is fully connected (relayed heartbeats, clock skew), so the old
        # "lastSeen < 5 min" heuristic produced false OFFLINE alerts. Fall back
        # to the staleness check only when the flag is absent.
        online_flag = device.get("online")
        if isinstance(online_flag, bool):
            is_online = online_flag
        else:
            is_online = last_seen_ago is not None and last_seen_ago < STALE_THRESHOLD_MIN

        # If online (and configured for it), query the device's :5000 API.
        gopro_count = 0
        gopro_names: tuple[str, ...] = ()
        disk_free_gb: Optional[float] = None
        gopro_error: Optional[str] = None

        if is_online and check_service:
            gopro_count, gopro_names, disk_free_gb, gopro_error = check_gopros(ts_ip)

        statuses.append(JetsonStatus(
            name=name, tailscale_hostname=ts_hostname,
            tailscale_online=is_online,
            last_seen=last_seen_str, last_seen_ago_min=last_seen_ago,
            disk_free_gb=disk_free_gb,
            gopro_count=gopro_count, gopro_names=gopro_names,
            gopro_error=gopro_error,
            service_checked=check_service,
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
        elif s.service_checked:
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
        elif not s.service_checked:
            gopro_info = "reachability only"
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
        if not s.service_checked:
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

def handle_api_failure(detail: str) -> None:
    """Alert (deduped) that the monitor itself couldn't reach the Tailscale API.

    Device state is UNKNOWN for this run — sending the normal "all devices
    OFFLINE" alert here would be a false alarm.
    """
    now = datetime.now(timezone.utc)
    today_utc = now.strftime("%Y-%m-%d")
    signature = "tailscale_api:unreachable"
    subject = "UBALL Alert: Tailscale API check failed (device state unknown)"
    body = (
        f"UBALL System Alert — {now.strftime('%Y-%m-%d %H:%M UTC')}\n"
        + "=" * 50
        + "\n\n"
        "The health monitor could not query the Tailscale API, so Jetson / "
        "GoPro state is UNKNOWN for this run.\n"
        "This does NOT mean the devices are offline.\n\n"
        f"   {detail}\n\n"
        "If this is an auth error, rotate TAILSCALE_API_KEY in the Tailscale "
        "admin console (Settings -> Keys) and update the monitor's environment.\n"
    )

    log.error("Tailscale API unreachable — %s", detail)

    if not should_send_alert(signature, today_utc):
        log.info("API-failure alert suppressed — already emailed today")
        return

    send_email(subject, body)
    save_alert_state({
        "signature": signature,
        "date": today_utc,
        "last_sent_at": now.isoformat(),
        "subject": subject,
    })


def main() -> None:
    log.info("Starting Jetson health check (%d devices)", len(JETSON_DEVICES))

    try:
        statuses = check_all_jetsons()
    except TailscaleAPIError as exc:
        handle_api_failure(str(exc))
        return

    for s in statuses:
        if not s.tailscale_online:
            log.warning("%s: OFFLINE — last seen %.0f min ago",
                        s.name, s.last_seen_ago_min or -1)
        elif not s.service_checked:
            log.info("%s: ONLINE (Tailscale reachability check only)", s.name)
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
    unreachable = [
        s.name for s in statuses
        if s.tailscale_online and s.service_checked and s.gopro_error
    ]
    low_gopros = [
        s.name for s in statuses
        if s.tailscale_online and s.service_checked
        and not s.gopro_error and s.gopro_count < EXPECTED_GOPROS
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
