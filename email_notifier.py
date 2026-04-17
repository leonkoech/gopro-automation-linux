"""
Email notifications for annotators when games finish uploading.

Sends a single email per pipeline run listing all games that are ready for
annotation (both LEFT + RIGHT angles registered in the annotation tool) and
any games that failed transcoding. Designed to replace the manual "games are
ready" text message the team sends today.

Environment variables (all optional — missing values disable the feature):
    ANNOTATOR_EMAIL_ENABLED   — "true"/"false" (default: true)
    SMTP_HOST                 — e.g. "smtp.gmail.com"
    SMTP_PORT                 — default 587
    SMTP_USER                 — sender login
    SMTP_PASSWORD             — sender password / app password
    SMTP_FROM_EMAIL           — From: header (default: SMTP_USER)
    ANNOTATOR_NOTIFY_EMAIL    — primary recipient
    ANNOTATOR_NOTIFY_CC       — CC recipient(s), comma-separated
    ANNOTATION_TOOL_URL       — base URL for deep links
                                (e.g. "https://uball-datacollection.vercel.app")
"""

from __future__ import annotations

import os
import smtplib
import ssl
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Callable, List, Optional, Tuple

from logging_service import get_logger

logger = get_logger('gopro.email_notifier')


@dataclass(frozen=True)
class GameNotification:
    """One game row in the annotator email."""

    game_number: int
    team_a_name: str
    team_b_name: str
    uball_game_id: Optional[str] = None
    error: Optional[str] = None  # None if ready; message if failed

    @property
    def label(self) -> str:
        teams = f"{self.team_a_name} vs {self.team_b_name}".strip()
        if not teams or teams == 'vs':
            teams = f"Game {self.game_number}"
        return f"Game {self.game_number} — {teams}"


def _bool_env(name: str, default: bool = True) -> bool:
    val = os.getenv(name, '').strip().lower()
    if not val:
        return default
    return val in ('1', 'true', 'yes', 'on')


def _editor_url(base_url: str, uball_game_id: Optional[str]) -> Optional[str]:
    if not base_url or not uball_game_id:
        return None
    return f"{base_url.rstrip('/')}/editor/{uball_game_id}"


def build_subject(
    jetson_name: str,
    recording_date: str,
    ready_count: int,
    failed_count: int,
) -> str:
    ready_word = 'game' if ready_count == 1 else 'games'
    parts = [f"UBALL: {ready_count} {ready_word} ready for annotation"]
    if failed_count:
        parts.append(f"({failed_count} failed)")
    parts.append(f"— {recording_date}")
    if jetson_name:
        parts.append(f"({jetson_name})")
    return ' '.join(parts)


def build_body(
    jetson_name: str,
    recording_date: str,
    ready_games: List[GameNotification],
    failed_games: List[GameNotification],
    annotation_base_url: str,
) -> Tuple[str, str]:
    """Return (plain_text_body, html_body)."""
    source = jetson_name or 'the court'
    plain_lines: List[str] = [
        "Hi,",
        "",
        f"The following games finished uploading from {source} on "
        f"{recording_date} and are ready to annotate:",
        "",
    ]
    if ready_games:
        for g in ready_games:
            plain_lines.append(f"  - {g.label}")
            url = _editor_url(annotation_base_url, g.uball_game_id)
            if url:
                plain_lines.append(f"    {url}")
    else:
        plain_lines.append("  (No games completed successfully.)")

    if failed_games:
        failed_word = 'game' if len(failed_games) == 1 else 'games'
        is_are = 'is' if len(failed_games) == 1 else 'are'
        plain_lines += [
            "",
            f"{len(failed_games)} {failed_word} failed transcoding and "
            f"{is_are} NOT yet ready:",
        ]
        for g in failed_games:
            reason = f" (error: {g.error})" if g.error else ""
            plain_lines.append(f"  - {g.label}{reason}")

    plain_lines += ["", "— UBALL automation", ""]
    plain_body = "\n".join(plain_lines)

    ready_items_html: List[str] = []
    for g in ready_games:
        url = _editor_url(annotation_base_url, g.uball_game_id)
        link_html = (
            f'<br/><a href="{url}">Open annotation editor</a>' if url else ''
        )
        ready_items_html.append(
            f"<li><strong>{g.label}</strong>{link_html}</li>"
        )
    ready_html = (
        "".join(ready_items_html)
        if ready_items_html
        else "<li>(No games completed successfully.)</li>"
    )

    failed_html_section = ''
    if failed_games:
        failed_items_html: List[str] = []
        for g in failed_games:
            reason_html = f" <em>(error: {g.error})</em>" if g.error else ""
            failed_items_html.append(f"<li>{g.label}{reason_html}</li>")
        failed_word = 'game' if len(failed_games) == 1 else 'games'
        failed_html_section = (
            f"<p>{len(failed_games)} {failed_word} failed transcoding and "
            f"{'is' if len(failed_games) == 1 else 'are'} NOT yet ready:</p>"
            f"<ul>{''.join(failed_items_html)}</ul>"
        )

    html_body = (
        "<html><body>"
        "<p>Hi,</p>"
        f"<p>The following games finished uploading from <strong>{source}</strong> "
        f"on <strong>{recording_date}</strong> and are ready to annotate:</p>"
        f"<ul>{ready_html}</ul>"
        f"{failed_html_section}"
        "<p>— UBALL automation</p>"
        "</body></html>"
    )
    return plain_body, html_body


# Factory type: (host, port) -> context-manager SMTP-like object
SmtpFactory = Callable[[str, int], smtplib.SMTP]


def _default_smtp_factory(host: str, port: int) -> smtplib.SMTP:
    return smtplib.SMTP(host, port, timeout=30)


def send_games_ready_email(
    jetson_name: str,
    recording_date: str,
    ready_games: List[GameNotification],
    failed_games: List[GameNotification],
    smtp_host: Optional[str] = None,
    smtp_port: Optional[int] = None,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
    smtp_from_email: Optional[str] = None,
    to_email: Optional[str] = None,
    cc_email: Optional[str] = None,
    annotation_base_url: Optional[str] = None,
    smtp_factory: Optional[SmtpFactory] = None,
) -> bool:
    """Send the games-ready email.

    Safe to call in any state: returns False (and logs a warning) if the
    feature is disabled or SMTP config is incomplete. Never raises.

    Returns True only when the email was actually handed off to SMTP.
    """
    if not _bool_env('ANNOTATOR_EMAIL_ENABLED', default=True):
        logger.info("[EmailNotifier] ANNOTATOR_EMAIL_ENABLED=false; skipping")
        return False

    if not ready_games and not failed_games:
        logger.info("[EmailNotifier] No games to report; skipping")
        return False

    host = smtp_host or os.getenv('SMTP_HOST', '').strip()
    port_str = (
        str(smtp_port) if smtp_port is not None else os.getenv('SMTP_PORT', '587')
    )
    try:
        port = int(port_str)
    except ValueError:
        logger.error(f"[EmailNotifier] Invalid SMTP_PORT: {port_str!r}")
        return False

    user = smtp_user or os.getenv('SMTP_USER', '').strip()
    password = smtp_password or os.getenv('SMTP_PASSWORD', '').strip()
    from_email = (
        smtp_from_email
        or os.getenv('SMTP_FROM_EMAIL', '').strip()
        or user
    )
    to = to_email or os.getenv('ANNOTATOR_NOTIFY_EMAIL', '').strip()
    cc = (
        cc_email
        if cc_email is not None
        else os.getenv('ANNOTATOR_NOTIFY_CC', '').strip()
    )
    base_url = (
        annotation_base_url or os.getenv('ANNOTATION_TOOL_URL', '').strip()
    )

    if not host or not user or not password or not from_email or not to:
        logger.warning(
            "[EmailNotifier] SMTP config incomplete "
            "(need SMTP_HOST/USER/PASSWORD/FROM_EMAIL + ANNOTATOR_NOTIFY_EMAIL); "
            "skipping"
        )
        return False

    subject = build_subject(
        jetson_name, recording_date, len(ready_games), len(failed_games)
    )
    plain_body, html_body = build_body(
        jetson_name=jetson_name,
        recording_date=recording_date,
        ready_games=ready_games,
        failed_games=failed_games,
        annotation_base_url=base_url,
    )

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to
    if cc:
        msg['Cc'] = cc
    msg.attach(MIMEText(plain_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    cc_addrs = [addr.strip() for addr in cc.split(',')] if cc else []
    recipients = [addr for addr in [to] + cc_addrs if addr]

    factory = smtp_factory or _default_smtp_factory
    try:
        with factory(host, port) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(user, password)
            server.sendmail(from_email, recipients, msg.as_string())
        logger.info(
            f"[EmailNotifier] Sent games-ready email to {recipients} "
            f"(subject: {subject!r})"
        )
        return True
    except Exception as exc:
        logger.error(f"[EmailNotifier] Failed to send email: {exc}")
        return False
