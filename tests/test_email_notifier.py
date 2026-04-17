"""Tests for email_notifier.py."""

from __future__ import annotations

import sys
from email import message_from_string
from email.header import decode_header, make_header
from pathlib import Path

import pytest

# Make the project root importable without installing the package.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from email_notifier import (  # noqa: E402
    GameNotification,
    build_body,
    build_subject,
    send_games_ready_email,
)


SMTP_ENV = {
    'ANNOTATOR_EMAIL_ENABLED': 'true',
    'SMTP_HOST': 'smtp.example.com',
    'SMTP_PORT': '587',
    'SMTP_USER': 'sender@example.com',
    'SMTP_PASSWORD': 'secret',
    'SMTP_FROM_EMAIL': 'sender@example.com',
    'ANNOTATOR_NOTIFY_EMAIL': 'annotator@example.com',
    'ANNOTATOR_NOTIFY_CC': 'cc@example.com, cc2@example.com',
    'ANNOTATION_TOOL_URL': 'https://annotate.example.com',
}


@pytest.fixture
def smtp_env(monkeypatch):
    for key, value in SMTP_ENV.items():
        monkeypatch.setenv(key, value)
    yield


@pytest.fixture
def captured_smtp():
    """A fake SMTP context manager that records the sendmail call."""
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port):
            sent['host'] = host
            sent['port'] = port

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def starttls(self, context=None):
            sent['starttls'] = True

        def login(self, user, password):
            sent['login'] = (user, password)

        def sendmail(self, from_addr, to_addrs, raw):
            sent['from'] = from_addr
            sent['to'] = list(to_addrs)
            sent['raw'] = raw

    def factory(host, port):
        return FakeSMTP(host, port)

    return factory, sent


def _make_game(
    number: int,
    uball_id: str | None = '95efaeaa-8475-4db4-8967',
    error: str | None = None,
) -> GameNotification:
    return GameNotification(
        game_number=number,
        team_a_name=f'Team {number}A',
        team_b_name=f'Team {number}B',
        uball_game_id=uball_id,
        error=error,
    )


# --- build_subject ---

def test_subject_pluralizes_and_includes_jetson_and_date():
    assert build_subject('jetson-1', '2026-04-17', 3, 0) == (
        'UBALL: 3 games ready for annotation — 2026-04-17 (jetson-1)'
    )


def test_subject_singular_and_failed_count():
    assert build_subject('jetson-2', '2026-04-17', 1, 2) == (
        'UBALL: 1 game ready for annotation (2 failed) — 2026-04-17 (jetson-2)'
    )


# --- build_body ---

def test_body_contains_editor_urls_for_ready_games():
    ready = [_make_game(1), _make_game(2, uball_id='other-uuid')]
    plain, html = build_body(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=ready,
        failed_games=[],
        annotation_base_url='https://annotate.example.com',
    )
    assert 'https://annotate.example.com/editor/95efaeaa-8475-4db4-8967' in plain
    assert 'https://annotate.example.com/editor/other-uuid' in plain
    assert 'Team 1A vs Team 1B' in plain
    assert '<a href="https://annotate.example.com/editor/other-uuid">' in html


def test_body_includes_failed_section_with_error():
    plain, html = build_body(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1)],
        failed_games=[_make_game(2, error='Batch job timeout')],
        annotation_base_url='https://annotate.example.com',
    )
    assert 'NOT yet ready' in plain
    assert 'Batch job timeout' in plain
    assert '<em>(error: Batch job timeout)</em>' in html


def test_body_trailing_fallback_when_no_url():
    plain, _ = build_body(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1, uball_id=None)],
        failed_games=[],
        annotation_base_url='https://annotate.example.com',
    )
    # No URL line should appear for games without a uball_game_id.
    assert '/editor/' not in plain


# --- send_games_ready_email ---

def test_send_disabled_returns_false(monkeypatch):
    monkeypatch.setenv('ANNOTATOR_EMAIL_ENABLED', 'false')
    result = send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1)],
        failed_games=[],
    )
    assert result is False


def test_send_empty_lists_returns_false(smtp_env, captured_smtp):
    factory, _ = captured_smtp
    result = send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[],
        failed_games=[],
        smtp_factory=factory,
    )
    assert result is False


def test_send_missing_config_returns_false(monkeypatch):
    # Only enable flag; leave SMTP_HOST etc. unset.
    for key in SMTP_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('ANNOTATOR_EMAIL_ENABLED', 'true')
    result = send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1)],
        failed_games=[],
    )
    assert result is False


def test_send_happy_path_delivers_to_all_recipients(smtp_env, captured_smtp):
    factory, sent = captured_smtp

    result = send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1), _make_game(2)],
        failed_games=[_make_game(3, error='bad')],
        smtp_factory=factory,
    )

    assert result is True
    assert sent['host'] == 'smtp.example.com'
    assert sent['port'] == 587
    assert sent['starttls'] is True
    assert sent['login'] == ('sender@example.com', 'secret')
    # TO + both CC addresses are in the envelope recipients.
    assert sent['to'] == [
        'annotator@example.com',
        'cc@example.com',
        'cc2@example.com',
    ]
    assert sent['from'] == 'sender@example.com'

    parsed = message_from_string(sent['raw'])
    assert parsed['To'] == 'annotator@example.com'
    assert parsed['Cc'] == 'cc@example.com, cc2@example.com'
    decoded_subject = str(make_header(decode_header(parsed['Subject'])))
    assert 'UBALL: 2 games ready for annotation (1 failed)' in decoded_subject
    # Decoded body should contain the editor URL (may be base64-encoded
    # on the wire because of the em dash in the plain-text body).
    decoded_parts = [
        part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
        for part in parsed.walk()
        if part.get_content_maintype() == 'text'
    ]
    assert any(
        'https://annotate.example.com/editor/' in body for body in decoded_parts
    )


def test_send_swallows_smtp_errors(smtp_env):
    def broken_factory(host, port):
        raise RuntimeError('no network')

    result = send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1)],
        failed_games=[],
        smtp_factory=broken_factory,
    )
    assert result is False


def test_send_uses_cc_override_not_env(smtp_env, captured_smtp):
    factory, sent = captured_smtp
    send_games_ready_email(
        jetson_name='jetson-1',
        recording_date='2026-04-17',
        ready_games=[_make_game(1)],
        failed_games=[],
        cc_email='',  # Explicit empty → no CC
        smtp_factory=factory,
    )
    parsed = message_from_string(sent['raw'])
    assert parsed['Cc'] is None
    assert sent['to'] == ['annotator@example.com']
