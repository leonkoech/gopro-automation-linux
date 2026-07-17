"""
Courtside league scraper — runs on the AGX and publishes to Firestore.

Why on the AGX: the Courtside league site (Cloudflare WAF + US geo-restriction)
blocks the production Firebase Cloud Function's datacenter IP, so the frontend's
server-side scrapers get a 403 / challenge page. The AGX sits at the facility on
a normal US IP the site already serves, so it fetches cleanly (verified: 200 +
`table.scheduleList` present, no challenge). It scrapes the schedule + roster and
writes them to Firestore `courtside_cache/{schedule,roster}`; the frontend routes
just read those docs instead of hitting Courtside.

Faithful port of the two Next.js `cheerio` scrapers:
  - schedule: `table.scheduleList` walk (isDay -> date, schedRow -> game). We
    scrape ALL days here; the route filters to today.
  - roster:   the "print" page, region/division -> team (parent_<ID>) ->
    players (in_<ID>), jersey "00"/blank -> null, leading "*" stripped.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("agx.courtside")

SCHEDULE_URL = (
    "https://www.courtsidebasketballleague.com/teams/default.asp"
    "?u=COURTSIDEBASKETBALLL&s=basketball&p=schedule"
)
ROSTER_URL = (
    "https://www.courtsidebasketballleague.com/teams/default.asp"
    "?p=roster&u=COURTSIDEBASKETBALLL&s=basketball&t=print"
)

# A believable desktop-Chrome fingerprint (matches the frontend courtsideFetch).
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

CACHE_COLLECTION = "courtside_cache"


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _clean(text: str) -> str:
    """Collapse whitespace + non-breaking spaces to single spaces, trimmed."""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _clean_player_name(raw: str) -> str:
    """Drop the league's leading '*' (unregistered) marker; collapse whitespace."""
    return re.sub(r"^\*\s*", "", _clean(raw)).strip()


def _parse_jersey(raw: str) -> Optional[int]:
    """'00'/blank -> None (league's "no number"); otherwise int, or None."""
    t = _clean(raw)
    if not t or t == "00":
        return None
    try:
        return int(t)
    except ValueError:
        return None


def fetch_html(url: str, timeout: int = 20) -> str:
    r = requests.get(url, headers=BROWSER_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# --------------------------------------------------------------------------- #
# Schedule
# --------------------------------------------------------------------------- #
def parse_schedule(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.scheduleList")
    if table is None:
        raise RuntimeError("table.scheduleList not found (site layout changed or WAF page?)")

    games: List[Dict] = []
    current_date: Optional[str] = None
    current_division: Optional[str] = None

    for row in table.find_all("tr"):
        classes = row.get("class") or []

        # isDay rows set the current date heading.
        if "isDay" in classes:
            current_date = _clean(row.get_text())
            continue

        cells = row.find_all("td")

        # A single-cell row is a division header (skip the season banner).
        if len(cells) == 1:
            text = cells[0].get_text().strip()
            if text != current_date and not text.startswith("SEASON"):
                current_division = text
            continue

        if "schedRow" not in classes:
            continue

        team_labels = row.select("span.teamLabel")
        time_cell = row.select_one("td.timeCell")
        game_time = time_cell.get_text().strip() if time_cell else ""
        loc_cell = row.select_one("td[class*='locationCell']")
        location = _clean(loc_cell.get_text()) if loc_cell else ""

        games.append({
            "date": current_date or "",
            "division": current_division or "",
            "time": game_time,
            "awayTeam": team_labels[0].get_text().strip() if len(team_labels) > 0 else "",
            "homeTeam": team_labels[1].get_text().strip() if len(team_labels) > 1 else "",
            "location": location,
            "status": "",
        })
    return games


# --------------------------------------------------------------------------- #
# Roster (print page)
# --------------------------------------------------------------------------- #
def parse_roster(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Pass 1: division context + team headers (parent_<ID>).
    current_division = ""
    team_name: Dict[str, str] = {}
    team_division: Dict[str, str] = {}
    team_order: List[str] = []

    for tb in soup.find_all("tbody"):
        cls = tb.get("class") or []

        sub = tb.select_one(".byTeamDivisionRow.isSubDiv .division-label")
        if sub is not None:
            current_division = _clean(sub.get_text())
            continue
        main = tb.select_one(".byTeamDivisionRow.isMainDiv .division-label")
        if main is not None:
            current_division = _clean(main.get_text())
            continue

        if "structItemTeam" in cls:
            raw_id = tb.get("id") or ""
            tid = raw_id[len("parent_"):] if raw_id.startswith("parent_") else raw_id
            label = tb.select_one(".teamLabel")
            name = _clean(label.get_text()) if label else ""
            if tid and name:
                if tid not in team_name:
                    team_order.append(tid)
                team_name[tid] = name
                team_division[tid] = current_division

    # Pass 2: players live in `modGroupDetailsWrapper` tbodies keyed `in_<ID>`.
    team_players: Dict[str, List[Dict]] = {}
    for tb in soup.select("tbody.modGroupDetailsWrapper"):
        cls_str = " ".join(tb.get("class") or [])
        m = re.search(r"\bin_(\d+)\b", cls_str)
        if not m:
            continue
        tid = m.group(1)

        players: List[Dict] = []
        for tr in tb.find_all("tr"):
            if "thead" in (tr.get("class") or []):
                continue
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            name = _clean_player_name(cells[1].get_text())
            if not name:
                continue
            players.append({"name": name, "jerseyNumber": _parse_jersey(cells[0].get_text())})
        if players:
            team_players[tid] = players

    return [{
        "division": team_division.get(tid, ""),
        "teamName": team_name.get(tid, ""),
        "players": team_players.get(tid, []),
    } for tid in team_order]


# --------------------------------------------------------------------------- #
# Publish + periodic refresh
# --------------------------------------------------------------------------- #
def scrape_and_publish(fb) -> Dict[str, int]:
    """Fetch + parse both pages and write them to Firestore. Each write is
    independent, so a roster failure doesn't lose a good schedule (or vice
    versa)."""
    result = {"games": -1, "teams": -1}
    try:
        games = parse_schedule(fetch_html(SCHEDULE_URL))
        fb.db.collection(CACHE_COLLECTION).document("schedule").set({
            "games": games, "count": len(games), "updatedAt": _utcnow(), "source": "agx",
        })
        result["games"] = len(games)
        logger.info("courtside: published %d schedule games", len(games))
    except Exception as e:  # noqa: BLE001
        logger.error("courtside: schedule scrape/publish failed: %s", e)
    try:
        teams = parse_roster(fetch_html(ROSTER_URL, timeout=30))
        fb.db.collection(CACHE_COLLECTION).document("roster").set({
            "teams": teams, "count": len(teams), "updatedAt": _utcnow(), "source": "agx",
        })
        result["teams"] = len(teams)
        logger.info("courtside: published %d roster teams", len(teams))
    except Exception as e:  # noqa: BLE001
        logger.error("courtside: roster scrape/publish failed: %s", e)
    return result


def start_refresh(fb, interval_min: int = 30) -> None:
    """Spawn a daemon thread that publishes on startup then every interval."""
    if not fb:
        return

    def _loop() -> None:
        while True:
            scrape_and_publish(fb)
            time.sleep(max(60, interval_min * 60))

    threading.Thread(target=_loop, name="courtside-refresh", daemon=True).start()
    logger.info("courtside: refresh thread started (every %d min)", interval_min)
