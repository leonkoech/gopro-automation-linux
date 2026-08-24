"""Ground truth for the shot-detection backtest.

GT = the human annotations in the annotation tool's `plays` table (source=manual).
We keep only *shot attempts* (`*_MAKE` / `*_MISS`) and normalize each to what the
backtest needs, including the shot cam derived from the annotated hoop:

    LEFT hoop  -> SL     RIGHT hoop -> SR

`load_gt` reads a frozen JSON cache if present (so the box needs no DB access);
`freeze_gt` (or `python -m ...gt --freeze`) fetches from the annotation Supabase
REST API and writes the cache. The frozen file is committed and is the source of
truth for a reproducible backtest.
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Shot-attempt taxonomy (the make/miss GT). Everything else (FOUL/STEAL/TURNOVER/
# REBOUND/TIPOFF/ASSIST/...) is not a shot and is excluded.
_MAKE_SUFFIX = "_MAKE"
_MISS_SUFFIX = "_MISS"


def _cam_for_hoop(hoop: str) -> str:
    """Annotated scoring hoop -> shot cam. LEFT->SL, RIGHT->SR."""
    return "SL" if (hoop or "").upper() == "LEFT" else "SR"


def _normalize(rows: List[Dict]) -> List[Dict]:
    """Keep shot attempts only; project to the backtest's shot record; sort by time."""
    shots: List[Dict] = []
    for r in rows:
        cls = (r.get("classification") or "").upper()
        made = cls.endswith(_MAKE_SUFFIX)
        miss = cls.endswith(_MISS_SUFFIX)
        if not (made or miss):
            continue
        ts = r.get("timestamp_seconds")
        if ts is None:
            continue
        hoop = (r.get("angle") or "").upper()
        shots.append({
            "id": r.get("id"),
            "t_track": round(float(ts), 3),
            "t_start": None if r.get("start_timestamp") is None else round(float(r["start_timestamp"]), 3),
            "t_end": None if r.get("end_timestamp") is None else round(float(r["end_timestamp"]), 3),
            "hoop": hoop,
            "cam": _cam_for_hoop(hoop),
            "made": bool(made),
            "kind": cls.split("_")[0],           # FG / 3PT / 4PT / FREE
            "classification": cls,
            "player": r.get("player_a"),
        })
    shots.sort(key=lambda s: s["t_track"])
    return shots


# --------------------------------------------------------------------------- #
# Annotation Supabase REST fetch (only needed for --freeze; the box reads cache)
# --------------------------------------------------------------------------- #
def _supabase_env() -> Dict[str, str]:
    """Pull annotation-tool Supabase creds from env, else the wb repo .env."""
    keys = {
        "url": os.getenv("ANNOTATION_SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
        "anon": os.getenv("ANNOTATION_SUPABASE_ANON_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY"),
        "email": os.getenv("NEXT_PUBLIC_SUPABASE_SYNC_EMAIL"),
        "password": os.getenv("NEXT_PUBLIC_SUPABASE_SYNC_PASSWORD"),
    }
    if not keys["url"]:
        env_path = os.getenv("WB_ENV_PATH", os.path.expanduser(
            "~/Cellstrat/GitHub_Repositories/gopro-automation-wb/.env"))
        if os.path.exists(env_path):
            with open(env_path) as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    v = v.strip().strip('"').strip("'")
                    if k == "NEXT_PUBLIC_SUPABASE_URL":
                        keys["url"] = keys["url"] or v
                    elif k == "NEXT_PUBLIC_SUPABASE_ANON_KEY":
                        keys["anon"] = keys["anon"] or v
                    elif k == "NEXT_PUBLIC_SUPABASE_SYNC_EMAIL":
                        keys["email"] = keys["email"] or v
                    elif k == "NEXT_PUBLIC_SUPABASE_SYNC_PASSWORD":
                        keys["password"] = keys["password"] or v
    return keys


def _http_json(url: str, headers: Dict[str, str], data: Optional[bytes] = None) -> object:
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _fetch_from_supabase(game_id: str) -> List[Dict]:
    env = _supabase_env()
    if not (env["url"] and env["anon"]):
        raise RuntimeError("annotation Supabase URL/anon key not found (env or wb .env)")
    base = env["url"].rstrip("/")
    # Sign in (RLS on plays needs an authenticated JWT); fall back to anon-only.
    token = env["anon"]
    if env["email"] and env["password"]:
        auth = _http_json(
            f"{base}/auth/v1/token?grant_type=password",
            {"apikey": env["anon"], "Content-Type": "application/json"},
            json.dumps({"email": env["email"], "password": env["password"]}).encode())
        token = auth.get("access_token", token)
    q = urllib.parse.urlencode({
        "game_id": f"eq.{game_id}",
        "select": "id,timestamp_seconds,start_timestamp,end_timestamp,angle,"
                  "classification,player_a,source",
        "order": "timestamp_seconds.asc",
        "limit": "2000",
    })
    rows = _http_json(f"{base}/rest/v1/plays?{q}",
                      {"apikey": env["anon"], "Authorization": f"Bearer {token}"})
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected plays response: {str(rows)[:200]}")
    return rows


def cache_path(game_id: str) -> str:
    short = game_id.split("-")[0]
    return os.path.join(DATA_DIR, f"{short}_gt.json")


def freeze_gt(game_id: str) -> Dict:
    """Fetch from the annotation DB, normalize, write the committed JSON cache."""
    rows = _fetch_from_supabase(game_id)
    shots = _normalize(rows)
    n_make = sum(1 for s in shots if s["made"])
    doc = {
        "game_id": game_id,
        "n_shots": len(shots),
        "n_make": n_make,
        "n_miss": len(shots) - n_make,
        "n_sl": sum(1 for s in shots if s["cam"] == "SL"),
        "n_sr": sum(1 for s in shots if s["cam"] == "SR"),
        "shots": shots,
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(cache_path(game_id), "w") as fh:
        json.dump(doc, fh, indent=1)
    return doc


def load_gt(game_id: str, allow_fetch: bool = False) -> Dict:
    """Load the frozen GT JSON; optionally freeze it first if missing."""
    path = cache_path(game_id)
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    if allow_fetch:
        return freeze_gt(game_id)
    raise FileNotFoundError(f"no frozen GT at {path}; run with --freeze first")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Freeze/inspect backtest ground truth")
    ap.add_argument("game_id")
    ap.add_argument("--freeze", action="store_true", help="fetch from annotation DB + write cache")
    a = ap.parse_args()
    doc = freeze_gt(a.game_id) if a.freeze else load_gt(a.game_id)
    print(f"game {doc['game_id']}: {doc['n_shots']} shots "
          f"({doc['n_make']} make / {doc['n_miss']} miss) "
          f"SL={doc['n_sl']} SR={doc['n_sr']} -> {cache_path(a.game_id)}")
