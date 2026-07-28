"""Unit tests for agx_pipeline.ingest._resolve_checkin_roster.

Covers the roster-selection logic that makes AGX ingestion register the live
checked-in roster (from game_schedules/<slotId>) on the annotation game,
falling back to the basketball-games snapshot. Uses fakes for the Firestore
handle so no network/Firebase is touched.
"""
import unittest

from agx_pipeline.ingest import _resolve_checkin_roster


# ── Firestore fakes ─────────────────────────────────────────────────────────
class _FakeDoc:
    def __init__(self, exists, data=None):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _FakeDocRef:
    def __init__(self, doc=None, raise_exc=None):
        self._doc = doc
        self._raise = raise_exc

    def get(self):
        if self._raise:
            raise self._raise
        return self._doc


class _FakeDB:
    def __init__(self, docref):
        self._docref = docref
        self.requested_collection = None
        self.requested_doc = None

    def collection(self, name):
        self.requested_collection = name
        return self

    def document(self, doc_id):
        self.requested_doc = doc_id
        return self._docref


class _FakeFB:
    def __init__(self, docref):
        self.db = _FakeDB(docref)


# Snapshot roster (frozen at Start Game); no checked_in flags — mirrors what
# basketball-games.rosterTeamN stores.
SNAP1 = [{"player_id": "s1", "name": "Snap One", "jersey_number": 1},
         {"player_id": "s2", "name": "Snap Two", "jersey_number": 2}]
SNAP2 = [{"player_id": "s3", "name": "Snap Three", "jersey_number": 3}]

GAME = {"scheduleSlotId": "slot-123", "rosterTeam1": SNAP1, "rosterTeam2": SNAP2}


class ResolveCheckinRosterTest(unittest.TestCase):
    def test_checked_in_slot_roster_preferred(self):
        slot = {
            "rosterTeam1": [
                {"player_id": "a", "name": "A", "checked_in": True},
                {"player_id": "b", "name": "B", "checked_in": False},
            ],
            "rosterTeam2": [
                {"player_id": "c", "name": "C", "checked_in": True},
            ],
        }
        fb = _FakeFB(_FakeDocRef(_FakeDoc(True, slot)))
        r1, r2 = _resolve_checkin_roster(fb, GAME)
        self.assertEqual([p["player_id"] for p in r1], ["a"])  # only checked-in
        self.assertEqual([p["player_id"] for p in r2], ["c"])
        # confirms it read the right slot
        self.assertEqual(fb.db.requested_collection, "game_schedules")
        self.assertEqual(fb.db.requested_doc, "slot-123")

    def test_one_side_checked_in_other_falls_back(self):
        # team1 has a checked-in player; team2 has none checked in -> team2 uses snapshot
        slot = {
            "rosterTeam1": [{"player_id": "a", "checked_in": True}],
            "rosterTeam2": [{"player_id": "x", "checked_in": False}],
        }
        fb = _FakeFB(_FakeDocRef(_FakeDoc(True, slot)))
        r1, r2 = _resolve_checkin_roster(fb, GAME)
        self.assertEqual([p["player_id"] for p in r1], ["a"])
        self.assertEqual(r2, SNAP2)  # fell back to snapshot for team2

    def test_no_checked_in_players_uses_snapshot(self):
        slot = {
            "rosterTeam1": [{"player_id": "a", "checked_in": False}],
            "rosterTeam2": [{"player_id": "x", "checked_in": False}],
        }
        fb = _FakeFB(_FakeDocRef(_FakeDoc(True, slot)))
        r1, r2 = _resolve_checkin_roster(fb, GAME)
        self.assertEqual(r1, SNAP1)
        self.assertEqual(r2, SNAP2)

    def test_slot_missing_uses_snapshot(self):
        fb = _FakeFB(_FakeDocRef(_FakeDoc(False)))
        r1, r2 = _resolve_checkin_roster(fb, GAME)
        self.assertEqual(r1, SNAP1)
        self.assertEqual(r2, SNAP2)

    def test_slot_fetch_error_uses_snapshot(self):
        fb = _FakeFB(_FakeDocRef(raise_exc=RuntimeError("firestore down")))
        r1, r2 = _resolve_checkin_roster(fb, GAME)  # must not raise
        self.assertEqual(r1, SNAP1)
        self.assertEqual(r2, SNAP2)

    def test_no_schedule_slot_id_uses_snapshot(self):
        fb = _FakeFB(_FakeDocRef(_FakeDoc(True, {"rosterTeam1": [{"checked_in": True}]})))
        game = {"rosterTeam1": SNAP1, "rosterTeam2": SNAP2}  # no scheduleSlotId
        r1, r2 = _resolve_checkin_roster(fb, game)
        self.assertEqual(r1, SNAP1)
        self.assertEqual(r2, SNAP2)

    def test_no_fb_uses_snapshot(self):
        r1, r2 = _resolve_checkin_roster(None, GAME)
        self.assertEqual(r1, SNAP1)
        self.assertEqual(r2, SNAP2)

    def test_missing_rosters_entirely_returns_none(self):
        fb = _FakeFB(_FakeDocRef(_FakeDoc(False)))
        r1, r2 = _resolve_checkin_roster(fb, {"scheduleSlotId": "slot-123"})
        self.assertIsNone(r1)
        self.assertIsNone(r2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
