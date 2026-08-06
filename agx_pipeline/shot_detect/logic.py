"""v4.1 make/miss decision logic as pure, parameterized functions.

Ported VERBATIM from uball_shot_detection_dual_fusion_v2/near_v0/highfps/logic.py
(the system of record) so the AGX windowed detector decides identically to the
validated 99-100% runner. Quirks are preserved deliberately — e.g. rim_visits/
attempt use squared rho thresholds while through/bounce compare rho against
un-squared constants. That asymmetry IS v4.1 behaviour; changing it is an
experiment to be measured, not a cleanup to be assumed.

Params live on Geo so a variant is `replace(geo, RHO_CLEAN=0.6)`.
`decide(G, track)` with classify=None decides UNCERTAIN crossings by the v4.2
aperture rule (rho <= UNCERTAIN_RHO), i.e. the 99-100% logic.
"""
from dataclasses import dataclass, field

import numpy as np

V41 = dict(
    DET_CONF=0.20, MIN_EVIDENCE=5, RIM_NEAR=1.6, BALL_OVER_RIM=0.80,
    RHO_CLEAN=0.55, THROUGH_RB=0.90, THROUGH_WIN_S=0.45, BOUNCE_WIN_S=0.60,
    VISIT_GAP_S=0.35, THROUGH_RHO=1.15, BOUNCE_RHO=1.4, BOUNCE_RB=0.9,
    OVER_RING=1.1, CLF_THRESH=0.5,
    # v4.2: UNCERTAIN crossings decided by aperture, not the color classifier.
    # Measured on 414 GT-matched shots (3 games): classifier 74.7% -> 94.7% on
    # that path, whole system 90.6% -> 94.2%. Holds under leave-one-game-out
    # (100% / 86% / 93% on held-out games; threshold refit picks 1.05-1.10).
    UNCERTAIN_RULE="aperture", UNCERTAIN_RHO=1.10,
    # attempt signature
    ATT_ZONE_RHO=3.0, ATT_RB_MIN=0.45, ATT_MIN_ZONE=3, ATT_V=5.0, ATT_FAST=2,
    ATT_DESC=0.85, ATT_HEIGHT=0.55, ATT_SPAN_S=1.6, ATT_DEADBALL_RHO=0.7,
)


@dataclass(frozen=True)
class Geo:
    rcx: float
    rcy: float
    sa: float
    sb: float
    ang: float                    # radians
    fps: float
    P: dict = field(default_factory=lambda: dict(V41))

    @property
    def rb_rim(self):
        return 0.525 * (self.sa + self.sb) / 2.0

    @property
    def px_per_inch(self):
        return (self.sa + self.sb) / 18.0

    @classmethod
    def from_rim(cls, rim, fps, P=None):
        (rcx, rcy), (sa, sb) = rim["center"], rim["semi_axes"]
        return cls(rcx, rcy, sa, sb, np.deg2rad(rim["angle"]), fps,
                   dict(V41, **(P or {})))

    def tweak(self, **kw):
        return Geo(self.rcx, self.rcy, self.sa, self.sb, self.ang, self.fps,
                   dict(self.P, **kw))

    def rho(self, bx, by):
        dx, dy = bx - self.rcx, by - self.rcy
        dxr = dx * np.cos(self.ang) + dy * np.sin(self.ang)
        dyr = -dx * np.sin(self.ang) + dy * np.cos(self.ang)
        return (dxr / self.sa) ** 2 + (dyr / self.sb) ** 2


def rim_visits(G, track):
    P, fps, rb_rim = G.P, G.fps, G.rb_rim
    at_rim = [t for t in track
              if G.rho(t[1], t[2]) < P["RIM_NEAR"] ** 2
              and t[3] >= P["BALL_OVER_RIM"] * rb_rim]
    visits, cur = [], []
    for t in at_rim:
        if cur and t[0] - cur[-1][0] > P["VISIT_GAP_S"] * fps:
            if len(cur) >= 2:
                visits.append(cur)
            cur = []
        cur.append(t)
    if len(cur) >= 2:
        visits.append(cur)
    return visits


def _speeds(track):
    """px/frame at each track index, from the nearest neighbours in time."""
    sp = {}
    for k, t in enumerate(track):
        best = None
        for j in (k - 1, k + 1):
            if 0 <= j < len(track):
                df = abs(track[j][0] - t[0])
                if 0 < df <= 6:
                    v = np.hypot(track[j][1] - t[1], track[j][2] - t[2]) / df
                    best = v if best is None else max(best, v)
        sp[t[0]] = 0.0 if best is None else best
    return sp


def noah_crossings(G, track):
    P, fps, rb_rim = G.P, G.fps, G.rb_rim
    sp = _speeds(track) if P.get("BOUNCE_MIN_SPEED", 0) > 0 else None
    visits = rim_visits(G, track)
    out = []
    for vi, visit in enumerate(visits):
        fcr, xcr, ycr, rbcr, _ = visit[-1]
        cap_f = visits[vi + 1][0][0] if vi + 1 < len(visits) else float("inf")
        rr = G.rho(xcr, ycr)
        info = dict(cross_frame=int(fcr), rho=round(float(np.sqrt(rr)), 2),
                    depth_in=round(float((ycr - G.rcy) / G.px_per_inch), 1),
                    lr_in=round(float((xcr - G.rcx) / G.px_per_inch), 1),
                    visit=vi + 1, n_visits=len(visits))
        after = [t for t in track
                 if fcr < t[0] <= min(fcr + int(P["THROUGH_WIN_S"] * fps), cap_f)]
        through = [t for t in after
                   if G.rho(t[1], t[2]) < P["THROUGH_RHO"]
                   and t[3] < P["THROUGH_RB"] * rb_rim]
        bounce_w = [t for t in track
                    if fcr < t[0] <= min(fcr + int(P["BOUNCE_WIN_S"] * fps), cap_f)]
        bounced = [t for t in bounce_w
                   if G.rho(t[1], t[2]) > P["BOUNCE_RHO"]
                   and t[3] >= P["BOUNCE_RB"] * rb_rim
                   and (sp is None or sp[t[0]] >= P["BOUNCE_MIN_SPEED"])]
        info["n_through"] = len(through)
        info["n_bounce"] = len(bounced)
        over_ring = np.sqrt(rr) <= P["OVER_RING"]
        if over_ring and np.sqrt(rr) <= P["RHO_CLEAN"] and len(through) >= 1:
            out.append(("GEO_MAKE", info))
        elif over_ring and len(through) >= 2:
            out.append(("GEO_MAKE", info))
        elif len(bounced) >= 2 and len(through) == 0:
            out.append(("GEO_MISS", info))
        else:
            out.append((None, info))
    return out


def attempt_signature(G, track):
    P, fps, rb_rim = G.P, G.fps, G.rb_rim
    zone = [t for t in track
            if G.rho(t[1], t[2]) < P["ATT_ZONE_RHO"] ** 2
            and t[3] >= P["ATT_RB_MIN"] * rb_rim]
    if len(zone) < P["ATT_MIN_ZONE"]:
        return None
    fast = 0
    for k in range(1, len(zone)):
        df = zone[k][0] - zone[k - 1][0]
        if df <= 0 or df > 0.2 * fps:
            continue
        v = np.hypot(zone[k][1] - zone[k - 1][1],
                     zone[k][2] - zone[k - 1][2]) / df
        if v >= P["ATT_V"]:
            fast += 1
    if fast < P["ATT_FAST"]:
        return None
    rbs = [t[3] for t in zone]
    if rbs[-1] > P["ATT_DESC"] * max(rbs):
        return None
    if max(rbs) < P["ATT_HEIGHT"] * rb_rim:
        return None
    span = (zone[-1][0] - zone[0][0]) / fps
    if span > P["ATT_SPAN_S"]:
        return None
    close = min(zone, key=lambda t: G.rho(t[1], t[2]))
    if np.sqrt(G.rho(close[1], close[2])) < P["ATT_DEADBALL_RHO"]:
        return None
    return dict(cross_frame=int(close[0]),
                rho=round(float(np.sqrt(G.rho(close[1], close[2]))), 2),
                depth_in=round(float((close[2] - G.rcy) / G.px_per_inch), 1),
                lr_in=round(float((close[1] - G.rcx) / G.px_per_inch), 1),
                visit=1, n_visits=1, n_through=0, n_bounce=0,
                transit_s=round(span, 2))


def decide(G, track, classify=None):
    """Event -> list of verdict dicts, mirroring makemiss_v2.finalize().

    classify(cross_frame) -> P(make); may be None (then UNCERTAIN -> aperture).
    Returns [] for gated events (with a single 'skipped' dict).
    """
    P, fps, rb_rim = G.P, G.fps, G.rb_rim
    evid = sum(1 for t in track if G.rho(t[1], t[2]) < P["RIM_NEAR"] ** 2)
    over_rim = any(t[3] >= P["BALL_OVER_RIM"] * rb_rim
                   and G.rho(t[1], t[2]) < P["RIM_NEAR"] ** 2 for t in track)
    if evid < P["MIN_EVIDENCE"] or not over_rim:
        att = attempt_signature(G, track)
        if att is not None:
            return [dict(t=round(att["cross_frame"] / fps, 1), geo="ATTEMPT_MISS",
                         clf_prob=None, decided_by="attempt", verdict="MISS", **att)]
        return [dict(skipped=f"gate (evidence {evid}, over_rim {over_rim})")]
    crossings = noah_crossings(G, track)
    if not crossings:
        att = attempt_signature(G, track)
        if att is not None:
            return [dict(t=round(att["cross_frame"] / fps, 1), geo="ATTEMPT_MISS",
                         clf_prob=None, decided_by="attempt", verdict="MISS", **att)]
        return [dict(skipped="gate (no rim visit >=2 frames)")]
    out = []
    for geo, info in crossings:
        prob = None if classify is None else float(classify(info["cross_frame"]))
        if geo == "GEO_MAKE":
            final, src = True, "geometry"
        elif geo == "GEO_MISS":
            final, src = False, "geometry"
        elif P["UNCERTAIN_RULE"] == "aperture":
            # ball's last at-rim position inside the ring aperture => it went in
            src, final = "aperture", info["rho"] <= P["UNCERTAIN_RHO"]
        else:
            src = "classifier"
            final = (prob is not None and prob >= P["CLF_THRESH"])
        out.append(dict(t=round(info["cross_frame"] / fps, 1), geo=geo,
                        clf_prob=None if prob is None else round(prob, 3),
                        decided_by=src, verdict="MAKE" if final else "MISS", **info))
    return out
