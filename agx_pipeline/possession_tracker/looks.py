"""Who is the shooter when his number cannot be read? What each numbered player LOOKS like.

Per clip (collect): for every on-court track fragment, the jersey reads, the kit colour and an
appearance feature of a few crops (ResNet-50, ImageNet) are saved beside the queue.
Per game, once it has ended (name_shooters): every fragment whose number vote is strong is a
labelled example of that player -- the labels come from the jersey reads alone. A linear
classifier on the features then names each shot's shooter among the shooting team's players.

Measured 2026-10-12 on annotated games, used only where the number read on the shooter is silent
and the classifier's confidence is >= 0.6:
  3 design games (371 shots)   shooter named right 62.8% -> 74.4%, added answers 81% right
  4 held-out games (520 shots) shooter named right 52.3% -> 67.7%, added answers 75% right
An appearance answer is a hint (yellow card), never a number that was read.
"""
import json
import os
from collections import Counter, defaultdict

import numpy as np

STEP = 2                 # read every 2nd tracked frame of each fragment
MIN_FRAMES = 8           # shorter fragments are ignored
CROPS = 8                # appearance crops per fragment
READ_MIN = 0.5
HOLD, HOLD_RATIO = 3.0, 2.0     # number vote that makes a fragment a labelled example
MIN_FRAGMENTS = 3        # labelled fragments a player needs to become a class
EVIDENCE_MIN = 0.8       # |number evidence| for a fragment to teach its team's kit colour
KIT_MIN, KIT_MARGIN = 5, 0.25
EPOCHS, LR, WD, SCALE = 200, 0.05, 1e-3, 20.0

_NET = None


def _embedder():
    global _NET
    if _NET is None:
        import torch
        import torchvision
        dev = "cuda" if torch.cuda.is_available() else "cpu"
        net = torchvision.models.resnet50(weights=torchvision.models.ResNet50_Weights.IMAGENET1K_V2)
        net.fc = torch.nn.Identity()
        net = net.to(dev).eval()
        net = net.half() if dev == "cuda" else net
        mean = torch.tensor([0.485, 0.456, 0.406], device=dev).view(1, 3, 1, 1)
        std = torch.tensor([0.229, 0.224, 0.225], device=dev).view(1, 3, 1, 1)
        _NET = (torch, net, dev, mean, std)
    return _NET


def embed(crops):
    """[N, 2048] float16, L2-normalised."""
    import cv2
    if not crops:
        return np.zeros((0, 2048), np.float16)
    torch, net, dev, mean, std = _embedder()
    out = []
    for s in range(0, len(crops), 128):
        x = np.stack([cv2.cvtColor(cv2.resize(c, (128, 256)), cv2.COLOR_BGR2RGB) for c in crops[s:s + 128]])
        x = (torch.from_numpy(x).to(dev).permute(0, 3, 1, 2).float() / 255.0 - mean) / std
        with torch.no_grad():
            f = net(x.half() if dev == "cuda" else x)
        out.append(torch.nn.functional.normalize(f.float(), dim=1).cpu().numpy().astype(np.float16))
    return np.concatenate(out)


def collect(js, S, clip, shooter, off_court):
    """Reads, kit colour and appearance features of every fragment in one tracked clip.
    js: the jersey stack; S: holder.load() state; off_court: holder.off_court_tracks()."""
    import cv2
    seen = {}
    for k, fr in enumerate(S["players"]):
        for oid in fr:
            seen.setdefault(oid, []).append(k)
    keep = {o: ks for o, ks in seen.items() if len(ks) >= MIN_FRAMES and o not in off_court}
    cap = cv2.VideoCapture(clip)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    want = {}
    for o, ks in keep.items():
        look = {ks[i] for i in np.linspace(0, len(ks) - 1, min(CROPS, len(ks))).astype(int)}
        for k in sorted(set(ks[::STEP]) | look):
            want.setdefault(int(round(float(S["times"][k]) * fps)), []).append((o, k, k in look, k in ks[::STEP]))
    read_crops, read_meta, look_crops, look_meta, labs = [], [], [], [], {}
    i, last = 0, max(want) if want else -1
    while i <= last:
        ok, im = cap.read()
        if not ok:
            break
        for o, k, is_look, is_read in want.get(i, ()):
            p = S["players"][k][o]
            b = [int(v) for v in p["box"]]
            hh, ww = im.shape[:2]
            x0, y0, x1, y1 = max(0, b[0]), max(0, b[1]), min(ww, b[2]), min(hh, b[3])
            if x1 - x0 <= 8 or y1 - y0 <= 16:
                continue
            crop = im[y0:y1, x0:x1].copy()
            if is_read:
                read_crops.append(crop)
                read_meta.append((o, round(float(S["times"][k]), 3)))
            if is_look:
                look_crops.append(crop)
                look_meta.append(o)
                m = cv2.resize(p["mask"].astype(np.uint8), (ww, hh), interpolation=cv2.INTER_NEAREST)[y0:y1, x0:x1] > 0
                h = y1 - y0
                band = np.zeros_like(m)
                band[int(0.2 * h):int(0.55 * h), :] = True
                if (m & band).sum() >= 30:
                    labs.setdefault(o, []).append(np.median(cv2.cvtColor(crop, cv2.COLOR_BGR2LAB)[m & band], axis=0))
        i += 1
    cap.release()
    tracks = {str(o): {"reads": [], "lab": [round(float(v), 1) for v in np.median(labs[o], axis=0)] if labs.get(o) else None}
              for o in keep}
    for (o, t), (n, c) in zip(read_meta, js.read_crops(read_crops) if read_crops else []):
        if n is not None and float(c or 0) >= READ_MIN:
            tracks[str(o)]["reads"].append([t, str(n), round(float(c), 3)])
    return {"shooter": None if shooter is None else str(shooter), "tracks": tracks,
            "emb": embed(look_crops), "emb_track": np.array([str(o) for o in look_meta])}


def save(path, rec, meta):
    """One clip's record: <path>.npz (features) + the rest as JSON inside it."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    info = dict(meta, shooter=rec["shooter"], tracks=rec["tracks"])
    tmp = path + ".tmp.npz"
    np.savez_compressed(tmp, emb=rec["emb"], emb_track=rec["emb_track"], info=json.dumps(info))
    os.replace(tmp, path)


def load_game(game_dir):
    out = []
    for f in sorted(os.listdir(game_dir)) if os.path.isdir(game_dir) else []:
        if not f.endswith(".npz") or f.endswith(".tmp.npz"):
            continue
        try:
            z = np.load(os.path.join(game_dir, f))
            out.append(dict(json.loads(str(z["info"])), emb=z["emb"].astype(np.float32),
                            emb_track=z["emb_track"].astype(str), name=f[:-4]))
        except Exception:  # noqa: BLE001 - a half-written file
            continue
    return out


def _votes(reads, allowed=None):
    v = Counter()
    for _, n, c in reads:
        if allowed is None or n in allowed:
            v[n] += c * STEP              # every STEP-th frame was read: same scale as a full read
    return v


def _pick(v, vmin, ratio):
    if not v:
        return None
    top = v.most_common(2)
    return top[0][0] if top[0][1] >= vmin and (len(top) == 1 or top[0][1] >= ratio * top[1][1]) else None


def _fit(X, y, ncls):
    import torch
    dev = "cuda" if torch.cuda.is_available() else "cpu"
    X = torch.tensor(X, dtype=torch.float32, device=dev)
    y = torch.tensor(y, device=dev)
    w = torch.zeros(X.shape[1], ncls, device=dev, requires_grad=True)
    b = torch.zeros(ncls, device=dev, requires_grad=True)
    cw = torch.tensor([1.0 / max(1, int((y == c).sum())) for c in range(ncls)], device=dev)
    cw = cw / cw.sum() * ncls
    opt = torch.optim.Adam([w, b], lr=LR, weight_decay=WD)
    for _ in range(EPOCHS):
        opt.zero_grad()
        torch.nn.functional.cross_entropy(SCALE * X @ w + b, y, weight=cw).backward()
        opt.step()
    return w.detach(), b.detach(), dev


def name_shooters(clips, rosters, team_of):
    """{clip name: {"n": number, "conf": 0..1}} for every clip whose shooter the model can place.

    clips: load_game() records (each with side, epoch); rosters: {"left": numbers, "right": numbers};
    team_of(clip) -> "left"/"right"/None, the team that took that shot."""
    import torch
    from agx_pipeline.team_assign import _number_evidence
    left, right = set(map(str, rosters.get("left") or ())), set(map(str, rosters.get("right") or ()))
    if not left or not right:
        return {}
    both, alln = left & right, left | right
    kit = defaultdict(list)
    for c in clips:
        for t in c["tracks"].values():
            if t["lab"] is None:
                continue
            e = _number_evidence(dict(_votes(t["reads"])), left, right)
            if abs(e) > EVIDENCE_MIN:
                kit["left" if e > 0 else "right"].append(t["lab"])
    kitm = {k: np.median(np.array(v), axis=0) for k, v in kit.items() if len(v) >= KIT_MIN}

    def kit_team(lab):
        if len(kitm) < 2 or lab is None:
            return None
        dl, dr = np.linalg.norm(np.array(lab) - kitm["left"]), np.linalg.norm(np.array(lab) - kitm["right"])
        if abs(dl - dr) < KIT_MARGIN * np.linalg.norm(kitm["left"] - kitm["right"]):
            return None
        return "left" if dl < dr else "right"

    label = {}
    for ci, c in enumerate(clips):
        for o, t in c["tracks"].items():
            n = _pick(_votes(t["reads"], alln), HOLD, HOLD_RATIO)
            if n is None:
                continue
            team = ("left" if n in left else "right") if n not in both else kit_team(t["lab"])
            if team:
                label[(ci, o)] = (team, n)
    count = Counter(label.values())
    classes = sorted(k for k, v in count.items() if v >= MIN_FRAGMENTS)
    if len(classes) < 2:
        return {}
    cid = {k: i for i, k in enumerate(classes)}
    X, y = [], []
    for (ci, o), k in label.items():
        if k in cid:
            rows = clips[ci]["emb"][clips[ci]["emb_track"] == o]
            X.append(rows)
            y += [cid[k]] * len(rows)
    w, b, dev = _fit(np.concatenate(X), np.array(y), len(classes))
    out = {}
    for c in clips:
        team, sh = team_of(c), c.get("shooter")
        ok = [i for i, k in enumerate(classes) if k[0] == team]
        rows = c["emb"][c["emb_track"] == sh] if sh is not None else []
        if not ok or len(rows) == 0:
            continue
        lp = torch.log_softmax(SCALE * torch.tensor(rows, device=dev) @ w + b, 1).mean(0)
        p = torch.softmax(lp[ok], 0).cpu().numpy()
        j = int(p.argmax())
        out[c["name"]] = {"n": classes[ok[j]][1], "conf": round(float(p[j]), 3)}
    return out
