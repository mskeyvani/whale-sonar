#!/usr/bin/env python3
"""
whale_collector.py - automatic data collector for Whale Sonar (runs on GitHub Actions).

What it does on every run:
  1. Downloads the public Hyperliquid leaderboard.
  2. Scans the top candidates in parallel (rate-limited, well inside the documented
     per-IP limit) and keeps the whales that really have open positions.
  3. Writes ONE JSON snapshot (default: out/data.json) in the format the dashboard reads.

Safety: if too many requests fail, it exits with an error and writes nothing,
so the previous snapshot stays in place (the dashboard will show it as Cached/Delayed).

Python 3.8+, standard library only.
    python whale_collector.py --out out/data.json
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
INFO_URL = "https://api.hyperliquid.xyz/info"
USER_AGENT = "Mozilla/5.0 (WhaleSonarCollector/0.6)"


def http_json(url, payload=None, timeout=30, retries=3):
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429 or e.code >= 500:
                time.sleep(3 * (attempt + 1))
                continue
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"request failed after {retries} tries: {last}")


class RateLimiter:
    """Allows at most one request start every `interval` seconds across all threads."""

    def __init__(self, interval):
        self.interval, self.next_t, self.lock = interval, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_t)
            self.next_t = start + self.interval
        if start > now:
            time.sleep(start - now)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fetch_leaderboard():
    raw = http_json(LEADERBOARD_URL, timeout=120)
    rows = raw.get("leaderboardRows", []) if isinstance(raw, dict) else raw
    out = []
    for r in rows:
        perf = {}
        for item in r.get("windowPerformances", []) or []:
            try:
                k, v = item
                perf[k] = {"pnl": _f(v.get("pnl")), "roi": _f(v.get("roi")), "vlm": _f(v.get("vlm"))}
            except (TypeError, ValueError, AttributeError):
                continue
        out.append({
            "address": r.get("ethAddress"),
            "name": r.get("displayName"),
            "board_account_value": _f(r.get("accountValue")) or 0.0,
            "perf": perf,
        })
    return out


def fetch_positions(address):
    st = http_json(INFO_URL, {"type": "clearinghouseState", "user": address})
    ms = st.get("marginSummary", {})
    positions = []
    for ap in st.get("assetPositions", []):
        p = ap.get("position", {})
        szi = _f(p.get("szi")) or 0.0
        if szi == 0:
            continue
        lev = p.get("leverage") or {}
        positions.append({
            "coin": p.get("coin"),
            "side": "LONG" if szi > 0 else "SHORT",
            "size": abs(szi),
            "notional": _f(p.get("positionValue")) or 0.0,
            "entry": _f(p.get("entryPx")),
            "liq": _f(p.get("liquidationPx")),
            "upnl": _f(p.get("unrealizedPnl")),
            "leverage": lev.get("value"),
        })
    return {
        "account_value": _f(ms.get("accountValue")) or 0.0,
        "total_notional": _f(ms.get("totalNtlPos")) or 0.0,
        "positions": positions,
    }


# ---------------------------------------------------------------- sentiment (free preview)
CATS = [
    ("majors", ["BTC", "ETH"]),
    ("memes", ["DOGE", "kPEPE", "kSHIB", "kBONK", "kFLOKI", "WIF", "FARTCOIN", "PENGU", "POPCAT", "MOODENG", "TRUMP",
               "MELANIA", "PUMP", "USELESS", "TURBO", "BOME", "MEME", "GOAT", "BRETT", "SPX", "CASHCAT", "PNUT"]),
    ("large", ["SOL", "BNB", "XRP", "ADA", "TRX", "AVAX", "DOT", "ATOM", "NEAR", "SUI", "APT", "LTC", "BCH", "XLM", "HBAR", "HYPE"]),
]


def cat_of(coin):
    for name, coins in CATS:
        if coin in coins:
            return name
    return "other"


def senti(ws):
    L = S = nl = ns = 0.0
    for w in ws:
        l = sum(p["notional"] for p in w["positions"] if p["side"] == "LONG")
        q = sum(p["notional"] for p in w["positions"] if p["side"] == "SHORT")
        L, S = L + l, S + q
        nl += l > q
        ns += q > l
    nb = (L - S) / ((L + S) or 1)
    cb = (nl - ns) / ((nl + ns) or 1)
    return {"L": L, "S": S, "nl": int(nl), "ns": int(ns), "idx": int(50 + 50 * (0.6 * nb + 0.4 * cb) + 0.5)}


def aggregate(whales, generated_at, stats):
    """Numbers only (no addresses, no positions): this is the free, public preview."""
    def month(w):
        return (w.get("perf", {}).get("month") or {}).get("pnl")
    best = sorted((w for w in whales if month(w) is not None), key=month, reverse=True)[:20]
    H, K = {}, {}
    for w in whales:
        for p in w["positions"]:
            for bucket, key in ((H, p["coin"]), (K, cat_of(p["coin"]))):
                h = bucket.setdefault(key, {"l": 0.0, "s": 0.0, "nl": 0, "ns": 0})
                if p["side"] == "LONG":
                    h["l"] += p["notional"]; h["nl"] += 1
                else:
                    h["s"] += p["notional"]; h["ns"] += 1
    top_h = dict(sorted(H.items(), key=lambda kv: kv[1]["l"] + kv[1]["s"], reverse=True)[:12])
    allp = senti(whales)
    top_sym = max(H.items(), key=lambda kv: kv[1]["l"] + kv[1]["s"], default=(None, {"l": 0, "s": 0}))
    return {
        "v": 1, "generated_at": generated_at, "stats": stats,
        "totals": {"whales": len(whales), "positions": sum(len(w["positions"]) for w in whales),
                   "L": allp["L"], "S": allp["S"], "top_symbol": top_sym[0],
                   "top_symbol_size": top_sym[1]["l"] + top_sym[1]["s"]},
        "all": allp, "top": senti(best) if len(best) >= 5 else None, "H": top_h, "K": K,
    }


# ---------------------------------------------------------------- licensing (encryption)
def encrypt_outputs(snapshot, lics, master):
    """Encrypts the full snapshot once; wraps the data key (plus expiry and plan) for every ACTIVE license.
    The license key depends only on the license id, so renewing = changing the expiry date, no new code."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    active = [l for l in lics if l["exp"] >= today and not l.get("revoked")]
    b64 = lambda b: base64.b64encode(b).decode()
    dk = AESGCM.generate_key(bit_length=256)
    iv = os.urandom(12)
    raw = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode()
    enc = {"v": 1, "generated_at": snapshot["generated_at"], "iv": b64(iv), "ct": b64(AESGCM(dk).encrypt(iv, raw, None))}
    keys = {}
    for l in active:
        k = hmac.new(master, l["id"].encode(), hashlib.sha256).digest()
        info = json.dumps({"dk": b64(dk), "exp": l["exp"], "plan": l.get("plan", "")}).encode()
        n = os.urandom(12)
        keys[l["id"]] = {"iv": b64(n), "ct": b64(AESGCM(k).encrypt(n, info, None))}
    return enc, {"v": 1, "generated_at": snapshot["generated_at"], "keys": keys}, len(active)


# ---------------------------------------------------------------- state: whale numbers, events, last good licenses
import re
HEX8 = re.compile(r"^[0-9a-f]{8}$")
DATE = re.compile(r"^\d{4}-\d\d-\d\d$")
EV_MIN, CHG_PCT = 100_000, 10          # ignore position changes smaller than $100K / 10%


def _state_key(master):
    return hmac.new(master, b"state-v1", hashlib.sha256).digest()


def load_state(path, master):
    fresh = {"v": 1, "next": 1, "reg": {}, "prev": {}, "events": [], "lics": None}
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return fresh
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    if "ct" in obj:
        if not master:
            raise RuntimeError("state is encrypted but WHALE_MASTER is not set")
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        obj = json.loads(AESGCM(_state_key(master)).decrypt(base64.b64decode(obj["iv"]), base64.b64decode(obj["ct"]), None))
    return obj


def dump_state(state, master):
    if not master:
        return state
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    iv = os.urandom(12)
    ct = AESGCM(_state_key(master)).encrypt(iv, json.dumps(state, separators=(",", ":")).encode(), None)
    return {"v": 1, "iv": base64.b64encode(iv).decode(), "ct": base64.b64encode(ct).decode()}


def assign_numbers(state, kept):
    for w in kept:
        a = w["address"].lower()
        if a not in state["reg"]:
            state["reg"][a] = state["next"]
            state["next"] += 1
        w["num"] = state["reg"][a]


def snapshot_positions(kept):
    return {str(w["num"]): {p["coin"]: ["L" if p["side"] == "LONG" else "S", p["size"], p["notional"], p["upnl"] or 0]
                            for p in w["positions"]} for w in kept}


def compute_events(prev, kept, now):
    out = []

    def ev(w, c, k, side, nt, d, up):
        out.append({"t": now, "n": w["num"], "coin": c, "k": k, "side": side, "nt": round(nt), "d": round(d), "up": round(up or 0)})

    for w in kept:
        old = prev.get(str(w["num"]))
        if old is None:
            continue                      # whale newly tracked: no events, avoids a flood
        cur = {p["coin"]: p for p in w["positions"]}
        for c, (sd, sz, nt, up) in old.items():
            p, osd = cur.get(c), ("LONG" if sd == "L" else "SHORT")
            if p is None:
                if nt >= EV_MIN:
                    ev(w, c, "close", osd, nt, -nt, up)
            elif p["side"] != osd:
                if max(nt, p["notional"]) >= EV_MIN:
                    ev(w, c, "flip", p["side"], p["notional"], p["notional"], p["upnl"])
            elif sz > 0 and p["size"] > 0:
                pct = (p["size"] - sz) / sz * 100
                d = (p["size"] - sz) * (p["notional"] / p["size"])
                if abs(pct) >= CHG_PCT and abs(d) >= EV_MIN:
                    k = "add" if pct > 0 else ("reduce_profit" if (p["upnl"] or 0) > 0 else "reduce")
                    ev(w, c, k, p["side"], p["notional"], d, p["upnl"])
        for c, p in cur.items():
            if c not in old and p["notional"] >= EV_MIN:
                ev(w, c, "open", p["side"], p["notional"], p["notional"], p["upnl"])
    return out


def read_licenses(path, state):
    """Valid file -> use it and remember it. Broken file -> keep using the last valid list (nobody gets locked out)."""
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        assert isinstance(raw, list)
        lics = []
        for l in raw:
            assert HEX8.match(str(l["id"])) and DATE.match(str(l["exp"]))
            lics.append({"id": l["id"], "exp": l["exp"], "plan": l.get("plan", ""), "revoked": bool(l.get("revoked"))})
        state["lics"] = lics
        return lics
    except Exception as e:
        last = state.get("lics") or []
        print(f"WARNING: {path} is missing or invalid ({e}). Using the last valid list ({len(last)} licenses).")
        return last


def main():
    ap = argparse.ArgumentParser(description="Whale Sonar collector")
    ap.add_argument("--candidates", type=int, default=500, help="leaderboard accounts to scan (largest accounts)")
    ap.add_argument("--by-pnl", type=int, default=300, help="extra candidates: best 30-day profit")
    ap.add_argument("--keep", type=int, default=200, help="max whales kept in the snapshot")
    ap.add_argument("--min-board-value", type=float, default=500_000)
    ap.add_argument("--min-notional", type=float, default=300_000)
    ap.add_argument("--rps", type=float, default=6.0, help="max requests per second (6 = ~720 of 1200 weight/min)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--min-ok", type=int, default=100, help="fail if fewer accounts were scanned OK")
    ap.add_argument("--out", default="out/data.json")
    ap.add_argument("--licenses", default="licenses.json")
    ap.add_argument("--state-in", default="state_prev.json", help="previous state file (whale numbers, events)")
    args = ap.parse_args()

    t0 = time.time()
    board = fetch_leaderboard()
    print(f"leaderboard rows: {len(board)} ({time.time() - t0:.1f}s)")
    pool = [w for w in board if w["address"] and w["board_account_value"] >= args.min_board_value]
    pool.sort(key=lambda w: w["board_account_value"], reverse=True)
    cands = pool[: args.candidates]
    have = {w["address"] for w in cands}
    month = lambda w: (w["perf"].get("month") or {}).get("pnl") or 0
    extra = sorted((w for w in board if w["address"] and w["address"] not in have and month(w) > 0), key=month, reverse=True)
    cands += extra[: args.by_pnl]

    limiter = RateLimiter(1.0 / args.rps)

    def work(w):
        limiter.wait()
        try:
            return {**w, **fetch_positions(w["address"])}, None
        except Exception as e:
            return None, {"address": w["address"], "error": str(e)}

    results, errors = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for ok, err in ex.map(work, cands):
            if ok:
                results.append(ok)
            else:
                errors.append(err)

    with_pos = [r for r in results if r["total_notional"] >= args.min_notional]
    with_pos.sort(key=lambda r: r["total_notional"], reverse=True)
    kept = with_pos[: args.keep]
    secs = time.time() - t0
    print(f"scanned ok: {len(results)} | failed: {len(errors)} | whales with positions: "
          f"{len(with_pos)} | kept: {len(kept)} | {secs:.1f}s")
    for e in errors[:3]:
        print("  error:", e["address"], e["error"])

    if len(results) < args.min_ok:
        print(f"ERROR: only {len(results)} accounts scanned OK (< {args.min_ok}); keeping the old snapshot.")
        return 1

    master_hex = os.environ.get("WHALE_MASTER", "").strip()
    master_b = bytes.fromhex(master_hex) if master_hex else b""
    now_iso = datetime.now(timezone.utc).isoformat()
    state = load_state(args.state_in, master_b)
    assign_numbers(state, kept)
    new_events = compute_events(state.get("prev", {}), kept, now_iso)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    events = [e for e in state.get("events", []) if e["t"] >= cutoff] + new_events
    events = events[-400:]
    state["prev"], state["events"] = snapshot_positions(kept), events
    print(f"whale numbers assigned up to #{state['next'] - 1} | new events this run: {len(new_events)}")
    snapshot = {
        "generated_at": now_iso,
        "source": "hyperliquid-public-api",
        "stats": {"scanned_ok": len(results), "failed": len(errors),
                  "with_positions": len(with_pos), "seconds": round(secs, 1)},
        "whales": kept,
        "events": events,
    }
    outdir = os.path.dirname(os.path.abspath(args.out))
    os.makedirs(outdir, exist_ok=True)

    def save(name, obj):
        path = os.path.join(outdir, name)
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(path + ".tmp", path)
        print(f"saved {path} ({os.path.getsize(path) / 1024:.0f} KB)")

    save("public.json", aggregate(kept, snapshot["generated_at"], snapshot["stats"]))
    # (state.json is saved after the license step, because that step may update the last-valid license list)
    master = master_hex
    if master:
        enc, keys, n = encrypt_outputs(snapshot, read_licenses(args.licenses, state), master_b)
        save("data.enc.json", enc)
        save("keys.json", keys)
        print(f"LICENSE MODE: full data encrypted for {n} active license(s)")
    else:
        save(os.path.basename(args.out), snapshot)
        print("OPEN MODE: no WHALE_MASTER secret -> full data is public (not protected)")
    save("state.json", dump_state(state, master_b))
    return 0


if __name__ == "__main__":
    sys.exit(main())
