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
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
INFO_URL = "https://api.hyperliquid.xyz/info"
USER_AGENT = "Mozilla/5.0 (WhaleSonarCollector/0.3)"


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


def main():
    ap = argparse.ArgumentParser(description="Whale Sonar collector")
    ap.add_argument("--candidates", type=int, default=300, help="leaderboard accounts to scan")
    ap.add_argument("--keep", type=int, default=60, help="max whales kept in the snapshot")
    ap.add_argument("--min-board-value", type=float, default=500_000)
    ap.add_argument("--min-notional", type=float, default=500_000)
    ap.add_argument("--rps", type=float, default=6.0, help="max requests per second (6 = ~720 of 1200 weight/min)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--min-ok", type=int, default=100, help="fail if fewer accounts were scanned OK")
    ap.add_argument("--out", default="out/data.json")
    args = ap.parse_args()

    t0 = time.time()
    board = fetch_leaderboard()
    print(f"leaderboard rows: {len(board)} ({time.time() - t0:.1f}s)")
    pool = [w for w in board if w["address"] and w["board_account_value"] >= args.min_board_value]
    pool.sort(key=lambda w: w["board_account_value"], reverse=True)
    cands = pool[: args.candidates]

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

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "hyperliquid-public-api",
        "stats": {"scanned_ok": len(results), "failed": len(errors),
                  "with_positions": len(with_pos), "seconds": round(secs, 1)},
        "whales": kept,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, args.out)
    print(f"saved {args.out} ({os.path.getsize(args.out) / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
