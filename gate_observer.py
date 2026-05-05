"""
gate_observer.py — Hybrid Sniper Phase 1
Gate.io Spot Observer — Full USDT → Any indirect conversion scanner.
Duke, 2025 — Accra, Ghana.

Strategy:
  Subscribe to all Gate.io spot tickers via WebSocket.
  Build dynamic intermediary/target matrix.
  Scan USDT → X → TARGET vs USDT → TARGET every 50ms.
  Log every opportunity to gate_gaps.db.

Fees: 0.09% taker per leg = 0.18% cycle cost.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

# ─── CONFIG ──────────────────────────────────────────────────────────────
DB_PATH = os.path.expanduser("~/gate-observer/gate_gaps.db")
TAKER_FEE = 0.0009  # 0.09% Gate.io spot taker
PRICE_STALE_SEC = 3.0
SCAN_INTERVAL = 0.05  # 50ms

# Gate.io WebSocket endpoint (public, no auth needed)
WS_URL = "wss://ws.gate.io/v4"

# Stablecoins to exclude
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "FDUSD", "TUSD", "USDD", "FRAX", "USDP", "PYUSD"}

# ─── LOGGING ─────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("gate_observer")

# ─── PRICE BOOK ──────────────────────────────────────────────────────────
prices: Dict[str, Tuple[float, float, float]] = {}  # symbol -> (bid, ask, timestamp)

def update_price(symbol: str, bid: float, ask: float):
    prices[symbol] = (bid, ask, time.time())

def get_price(symbol: str) -> Optional[Tuple[float, float, float]]:
    entry = prices.get(symbol)
    if entry is None:
        return None
    if time.time() - entry[2] > PRICE_STALE_SEC:
        return None
    return entry

# ─── DATABASE ────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS indirect_scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            direction TEXT,
            intermediary TEXT,
            target TEXT,
            net_spread_pct REAL,
            gross_spread_pct REAL,
            direct_rate REAL,
            indirect_rate REAL,
            would_execute INTEGER,
            depth_score TEXT
        )
    """)
    conn.commit()
    return conn

def log_scan(conn, direction, intermediary, target, net_spread, gross_spread, direct_rate, indirect_rate, depth):
    conn.execute(
        "INSERT INTO indirect_scans VALUES (NULL,?,?,?,?,?,?,?,?,?,?)",
        (
            datetime.utcnow().isoformat(),
            direction,
            intermediary,
            target,
            round(net_spread, 6),
            round(gross_spread, 6),
            round(direct_rate, 10),
            round(indirect_rate, 10),
            1 if net_spread > 0 else 0,
            depth,
        )
    )
    conn.commit()

# ─── ENGINE ──────────────────────────────────────────────────────────────
def build_matrix() -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Scans the price book and builds:
      - intermediaries: coins that have both USDT pair AND at least one other pair
      - targets: for each intermediary, coins with both USDT pair AND X/TARGET pair
    """
    intermediaries = []
    targets = {}

    for sym in list(prices.keys()):
        entry = get_price(sym)
        if entry is None:
            continue
        # sym format from Gate.io ticker: "BTC_USDT"
        parts = sym.split("_")
        if len(parts) != 2:
            continue
        base, quote = parts

        if base in STABLECOINS or quote in STABLECOINS:
            continue

        if quote == "USDT":
            intermediaries.append(base)

    for x in intermediaries:
        x_targets = []
        for sym in list(prices.keys()):
            parts = sym.split("_")
            if len(parts) != 2:
                continue
            base, quote = parts
            # Looking for X/TARGET pairs where TARGET also has a USDT pair
            if base == x and quote != "USDT" and f"{quote}_USDT" in prices:
                if quote not in STABLECOINS:
                    x_targets.append(quote)
        if x_targets:
            targets[x] = x_targets

    return intermediaries, targets

def evaluate_route(intermediary: str, target: str) -> Optional[Dict]:
    """
    USDT → intermediary → target vs USDT → target directly.
    Returns None if any price missing.
    """
    x_usdt = get_price(f"{intermediary}_USDT")
    x_target = get_price(f"{intermediary}_{target}")
    target_usdt = get_price(f"{target}_USDT")

    if x_usdt is None or x_target is None or target_usdt is None:
        return None

    # Direct: USDT → target
    target_ask = target_usdt[1]  # ask = buy target with USDT
    if target_ask <= 0:
        return None
    direct_rate = (1.0 / target_ask) * (1.0 - TAKER_FEE)

    # Indirect: USDT → X → target
    x_ask = x_usdt[1]  # buy X with USDT
    target_via_x_ask = x_target[1]  # buy target with X
    if x_ask <= 0 or target_via_x_ask <= 0:
        return None

    amount_x = (1.0 / x_ask) * (1.0 - TAKER_FEE)
    indirect_rate = amount_x * (1.0 / target_via_x_ask) * (1.0 - TAKER_FEE)

    net_spread = ((indirect_rate / direct_rate) - 1.0) * 100

    # Depth score based on spread width (proxy for liquidity)
    spread = (target_usdt[1] - target_usdt[0]) / target_usdt[1] * 100
    depth = "high" if spread < 0.05 else "medium" if spread < 0.2 else "low"

    return {
        "net_spread_pct": net_spread,
        "gross_spread_pct": ((amount_x * (1.0 / target_via_x_ask)) / (1.0 / target_ask) - 1.0) * 100,
        "direct_rate": direct_rate,
        "indirect_rate": indirect_rate,
        "would_execute": net_spread > 0,
        "depth_score": depth,
    }

# ─── WEBSOCKET ───────────────────────────────────────────────────────────
async def gate_ws_stream():
    """Subscribe to all spot tickers. Updates price book continuously."""
    reconnect_delay = 3

    while True:
        try:
            log.info(f"Gate.io WS connecting → {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, open_timeout=15) as ws:
                # Subscribe to spot tickers for all pairs
                sub_msg = {
                    "time": int(time.time()),
                    "channel": "spot.tickers",
                    "event": "subscribe",
                    "payload": []
                }
                await ws.send(json.dumps(sub_msg))
                log.info("Gate.io WS ✓ Connected — subscribed to all spot tickers")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("channel") != "spot.tickers":
                            continue
                        result = msg.get("result", {})
                        symbol = result.get("currency_pair", "")
                        bid = float(result.get("highest_bid", 0))
                        ask = float(result.get("lowest_ask", 0))
                        if symbol and bid > 0 and ask > 0:
                            update_price(symbol, bid, ask)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        pass

        except (ConnectionClosedError, ConnectionClosedOK):
            log.warning(f"Gate.io WS disconnected. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            log.error(f"Gate.io WS error: {e}")

        await asyncio.sleep(reconnect_delay)

# ─── SCAN LOOP ───────────────────────────────────────────────────────────
async def scan_loop(conn):
    """Every 50ms: build matrix, evaluate all routes, log opportunities."""
    scan_count = 0
    positive_count = 0
    last_heartbeat = time.time()

    log.info("Gate.io observer started — scanning USDT → Any indirect routes")

    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        scan_count += 1

        intermediaries, targets = build_matrix()

        for x in intermediaries:
            if x not in targets:
                continue
            for t in targets[x]:
                result = evaluate_route(x, t)
                if result is None:
                    continue

                if result["net_spread_pct"] > 0:
                    positive_count += 1

                # Log to database
                log_scan(conn, "USDT_TO_TARGET", x, t,
                         result["net_spread_pct"], result["gross_spread_pct"],
                         result["direct_rate"], result["indirect_rate"],
                         result["depth_score"])

                if result["would_execute"]:
                    log.info(
                        f"📊 GAP | USDT→{x}→{t} | "
                        f"spread={result['net_spread_pct']:+.4f}% | "
                        f"depth={result['depth_score']} | "
                        f"total positive: {positive_count}"
                    )

        # Heartbeat every 60 seconds
        if time.time() - last_heartbeat > 60:
            log.info(
                f"💓 Heartbeat | {len(prices)} pairs tracked | "
                f"{len(intermediaries)} intermediaries | "
                f"{scan_count} scans | {positive_count} positive gaps"
            )
            last_heartbeat = time.time()

# ─── MAIN ────────────────────────────────────────────────────────────────
async def main():
    log.info("=" * 55)
    log.info("  GATE.IO OBSERVER — Phase 1")
    log.info(f"  Strategy : USDT → Any indirect conversion")
    log.info(f"  Fee      : {TAKER_FEE*100:.2f}% per leg ({TAKER_FEE*2*100:.2f}% cycle)")
    log.info(f"  Database : {DB_PATH}")
    log.info("=" * 55)

    conn = init_db()
    log.info("Database initialized.")

    # Run WebSocket and scanner concurrently
    await asyncio.gather(
        gate_ws_stream(),
        scan_loop(conn),
    )

if __name__ == "__main__":
    asyncio.run(main())
