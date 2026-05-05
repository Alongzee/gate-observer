"""
gate_observer.py — FIXED v3
Gate.io Spot Observer — USDT → Any indirect conversion scanner.
Subscribes to USDT pairs only. SDK callback fixed.
"""

import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
from gate_ws import Configuration, Connection, WebSocketResponse
from gate_ws.spot import SpotTickerChannel

# ─── CONFIG ──────────────────────────────────────────────────────────────
DB_PATH = os.path.expanduser("~/gate-observer/gate_gaps.db")
TAKER_FEE = 0.0009
PRICE_STALE_SEC = 3.0
SCAN_INTERVAL = 0.05

STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "FDUSD", "TUSD", "USDD", "FRAX", "USDP", "PYUSD"}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger("gate_observer")

# ─── PRICE BOOK ──────────────────────────────────────────────────────────
prices: Dict[str, Tuple[float, float, float]] = {}

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
        (datetime.utcnow().isoformat(), direction, intermediary, target,
         round(net_spread, 6), round(gross_spread, 6),
         round(direct_rate, 10), round(indirect_rate, 10),
         1 if net_spread > 0 else 0, depth)
    )
    conn.commit()

# ─── FETCH ALL SPOT PAIRS ────────────────────────────────────────────────
async def fetch_all_pairs() -> List[str]:
    """Fetch all spot trading pairs from Gate.io REST API."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get("https://api.gateio.ws/api/v4/spot/currency_pairs")
        data = resp.json()
    pairs = []
    for item in data:
        if item.get("trade_status") == "tradable":
            pair_id = item.get("id", "")
            if "_" in pair_id:
                base, quote = pair_id.split("_", 1)
                if base not in STABLECOINS or quote not in STABLECOINS:
                    pairs.append(pair_id)
    log.info(f"Fetched {len(pairs)} tradable spot pairs")
    return pairs

# ─── WEBSOCKET CALLBACK ──────────────────────────────────────────────────
def on_ticker(conn: Connection, response: WebSocketResponse):
    if response.error:
        return
    result = response.result
    # Handle both list and dict formats
    items = []
    if isinstance(result, list):
        items = result
    elif isinstance(result, dict):
        items = [result]
    for ticker in items:
        symbol = ticker.get("currency_pair", "")
        bid = float(ticker.get("highest_bid", 0))
        ask = float(ticker.get("lowest_ask", 0))
        if symbol and bid > 0 and ask > 0:
            update_price(symbol, bid, ask)

# ─── ENGINE ──────────────────────────────────────────────────────────────
def build_matrix():
    intermediaries = []
    targets = {}
    for sym in list(prices.keys()):
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
            if base == x and quote != "USDT" and f"{quote}_USDT" in prices:
                if quote not in STABLECOINS:
                    x_targets.append(quote)
        if x_targets:
            targets[x] = x_targets
    return intermediaries, targets

def evaluate_route(intermediary: str, target: str) -> Optional[Dict]:
    x_usdt = get_price(f"{intermediary}_USDT")
    x_target = get_price(f"{intermediary}_{target}")
    target_usdt = get_price(f"{target}_USDT")
    if x_usdt is None or x_target is None or target_usdt is None:
        return None
    target_ask = target_usdt[1]
    if target_ask <= 0:
        return None
    direct_rate = (1.0 / target_ask) * (1.0 - TAKER_FEE)
    x_ask = x_usdt[1]
    target_via_x_ask = x_target[1]
    if x_ask <= 0 or target_via_x_ask <= 0:
        return None
    amount_x = (1.0 / x_ask) * (1.0 - TAKER_FEE)
    indirect_rate = amount_x * (1.0 / target_via_x_ask) * (1.0 - TAKER_FEE)
    net_spread = ((indirect_rate / direct_rate) - 1.0) * 100
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

# ─── SCAN LOOP ───────────────────────────────────────────────────────────
async def scan_loop(conn_db):
    log.info("Scanning USDT → Any indirect routes")
    scan_count = 0
    positive_count = 0
    last_heartbeat = time.time()
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
                log_scan(conn_db, "USDT_TO_TARGET", x, t,
                         result["net_spread_pct"], result["gross_spread_pct"],
                         result["direct_rate"], result["indirect_rate"],
                         result["depth_score"])
                if result["would_execute"]:
                    log.info(f"📊 GAP | USDT→{x}→{t} | spread={result['net_spread_pct']:+.4f}% | depth={result['depth_score']} | total: {positive_count}")
        if time.time() - last_heartbeat > 60:
            log.info(f"💓 Heartbeat | {len(prices)} pairs | {len(intermediaries)} intermediaries | {scan_count} scans | {positive_count} positive")
            last_heartbeat = time.time()

# ─── MAIN ────────────────────────────────────────────────────────────────
async def main():
    log.info("=" * 55)
    log.info("  GATE.IO OBSERVER — Phase 1 (SDK v3)")
    log.info(f"  Fee: {TAKER_FEE*100:.2f}% per leg ({TAKER_FEE*2*100:.2f}% cycle)")
    log.info("=" * 55)

    all_pairs = await fetch_all_pairs()
    if not all_pairs:
        log.error("No pairs fetched. Exiting.")
        return

    usdt_pairs = [p for p in all_pairs if p.endswith("_USDT")]
    log.info(f"Subscribing to {min(len(usdt_pairs), 100)} USDT pairs")

    conn = Connection(Configuration())
    channel = SpotTickerChannel(conn, on_ticker)
    channel.subscribe(usdt_pairs[:100])

    await asyncio.gather(
        conn.run(),
        scan_loop(init_db()),
    )

if __name__ == "__main__":
    asyncio.run(main())
