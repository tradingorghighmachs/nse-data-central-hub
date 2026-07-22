import os
import sys
import json
import time
import threading
import datetime
import random
import urllib.parse
import urllib.request
from queue import Queue, Empty

import zmq
import requests
import websocket
from fastapi import FastAPI
import uvicorn

# Scrapling for Akamai-protected REST endpoints (new API path)
try:
    from scrapling.fetchers import Fetcher as ScraplingFetcher
except ImportError:
    ScraplingFetcher = None

# ==============================================================================
# CONFIGURATION & ENVIRONMENT ENDPOINTS
# ==============================================================================
ZMQ_PORT = 5555
REST_PORT = int(os.environ.get("PORT", 41937))

NSE_BASE_URL = os.environ.get("NSE_BASE_URL", "https://www.nseindia.com")
NSE_STREAMER_URL = os.environ.get("NSE_STREAMER_URL", "wss://streamer.nseindia.com")
NSE_ARCHIVE_URL = os.environ.get("NSE_ARCHIVE_URL", "https://nsearchives.nseindia.com")

# The 5 Core Indices
INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"]

# F&O Equities (All 211 stocks)
EQUITIES = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER",
    "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT",
    "ASTRAL", "AUBANK", "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHLDNG", "BAJFINANCE",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL", "BHEL",
    "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE", "CAMS", "CANBK",
    "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL",
    "CONCOR", "CROMPTON", "CUMMINSIND", "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON",
    "DLF", "DMART", "DRREDDY", "EICHERMOT", "ETERNAL", "EXIDEIND", "FEDERALBNK", "FORCEMOT",
    "FORTIS", "GAIL", "GLENMARK", "GMRAIRPORT", "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM",
    "GVT&D", "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "IDEA", "IDFCFIRSTB", "IEX", "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER",
    "INFY", "INOXWIND", "IOC", "IREDA", "IRFC", "ITC", "JINDALSTEL", "JIOFIN",
    "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK",
    "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTM",
    "LUPIN", "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK",
    "MCX", "MFSL", "MOTHERSON", "MOTILALOFS", "MPHASIS", "MUTHOOTFIN", "NAM-INDIA", "NATIONALUM",
    "NAUKRI", "NBCC", "NESTLEIND", "NHPC", "NMDC", "NTPC", "NUVAMA", "NYKAA",
    "OBEROIRLTY", "OFSS", "OIL", "ONGC", "PAGEIND", "PATANJALI", "PAYTM", "PERSISTENT",
    "PETRONET", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING",
    "POLICYBZR", "POLYCAB", "POWERGRID", "POWERINDIA", "PREMIERENE", "PRESTIGE", "RADICO", "RBLBANK",
    "RECLTD", "RELIANCE", "RVNL", "SAIL", "SAMMAANCAP", "SBICARD", "SBILIFE", "SBIN",
    "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUNPHARMA", "SUPREMEIND",
    "SUZLON", "SWIGGY", "TATACONSUM", "TATAELXSI", "TATAPOWER", "TATASTEEL", "TCS", "TECHM",
    "TIINDIA", "TITAN", "TMPV", "TORNTPHARM", "TRENT", "TVSMOTOR", "ULTRACEMCO", "UNIONBANK",
    "UNITDSPR", "UNOMINDA", "UPL", "VBL", "VEDL", "VMM", "VOLTAS", "WAAREEENER",
    "WIPRO", "YESBANK", "ZYDUSLIFE"
]

WS_HEADERS = {
    "Origin": NSE_BASE_URL,
    "User-Agent": os.environ.get("NSE_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"),
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Accept-Language": "en-US,en;q=0.9"
}

cookie_lock = threading.Lock()
last_cookie_fetch = 0
NSE_COOKIES = ""

def refresh_nse_cookies(force=False):
    global NSE_COOKIES, last_cookie_fetch
    with cookie_lock:
        if not force and NSE_COOKIES:
            return
        # If force refresh was requested, check if we already refreshed recently
        if force and time.time() - last_cookie_fetch < 15:
            return
            
        print("[AUTH] Fetching fresh session cookies from NSE homepage...")
        try:
            r = requests.get(NSE_BASE_URL, headers=WS_HEADERS, timeout=10)
            cookie_dict = r.cookies.get_dict()
            if cookie_dict:
                NSE_COOKIES = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
                last_cookie_fetch = time.time()
                print(f"[AUTH] Successfully extracted cookies: {list(cookie_dict.keys())}")
            else:
                print("[AUTH WARN] No cookies returned from NSE.")
        except Exception as e:
            print(f"[AUTH ERROR] Failed to fetch cookies: {e}")

# ==============================================================================
# ZERO MQ & STORAGE LAKE SETUP
# ==============================================================================
context = zmq.Context()
zmq_publisher = context.socket(zmq.PUB)
zmq_publisher.bind(f"tcp://0.0.0.0:{ZMQ_PORT}")
zmq_lock = threading.Lock()
print(f"[ZMQ] Publisher Bound on tcp://127.0.0.1:{ZMQ_PORT}")

# Permanent Storage Lake
ARCHIVE_DIR = os.path.join(os.path.dirname(__file__), "data_lake")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
storage_queue = Queue()
turso_queue = Queue()

def data_lake_worker():
    print("[STORAGE] Permanent Data Lake Writer Started (with I/O Handle Caching).")
    file_handles = {}
    current_date = datetime.date.today().strftime("%Y_%m_%d")

    while True:
        try:
            msg_type, topic, data = storage_queue.get()
            if data is None: break
            
            today_str = datetime.date.today().strftime("%Y_%m_%d")
            
            # Rotate file handles automatically at midnight
            if today_str != current_date:
                for f in file_handles.values():
                    try: f.close()
                    except: pass
                file_handles.clear()
                current_date = today_str

            filepath = os.path.join(ARCHIVE_DIR, f"{msg_type}_ticks_{today_str}.jsonl")
            
            if filepath not in file_handles:
                file_handles[filepath] = open(filepath, 'a', encoding='utf-8', buffering=8192)
                
            f = file_handles[filepath]
            record = {"received_at": datetime.datetime.now().isoformat(), "topic": topic, "payload": data}
            f.write(json.dumps(record) + '\n')
            
            storage_queue.task_done()
        except Exception as e:
            print(f"[STORAGE ERROR] {e}")

threading.Thread(target=data_lake_worker, daemon=True).start()

def turso_sync_worker():
    url = os.environ.get("TURSO_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    
    if not url or not token:
        print("[TURSO] Database URL or Token missing. Skipping live sync.")
        return
        
    print("[TURSO] Starting Turso DB Live Synchronizer worker...")
    
    try:
        from libsql_client import create_client_sync
    except ImportError:
        print("[TURSO ERROR] libsql-client is not installed. Skipping live sync.")
        return
        
    try:
        with create_client_sync(url=url, auth_token=token) as client:
            client.execute("CREATE TABLE IF NOT EXISTS ticks (received_at TEXT, topic TEXT, symbol TEXT, price REAL, payload TEXT)")
            print("[TURSO] Database table initialized/verified.")
    except Exception as e:
        print(f"[TURSO ERROR] Database connection/init failed: {e}")
        return

    buffer = []
    last_flush = time.time()
    
    while True:
        try:
            try:
                item = turso_queue.get(timeout=1.0)
                buffer.append(item)
                turso_queue.task_done()
            except Empty:
                pass
                
            if len(buffer) >= 100 or (time.time() - last_flush >= 10 and buffer):
                try:
                    with create_client_sync(url=url, auth_token=token) as client:
                        statements = []
                        for msg_type, topic, data in buffer:
                            received_at = datetime.datetime.now().isoformat()
                            symbol = topic.split(":", 1)[1] if ":" in topic else ""
                            if msg_type == "SPOT":
                                price = float(data.get("currentPrice") or data.get("ltp") or 0.0)
                            else:
                                price = 0.0
                                ce = data.get("CE", {})
                                pe = data.get("PE", {})
                                if ce: price = float(ce.get("lastPrice") or 0.0)
                                elif pe: price = float(pe.get("lastPrice") or 0.0)
                            
                            payload_str = json.dumps(data)
                            statements.append((
                                "INSERT INTO ticks (received_at, topic, symbol, price, payload) VALUES (?, ?, ?, ?, ?)",
                                (received_at, topic, symbol, price, payload_str)
                            ))
                        
                        client.batch(statements)
                except Exception as e:
                    print(f"[TURSO ERROR] Batch insert failed: {e}")
                
                buffer.clear()
                last_flush = time.time()
                
        except Exception as e:
            print(f"[TURSO ERROR] Sync worker error: {e}")
            time.sleep(2)

threading.Thread(target=turso_sync_worker, daemon=True).start()

# ==============================================================================
# WEBSOCKET STREAM GHOSTING (AKAMAI EVASION)
# ==============================================================================

def ws_on_message(topic, message):
    try:
        data = json.loads(message)

        # ── NORMALIZE & FIX NSE PAYLOAD DISCREPANCIES ──────────────────────────
        if isinstance(data, dict):
            # 1. Fix NSE backend spelling typos
            if "recievedTime" in data:
                data["receivedTime"] = data.pop("recievedTime")
            if "dessiminationTime" in data:
                data["disseminationTime"] = data.pop("dessiminationTime")

            # 2. Resolve contradictory status flags (indStatus vs mktStatus)
            if data.get("mktStatus") == "Open" and data.get("indStatus") == "Close":
                data["indStatus"] = "Open"

            # 3. Populate missing/deprecated schema fields for Spot indices & equities
            if topic.startswith("SPOT:"):
                sym = topic.split(":", 1)[1]
                if not data.get("indexName") and sym in EQUITIES:
                    data["indexName"] = "EQUITY"
                    data["index"] = sym
                elif not data.get("indexName") and sym in INDICES:
                    data["indexName"] = sym
                    data["brdCstIndexName"] = sym

                # Populate indValue, indChange, indPerChange from active fields if 0.0
                if data.get("indValue", 0.0) == 0.0 and data.get("currentPrice") is not None:
                    data["indValue"] = data.get("currentPrice", 0.0)
                    data["indChange"] = data.get("change", 0.0)
                    data["indPerChange"] = data.get("perChange", 0.0)
        
        # Re-serialize cleaned payload for downstream consumers and data lake
        clean_message = json.dumps(data)
        # ──────────────────────────────────────────────────────────────────────

        # 1. Publish to ZMQ
        with zmq_lock:
            zmq_publisher.send_string(f"{topic} {clean_message}")
        
        # 2. Dump to Permanent Data Lake
        msg_type = topic.split(":")[0]  # SPOT or OPT
        storage_queue.put((msg_type, topic, data))
        turso_queue.put((msg_type, topic, data))

        # 3. Maintain live in-memory OI snapshot for /api/quote
        if msg_type == "OPT":
            sym = topic.split(":", 1)[1]  # e.g. NIFTY or INFY
            if sym not in GLOBAL_CACHE["quotes"]:
                GLOBAL_CACHE["quotes"][sym] = {}
            # Each OPT tick has CE and/or PE sub-objects, each with their own identifier
            for leg in ("CE", "PE"):
                leg_data = data.get(leg)
                if leg_data and isinstance(leg_data, dict):
                    identifier = leg_data.get("identifier")
                    if identifier:
                        GLOBAL_CACHE["quotes"][sym][identifier] = leg_data
        elif msg_type == "SPOT":
            sym = topic.split(":", 1)[1]
            GLOBAL_CACHE["spot"][sym] = data

    except Exception as e:
        pass

def is_market_closed() -> bool:
    """Returns True if current IST time is after 15:35 or before 08:55, or weekend."""
    now_utc = datetime.datetime.utcnow()
    ist_now = now_utc + datetime.timedelta(hours=5, minutes=30)
    if ist_now.weekday() >= 5:  # Saturday or Sunday
        return True
    t = ist_now.time()
    return t >= datetime.time(15, 35) or t < datetime.time(8, 55)

def connect_ghost_stream(url_source, topic):
    backoff = 2  # Start at 2s, doubles on each failure, caps at 120s
    logged_closed = False

    while True:
        if is_market_closed():
            if not logged_closed:
                print(f"[SESSION CLOSED] {topic}: Market closed (after 15:35 IST / weekend). Sleeping until 08:55 IST...")
                logged_closed = True
            time.sleep(60)
            continue
        logged_closed = False

        connected_at = time.time()
        
        if not NSE_COOKIES:
            refresh_nse_cookies()

            
        header_list = [f"{k}: {v}" for k, v in WS_HEADERS.items()]
        if NSE_COOKIES:
            header_list.append(f"Cookie: {NSE_COOKIES}")

        # Resolve URL dynamically (can be string or callable)
        if callable(url_source):
            target_url = url_source()
        else:
            target_url = url_source

        def on_open_callback(ws_instance):
            # AKAMAI EVASION: The 28-second stream kill logic
            def evasion_closer():
                time.sleep(28)
                try:
                    ws_instance.close()
                except:
                    pass
            threading.Thread(target=evasion_closer, daemon=True).start()

        def on_msg(ws, msg):
            ws_on_message(topic, msg)

        def on_err(ws, error):
            try:
                if hasattr(error, 'status_code'):
                    print(f"[WS ERROR] {topic} HTTP {error.status_code}: {error}")
                else:
                    print(f"[WS ERROR] {topic}: {repr(error)}")
            except Exception as e:
                print(f"[WS ERROR FAIL] {topic}: {e}")

        def on_close_callback(ws, close_status_code, close_msg):
            try:
                print(f"[WS CLOSE] {topic} code: {repr(close_status_code)}, msg: {repr(close_msg)}")
            except Exception as e:
                print(f"[WS CLOSE FAIL] {topic}: {e}")

        try:
            ws = websocket.WebSocketApp(
                target_url,
                header=header_list,
                on_message=on_msg,
                on_error=on_err,
                on_close=on_close_callback,
                on_open=on_open_callback
            )
            ws.run_forever(ping_interval=10, ping_timeout=5)
        except Exception as e:
            print(f"[WS EXCEPTION] {topic}: {e}")
            
        # If connection drops repeatedly quickly, refresh cookies
        if time.time() - connected_at < 5:
            print(f"[WS REFRESH] {topic} connection dropped instantly. Purging stale cookies.")
            refresh_nse_cookies(force=True)
            time.sleep(random.uniform(1.0, 3.0))  # Add jitter to prevent reconnection storms

        # If connection lasted > 10s, treat as successful — reset backoff
        if time.time() - connected_at > 10:
            backoff = 2
        else:
            backoff = min(backoff * 2, 120)

        print(f"[WS RECONNECT] {topic}: retrying in {backoff}s...")
        time.sleep(backoff)

# Exact index names as NSE WebSocket server expects them
INDEX_WS_NAMES = {
    "NIFTY":       "Nifty 50",
    "BANKNIFTY":   "Nifty Bank",
    "FINNIFTY":    "Nifty Fin Service",
    "MIDCPNIFTY":  "Nifty Midcap Select",
    "NIFTYNXT50":  "Nifty Next 50",
    "INDIAVIX":    "India VIX",
    "INDIA VIX":   "India VIX",
}

INDEX_WS_PATHS = {
    "NIFTY":       "nifty50",
    "BANKNIFTY":   "niftyBank",
    "FINNIFTY":    "niftyFinService",
    "MIDCPNIFTY":  "niftyMidcapSelect",
    "NIFTYNXT50":  "niftyNext50",
    "INDIAVIX":    "indiaVix",
    "INDIA VIX":   "indiaVix",
}

def get_dynamic_fallback_expiry(symbol: str) -> str:
    """Calculates a realistic default expiry date (e.g., nearest Thursday or Wednesday) in DD-MMM-YYYY format."""
    import calendar
    today = datetime.date.today()
    
    # 0 = Monday, 1 = Tuesday, 2 = Wednesday, 3 = Thursday, 4 = Friday, 5 = Saturday, 6 = Sunday
    if symbol == "BANKNIFTY":
        # Bank Nifty weekly is Wednesday (day 2)
        days_ahead = (2 - today.weekday()) % 7
    elif symbol == "FINNIFTY":
        # Fin Nifty weekly is Tuesday (day 1)
        days_ahead = (1 - today.weekday()) % 7
    elif symbol == "MIDCPNIFTY":
        # Midcap Nifty weekly is Monday (day 0)
        days_ahead = (0 - today.weekday()) % 7
    else:
        # Nifty weekly and all Equities monthly are Thursday (day 3)
        days_ahead = (3 - today.weekday()) % 7
        # For equities, find the last Thursday of the current month
        if symbol not in ["NIFTY", "NIFTYNXT50"]:
            _, num_days = calendar.monthrange(today.year, today.month)
            last_day = datetime.date(today.year, today.month, num_days)
            offset = (last_day.weekday() - 3) % 7
            last_thursday = last_day - datetime.timedelta(days=offset)
            if last_thursday < today:
                # If passed, get last Thursday of next month
                next_month = today.replace(day=28) + datetime.timedelta(days=7)
                _, num_days_next = calendar.monthrange(next_month.year, next_month.month)
                last_day_next = datetime.date(next_month.year, next_month.month, num_days_next)
                offset_next = (last_day_next.weekday() - 3) % 7
                last_thursday = last_day_next - datetime.timedelta(days=offset_next)
            return last_thursday.strftime("%d-%b-%Y").upper()

    target_date = today + datetime.timedelta(days=days_ahead)
    return target_date.strftime("%d-%b-%Y").upper()

def start_streams():
    print("[STREAMER] Igniting WebSockets with 28s Ghosting...")

    # 1. Start Index Spot Streams with EXACT NSE WebSocket endpoint paths
    for idx in INDICES:
        path_str = INDEX_WS_PATHS.get(idx, "nifty50")
        idx_name = INDEX_WS_NAMES.get(idx, idx)
        idx_encoded = urllib.parse.quote(idx_name)
        url = f"{NSE_STREAMER_URL}/streams/indices/high/{path_str}?index={idx_encoded}"
        print(f"[STREAMER] Starting SPOT stream: {idx} -> {url}")
        threading.Thread(target=connect_ghost_stream, args=(url, f"SPOT:{idx}"), daemon=True).start()
        time.sleep(0.15)  # Stagger to prevent Akamai DDoS block

    # 2. Start Equity Spot Streams
    for eq in EQUITIES:
        url = f"{NSE_STREAMER_URL}/streams/equity/high/equityStockBySymbol?symbol={eq}"
        threading.Thread(target=connect_ghost_stream, args=(url, f"SPOT:{eq}"), daemon=True).start()
        time.sleep(0.15)  # Stagger

    # 3. Start Options Streams (Requires active expiry fetch - delayed 10s for cache warmup)
    def start_options_delayed():
        time.sleep(10)
        for sym in INDICES + EQUITIES:
            def make_url(s=sym):
                expiry = GLOBAL_CACHE["expiry"].get(s)
                if not expiry:
                    expiry = get_dynamic_fallback_expiry(s)
                return f"{NSE_STREAMER_URL}/streams/fo/mbp?symbol={s}&expiry={expiry}"
            
            print(f"[STREAMER] Starting OPT stream: {sym} (dynamic resolver)")
            threading.Thread(target=connect_ghost_stream, args=(make_url, f"OPT:{sym}"), daemon=True).start()
            time.sleep(0.15)  # Stagger

    threading.Thread(target=start_options_delayed, daemon=True).start()


# ==============================================================================
# FASTAPI LOCAL CACHE & SCRAPLING ENGINE
# ==============================================================================
app = FastAPI(title="Central Market Hub Cache")

GLOBAL_CACHE = {
    "expiry": {},
    "fiidii": {},
    "participant_oi": "",  # CSV string
    "quotes": {},          # {sym: {identifier: tick_data}} — live OI snapshot
    "spot": {},            # {sym: latest_spot_tick}
}

def warm_quotes_from_lake():
    """On startup, replay today's OPT JSONL so /api/quote is immediately populated."""
    today_str = datetime.date.today().strftime("%Y_%m_%d")
    filepath = os.path.join(ARCHIVE_DIR, f"OPT_ticks_{today_str}.jsonl")
    if not os.path.exists(filepath):
        print(f"[WARMUP] No data lake file for today: {filepath}")
        return
    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    topic = record.get("topic", "")
                    data = record.get("payload", {})
                    if not topic.startswith("OPT:"):
                        continue
                    sym = topic.split(":", 1)[1]
                    if sym not in GLOBAL_CACHE["quotes"]:
                        GLOBAL_CACHE["quotes"][sym] = {}
                    # Each record has CE and/or PE sub-objects with identifiers
                    for leg in ("CE", "PE"):
                        leg_data = data.get(leg)
                        if leg_data and isinstance(leg_data, dict):
                            identifier = leg_data.get("identifier")
                            if identifier:
                                GLOBAL_CACHE["quotes"][sym][identifier] = leg_data
                                count += 1
                except Exception:
                    continue
        # Report per-symbol counts
        sym_summary = {k: len(v) for k, v in GLOBAL_CACHE["quotes"].items()}
        print(f"[WARMUP] Quotes cache warmed: {count} contracts across {sym_summary}")
    except Exception as e:
        print(f"[WARMUP ERROR] {e}")

# Warm the cache in a background thread so startup isn't blocked
threading.Thread(target=warm_quotes_from_lake, daemon=True).start()


def warm_spot_from_lake():
    """Replay today's SPOT JSONL to pre-populate /api/spot on boot."""
    today_str = datetime.date.today().strftime("%Y_%m_%d")
    filepath = os.path.join(ARCHIVE_DIR, f"SPOT_ticks_{today_str}.jsonl")
    if not os.path.exists(filepath):
        print(f"[WARMUP] No SPOT file for today: {filepath}")
        return
    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    topic = record.get("topic", "")
                    data  = record.get("payload", {})
                    if not topic.startswith("SPOT:"):
                        continue
                    sym = topic.split(":", 1)[1]
                    price = data.get("currentPrice") or data.get("ltp") or 0.0
                    # Keep latest valid tick (ignore 0.0 post-market heartbeats if valid price exists)
                    if price > 0.0 or sym not in GLOBAL_CACHE["spot"]:
                        GLOBAL_CACHE["spot"][sym] = data
                    count += 1
                except Exception:
                    continue
        sym_summary = {k: (v.get("currentPrice") or v.get("ltp") or 0)
                       for k, v in GLOBAL_CACHE["spot"].items()}
        print(f"[WARMUP] SPOT cache warmed from {count} ticks: {sym_summary}")
    except Exception as e:
        print(f"[WARMUP-SPOT ERROR] {e}")


threading.Thread(target=warm_spot_from_lake, daemon=True).start()

@app.get("/api/health")
def health():
    return {"status": "ok", "streams": len(INDICES) + len(EQUITIES)}

@app.get("/api/expiry")
def get_expiry(symbol: str):
    expiries = GLOBAL_CACHE["expiry"].get(symbol.upper())
    # Return in shape verify_hub_connection.py expects
    return {"symbol": symbol, "expiryDates": [expiries] if expiries else []}

@app.get("/api/fiidii")
@app.get("/api/fii-dii")  # alias used by downstream consumers
def get_fiidii():
    data = GLOBAL_CACHE["fiidii"]
    # Normalise: some callers expect a plain list
    return data if isinstance(data, list) else data

@app.get("/api/participant-oi")
@app.get("/api/participant_oi")  # legacy alias
def get_participant_oi():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(GLOBAL_CACHE["participant_oi"])

@app.get("/api/quote")
def get_quote(symbol: str):
    sym = symbol.upper()
    ticks = GLOBAL_CACHE["quotes"].get(sym)  # dict of {identifier: tick_data}
    # Return as a list so consumers can iterate over strikes
    quote_list = list(ticks.values()) if ticks else []
    return {
        "symbol": sym,
        "count": len(quote_list),
        "quote": quote_list
    }

@app.get("/api/spot")
def get_spot(symbol: str):
    sym  = symbol.upper()
    tick = GLOBAL_CACHE["spot"].get(sym)
    if tick:
        # Normalise: indices use currentPrice, equities use ltp
        price = tick.get("currentPrice") or tick.get("ltp") or 0
        return {"symbol": sym, "price": price, "spot": tick}
    return {"symbol": sym, "price": 0, "spot": None}

def cache_updater_worker():
    print("[CACHE] Background REST Updater Started.")
    
    # -- Scrapling session for Akamai-protected endpoints --
    session = None
    if ScraplingFetcher:
        try:
            session = ScraplingFetcher()
            session.get(NSE_BASE_URL) # Initialize Akamai cookies
            print("[CACHE] Scrapling Akamai session initialized.")
        except Exception as e:
            print(f"[CACHE WARN] Scrapling session failed: {e}. Will use plain requests.")
            session = None

    while True:
        if is_market_closed():
            time.sleep(300)
            continue

        try:
            # 1. Update Expiries (Akamai-protected - use scrapling if available)
            for sym in INDICES + EQUITIES:
                try:
                    if session:
                        res = session.get(f"{NSE_BASE_URL}/api/option-chain-contract-info?symbol={sym}")
                        data = res.json()
                    else:
                        if not NSE_COOKIES:
                            refresh_nse_cookies()
                        headers = {
                            **WS_HEADERS,
                            "Referer": f"{NSE_BASE_URL}/option-chain?symbol={sym}"
                        }
                        if NSE_COOKIES:
                            headers["Cookie"] = NSE_COOKIES
                        r = requests.get(
                            f"{NSE_BASE_URL}/api/option-chain-contract-info?symbol={sym}",
                            headers=headers,
                            timeout=10
                        )
                        if r.status_code == 200:
                            data = r.json()
                        else:
                            data = {}
                            if r.status_code in (401, 403):
                                print(f"[CACHE WARN] Expiry fetch for {sym} got status {r.status_code}. Refreshing cookies.")
                                refresh_nse_cookies(force=True)
                    expiries = data.get('expiryDates', [])
                    if expiries:
                        GLOBAL_CACHE["expiry"][sym] = expiries[0]
                except Exception as e:
                    pass
            
            # 2. Update FII/DII JSON (Akamai-protected)
            try:
                if session:
                    res = session.get(f"{NSE_BASE_URL}/api/fiidiiTradeNse")
                    GLOBAL_CACHE["fiidii"] = res.json()
                else:
                    if not NSE_COOKIES:
                        refresh_nse_cookies()
                    headers = {
                        **WS_HEADERS,
                        "Referer": f"{NSE_BASE_URL}/reports/fii-dii"
                    }
                    if NSE_COOKIES:
                        headers["Cookie"] = NSE_COOKIES
                    r = requests.get(
                        f"{NSE_BASE_URL}/api/fiidiiTradeNse",
                        headers=headers,
                        timeout=10
                    )
                    if r.status_code == 200:
                        GLOBAL_CACHE["fiidii"] = r.json()
                if GLOBAL_CACHE["fiidii"]:
                    storage_queue.put(("FIIDII", "REST:FIIDII", GLOBAL_CACHE["fiidii"]))
            except Exception as e:
                pass
                
            # 3. Update Participant OI CSV
            # nsearchives subdomain does NOT need Akamai - use plain requests!
            for d in range(1, 6):
                target_date = datetime.date.today() - datetime.timedelta(days=d)
                date_str = target_date.strftime("%d%m%Y")
                url = f"{NSE_ARCHIVE_URL}/content/nsccl/fao_participant_oi_{date_str}.csv"
                try:
                    r = requests.get(url, timeout=10)
                    if r.status_code == 200:
                        GLOBAL_CACHE["participant_oi"] = r.text
                        storage_queue.put(("OI", f"REST:OI:{date_str}", r.text))
                        print(f"[CACHE] Participant OI updated for {date_str}")
                        break
                except Exception:
                    continue

        except Exception as e:
            print(f"[CACHE WARN] Update loop error: {e}")

            
        time.sleep(300) # Update every 5 minutes

threading.Thread(target=cache_updater_worker, daemon=True).start()

# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    # Start the ZMQ streams in a background thread so Uvicorn boots immediately!
    threading.Thread(target=start_streams, daemon=True).start()
    
    # Start FastAPI blocking
    print(f"[REST] Starting FastAPI Cache Server on http://0.0.0.0:{REST_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=REST_PORT, log_level="error")
