"""
PRO Trader Server v2.0
======================
Clean layered architecture:

Layer 1 — DATA SOURCES      : Zerodha Kite API + Yahoo Finance
Layer 2 — CACHE ENGINE      : In-memory TTL cache for all data
Layer 3 — MARKET DATA ENGINE: Prices, technicals, OI computation
Layer 4 — SIGNAL ENGINE     : CPR, SMC, OI interpretation
Layer 5 — API ROUTES        : Clean REST endpoints

Author: PRO Trader
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ═══════════════════════════════════════════════════════════════
# LAYER 0 — SETUP & CONSTANTS
# ═══════════════════════════════════════════════════════════════

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def ist_str(): return now_ist().strftime("%H:%M:%S IST")

app = Flask(__name__)
CORS(app)

# NSE 50 stocks — Zerodha symbol → Yahoo symbol
INSTRUMENTS = {
    # ── Indices ──
    "NIFTY":     {"kite":"NSE:NIFTY 50",         "yahoo":"^NSEI",                "step":50,  "sector":"INDEX"},
    "BANKNIFTY": {"kite":"NSE:NIFTY BANK",        "yahoo":"^NSEBANK",             "step":100, "sector":"INDEX"},
    "FINNIFTY":  {"kite":"NSE:NIFTY FIN SERVICE", "yahoo":"NIFTY_FIN_SERVICE.NS", "step":50,  "sector":"INDEX"},
    "SENSEX":    {"kite":"BSE:SENSEX",            "yahoo":"^BSESN",               "step":100, "sector":"INDEX"},
    # ── Commodities (MCX — Yahoo only) ──
    "CRUDEOIL":  {"kite":None, "yahoo":"CL=F",  "step":50,  "sector":"COMMODITY", "mcx":True},
    "GOLD":      {"kite":None, "yahoo":"GC=F",  "step":100, "sector":"COMMODITY", "mcx":True},
    # ── Banking & Finance ──
    "HDFCBANK":  {"kite":"NSE:HDFCBANK",  "yahoo":"HDFCBANK.NS",  "step":50,  "sector":"BANK"},
    "ICICIBANK": {"kite":"NSE:ICICIBANK", "yahoo":"ICICIBANK.NS", "step":50,  "sector":"BANK"},
    "KOTAKBANK": {"kite":"NSE:KOTAKBANK", "yahoo":"KOTAKBANK.NS", "step":50,  "sector":"BANK"},
    "AXISBANK":  {"kite":"NSE:AXISBANK",  "yahoo":"AXISBANK.NS",  "step":20,  "sector":"BANK"},
    "SBIN":      {"kite":"NSE:SBIN",      "yahoo":"SBIN.NS",      "step":10,  "sector":"BANK"},
    "INDUSINDBK":{"kite":"NSE:INDUSINDBK","yahoo":"INDUSINDBK.NS","step":20,  "sector":"BANK"},
    "BAJFINANCE":{"kite":"NSE:BAJFINANCE","yahoo":"BAJFINANCE.NS","step":100, "sector":"FINANCE"},
    "BAJAJFINSV":{"kite":"NSE:BAJAJFINSV","yahoo":"BAJAJFINSV.NS","step":50, "sector":"FINANCE"},
    # ── IT ──
    "TCS":    {"kite":"NSE:TCS",    "yahoo":"TCS.NS",    "step":100, "sector":"IT"},
    "INFY":   {"kite":"NSE:INFY",   "yahoo":"INFY.NS",   "step":50,  "sector":"IT"},
    "WIPRO":  {"kite":"NSE:WIPRO",  "yahoo":"WIPRO.NS",  "step":10,  "sector":"IT"},
    "HCLTECH":{"kite":"NSE:HCLTECH","yahoo":"HCLTECH.NS","step":50,  "sector":"IT"},
    "TECHM":  {"kite":"NSE:TECHM",  "yahoo":"TECHM.NS",  "step":20,  "sector":"IT"},
    "LTIM":   {"kite":"NSE:LTIM",   "yahoo":"LTIM.NS",   "step":100, "sector":"IT"},
    # ── Energy ──
    "RELIANCE": {"kite":"NSE:RELIANCE","yahoo":"RELIANCE.NS","step":50, "sector":"ENERGY"},
    "ONGC":     {"kite":"NSE:ONGC",   "yahoo":"ONGC.NS",    "step":5,  "sector":"ENERGY"},
    "BPCL":     {"kite":"NSE:BPCL",   "yahoo":"BPCL.NS",    "step":10, "sector":"ENERGY"},
    "POWERGRID":{"kite":"NSE:POWERGRID","yahoo":"POWERGRID.NS","step":10,"sector":"ENERGY"},
    "NTPC":     {"kite":"NSE:NTPC",   "yahoo":"NTPC.NS",    "step":5,  "sector":"ENERGY"},
    "COALINDIA":{"kite":"NSE:COALINDIA","yahoo":"COALINDIA.NS","step":10,"sector":"ENERGY"},
    # ── Auto ──
    "MARUTI":    {"kite":"NSE:MARUTI",    "yahoo":"MARUTI.NS",    "step":100, "sector":"AUTO"},
    "TATAMOTORS":{"kite":"NSE:TATAMOTORS","yahoo":"TATAMOTORS.NS","step":10,  "sector":"AUTO"},
    "M&M":       {"kite":"NSE:M&M",       "yahoo":"M&M.NS",       "step":50,  "sector":"AUTO"},
    "BAJAJ-AUTO":{"kite":"NSE:BAJAJ-AUTO","yahoo":"BAJAJ-AUTO.NS","step":100, "sector":"AUTO"},
    "EICHERMOT": {"kite":"NSE:EICHERMOT", "yahoo":"EICHERMOT.NS", "step":100, "sector":"AUTO"},
    "HEROMOTOCO":{"kite":"NSE:HEROMOTOCO","yahoo":"HEROMOTOCO.NS","step":50,  "sector":"AUTO"},
    # ── Pharma ──
    "SUNPHARMA":{"kite":"NSE:SUNPHARMA","yahoo":"SUNPHARMA.NS","step":50, "sector":"PHARMA"},
    "DRREDDY":  {"kite":"NSE:DRREDDY",  "yahoo":"DRREDDY.NS",  "step":100,"sector":"PHARMA"},
    "CIPLA":    {"kite":"NSE:CIPLA",    "yahoo":"CIPLA.NS",    "step":20, "sector":"PHARMA"},
    "DIVISLAB": {"kite":"NSE:DIVISLAB", "yahoo":"DIVISLAB.NS", "step":100,"sector":"PHARMA"},
    "APOLLOHOSP":{"kite":"NSE:APOLLOHOSP","yahoo":"APOLLOHOSP.NS","step":50,"sector":"PHARMA"},
    # ── FMCG ──
    "HINDUNILVR":{"kite":"NSE:HINDUNILVR","yahoo":"HINDUNILVR.NS","step":50, "sector":"FMCG"},
    "NESTLEIND": {"kite":"NSE:NESTLEIND", "yahoo":"NESTLEIND.NS", "step":100,"sector":"FMCG"},
    "ITC":       {"kite":"NSE:ITC",       "yahoo":"ITC.NS",       "step":10, "sector":"FMCG"},
    "BRITANNIA": {"kite":"NSE:BRITANNIA", "yahoo":"BRITANNIA.NS", "step":100,"sector":"FMCG"},
    "TITAN":     {"kite":"NSE:TITAN",     "yahoo":"TITAN.NS",     "step":50, "sector":"FMCG"},
    "ASIANPAINT":{"kite":"NSE:ASIANPAINT","yahoo":"ASIANPAINT.NS","step":50, "sector":"FMCG"},
    "TATACONSUM":{"kite":"NSE:TATACONSUM","yahoo":"TATACONSUM.NS","step":20, "sector":"FMCG"},
    "TRENT":     {"kite":"NSE:TRENT",     "yahoo":"TRENT.NS",     "step":50, "sector":"FMCG"},
    # ── Metal ──
    "TATASTEEL": {"kite":"NSE:TATASTEEL", "yahoo":"TATASTEEL.NS", "step":5,  "sector":"METAL"},
    "HINDALCO":  {"kite":"NSE:HINDALCO",  "yahoo":"HINDALCO.NS",  "step":5,  "sector":"METAL"},
    "JSWSTEEL":  {"kite":"NSE:JSWSTEEL",  "yahoo":"JSWSTEEL.NS",  "step":10, "sector":"METAL"},
    # ── Infra & Others ──
    "LT":         {"kite":"NSE:LT",        "yahoo":"LT.NS",        "step":50, "sector":"INFRA"},
    "ADANIPORTS": {"kite":"NSE:ADANIPORTS","yahoo":"ADANIPORTS.NS","step":20, "sector":"INFRA"},
    "ULTRACEMCO": {"kite":"NSE:ULTRACEMCO","yahoo":"ULTRACEMCO.NS","step":100,"sector":"CEMENT"},
    "GRASIM":     {"kite":"NSE:GRASIM",    "yahoo":"GRASIM.NS",    "step":50, "sector":"CEMENT"},
    "BHARTIARTL": {"kite":"NSE:BHARTIARTL","yahoo":"BHARTIARTL.NS","step":20, "sector":"TELECOM"},
    "SHRIRAMFIN": {"kite":"NSE:SHRIRAMFIN","yahoo":"SHRIRAMFIN.NS","step":20, "sector":"FINANCE"},
    "BEL":        {"kite":"NSE:BEL",       "yahoo":"BEL.NS",       "step":5,  "sector":"DEFENCE"},
    "INDIGO":     {"kite":"NSE:INDIGO",    "yahoo":"INDIGO.NS",    "step":50, "sector":"AVIATION"},
    "HAL":        {"kite":"NSE:HAL",       "yahoo":"HAL.NS",       "step":100,"sector":"DEFENCE"},
}

OI_INDICES = {"NIFTY","BANKNIFTY","FINNIFTY","SENSEX"}  # These have F&O option chains


# ═══════════════════════════════════════════════════════════════
# LAYER 1 — CACHE ENGINE
# Simple TTL in-memory cache. No Redis needed for now.
# ═══════════════════════════════════════════════════════════════

class Cache:
    """Thread-safe TTL cache with stale-while-revalidate support."""
    def __init__(self):
        self._store = {}
        self._lock  = threading.Lock()

    def get(self, key):
        with self._lock:
            entry = self._store.get(key)
            if entry:
                return entry["val"], time.time() - entry["ts"]
            return None, None

    def set(self, key, val):
        with self._lock:
            self._store[key] = {"val": val, "ts": time.time()}

    def fresh(self, key, ttl):
        """Returns True if key exists and is within TTL seconds."""
        val, age = self.get(key)
        return val is not None and age < ttl

    def get_val(self, key):
        val, _ = self.get(key)
        return val

    def age(self, key):
        _, age = self.get(key)
        return age

CACHE = Cache()

# Cache TTLs (seconds)
TTL = {
    "prices":      30,    # Zerodha prices — refresh every 30s
    "technicals": 300,    # SMA/RSI — refresh every 5 min
    "vix":         60,    # VIX — refresh every 1 min
    "usdinr":     300,    # USD/INR — refresh every 5 min
    "oi":         120,    # OI data — refresh every 2 min
    "instruments": 14400, # NFO instruments CSV — refresh every 4 hours
    "smc":        300,    # SMC calc — refresh every 5 min
    "cpr":       86400,   # CPR — daily, refresh once per day
}


# ═══════════════════════════════════════════════════════════════
# LAYER 2 — DATA SOURCE ADAPTERS
# Raw fetchers — each returns clean data or None on failure
# ═══════════════════════════════════════════════════════════════

def _kite_headers(key, token):
    return {"X-Kite-Version":"3","Authorization":f"token {key}:{token}","User-Agent":"Mozilla/5.0"}

def fetch_kite_quotes(key, token, kite_syms):
    """Fetch bulk quotes from Zerodha. Uses params= for proper URL encoding."""
    if not key or not token or not kite_syms: return {}
    try:
        # Use params= so requests handles URL encoding correctly
        # e.g. "NSE:NIFTY 50" → "NSE%3ANIFTY+50"
        params = [("i", s) for s in kite_syms]
        r = requests.get("https://api.kite.trade/quote",
            params=params,
            headers=_kite_headers(key,token), timeout=20)
        if r.status_code in [401,403]:
            print(f"[Kite] Token expired ({r.status_code})")
            return {"_token_expired": True}
        if r.status_code == 200:
            return r.json().get("data", {})
        print(f"[Kite] Quote error status: {r.status_code} — {r.text[:200]}")
    except Exception as e:
        print(f"[Kite] Quote error: {e}")
    return {}

def fetch_yahoo_candles(ticker, interval="5m", rng="2d"):
    """Fetch OHLCV candles from Yahoo Finance. Returns parsed dict or None."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={rng}&includePrePost=false"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10).json()
        res  = r["chart"]["result"][0]
        meta = res["meta"]
        ts   = res.get("timestamp",[])
        q    = res["indicators"]["quote"][0]
        vol  = q.get("volume",[])
        candles = [
            {"t":ts[i],
             "o":round(q["open"][i] or 0,2),
             "h":round(q["high"][i] or 0,2),
             "l":round(q["low"][i] or 0,2),
             "c":round(q["close"][i] or 0,2),
             "v":int(vol[i] or 0) if i<len(vol) else 0}
            for i in range(len(ts)) if q["close"][i]
        ]
        closes  = [c["c"] for c in candles]
        highs   = [c["h"] for c in candles]
        lows    = [c["l"] for c in candles]
        volumes = [c["v"] for c in candles]

        def sma(n):
            return round(sum(closes[-n:])/n,2) if len(closes)>=n else None
        def rsi14():
            if len(closes)<15: return None
            g=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
            l=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
            ag=sum(g[-14:])/14; al=sum(l[-14:])/14
            return round(100-100/(1+ag/al),1) if al else 100.0

        s20=sma(20); s50=sma(50); s200=sma(200)
        px=meta.get("regularMarketPrice",0)
        pc=meta.get("chartPreviousClose",0)

        cross="NONE"
        if len(closes)>=22 and s20 and s50:
            ps20=sum(closes[-21:-1])/20
            if ps20<s50 and s20>s50: cross="GOLDEN_CROSS"
            elif ps20>s50 and s20<s50: cross="DEATH_CROSS"

        # Volume analysis
        avg_vol = int(sum(volumes[-20:])/20) if len(volumes)>=20 else 0
        cur_vol = volumes[-1] if volumes else 0
        vol_ratio = round(cur_vol/avg_vol,2) if avg_vol else 0

        return {
            "px":px, "chg":round(px-pc,2),
            "pct":round((px-pc)/pc*100,2) if pc else 0,
            "high":meta.get("regularMarketDayHigh",0),
            "low": meta.get("regularMarketDayLow",0),
            "open":meta.get("regularMarketOpen",0),
            "prev_close":pc,
            "sma20":s20,"sma50":s50,"sma200":s200,
            "rsi":rsi14(),
            "crossover":cross,
            "trend":"BULLISH" if (s20 and s50 and s20>s50) else "BEARISH" if (s20 and s50 and s20<s50) else "NEUTRAL",
            "breakout":  bool(highs and px >= max(highs)*0.998),
            "breakdown": bool(lows  and px <= min(lows)*1.002),
            "volume": cur_vol, "avg_volume": avg_vol, "vol_ratio": vol_ratio,
            "candles": candles[-78:],  # ~6.5 hrs of 5min candles
        }
    except Exception as e:
        return None

def fetch_kite_instruments_nfo(key, token):
    """Fetch NFO instruments CSV from Zerodha. Cached 4 hours."""
    cache_key = "instruments_nfo"
    if CACHE.fresh(cache_key, TTL["instruments"]):
        return CACHE.get_val(cache_key)
    try:
        r = requests.get("https://api.kite.trade/instruments/NFO",
            headers=_kite_headers(key,token), timeout=30)
        if r.status_code in [401,403]:
            return None
        if r.status_code == 200:
            CACHE.set(cache_key, r.text)
            print("[Kite] NFO instruments CSV cached")
            return r.text
    except Exception as e:
        print(f"[Kite] Instruments error: {e}")
    return CACHE.get_val(cache_key)  # stale


# ═══════════════════════════════════════════════════════════════
# LAYER 3 — MARKET DATA ENGINE
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# LAYER 3B — MARKET INTELLIGENCE ENGINE
# VWAP, ORB, Volume, Regime, OI Writer Behavior
# ═══════════════════════════════════════════════════════════════

def calc_vwap(candles):
    """
    Calculate VWAP from intraday candles.
    VWAP = Σ(typical_price × volume) / Σ(volume)
    Only uses today's candles (reset at market open 9:15 IST).
    """
    if not candles: return None
    # Filter to today's candles only (after 9:15 IST = 3:45 UTC)
    today = datetime.now(IST).date()
    today_ts = int(datetime(today.year, today.month, today.day, 3, 45, tzinfo=timezone.utc).timestamp())
    today_c = [c for c in candles if c.get("t",0) >= today_ts]
    if not today_c: today_c = candles[-30:]  # fallback to last 30

    cum_pv = sum((c["h"]+c["l"]+c["c"])/3 * c.get("v",0) for c in today_c)
    cum_v  = sum(c.get("v",0) for c in today_c)
    if not cum_v: return None
    return round(cum_pv / cum_v, 2)

def calc_orb(candles):
    """
    Opening Range Breakout — first 15 minutes (3 × 5min candles after 9:15).
    Returns ORB high, ORB low, and whether price has broken out.
    """
    if not candles: return None
    today = datetime.now(IST).date()
    # 9:15 IST = 3:45 UTC, 9:30 IST = 4:00 UTC
    orb_start = int(datetime(today.year, today.month, today.day, 3, 45, tzinfo=timezone.utc).timestamp())
    orb_end   = int(datetime(today.year, today.month, today.day, 4,  0, tzinfo=timezone.utc).timestamp())
    orb_c = [c for c in candles if orb_start <= c.get("t",0) < orb_end]
    if not orb_c: return None
    orb_h = max(c["h"] for c in orb_c)
    orb_l = min(c["l"] for c in orb_c)
    cur   = candles[-1]["c"]
    return {
        "high": round(orb_h, 2),
        "low":  round(orb_l, 2),
        "range_pct": round((orb_h-orb_l)/orb_l*100, 2),
        "breakout":  cur > orb_h * 1.001,
        "breakdown": cur < orb_l * 0.999,
        "inside":    orb_l * 0.999 <= cur <= orb_h * 1.001,
    }

def calc_volume_analysis(candles):
    """
    Detect volume expansion/contraction vs 20-candle average.
    Also detects volume climax and drying up.
    """
    if not candles or len(candles) < 5: return {}
    vols = [c.get("v",0) for c in candles]
    avg20 = sum(vols[-20:])/20 if len(vols)>=20 else sum(vols)/len(vols)
    cur   = vols[-1]
    prev5_avg = sum(vols[-6:-1])/5 if len(vols)>=6 else avg20
    ratio = round(cur/avg20, 2) if avg20 else 0

    # Detect volume trend in last 5 candles
    recent = vols[-5:]
    vol_rising = all(recent[i] <= recent[i+1] for i in range(len(recent)-1))
    vol_falling = all(recent[i] >= recent[i+1] for i in range(len(recent)-1))

    return {
        "current": cur,
        "avg_20":  int(avg20),
        "ratio":   ratio,
        "expanding": ratio > 1.5,
        "contracting": ratio < 0.5,
        "climax": ratio > 3.0,   # volume spike — often reversal
        "drying_up": ratio < 0.3, # very low volume — no conviction
        "rising": vol_rising,
        "falling": vol_falling,
        "label": ("🚀 CLIMAX" if ratio>3 else
                  "📈 EXPANDING" if ratio>1.5 else
                  "📉 CONTRACTING" if ratio<0.5 else
                  "💧 DRYING UP" if ratio<0.3 else "NORMAL"),
    }

def calc_oi_writer_behavior(oi_data):
    """
    Interpret OI data to detect writer behavior:
    - CE wall shifting up/down (writers rolling)
    - Trapped writers (shorts/longs caught)
    - Aggressive writing vs unwinding
    - Gamma squeeze zones
    """
    if not oi_data: return {}
    ce_oi  = oi_data.get("ce_oi", 0)
    pe_oi  = oi_data.get("pe_oi", 0)
    ce_chg = oi_data.get("ce_chg", 0)
    pe_chg = oi_data.get("pe_chg", 0)
    pcr    = oi_data.get("pcr", 0)
    mp     = oi_data.get("max_pain", 0)
    spot   = oi_data.get("spot", 0)
    ce_wall= oi_data.get("ce_wall", 0)
    pe_wall= oi_data.get("pe_wall", 0)

    signals = []
    writer_bias = "NEUTRAL"

    # CE writer behavior
    if ce_chg > 0:
        if ce_chg > ce_oi * 0.05:  # >5% fresh CE writing
            signals.append(f"Aggressive CE writing at ₹{ce_wall} — strong resistance")
            writer_bias = "BEARISH"
        else:
            signals.append(f"Fresh CE writing building at ₹{ce_wall}")
    elif ce_chg < 0:
        signals.append(f"CE writers exiting (short covering) — bullish pressure")
        writer_bias = "BULLISH"

    # PE writer behavior
    if pe_chg > 0:
        if pe_chg > pe_oi * 0.05:
            signals.append(f"Aggressive PE writing at ₹{pe_wall} — strong support")
            writer_bias = "BULLISH" if writer_bias == "NEUTRAL" else writer_bias
        else:
            signals.append(f"Fresh PE writing building at ₹{pe_wall}")
    elif pe_chg < 0:
        signals.append(f"PE writers exiting (long unwinding) — bearish pressure")

    # Max pain dynamics
    if spot and mp:
        dist = spot - mp
        dist_pct = round(dist/mp*100, 2)
        if abs(dist_pct) < 0.3:
            signals.append(f"Price pinned near Max Pain ₹{mp} — expiry magnet active")
        elif dist_pct > 1.0:
            signals.append(f"Price ₹{abs(dist):.0f} above Max Pain — gravitational pull down")
        elif dist_pct < -1.0:
            signals.append(f"Price ₹{abs(dist):.0f} below Max Pain — gravitational pull up")

    # Gamma squeeze detection
    if ce_wall and pe_wall and spot:
        range_width = (ce_wall - pe_wall) / spot * 100
        if range_width < 1.5:
            signals.append(f"Tight OI band ₹{pe_wall}–₹{ce_wall} ({range_width:.1f}%) — gamma squeeze risk")

    # Trapped writers detection
    if spot and ce_wall and spot > ce_wall * 1.005:
        signals.append(f"CE writers at ₹{ce_wall} trapped — price above wall, forced to cover")
        writer_bias = "BULLISH"
    if spot and pe_wall and spot < pe_wall * 0.995:
        signals.append(f"PE writers at ₹{pe_wall} trapped — price below wall, forced to cover")
        writer_bias = "BEARISH"

    return {
        "writer_bias": writer_bias,
        "signals": signals[:3],
        "ce_writing": ce_chg > 0,
        "pe_writing": pe_chg > 0,
        "ce_unwinding": ce_chg < 0,
        "pe_unwinding": pe_chg < 0,
        "pinned": bool(spot and mp and abs((spot-mp)/mp) < 0.003),
        "trapped_ce_writers": bool(spot and ce_wall and spot > ce_wall*1.005),
        "trapped_pe_writers": bool(spot and pe_wall and spot < pe_wall*0.995),
    }

def detect_market_regime(candles, oi_data, vwap, orb, vol_analysis):
    """
    Detect current market regime with confidence score.
    Regimes: TRENDING_UP, TRENDING_DOWN, BREAKOUT_DAY,
             EXPIRY_PINNING, RANGE_BOUND, MEAN_REVERT, CHOPPY
    Each regime has different valid strategies.
    """
    if not candles or len(candles) < 10:
        return {"regime": "UNKNOWN", "label": "Insufficient data", "confidence": 0,
                "valid_strategies": [], "no_trade": True}

    closes = [c["c"] for c in candles[-20:]]
    highs  = [c["h"] for c in candles[-20:]]
    lows   = [c["l"] for c in candles[-20:]]
    vols   = [c.get("v",0) for c in candles[-20:]]
    px     = closes[-1]

    # VWAP context
    above_vwap = px > vwap if vwap else None
    vwap_dist  = abs(px - vwap) / vwap * 100 if vwap else 0

    # ATR — volatility measure
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    atr     = sum(trs[-10:])/10 if len(trs)>=10 else (sum(trs)/len(trs) if trs else 0)
    atr_pct = round(atr/px*100, 2) if px else 0

    # Day range
    day_h = max(highs); day_l = min(lows)
    day_range_pct = round((day_h-day_l)/day_l*100, 2) if day_l else 0

    # Price momentum — last 5 vs previous 5
    recent_avg  = sum(closes[-5:])/5
    prev_avg    = sum(closes[-10:-5])/5
    momentum    = (recent_avg - prev_avg) / prev_avg * 100 if prev_avg else 0

    # Volume trend
    vol_avg     = sum(vols[-20:])/20 if len(vols)>=20 else sum(vols)/len(vols) if vols else 1
    vol_cur     = vols[-1] if vols else 0
    vol_ratio   = vol_cur/vol_avg if vol_avg else 1
    vol_expand  = vol_analysis.get("expanding", False) if vol_analysis else vol_ratio > 1.5
    vol_dry     = vol_analysis.get("drying_up", False) if vol_analysis else vol_ratio < 0.3

    # OI context
    pcr    = oi_data.get("pcr", 0) if oi_data else 0
    mp     = oi_data.get("max_pain", 0) if oi_data else 0
    ce_wall= oi_data.get("ce_wall", 0) if oi_data else 0
    pe_wall= oi_data.get("pe_wall", 0) if oi_data else 0
    # OI wall tightness — how much room between pe_wall and ce_wall
    wall_width_pct = abs(ce_wall-pe_wall)/px*100 if (ce_wall and pe_wall and px) else 5
    mp_dist_pct    = abs((px-mp)/mp*100) if mp else 5
    pinned         = mp_dist_pct < 0.3

    # Expiry check — is today expiry?
    from datetime import datetime, timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist)
    is_thursday = now.weekday() == 3
    is_expiry_time = now.hour >= 13  # afternoon on expiry

    # ── REGIME CLASSIFICATION ──
    # Priority order matters — more specific wins

    # 1. EXPIRY PINNING — expiry day + price near max pain + tight walls
    if is_thursday and pinned and wall_width_pct < 2.0:
        return {
            "regime": "EXPIRY_PINNING",
            "label": "Expiry Pinning — Max Pain ₹{} dominant".format(int(mp)),
            "confidence": 85,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": above_vwap, "pinned_to_mp": pinned,
            "wall_width_pct": wall_width_pct, "mp_dist_pct": round(mp_dist_pct,2),
            # What strategies work in expiry pinning
            "valid_strategies": ["IRON_CONDOR", "SHORT_STRADDLE", "MAX_PAIN_FADE"],
            "no_trade_for": ["DIRECTIONAL_CE", "DIRECTIONAL_PE"],
            "note": "Sell premium near walls. Avoid buying options — theta destroys value."
        }

    # 2. TRENDING UP — price above VWAP + momentum + volume
    if (above_vwap and momentum > 0.2 and vol_expand and
        closes[-1] > closes[-3] > closes[-6]):
        return {
            "regime": "TRENDING_UP",
            "label": "Trending Up — buy dips to VWAP",
            "confidence": 80,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": True, "pinned_to_mp": pinned,
            "valid_strategies": ["BUY_CE_ON_PULLBACK", "ORB_BREAKOUT", "VWAP_RECLAIM"],
            "no_trade_for": ["BUY_PE", "SHORT_CE"],
            "note": "Only buy CE. Enter on VWAP pullbacks, not at highs."
        }

    # 3. TRENDING DOWN — price below VWAP + momentum + volume
    if (not above_vwap and momentum < -0.2 and vol_expand and
        closes[-1] < closes[-3] < closes[-6]):
        return {
            "regime": "TRENDING_DOWN",
            "label": "Trending Down — sell bounces to VWAP",
            "confidence": 80,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": False, "pinned_to_mp": pinned,
            "valid_strategies": ["BUY_PE_ON_BOUNCE", "ORB_BREAKDOWN", "VWAP_REJECT"],
            "no_trade_for": ["BUY_CE", "SHORT_PE"],
            "note": "Only buy PE. Enter on VWAP bounce-rejections, not at lows."
        }

    # 4. BREAKOUT DAY — narrow ORB + expansion with volume
    if (orb and orb.get("range_pct", 5) < 0.3 and
        (orb.get("breakout") or orb.get("breakdown")) and vol_expand):
        direction = "UP" if orb.get("breakout") else "DOWN"
        return {
            "regime": "BREAKOUT_DAY",
            "label": "Breakout Day — ORB {} with volume".format(direction),
            "confidence": 82,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": above_vwap, "pinned_to_mp": pinned,
            "valid_strategies": ["ORB_BREAKOUT" if direction=="UP" else "ORB_BREAKDOWN"],
            "no_trade_for": ["COUNTER_TREND"],
            "note": "Trade ORB direction only. Do not fade the breakout."
        }

    # 5. MEAN REVERSION — price extended from VWAP + volume climax
    if vwap_dist > 1.0 and (vol_analysis.get("climax") if vol_analysis else vol_ratio > 3):
        side = "ABOVE" if above_vwap else "BELOW"
        return {
            "regime": "MEAN_REVERT",
            "label": "Mean Reversion — price extended {} VWAP".format(side),
            "confidence": 72,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": above_vwap, "pinned_to_mp": pinned,
            "valid_strategies": ["VWAP_FADE", "LIQUIDITY_SWEEP_REVERSAL"],
            "no_trade_for": ["MOMENTUM_CONTINUATION"],
            "note": "Fade the extension. Target: VWAP retest. Tight SL."
        }

    # 6. RANGE BOUND — price between walls, low volatility, no clear momentum
    if wall_width_pct > 0 and day_range_pct < atr_pct * 1.5 and abs(momentum) < 0.15:
        return {
            "regime": "RANGE_BOUND",
            "label": "Range Bound — ₹{} to ₹{} zone".format(int(pe_wall), int(ce_wall)),
            "confidence": 65,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": above_vwap, "pinned_to_mp": pinned,
            "valid_strategies": ["BUY_AT_PE_WALL", "SELL_AT_CE_WALL", "IRON_CONDOR"],
            "no_trade_for": ["BREAKOUT_CHASE"],
            "note": "Trade only at extremes. Avoid mid-range entries."
        }

    # 7. CHOPPY — inside ORB, drying volume, no momentum
    if (orb and orb.get("inside") and vol_dry and abs(momentum) < 0.1):
        return {
            "regime": "CHOPPY",
            "label": "Choppy — no directional edge",
            "confidence": 60,
            "atr_pct": atr_pct, "day_range_pct": day_range_pct,
            "above_vwap": above_vwap, "pinned_to_mp": pinned,
            "valid_strategies": [],
            "no_trade_for": ["ALL_DIRECTIONAL"],
            "no_trade": True,
            "note": "Stay out. No clear edge. Wait for breakout or session change."
        }

    # Default — insufficient data for clear regime
    return {
        "regime": "DEVELOPING",
        "label": "Developing — wait for confirmation",
        "confidence": 55,
        "atr_pct": atr_pct, "day_range_pct": day_range_pct,
        "above_vwap": above_vwap, "pinned_to_mp": pinned,
        "valid_strategies": ["WAIT"],
        "no_trade_for": [],
        "note": "No clear regime yet. Wait for VWAP direction + volume confirmation."
    }

def generate_narrative(sym, px, regime, vwap, orb, vol, oi_data, writer, smc, tech):
    """
    Generate a trader-readable intelligence narrative.
    Interprets market behavior — NOT a prediction, an interpretation.
    """
    parts = []
    bias_signals = []
    bear_signals = []

    # 1. Market structure context
    r = regime.get("regime","")
    if r == "TRENDING_UP":
        parts.append(f"{sym} is in a confirmed uptrend — price holding above VWAP ₹{vwap} with expanding volume.")
    elif r == "TRENDING_DOWN":
        parts.append(f"{sym} is in a confirmed downtrend — price below VWAP ₹{vwap} with sellers in control.")
    elif r == "BREAKOUT_DAY":
        orb_d = "above" if orb and orb.get("breakout") else "below"
        parts.append(f"{sym} has broken {orb_d} the Opening Range (₹{orb.get('low',0) if orb else 0}–₹{orb.get('high',0) if orb else 0}) — breakout day setup active.")
    elif r == "EXPIRY_CHAOS":
        parts.append(f"{sym} is pinned near Max Pain ₹{oi_data.get('max_pain',0)} — expiry gravity dominant, expect range-bound action.")
    elif r == "MEAN_REVERT":
        parts.append(f"{sym} showing mean reversion setup — price extended from VWAP, likely to retrace.")
    elif r == "CHOPPY":
        parts.append(f"{sym} is in low-conviction chop — volume contracting, no clear directional edge. Avoid new positions.")

    # 2. OI/Options intelligence
    if writer and writer.get("signals"):
        parts.append(writer["signals"][0])

    # 3. VWAP context
    if vwap and px:
        dist = round((px - vwap)/vwap*100, 2)
        if abs(dist) < 0.1:
            parts.append(f"Price at VWAP ₹{vwap} — key decision zone. Direction of break matters.")
        elif dist > 0.3:
            bias_signals.append(f"Holding above VWAP ₹{vwap} (+{dist}%) — bulls in control")
        elif dist < -0.3:
            bear_signals.append(f"Below VWAP ₹{vwap} ({dist}%) — bears defending")

    # 4. Liquidity sweep context
    sweeps = smc.get("sweep",[]) if smc else []
    if sweeps:
        parts.append(sweeps[0].get("signal",""))

    # 5. Volume confirmation
    vol_lbl = vol.get("label","") if vol else ""
    if vol_lbl in ["🚀 CLIMAX","📈 EXPANDING"]:
        parts.append(f"Volume {vol_lbl.lower()} ({vol.get('ratio',0)}x avg) — move has conviction.")
    elif vol_lbl in ["💧 DRYING UP","📉 CONTRACTING"]:
        parts.append(f"Volume {vol_lbl.lower()} ({vol.get('ratio',0)}x avg) — caution, low participation.")

    # 6. Build bias conclusion
    writer_bias = writer.get("writer_bias","NEUTRAL") if writer else "NEUTRAL"
    struct = smc.get("structure","") if smc else ""
    crossover = tech.get("crossover","NONE") if tech else "NONE"

    if crossover == "GOLDEN_CROSS":
        bias_signals.append("Golden Cross — major bullish trend change")
    elif crossover == "DEATH_CROSS":
        bear_signals.append("Death Cross — major bearish trend change")

    if struct in ["HH_HL","BULLISH"]:
        bias_signals.append(f"Market structure {struct} — uptrend intact")
    elif struct in ["LH_LL","BEARISH"]:
        bear_signals.append(f"Market structure {struct} — downtrend intact")

    if writer_bias == "BULLISH":
        bias_signals.append("Option writers supporting upside")
    elif writer_bias == "BEARISH":
        bear_signals.append("Option writers capping upside")

    # Conclusion
    if len(bias_signals) > len(bear_signals):
        conclusion = f"Bias: BULLISH above ₹{vwap or px}. " + " | ".join(bias_signals[:2])
    elif len(bear_signals) > len(bias_signals):
        conclusion = f"Bias: BEARISH below ₹{vwap or px}. " + " | ".join(bear_signals[:2])
    else:
        conclusion = f"Bias: NEUTRAL — wait for VWAP reclaim or break with volume."

    parts.append(conclusion)
    return " ".join(p for p in parts if p)[:500]  # max 500 chars

def get_vix():
    """India VIX from Yahoo. Cached 1 min."""
    if CACHE.fresh("vix", TTL["vix"]):
        return CACHE.get_val("vix")
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX?interval=1d&range=1d",
            headers={"User-Agent":"Mozilla/5.0"}, timeout=5).json()
        px = r["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if px:
            val = round(px, 2)
            CACHE.set("vix", val)
            return val
    except: pass
    return CACHE.get_val("vix") or 17.5

def get_usdinr():
    """Live USD/INR from Yahoo. Cached 5 min."""
    if CACHE.fresh("usdinr", TTL["usdinr"]):
        return CACHE.get_val("usdinr")
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDINR=X?interval=1m&range=1d",
            headers={"User-Agent":"Mozilla/5.0"}, timeout=5).json()
        rate = r["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if rate and 80 < rate < 110:
            CACHE.set("usdinr", rate)
            return rate
    except: pass
    return CACHE.get_val("usdinr") or 84.0

def get_technicals(sym):
    """SMA/RSI/crossover from Yahoo candles. Cached 5 min. Always returns stale on failure."""
    cache_key = f"tech_{sym}"
    if CACHE.fresh(cache_key, TTL["technicals"]):
        return CACHE.get_val(cache_key)

    inst = INSTRUMENTS.get(sym, {})
    ticker = inst.get("yahoo","")
    if not ticker:
        return CACHE.get_val(cache_key) or {}

    d = fetch_yahoo_candles(ticker, "5m", "2d")
    if d and d.get("sma20"):
        tech = {
            "sma20":d["sma20"],"sma50":d["sma50"],
            "rsi":d.get("rsi"),
            "trend":d.get("trend","NEUTRAL"),
            "crossover":d.get("crossover","NONE"),
            "breakout":d.get("breakout",False),
            "breakdown":d.get("breakdown",False),
            "above_sma20": d["px"] > d["sma20"] if d.get("px") and d.get("sma20") else None,
            "high":d.get("high",0),"low":d.get("low",0),
            "open":d.get("open",0),"prev_close":d.get("prev_close",0),
            "volume":d.get("volume",0),"vol_ratio":d.get("vol_ratio",0),
        }
        CACHE.set(cache_key, tech)
        # Store candles separately — used by SMC/VWAP but NOT sent in /market
        if d.get("candles"):
            CACHE.set(f"candles5_{sym}", d["candles"])
        return tech
    return CACHE.get_val(cache_key) or {}

def get_all_prices(key, token):
    """
    Live prices from Zerodha ONLY — one bulk call, <3s target.
    Yahoo only for Crude Oil and Gold (MCX).
    """
    cache_key = "all_prices"
    t0 = time.time()

    # Return fresh cache immediately
    if CACHE.fresh(cache_key, TTL["prices"]):
        return CACHE.get_val(cache_key)

    result = {}
    usd_inr = get_usdinr()

    # ── Zerodha bulk quote ──
    kite_syms = [inst["kite"] for sym,inst in INSTRUMENTS.items() if inst.get("kite")]
    kite_data = fetch_kite_quotes(key, token, kite_syms) if key and token else {}
    t1 = time.time()
    print(f"[Prices] Zerodha quote: {t1-t0:.2f}s, {len(kite_data)} symbols")

    token_expired = kite_data.get("_token_expired", False)
    if token_expired:
        stale = CACHE.get_val(cache_key)
        if stale:
            print(f"[Prices] Token expired — stale cache ({len(stale)} syms)")
            return stale
        return {}

    use_kite = len(kite_data) >= 1
    if use_kite:
        for sym, inst in INSTRUMENTS.items():
            if inst.get("mcx"): continue
            kite_sym = inst.get("kite","")
            if not kite_sym: continue
            q = kite_data.get(kite_sym, {})
            if not q or not q.get("last_price"): continue
            px = q["last_price"]
            pc = q.get("ohlc",{}).get("close", px)
            result[sym] = {
                "px":px, "chg":round(px-pc,2),
                "pct":round((px-pc)/pc*100,2) if pc else 0,
                "high":q.get("ohlc",{}).get("high",0),
                "low": q.get("ohlc",{}).get("low",0),
                "open":q.get("ohlc",{}).get("open",0),
                "prev_close":pc, "volume":q.get("volume",0),
                "source":"zerodha"
            }
        print(f"[Prices] Zerodha mapped: {len(result)} symbols in {time.time()-t0:.2f}s")
    else:
        stale = CACHE.get_val(cache_key)
        if stale:
            print(f"[Prices] No Kite data — stale cache")
            return stale

    # MCX only — 2 calls
    for sym in ["CRUDEOIL","GOLD"]:
        inst = INSTRUMENTS[sym]
        d = fetch_yahoo_candles(inst["yahoo"])
        if not d or not d.get("px"): continue
        px = d["px"]
        if sym=="CRUDEOIL" and px<500: px=round(px*usd_inr,2); chg=round(d["chg"]*usd_inr,2)
        elif sym=="GOLD" and px<5000: f=usd_inr/31.1*10; px=round(d["px"]*f,2); chg=round(d["chg"]*f,2)
        else: chg=d["chg"]
        result[sym]={"px":px,"chg":chg,"pct":d.get("pct",0),
                     "high":d.get("high",0),"low":d.get("low",0),
                     "open":d.get("open",0),"source":"yahoo_mcx"}

    print(f"[Prices] TOTAL: {len(result)} symbols in {time.time()-t0:.2f}s")
    if result:
        CACHE.set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════
# LAYER 4A — OI ENGINE
# Smart ATM ±10 fetching via Zerodha
# ═══════════════════════════════════════════════════════════════

def calc_cpr(candles_daily):
    """Central Pivot Range from previous day's candle."""
    if not candles_daily or len(candles_daily) < 2: return None
    prev = candles_daily[-2]
    h,l,c = prev["h"], prev["l"], prev["c"]
    pivot = (h+l+c)/3
    tc    = (h+l)/2
    bc    = 2*pivot - tc
    width_pct = abs(tc-bc)/pivot*100
    today_px = candles_daily[-1]["c"]
    return {
        "pivot": round(pivot,2), "tc": round(tc,2), "bc": round(bc,2),
        "width": round(abs(tc-bc),2), "width_pct": round(width_pct,3),
        "type": "NARROW" if width_pct < 0.3 else "WIDE",
        "bias": "BULLISH" if today_px > pivot else "BEARISH",
        "px_vs_pivot": round(today_px - pivot, 2)
    }

def get_oi(sym, key, token, spot=0):
    """
    Fetch OI from Zerodha for ATM ±10 strikes only.
    Persists last result to disk — survives server restarts.
    """
    import os, json as _json

    cache_key = f"oi_{sym}"
    if CACHE.fresh(cache_key, TTL["oi"]):
        return CACHE.get_val(cache_key)

    # Disk persistence
    DISK_DIR = "/tmp/oi_cache"
    os.makedirs(DISK_DIR, exist_ok=True)
    disk_file = f"{DISK_DIR}/{sym}.json"

    def load_disk():
        try:
            with open(disk_file) as f:
                c = _json.load(f)
                age = int((time.time()-c["ts"])/60)
                d = dict(c["data"]); d["cached"]=True; d["cache_age_min"]=age
                return d
        except: return None

    def save_disk(data):
        try:
            with open(disk_file,"w") as f:
                _json.dump({"data":data,"ts":time.time()},f)
        except: pass

    if not key or not token:
        return load_disk()

    try:
        hdrs = _kite_headers(key, token)

        # Step 1 — spot price
        if not spot:
            idx_map = {"NIFTY":"NSE:NIFTY 50","BANKNIFTY":"NSE:NIFTY BANK",
                       "FINNIFTY":"NSE:NIFTY FIN SERVICE","SENSEX":"BSE:SENSEX"}
            r = requests.get("https://api.kite.trade/quote",
                params={"i":idx_map[sym]}, headers=hdrs, timeout=10)
            if r.status_code in [401,403]:
                return load_disk()
            for v in (r.json().get("data") or {}).values():
                spot = v.get("last_price",0); break

        if not spot: return load_disk()

        # Step 2 — instruments CSV (cached 4h)
        csv = fetch_kite_instruments_nfo(key, token)
        if not csv: return load_disk()

        lines = csv.strip().split("\n")
        step  = INSTRUMENTS.get(sym,{}).get("step",50)
        atm   = int(round(spot/step)*step)
        # Smart ATM ±10 strikes only
        target_strikes = set(range(atm - step*10, atm + step*11, step))

        # Find nearest expiry
        today_n = datetime.now(IST).replace(tzinfo=None)
        best_exp=None; best_days=999
        for line in lines[1:3000]:
            cols=line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym) or cols[9] not in ["CE","PE"]: continue
            try:
                exp=datetime.strptime(cols[5],"%Y-%m-%d")
                d2=(exp-today_n).days
                if 0<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue
        if not best_exp: return load_disk()

        # Collect instruments for ATM ±10
        instruments=[]
        for line in lines[1:]:
            cols=line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym): continue
            if cols[9] not in ["CE","PE"] or cols[5]!=best_exp: continue
            try:
                sk=float(cols[6])
                if sk in target_strikes:
                    instruments.append({"sym":f"NFO:{cols[2]}","strike":int(sk),"type":cols[9]})
            except: continue

        if not instruments: return load_disk()

        # Step 3 — ONE bulk quote call for all ATM ±10 strikes (use params= for encoding)
        params2 = [("i", i["sym"]) for i in instruments]
        r2 = requests.get("https://api.kite.trade/quote", params=params2, headers=hdrs, timeout=20)
        if r2.status_code in [401,403]: return load_disk()
        qdata = r2.json().get("data",{})

        # Step 4 — Aggregate OI
        ce_oi=0;pe_oi=0;ce_chg=0;pe_chg=0
        strikes_data={}
        for inst in instruments:
            q=qdata.get(inst["sym"],{})
            if not q: continue
            oi  = q.get("oi",0) or 0
            low = q.get("oi_day_low",0) or 0
            chg = oi-low if low else 0
            s   = inst["strike"]
            if s not in strikes_data:
                strikes_data[s]={"ce":0,"pe":0,"ce_chg":0,"pe_chg":0}
            if inst["type"]=="CE":
                ce_oi+=oi;ce_chg+=chg
                strikes_data[s]["ce"]=oi;strikes_data[s]["ce_chg"]=chg
            else:
                pe_oi+=oi;pe_chg+=chg
                strikes_data[s]["pe"]=oi;strikes_data[s]["pe_chg"]=chg

        if not ce_oi and not pe_oi:
            return load_disk()

        # Step 5 — Compute metrics
        pcr = round(pe_oi/ce_oi,2) if ce_oi else 0

        mp=0;mpv=float("inf")
        for s in strikes_data:
            pain=sum(max(0,x-s)*strikes_data[x]["ce"]+max(0,s-x)*strikes_data[x]["pe"]
                     for x in strikes_data)
            if pain<mpv: mpv=pain;mp=s

        ce_wall=max(strikes_data,key=lambda x:strikes_data[x]["ce"],default=0)
        pe_wall=max(strikes_data,key=lambda x:strikes_data[x]["pe"],default=0)

        buildup="UNKNOWN"
        if ce_chg>0 and pe_chg>0: buildup="LONG_BUILDUP" if pe_chg>ce_chg else "SHORT_BUILDUP"
        elif ce_chg<0 and pe_chg<0: buildup="SHORT_COVERING" if ce_chg<pe_chg else "LONG_UNWINDING"
        elif ce_chg>0: buildup="SHORT_BUILDUP"
        elif pe_chg>0: buildup="LONG_BUILDUP"

        pcr_interp=("EXTREME_BULL" if pcr>1.4 else "BULLISH" if pcr>1.1
                    else "NEUTRAL" if pcr>0.9 else "BEARISH" if pcr>0.7 else "EXTREME_BEAR")

        result = {
            "ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,
            "pcr":pcr,"max_pain":int(mp),"iv":0,
            "ce_wall":int(ce_wall),"pe_wall":int(pe_wall),
            "spot":round(spot,1),"buildup":buildup,"pcr_interp":pcr_interp,
            "mp_dist":round(spot-mp,0) if mp else 0,
            "source":"zerodha_kite","expiry":best_exp,
            "strikes_count":len(strikes_data),
            "atm":atm
        }
        CACHE.set(cache_key, result)
        save_disk(result)
        print(f"[OI ✅] {sym} PCR:{pcr} MP:{mp} CE:{ce_oi} PE:{pe_oi} Exp:{best_exp} Strikes:{len(strikes_data)}")
        return result

    except Exception as e:
        import traceback
        print(f"[OI ERROR] {sym}: {e}\n{traceback.format_exc()[-300:]}")
        disk = load_disk()
        if disk: return disk
        CACHE.set(cache_key, None)
        return None


# ═══════════════════════════════════════════════════════════════
# LAYER 4B — SMC ENGINE
# ═══════════════════════════════════════════════════════════════

def calc_smc(candles):
    """Calculate SMC signals: FVG, Order Blocks, Liquidity Sweeps, Structure."""
    if not candles or len(candles) < 10: return {}
    n = len(candles)
    result = {"fvg":[],"ob":[],"liquidity":[],"sweep":[],"structure":"UNKNOWN"}

    # FVGs
    for i in range(1, n-1):
        prev,curr,nxt = candles[i-1],candles[i],candles[i+1]
        if prev["h"] < nxt["l"]:
            sz = (nxt["l"]-prev["h"])/curr["c"]*100
            if sz>0.1:
                result["fvg"].append({"type":"BULLISH","top":round(nxt["l"],2),
                    "bot":round(prev["h"],2),"size_pct":round(sz,2),
                    "filled":curr["l"]<=prev["h"]})
        if prev["l"] > nxt["h"]:
            sz = (prev["l"]-nxt["h"])/curr["c"]*100
            if sz>0.1:
                result["fvg"].append({"type":"BEARISH","top":round(prev["l"],2),
                    "bot":round(nxt["h"],2),"size_pct":round(sz,2),
                    "filled":curr["h"]>=prev["l"]})
    unfilled=[f for f in result["fvg"] if not f["filled"]]
    result["fvg"] = unfilled[-5:] if unfilled else result["fvg"][-3:]

    # Order Blocks
    for i in range(2, n-2):
        c,cn,cn2 = candles[i],candles[i+1],(candles[i+2] if i+2<n else candles[i+1])
        body=abs(c["c"]-c["o"]); nm=abs(cn["c"]-cn["o"])
        if c["c"]<c["o"] and cn["c"]>cn["o"] and nm>body*1.5 and cn["h"]>c["h"]*1.002:
            result["ob"].append({"type":"BULLISH","high":round(max(c["o"],c["c"]),2),
                "low":round(min(c["o"],c["c"]),2),"strength":round(nm/body,1)if body else 0})
        if c["c"]>c["o"] and cn["c"]<cn["o"] and nm>body*1.5 and cn["l"]<c["l"]*0.998:
            result["ob"].append({"type":"BEARISH","high":round(max(c["o"],c["c"]),2),
                "low":round(min(c["o"],c["c"]),2),"strength":round(nm/body,1)if body else 0})
    bull_obs=[o for o in result["ob"] if o["type"]=="BULLISH"][-2:]
    bear_obs=[o for o in result["ob"] if o["type"]=="BEARISH"][-2:]
    result["ob"] = bull_obs + bear_obs

    # Liquidity sweeps
    curr_px = candles[-1]["c"]
    highs=[(i,candles[i]["h"]) for i in range(n)]
    lows= [(i,candles[i]["l"]) for i in range(n)]
    for i in range(len(highs)-3, max(0,len(highs)-20),-1):
        h1=highs[i][1]
        similar=[h for h in highs[i+1:i+10] if abs(h[1]-h1)/h1<0.0015]
        if len(similar)>=2:
            level=round(max([h1]+[h[1] for h in similar]),2)
            swept=any(candles[j]["h"]>level*1.001 and candles[j]["c"]<level for j in range(i+1,n))
            if swept: result["sweep"].append({"type":"BULL_SWEEP","level":level,
                "signal":f"Swept highs ₹{level} — reversal down possible"})
            break
    for i in range(len(lows)-3, max(0,len(lows)-20),-1):
        l1=lows[i][1]
        similar=[l for l in lows[i+1:i+10] if abs(l[1]-l1)/l1<0.0015]
        if len(similar)>=2:
            level=round(min([l1]+[l[1] for l in similar]),2)
            swept=any(candles[j]["l"]<level*0.999 and candles[j]["c"]>level for j in range(i+1,n))
            if swept: result["sweep"].append({"type":"BEAR_SWEEP","level":level,
                "signal":f"Swept lows ₹{level} — reversal up possible"})
            break

    # Market structure
    if n>=10:
        recent=candles[-10:]; mid=candles[-5:]
        rh=max(c["h"] for c in recent); rl=min(c["l"] for c in recent)
        mh=max(c["h"] for c in mid);   ml=min(c["l"] for c in mid)
        if mh>rh*0.998 and ml>rl*1.002: result["structure"]="HH_HL"
        elif mh<rh*0.998 and ml<rl*0.998: result["structure"]="LH_LL"
        elif mh>rh*0.998: result["structure"]="BULLISH"
        elif ml<rl*0.998: result["structure"]="BEARISH"
        else: result["structure"]="RANGING"

    return result

def get_smc_cpr(sym, oi_data=None):
    """Get full market intelligence for a symbol. Cached 5 min."""
    cache_key = f"smc_{sym}"
    if CACHE.fresh(cache_key, TTL["smc"]):
        return CACHE.get_val(cache_key)

    inst   = INSTRUMENTS.get(sym,{})
    ticker = inst.get("yahoo","")
    if not ticker: return {}

    result = {}

    # ── 5min candles (intraday intelligence) ──
    d5 = fetch_yahoo_candles(ticker,"5m","2d")
    # Use cached candles if available (avoids duplicate Yahoo fetch)
    candles5 = CACHE.get_val(f"candles5_{sym}") or (d5.get("candles",[]) if d5 else [])

    # SMC from 5min
    if candles5:
        result["smc"] = calc_smc(candles5)

    # VWAP
    vwap = calc_vwap(candles5)
    if vwap: result["vwap"] = vwap

    # ORB
    orb = calc_orb(candles5)
    if orb: result["orb"] = orb

    # Volume analysis
    vol = calc_volume_analysis(candles5)
    if vol: result["volume"] = vol

    # ── Daily candles (CPR + MTF) ──
    d1d = fetch_yahoo_candles(ticker,"1d","1mo")
    if d1d and d1d.get("candles"):
        cpr = calc_cpr(d1d["candles"])
        if cpr: result["cpr"] = cpr

    # ── MTF alignment ──
    d15 = fetch_yahoo_candles(ticker,"15m","5d")
    d1h = fetch_yahoo_candles(ticker,"1h","1mo")
    tf5  = d5.get("trend","")  if d5  else ""
    tf15 = d15.get("trend","") if d15 else ""
    tf1h = d1h.get("trend","") if d1h else ""
    trends=[tf5,tf15,tf1h]
    bulls=trends.count("BULLISH"); bears=trends.count("BEARISH")
    result["mtf"] = {
        "tf5":tf5,"tf15":tf15,"tf1h":tf1h,
        "alignment": ("STRONG_BULL" if bulls==3 else "BULL" if bulls==2
                      else "STRONG_BEAR" if bears==3 else "BEAR" if bears==2 else "MIXED")
    }

    # ── OI writer behavior ──
    writer = calc_oi_writer_behavior(oi_data)
    if writer: result["writer"] = writer

    # ── Market regime ──
    tech = get_technicals(sym)
    regime = detect_market_regime(candles5, oi_data, vwap, orb, vol)
    result["regime"] = regime

    # ── AI narrative ──
    px = d5.get("px",0) if d5 else 0
    narrative = generate_narrative(
        sym, px, regime, vwap, orb, vol, oi_data, writer,
        result.get("smc",{}), tech
    )
    result["narrative"] = narrative

    CACHE.set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════
# LAYER 5 — API ROUTES
# Clean, thin, no business logic here
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def home():
    return jsonify({"status":"PRO Trader Server","version":"2.0",
                    "time":now_ist().strftime("%d %b %Y %H:%M IST"),
                    "ok":True})

@app.route("/ping")
def ping():
    return jsonify({"ok":True,"time":ist_str()})

@app.route("/usdinr")
def usdinr():
    return jsonify({"ok":True,"rate":get_usdinr(),"time":ist_str()})

@app.route("/lot_sizes")
def lot_sizes():
    """Get current lot sizes from Zerodha NFO instruments CSV."""
    key   = request.args.get("key","")
    token = request.args.get("token","")

    # Fallback lot sizes (updated periodically by SEBI)
    fallback = {"NIFTY":75,"BANKNIFTY":30,"FINNIFTY":40,"SENSEX":20,
                "MIDCPNIFTY":120,"CRUDEOIL":100,"GOLD":100}

    if not key or not token:
        return jsonify({"ok":True,"data":fallback,"source":"fallback"})

    try:
        csv = fetch_kite_instruments_nfo(key, token)
        if not csv:
            return jsonify({"ok":True,"data":fallback,"source":"fallback"})

        lines = csv.strip().split("\n")
        result = {}
        seen = set()
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < 10: continue
            sym = cols[2]  # tradingsymbol
            opt_type = cols[9]  # CE/PE
            if opt_type not in ["CE","PE"]: continue
            # Extract underlying from symbol name
            # e.g. NIFTY25JUN24500CE -> NIFTY
            underlying = None
            for idx in ["NIFTY","BANKNIFTY","FINNIFTY","SENSEX","MIDCPNIFTY"]:
                if sym.startswith(idx):
                    underlying = idx
                    break
            if not underlying or underlying in seen: continue
            try:
                lot = int(cols[11]) if cols[11] else 0  # lot_size column
                if lot > 0:
                    result[underlying] = lot
                    seen.add(underlying)
            except: continue
            if len(seen) >= 8: break

        # Merge with fallback for any missing
        for k,v in fallback.items():
            if k not in result:
                result[k] = v

        # Cache it
        CACHE.set("lot_sizes", result)
        print(f"[LotSizes] {result}")
        return jsonify({"ok":True,"data":result,"source":"zerodha"})

    except Exception as e:
        return jsonify({"ok":True,"data":fallback,"source":"fallback","error":str(e)})


@app.route("/hero")
def hero():
    """
    Ultra-fast hero data — NIFTY + BANKNIFTY only.
    Target: <1 second. Called first, independently of /market.
    """
    key   = request.args.get("key","")
    token = request.args.get("token","")
    t0    = time.time()

    # Check cache first
    cached_prices = CACHE.get_val("all_prices")
    if cached_prices:
        result = {sym: cached_prices[sym] for sym in ["NIFTY","BANKNIFTY","FINNIFTY","SENSEX"] if sym in cached_prices}
        if result:
            return jsonify({"ok":True,"data":result,"source":"cache","time":now_ist().strftime("%H:%M:%S")})

    # Fetch only 4 index quotes — very fast
    if key and token:
        hdrs = _kite_headers(key, token)
        indices = {"NIFTY":"NSE:NIFTY 50","BANKNIFTY":"NSE:NIFTY BANK",
                   "FINNIFTY":"NSE:NIFTY FIN SERVICE","SENSEX":"BSE:SENSEX"}
        params_h = [("i", v) for v in indices.values()]
        try:
            r = requests.get("https://api.kite.trade/quote", params=params_h, headers=hdrs, timeout=8)
            if r.status_code == 200:
                data = r.json().get("data",{})
                result = {}
                for sym, kite_sym in indices.items():
                    q = data.get(kite_sym,{})
                    if not q: continue
                    px = q.get("last_price",0)
                    pc = q.get("ohlc",{}).get("close",px)
                    result[sym] = {"px":px,"chg":round(px-pc,2),"pct":round((px-pc)/pc*100,2) if pc else 0,"source":"zerodha"}
                print(f"[Hero] {len(result)} indices in {time.time()-t0:.2f}s")
                return jsonify({"ok":True,"data":result,"vix":get_vix(),"time":now_ist().strftime("%H:%M:%S")})
        except Exception as e:
            print(f"[Hero] Error: {e}")

    return jsonify({"ok":False,"error":"No data"}),503

@app.route("/debug_market")
def debug_market():
    """Shows exactly what /market sees — use to diagnose issues."""
    key   = request.args.get("key","")
    token = request.args.get("token","")
    
    result = {"time": ist_str(), "key_provided": bool(key), "token_provided": bool(token)}
    
    # Test Zerodha
    if key and token:
        kite_syms = ["NSE:NIFTY 50","NSE:HDFCBANK","NSE:RELIANCE","NSE:TCS"]
        kite_data = fetch_kite_quotes(key, token, kite_syms)
        result["zerodha_token_expired"] = kite_data.get("_token_expired", False)
        result["zerodha_symbols_returned"] = len(kite_data)
        result["zerodha_sample"] = {k: {"px": v.get("last_price"), "source": "kite"} 
                                     for k,v in list(kite_data.items())[:3] 
                                     if not k.startswith("_")}
    
    # Test cache
    cached = CACHE.get_val("all_prices")
    result["cache_has_prices"] = bool(cached)
    result["cache_symbol_count"] = len(cached) if cached else 0
    result["cache_age_s"] = int(CACHE.age("all_prices") or 0)
    if cached:
        nifty = cached.get("NIFTY", {})
        result["cached_nifty_px"] = nifty.get("px")
        result["cached_nifty_source"] = nifty.get("source")
    
    return jsonify(result)

@app.route("/prices")
def prices_only():
    """Ultra-fast prices-only endpoint. No technicals. ~2s response."""
    key   = request.args.get("key","")
    token = request.args.get("token","")
    prices = get_all_prices(key, token)
    if not prices:
        return jsonify({"ok":False,"error":"No price data"}),503
    vix = get_vix()
    return jsonify({"ok":True,"data":prices,"vix":vix,"time":now_ist().strftime("%H:%M:%S")})

@app.route("/market")
def market():
    """
    Fast market snapshot: prices (Zerodha, one bulk call) + cached technicals.
    Never blocks on Yahoo — uses cached technicals if available, skips if not.
    Technicals are populated by background warmup and /smc calls.
    """
    key   = request.args.get("key","")
    token = request.args.get("token","")

    prices = get_all_prices(key, token)
    if not prices:
        # Before giving up — return stale cache if available (after-hours, weekend)
        stale = CACHE.get_val("all_prices")
        if stale:
            vix = get_vix()
            return jsonify({"ok":True,"data":stale,"vix":vix,
                            "source":"cache_stale","stale":True,
                            "time":now_ist().strftime("%H:%M:%S"),
                            "cached_age_s": int(CACHE.age("all_prices") or 0)})
        return jsonify({"ok":False,"error":"No market data available"}),503

    # Merge technicals from cache ONLY — never fetch synchronously here
    result = {}
    for sym, p in prices.items():
        d = dict(p)
        inst = INSTRUMENTS.get(sym,{})
        if not inst.get("mcx"):
            # Only use cached technicals — don't block on Yahoo fetch
            cache_key = f"tech_{sym}"
            tech = CACHE.get_val(cache_key)
            if tech:
                d["sma20"] = tech.get("sma20")
                d["sma50"] = tech.get("sma50")
                d["technicals"] = {
                    "rsi14":      tech.get("rsi"),
                    "trend":      tech.get("trend","NEUTRAL"),
                    "crossover":  tech.get("crossover","NONE"),
                    "breakout":   tech.get("breakout",False),
                    "breakdown":  tech.get("breakdown",False),
                    "above_sma20":tech.get("above_sma20"),
                }
        result[sym] = d

    vix = get_vix()
    sources = set(d.get("source","") for d in result.values())

    # Trigger background technical refresh for uncached symbols
    uncached = [sym for sym in result if not CACHE.fresh(f"tech_{sym}", TTL["technicals"])
                and not INSTRUMENTS.get(sym,{}).get("mcx")]
    if uncached:
        def _bg_tech():
            for sym in uncached[:10]:  # limit to 10 per cycle
                try: get_technicals(sym)
                except: pass
        threading.Thread(target=_bg_tech, daemon=True).start()

    return jsonify({"ok":True,"data":result,"vix":vix,
                    "source":"zerodha" if "zerodha" in sources else "yahoo",
                    "time":now_ist().strftime("%H:%M:%S"),
                    "cached_age_s": int(CACHE.age("all_prices") or 0)})

@app.route("/zerodha_oi")
def zerodha_oi():
    """Live OI from Zerodha — ATM ±10 strikes only."""
    sym   = request.args.get("sym","NIFTY").upper()
    key   = request.args.get("key","")
    token = request.args.get("token","")

    if sym not in OI_INDICES:
        return jsonify({"ok":False,"error":f"{sym} options not supported"}),400

    # Try to get spot from prices cache first (avoids extra API call)
    # If cache is cold, get_oi() will fetch spot directly from Zerodha
    prices = CACHE.get_val("all_prices") or {}
    spot   = prices.get(sym,{}).get("px",0)

    data = get_oi(sym, key, token, spot)
    if data:
        age = data.get("cache_age_min",0)
        note = f" (cached {age}min ago)" if data.get("cached") else ""
        return jsonify({"ok":True,"sym":sym,"data":data,"time":ist_str(),"note":note})

    # Return detailed error to help diagnose
    return jsonify({
        "ok":False,
        "error":"OI unavailable",
        "debug":{
            "has_key": bool(key),
            "has_token": bool(token),
            "spot": spot,
            "sym": sym
        }
    }),503

@app.route("/smc/<sym>")
def smc_route(sym):
    """Full market intelligence: SMC + CPR + MTF + VWAP + ORB + Volume + Regime + Narrative."""
    sym = sym.upper()
    if sym not in INSTRUMENTS:
        return jsonify({"ok":False,"error":"Unknown symbol"}),400
    # Pass OI data if available for writer behavior analysis
    oi_data = CACHE.get_val(f"oi_{sym}")
    data = get_smc_cpr(sym, oi_data)
    return jsonify({"ok":True,"sym":sym,**data,"time":ist_str()})

@app.route("/price/<sym>")
def price_route(sym):
    """Raw candle data for a symbol."""
    sym  = sym.upper()
    inst = INSTRUMENTS.get(sym)
    if not inst: return jsonify({"error":"Unknown symbol"}),400
    interval = request.args.get("interval","5m")
    rng      = request.args.get("range","1d")
    data     = fetch_yahoo_candles(inst["yahoo"], interval, rng)
    return jsonify(data or {"error":"Failed to fetch"})

@app.route("/mtf/<sym>")
def mtf_route(sym):
    """Multi-timeframe analysis."""
    sym  = sym.upper()
    data = get_smc_cpr(sym)
    return jsonify({"ok":True,"sym":sym,"mtf":data.get("mtf",{}),"cpr":data.get("cpr",{}),"time":ist_str()})

@app.route("/full/<sym>")
def full_route(sym):
    """Full analysis: price + technicals + OI + SMC + CPR."""
    sym   = sym.upper()
    key   = request.args.get("key","")
    token = request.args.get("token","")
    if sym not in INSTRUMENTS:
        return jsonify({"ok":False,"error":"Unknown"}),400

    prices = get_all_prices(key, token)
    tech   = get_technicals(sym)
    smc    = get_smc_cpr(sym)
    oi     = get_oi(sym,key,token,prices.get(sym,{}).get("px",0)) if sym in OI_INDICES else None

    return jsonify({"ok":True,"sym":sym,
        "price": prices.get(sym,{}),
        "technicals": tech,
        "smc": smc.get("smc",{}),
        "cpr": smc.get("cpr",{}),
        "mtf": smc.get("mtf",{}),
        "oi":  oi,
        "vix": get_vix(),
        "time":ist_str()})

@app.route("/kite_token", methods=["POST"])
def kite_token():
    """
    Exchange Zerodha request_token for access_token server-side.
    Avoids CORS issues with direct browser calls to api.kite.trade.
    POST body: api_key, request_token, api_secret
    """
    import hashlib
    data      = request.get_json() or {}
    api_key   = data.get("api_key","")
    req_token = data.get("request_token","")
    api_secret= data.get("api_secret","")

    if not api_key or not req_token or not api_secret:
        return jsonify({"ok":False,"error":"Need api_key, request_token, api_secret"}),400

    # Generate checksum: SHA256(api_key + request_token + api_secret)
    checksum = hashlib.sha256(f"{api_key}{req_token}{api_secret}".encode()).hexdigest()

    try:
        r = requests.post("https://api.kite.trade/session/token",
            headers={"X-Kite-Version":"3","User-Agent":"Mozilla/5.0"},
            data={"api_key":api_key,"request_token":req_token,"checksum":checksum},
            timeout=15)
        d = r.json()
        if d.get("status")=="success" and d.get("data",{}).get("access_token"):
            tok = d["data"]["access_token"]
            print(f"[Kite] Token generated for {d['data'].get('user_name','')}")
            return jsonify({
                "ok":True,
                "access_token": tok,
                "user_name": d["data"].get("user_name",""),
                "email": d["data"].get("email",""),
                "time": ist_str()
            })
        else:
            return jsonify({"ok":False,"error":d.get("message","Token exchange failed"),
                           "response":d}),400
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),503


def test_kite():
    """Diagnose Zerodha connection — open in browser to check."""
    key   = request.args.get("key","")
    token = request.args.get("token","")
    if not key or not token:
        return jsonify({"error":"Pass ?key=YOUR_API_KEY&token=YOUR_ACCESS_TOKEN"})
    hdrs = _kite_headers(key,token)
    result = {"key_provided":bool(key),"token_provided":bool(token)}
    try:
        # Test 1: Profile
        r1 = requests.get("https://api.kite.trade/user/profile",headers=hdrs,timeout=10)
        result["profile_status"] = r1.status_code
        result["profile_ok"] = r1.status_code==200
        result["profile_response"] = r1.json() if r1.status_code!=200 else {"name": r1.json().get("data",{}).get("user_name","")}

        # Test 2: Spot price
        r2 = requests.get("https://api.kite.trade/quote?i=NSE%3ANIFTY+50",headers=hdrs,timeout=10)
        result["quote_status"] = r2.status_code
        result["quote_ok"] = r2.status_code==200
        result["quote_response"] = r2.json() if r2.status_code!=200 else {"price": list(r2.json().get("data",{}).values())[0].get("last_price",0) if r2.json().get("data") else 0}

        # Test 3: NFO instruments (just count lines)
        r3 = requests.get("https://api.kite.trade/instruments/NFO",headers=hdrs,timeout=20)
        result["instruments_status"] = r3.status_code
        result["instruments_ok"] = r3.status_code==200
        if r3.status_code==200:
            result["nfo_instruments_count"] = len(r3.text.strip().split("\n"))

        result["time"] = ist_str()
        result["verdict"] = "✅ ALL OK — OI should work" if (result.get("profile_ok") and result.get("quote_ok") and result.get("instruments_ok")) else "❌ Some checks failed — see above"
    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# BACKGROUND WARMUP
# Pre-warm cache on startup so first request is fast
# ═══════════════════════════════════════════════════════════════

def _warmup():
    """Pre-warm ALL technicals cache at startup using parallel threads."""
    time.sleep(3)
    print("[Warmup] Starting parallel technical fetch for all symbols...")
    all_syms = [s for s,i in INSTRUMENTS.items() if not i.get("mcx")]

    def fetch_one(sym):
        try: get_technicals(sym)
        except: pass

    with ThreadPoolExecutor(max_workers=10) as ex:
        ex.map(fetch_one, all_syms)
    print(f"[Warmup] Done — {len(all_syms)} symbols cached.")

# Start warmup for both direct run AND gunicorn
_warmup_thread = threading.Thread(target=_warmup, daemon=True)
_warmup_thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=False)
