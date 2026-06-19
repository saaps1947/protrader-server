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
import requests, time, threading, re
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

OI_INDICES = {"NIFTY","BANKNIFTY","FINNIFTY","SENSEX"}  # Index option chains
OI_MCX     = {"CRUDEOIL","GOLD"}                        # MCX commodity options

# 18 most liquid F&O stocks — meaningful OI, real CE/PE walls
# Verified by ADV (average daily volume) in NSE F&O segment
OI_STOCKS = {
    # Banking (highest F&O liquidity in Nifty50)
    "HDFCBANK","ICICIBANK","KOTAKBANK","AXISBANK","SBIN","INDUSINDBK",
    # NBFC
    "BAJFINANCE","BAJAJFINSV",
    # IT
    "TCS","INFY","WIPRO","HCLTECH",
    # Energy / Conglomerate
    "RELIANCE",
    # Auto
    "TATAMOTORS","MARUTI",
    # Infra
    "LT","ADANIPORTS",
    # Telecom
    "BHARTIARTL",
}

# Extended Nifty50 stocks — OI fetched every 15 min (less liquid options)
OI_STOCKS_EXT = {
    "APOLLOHOSP","ASIANPAINT","BEL","BPCL","BRITANNIA","CIPLA","COALINDIA",
    "DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HAL","HEROMOTOCO","HINDALCO",
    "HINDUNILVR","INDIGO","ITC","JSWSTEEL","LTIM","M&M","NESTLEIND","NTPC",
    "ONGC","POWERGRID","SHRIRAMFIN","SUNPHARMA","TATACONSUM","TATASTEEL",
    "TECHM","TITAN","TRENT","ULTRACEMCO",
}

OI_ALL = OI_INDICES | OI_STOCKS | OI_STOCKS_EXT | OI_MCX  # Full coverage


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

        # ── 15-DAY TREND + PREVIOUS DAY HIGH/LOW ──
        trend15 = "UNKNOWN"; trend_strength = 0; hh_hl = False; lh_ll = False
        trend_up_count = 0; trend_sessions = 0
        prev_day_high = 0; prev_day_low = 0  # PDH / PDL
        try:
            url_d = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1mo&includePrePost=false"
            r_d = requests.get(url_d, headers={"User-Agent":"Mozilla/5.0"}, timeout=8).json()
            res_d = r_d["chart"]["result"][0]
            q_d = res_d["indicators"]["quote"][0]
            dc = [q_d["close"][i] for i in range(len(res_d.get("timestamp",[]))) if q_d["close"][i]]
            dh = [q_d["high"][i]  for i in range(len(res_d.get("timestamp",[]))) if q_d["high"][i]]
            dl = [q_d["low"][i]   for i in range(len(res_d.get("timestamp",[]))) if q_d["low"][i]]
            if len(dc) >= 2:
                # Previous day high/low (index -2 = yesterday's completed session)
                prev_day_high = round(dh[-2], 2) if len(dh)>=2 else 0
                prev_day_low  = round(dl[-2], 2) if len(dl)>=2 else 0
            if len(dc) >= 10:
                # Exclude today's partial session from ALL trend calculations.
                # dc[-1] is the live intraday price during market hours, not a
                # completed close. Using it would poison both the up/down count
                # AND the SMA5-vs-SMA10 direction AND the HH/HL structure check.
                # FIX: previously d_s5/d_s10/hh_hl used raw dc (with today) while
                # the up-day count used completed[] — mixed windows in one decision.
                completed   = dc[:-1] if len(dc) > 1 else dc
                completed_h = dh[:-1] if len(dh) > 1 else dh
                completed_l = dl[:-1] if len(dl) > 1 else dl

                # SMA direction from completed closes only
                d_s5  = sum(completed[-5:])/min(5,len(completed))  if completed else 0
                d_s10 = sum(completed[-10:])/min(10,len(completed)) if completed else 0

                days = completed[-15:] if len(completed)>=15 else completed
                n_comp = len(days)-1  # number of completed session comparisons
                up_days = sum(1 for i in range(1,len(days)) if days[i]>days[i-1])
                dn_days = n_comp - up_days
                # % of up-days (not net balance) — clearer for display
                # 8 up of 14 = 57%  |  7 up of 14 = 50%  |  14 up of 14 = 100%
                trend_strength = round(up_days/n_comp*100) if n_comp>0 else 0
                trend_up_count = up_days
                trend_dn_count = dn_days
                trend_sessions = n_comp
                # HH/HL structure from completed highs/lows (today excluded)
                if len(completed_h)>=3 and len(completed_l)>=3:
                    hh_hl = completed_h[-1]>completed_h[-3] and completed_l[-1]>completed_l[-3]
                    lh_ll = completed_h[-1]<completed_h[-3] and completed_l[-1]<completed_l[-3]
                if d_s5 > d_s10 and hh_hl: trend15="STRONG_BULL"
                elif d_s5 > d_s10:          trend15="BULL"
                elif d_s5 < d_s10 and lh_ll: trend15="STRONG_BEAR"
                elif d_s5 < d_s10:           trend15="BEAR"
                else:                         trend15="NEUTRAL"
        except Exception as te:
            pass

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
            "trend15":trend15, "trend_strength":trend_strength,
            "trend_up_count":trend_up_count, "trend_sessions":trend_sessions,
            "hh_hl":hh_hl, "lh_ll":lh_ll,
            "prev_day_high":prev_day_high,   # PDH — previous session high
            "prev_day_low":prev_day_low,     # PDL — previous session low
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

def fetch_kite_instruments_bfo(key, token):
    """Fetch BFO instruments CSV from Zerodha (BSE derivatives — SENSEX options). Cached 4 hours."""
    cache_key = "instruments_bfo"
    if CACHE.fresh(cache_key, TTL["instruments"]):
        return CACHE.get_val(cache_key)
    try:
        r = requests.get("https://api.kite.trade/instruments/BFO",
            headers=_kite_headers(key,token), timeout=30)
        if r.status_code in [401,403]:
            return None
        if r.status_code == 200:
            CACHE.set(cache_key, r.text)
            print("[Kite] BFO instruments CSV cached (SENSEX options)")
            return r.text
    except Exception as e:
        print(f"[Kite] BFO Instruments error: {e}")
    return CACHE.get_val(cache_key)

def fetch_kite_instruments_mcx(key, token):
    """Fetch MCX instruments CSV from Zerodha (Crude Oil, Gold options). Cached 4 hours."""
    cache_key = "instruments_mcx"
    if CACHE.fresh(cache_key, TTL["instruments"]):
        return CACHE.get_val(cache_key)
    try:
        r = requests.get("https://api.kite.trade/instruments/MCX",
            headers=_kite_headers(key,token), timeout=30)
        if r.status_code in [401,403]:
            return None
        if r.status_code == 200:
            CACHE.set(cache_key, r.text)
            print("[Kite] MCX instruments CSV cached (Crude Oil, Gold options)")
            return r.text
    except Exception as e:
        print(f"[Kite] MCX Instruments error: {e}")
    return CACHE.get_val(cache_key)


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
    Tracks failed breakouts: if price breaks out then returns inside range,
    the breakout is invalidated. A second breakout requires volume confirmation.
    """
    if not candles: return None
    today = datetime.now(IST).date()
    orb_start = int(datetime(today.year, today.month, today.day, 3, 45, tzinfo=timezone.utc).timestamp())
    orb_end   = int(datetime(today.year, today.month, today.day, 4,  0, tzinfo=timezone.utc).timestamp())
    orb_c = [c for c in candles if orb_start <= c.get("t",0) < orb_end]
    if not orb_c: return None

    orb_h = max(c["h"] for c in orb_c)
    orb_l = min(c["l"] for c in orb_c)
    range_pct = (orb_h - orb_l) / orb_l * 100 if orb_l else 0

    # Get post-ORB candles (after 9:30 IST = 4:00 UTC)
    post_orb = [c for c in candles if c.get("t",0) >= orb_end]
    cur = candles[-1]["c"]

    # Detect if breakout occurred then failed (price returned inside range)
    breakout_occurred = False
    breakdown_occurred = False
    breakout_failed = False   # was above, came back in
    breakdown_failed = False  # was below, came back in

    for c in post_orb:
        if c["c"] > orb_h * 1.001: breakout_occurred = True
        if c["c"] < orb_l * 0.999: breakdown_occurred = True
        # After breakout, if candle CLOSES back inside → failed
        if breakout_occurred and c["c"] < orb_h * 1.001 and c["c"] > orb_l * 0.999:
            breakout_failed = True
        if breakdown_occurred and c["c"] > orb_l * 0.999 and c["c"] < orb_h * 1.001:
            breakdown_failed = True
        # Reset: if price breaks out again after re-entering, clear the failed flag
        if breakout_failed and c["c"] > orb_h * 1.001: breakout_failed = False
        if breakdown_failed and c["c"] < orb_l * 0.999: breakdown_failed = False

    # Current state
    above_orb = cur > orb_h * 1.001
    below_orb = cur < orb_l * 0.999
    inside_orb = not above_orb and not below_orb

    # Volume check on last candle (for re-entry confirmation)
    vols = [c.get("v",0) for c in candles[-20:]] if len(candles)>=5 else []
    avg_vol = sum(vols[:-1])/len(vols[:-1]) if len(vols)>1 else 0
    cur_vol = vols[-1] if vols else 0
    vol_confirmed = cur_vol >= avg_vol * 1.5 if avg_vol else False

    # Valid breakout: above ORB AND not failed (or failed but back above with volume)
    valid_breakout  = above_orb and not breakout_failed
    valid_breakdown = below_orb and not breakdown_failed

    return {
        "high": round(orb_h, 2),
        "low":  round(orb_l, 2),
        "range_pct": round(range_pct, 2),
        "breakout":    valid_breakout,      # clean break — price outside + never returned
        "breakdown":   valid_breakdown,
        "inside":      inside_orb,
        "breakout_failed":  breakout_failed,  # was above, came back = sideways trap
        "breakdown_failed": breakdown_failed,
        "vol_confirmed": vol_confirmed,       # volume supports the move
        "retest_zone": breakout_failed and above_orb,  # failed then back above = re-breakout
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
        # MCX commodities: Yahoo returns USD prices (CL=F in $/barrel, GC=F in $/troy oz)
        # Convert to INR using live USD/INR rate
        # CRUDEOIL MCX lot = 100 barrels, price per barrel in INR
        # GOLD MCX = per 10g in INR; GC=F is per troy oz (31.1g) in USD
        mcx_scale = 1.0
        if inst.get("mcx"):
            usdinr = get_usdinr()
            if sym == "CRUDEOIL":
                mcx_scale = usdinr          # $/barrel → ₹/barrel
            elif sym == "GOLD":
                mcx_scale = usdinr * 10 / 31.1035  # $/troy_oz → ₹/10g
            if mcx_scale > 1:
                for field in ["sma20","sma50","high","low","open","prev_close",
                              "prev_day_high","prev_day_low"]:
                    if d.get(field): d[field] = round(d[field] * mcx_scale, 2)

        tech = {
            "sma20":        d["sma20"],
            "sma50":        d["sma50"],
            "rsi":          d.get("rsi"),
            "trend":        d.get("trend","NEUTRAL"),
            "crossover":    d.get("crossover","NONE"),
            "breakout":     d.get("breakout",False),
            "breakdown":    d.get("breakdown",False),
            "above_sma20":  d["px"] > d["sma20"] if d.get("px") and d.get("sma20") else None,
            "high":         d.get("high",0),
            "low":          d.get("low",0),
            "open":         d.get("open",0),
            "prev_close":   d.get("prev_close",0),
            "volume":       d.get("volume",0),
            "vol_ratio":    d.get("vol_ratio",0),
            "prev_day_high":   d.get("prev_day_high",0),
            "prev_day_low":    d.get("prev_day_low",0),
            "trend15":         d.get("trend15","UNKNOWN"),
            "trend_strength":  d.get("trend_strength",0),
            "trend_up_count":  d.get("trend_up_count",0),
            "trend_sessions":  d.get("trend_sessions",0),
            "hh_hl":           d.get("hh_hl",False),
            "lh_ll":           d.get("lh_ll",False),
        }
        CACHE.set(cache_key, tech)
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

    # MCX commodities — fetch from Zerodha using front-month futures
    for sym in ["CRUDEOIL","GOLD"]:
        try:
            # Fetch MCX instruments to find front-month contract
            mcx_cache_key = f"mcx_sym_{sym}"
            mcx_kite_sym = CACHE.get_val(mcx_cache_key)
            if not mcx_kite_sym:
                r_inst = requests.get("https://api.kite.trade/instruments/MCX",
                    headers=_kite_headers(key, token), timeout=10)
                if r_inst.status_code == 200:
                    from io import StringIO
                    import csv as _csv
                    reader = _csv.DictReader(StringIO(r_inst.text))
                    contracts = []
                    for row in reader:
                        if row.get("name","").upper() == sym and row.get("instrument_type","") == "FUT":
                            contracts.append((row.get("expiry",""), f"MCX:{row.get('tradingsymbol','')}"))
                    if contracts:
                        contracts.sort()  # earliest expiry first
                        mcx_kite_sym = contracts[0][1]
                        CACHE.set(mcx_cache_key, mcx_kite_sym)
                        print(f"[MCX] {sym} front-month: {mcx_kite_sym}")

            if mcx_kite_sym:
                r_q = requests.get("https://api.kite.trade/quote",
                    params={"i": mcx_kite_sym},
                    headers=_kite_headers(key, token), timeout=8)
                if r_q.status_code == 200:
                    qdata = r_q.json().get("data", {})
                    for v in qdata.values():
                        px  = v.get("last_price", 0)
                        pc  = v.get("ohlc", {}).get("close", px)
                        chg = round(px - pc, 2)
                        pct = round((px - pc) / pc * 100, 2) if pc else 0
                        result[sym] = {
                            "px": px, "chg": chg, "pct": pct,
                            "h": v.get("ohlc", {}).get("high", px),
                            "l": v.get("ohlc", {}).get("low", px),
                            "source": "zerodha_mcx", "contract": mcx_kite_sym
                        }
                        break
        except Exception as ex:
            print(f"[MCX] {sym} Zerodha fetch failed: {ex}")
        # Fallback to Yahoo if Zerodha MCX fetch failed
        if sym not in result:
            inst = INSTRUMENTS[sym]
            d = fetch_yahoo_candles(inst["yahoo"])
            if d and d.get("px"):
                px = d["px"]
                if sym=="CRUDEOIL" and px<500: px=round(px*usd_inr,2); chg=round(d["chg"]*usd_inr,2)
                elif sym=="GOLD" and px<5000: f=usd_inr/31.1*10; px=round(d["px"]*f,2); chg=round(d["chg"]*f,2)
                else: chg=d["chg"]
                result[sym]={"px":px,"chg":chg,"pct":d.get("pct",0),
                             "h":d.get("high",0),"l":d.get("low",0),
                             "source":"yahoo_mcx_fallback"}

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
                age_min = int((time.time()-c["ts"])/60)
                # Don't serve disk cache older than 30 min during market hours
                ist_now = datetime.now(IST)
                market_open = ist_now.hour >= 9 and ist_now.hour < 16
                if market_open and age_min > 30:
                    print(f"[OI] Disk cache for {sym} is {age_min}min old — too stale during market hours")
                    return None
                d = dict(c["data"]); d["cached"]=True; d["cache_age_min"]=age_min
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
            if sym in OI_INDICES:
                idx_map = {"NIFTY":"NSE:NIFTY 50","BANKNIFTY":"NSE:NIFTY BANK",
                           "FINNIFTY":"NSE:NIFTY FIN SERVICE","SENSEX":"BSE:SENSEX"}
                kite_sym = idx_map.get(sym)
            elif sym in OI_MCX:
                # MCX commodities — try cached front-month kite symbol first
                kite_sym = CACHE.get_val(f"mcx_sym_{sym}")
                if not kite_sym:
                    # Fallback: find front-month MCX futures directly from instruments CSV
                    mcx_csv = fetch_kite_instruments_mcx(key, token)
                    if mcx_csv:
                        today_n = datetime.now(IST).replace(tzinfo=None)
                        best_fut_exp = None; best_fut_days = 999; best_fut_sym = None
                        for line in mcx_csv.strip().split("\n")[1:]:
                            cols = line.split(",")
                            if len(cols) < 10: continue
                            if not cols[2].startswith(sym): continue
                            if cols[9] != "FUT": continue
                            try:
                                exp = datetime.strptime(cols[5], "%Y-%m-%d")
                                d2 = (exp - today_n).days
                                if 0 <= d2 < best_fut_days:
                                    best_fut_days = d2
                                    best_fut_sym = f"MCX:{cols[2]}"
                            except: continue
                        if best_fut_sym:
                            kite_sym = best_fut_sym
                            CACHE.set(f"mcx_sym_{sym}", kite_sym)
                            print(f"[OI] MCX spot resolved: {sym} → {kite_sym}")
            else:
                kite_sym = INSTRUMENTS.get(sym,{}).get("kite","")

            if kite_sym:
                r = requests.get("https://api.kite.trade/quote",
                    params={"i": kite_sym}, headers=hdrs, timeout=10)
                if r.status_code in [401,403]:
                    print(f"[OI] Auth failed for {sym} — token expired?")
                    return load_disk()
                if r.status_code == 200:
                    for v in (r.json().get("data") or {}).values():
                        spot = v.get("last_price",0); break
                else:
                    print(f"[OI] Spot fetch HTTP {r.status_code} for {sym}")
            else:
                print(f"[OI] No kite_sym found for {sym}")

        if not spot: return load_disk()

        # Step 2 — instruments CSV (cached 4h)
        # Route to correct exchange instruments file
        if sym == "SENSEX":
            csv = fetch_kite_instruments_bfo(key, token)   # BSE/BFO
        elif sym in OI_MCX:
            csv = fetch_kite_instruments_mcx(key, token)   # MCX commodities
        else:
            csv = fetch_kite_instruments_nfo(key, token)   # NSE/NFO (default)
        if not csv: return load_disk()

        lines = csv.strip().split("\n")
        step  = INSTRUMENTS.get(sym,{}).get("step",50)
        atm   = int(round(spot/step)*step)
        # Smart ATM ±10 strikes only
        target_strikes = set(range(atm - step*10, atm + step*11, step))

        # Find nearest expiry — scan ALL lines with early symbol filter
        # NFO/BFO CSVs are 150k+ rows. Filter by tradingsymbol prefix.
        # SENSEX options in BFO are prefixed "SENSEX"
        # For stocks: "HDFCBANK24JUN..." etc.
        sym_prefix = sym  # default matches tradingsymbol start
        today_n = datetime.now(IST).replace(tzinfo=None)
        best_exp=None; best_days=999
        for line in lines[1:]:
            cols=line.split(",")
            if len(cols)<10: continue
            ts = cols[2] if len(cols)>2 else ""
            if not ts.startswith(sym_prefix): continue
            if cols[9] not in ["CE","PE"]: continue
            try:
                exp=datetime.strptime(cols[5],"%Y-%m-%d")
                d2=(exp-today_n).days
                if 0<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue

        if not best_exp: return load_disk()
        best_days = best_days  # days to expiry — used for IV calculation

        # Collect instruments for ATM ±10
        # Each exchange uses its own prefix for Kite quote API
        if sym == "SENSEX":
            exchange_prefix = "BFO"
        elif sym in OI_MCX:
            exchange_prefix = "MCX"
        else:
            exchange_prefix = "NFO"
        instruments=[]
        for line in lines[1:]:
            cols=line.split(",")
            if len(cols)<10: continue
            name = cols[13].strip() if len(cols)>13 else ""
            sym_match = (name==sym) or cols[2].startswith(sym)
            if not sym_match: continue
            if cols[9] not in ["CE","PE"] or cols[5]!=best_exp: continue
            try:
                sk=float(cols[6])
                if sk in target_strikes:
                    instruments.append({"sym":f"{exchange_prefix}:{cols[2]}","strike":int(sk),"type":cols[9]})
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

        # Step 5b — Estimate IV from ATM option premiums
        # Use ATM straddle price as proxy: IV ≈ (CE_premium + PE_premium) / spot * sqrt(365/DTE) * 100
        iv_est = 0
        try:
            atm_key = f"NFO:{sym}{best_exp.replace('-','')[2:]}{'%05d' % atm}CE" if sym != "SENSEX" else None
            # Simpler: use average of ATM CE + PE last price vs spot
            atm_ce_q = None; atm_pe_q = None
            for inst in instruments:
                if inst["strike"]==atm:
                    q2 = qdata.get(inst["sym"],{})
                    if inst["type"]=="CE": atm_ce_q=q2
                    elif inst["type"]=="PE": atm_pe_q=q2
            if atm_ce_q and atm_pe_q and spot:
                ce_ltp = atm_ce_q.get("last_price",0) or 0
                pe_ltp = atm_pe_q.get("last_price",0) or 0
                straddle = ce_ltp + pe_ltp
                dte = max(best_days, 1)
                # Simplified IV proxy: straddle/(spot * sqrt(dte/365)) * 100
                import math
                iv_est = round(straddle / (spot * math.sqrt(dte/365)) * 100, 1)
                iv_est = min(iv_est, 80)  # cap at realistic max
        except Exception as iv_err:
            print(f"[OI] IV calc error: {iv_err}")

        result = {
            "ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,
            "pcr":pcr,"max_pain":int(mp),"iv":iv_est,
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
    """
    Enhanced SMC engine — aligned with LuxAlgo Smart Money Concepts.

    Improvements over v1:
    1. BOS vs CHoCH distinction (continuation vs reversal)
    2. Dual structure layers: Internal (5 bars) + Swing (20 bars)
    3. EQH/EQL — equal highs/lows as liquidity pools
    4. OB mitigation — stale OBs removed when price crosses through
    5. Strong vs Weak High/Low
    6. Premium/Discount/Equilibrium zone scoring
    7. Volatility-filtered OB selection (spike bars handled correctly)
    """
    if not candles or len(candles) < 15:
        return {}

    n     = len(candles)
    highs = [c["h"] for c in candles]
    lows  = [c["l"] for c in candles]
    closes= [c["c"] for c in candles]
    opens = [c["o"] for c in candles]
    curr  = candles[-1]["c"]

    # ATR (20-bar) — used for EQH/EQL threshold and OB filter
    def atr(period=20):
        trs=[]
        for i in range(1, min(period+1, n)):
            trs.append(max(highs[n-i]-lows[n-i],
                           abs(highs[n-i]-closes[n-i-1]),
                           abs(lows[n-i]-closes[n-i-1])))
        return sum(trs)/len(trs) if trs else 1

    atr_val = atr()

    # ── Step 1: Find swing pivots at two scales ────────────────────────────
    # Internal pivots: 5-bar lookback (short-term structure)
    # Swing pivots: 20-bar lookback (major structure)
    def find_pivots(size, start=0):
        """Find pivot highs and lows using size-bar lookback."""
        pvt_highs = []  # (index, price)
        pvt_lows  = []
        for i in range(size, n - size):
            if all(highs[i] >= highs[i-j] for j in range(1,size+1)) and \
               all(highs[i] >= highs[i+j] for j in range(1,size+1)):
                pvt_highs.append((i, highs[i]))
            if all(lows[i] <= lows[i-j] for j in range(1,size+1)) and \
               all(lows[i] <= lows[i+j] for j in range(1,size+1)):
                pvt_lows.append((i, lows[i]))
        return pvt_highs, pvt_lows

    int_highs, int_lows = find_pivots(5)   # Internal structure
    swg_highs, swg_lows = find_pivots(20)  # Swing structure

    # ── Step 2: BOS vs CHoCH detection ────────────────────────────────────
    # CHoCH = Change of Character → price breaks structure AGAINST current trend = reversal signal
    # BOS   = Break of Structure  → price breaks structure WITH current trend = continuation
    def detect_structure(pvt_highs, pvt_lows, label="swing"):
        """
        Detect structure breaks. Returns list of structure events.
        Each event: {type: BOS|CHoCH, bias: BULLISH|BEARISH, level, index, label}
        """
        events = []
        if not pvt_highs or not pvt_lows:
            return events, "UNKNOWN"

        # Determine trend using last two pivots of each type
        last_sh = pvt_highs[-1][1] if pvt_highs else 0
        prev_sh = pvt_highs[-2][1] if len(pvt_highs)>=2 else 0
        last_sl = pvt_lows[-1][1]  if pvt_lows else 0
        prev_sl = pvt_lows[-2][1]  if len(pvt_lows)>=2 else 0

        # HH+HL = BULLISH trend, LH+LL = BEARISH trend
        trend_bias = "BULLISH" if (last_sh > prev_sh and last_sl > prev_sl) else \
                     "BEARISH" if (last_sh < prev_sh and last_sl < prev_sl) else "NEUTRAL"

        # Check last few candles for structure break
        for pvt_i, pvt_price in pvt_highs[-3:]:
            if curr > pvt_price:
                struct_type = "BOS" if trend_bias == "BULLISH" else "CHoCH"
                events.append({
                    "type": struct_type, "bias": "BULLISH",
                    "level": round(pvt_price, 2), "index": pvt_i,
                    "layer": label,
                    "significance": 3 if struct_type == "CHoCH" else 1  # CHoCH is reversal = higher significance
                })

        for pvt_i, pvt_price in pvt_lows[-3:]:
            if curr < pvt_price:
                struct_type = "BOS" if trend_bias == "BEARISH" else "CHoCH"
                events.append({
                    "type": struct_type, "bias": "BEARISH",
                    "level": round(pvt_price, 2), "index": pvt_i,
                    "layer": label,
                    "significance": 3 if struct_type == "CHoCH" else 1
                })

        return events, trend_bias

    int_events, int_trend = detect_structure(int_highs, int_lows, "internal")
    swg_events, swg_trend = detect_structure(swg_highs, swg_lows, "swing")

    all_structure_events = int_events + swg_events
    # Use swing trend as primary market structure
    structure = swg_trend if swg_trend != "NEUTRAL" else int_trend

    # ── Step 3: Strong vs Weak High/Low ───────────────────────────────────
    # Strong High = where the last bearish CHoCH/BOS formed (key resistance)
    # Weak High   = a pivot high in existing uptrend (just a pause)
    strong_high = None; weak_high = None
    strong_low  = None; weak_low  = None

    if swg_highs:
        last_sh_idx, last_sh_price = swg_highs[-1]
        # Strong if trend is bearish (price struggled here), Weak if bullish continuation
        if swg_trend == "BEARISH":
            strong_high = round(last_sh_price, 2)
        else:
            weak_high   = round(last_sh_price, 2)

    if swg_lows:
        last_sl_idx, last_sl_price = swg_lows[-1]
        if swg_trend == "BULLISH":
            strong_low = round(last_sl_price, 2)
        else:
            weak_low   = round(last_sl_price, 2)

    # ── Step 4: EQH/EQL — Equal Highs/Lows (liquidity pools) ─────────────
    # Two consecutive pivots within ATR distance = liquidity pool
    # Smart money will sweep these before reversing
    eqh_levels = []  # Equal highs = sell-side liquidity above
    eql_levels = []  # Equal lows  = buy-side liquidity below
    eq_threshold = atr_val * 0.5  # within 0.5 ATR = equal

    for i in range(1, len(swg_highs)):
        p1 = swg_highs[i-1][1]
        p2 = swg_highs[i][1]
        if abs(p1 - p2) < eq_threshold:
            level = round(max(p1, p2), 2)
            # Check if already swept
            swept = any(highs[j] > level * 1.001 and closes[j] < level
                       for j in range(swg_highs[i][0], n))
            eqh_levels.append({
                "level": level, "swept": swept,
                "signal": f"EQH liquidity pool ₹{level}" + (" — already swept" if swept else " — target for sweep")
            })

    for i in range(1, len(swg_lows)):
        p1 = swg_lows[i-1][1]
        p2 = swg_lows[i][1]
        if abs(p1 - p2) < eq_threshold:
            level = round(min(p1, p2), 2)
            swept = any(lows[j] < level * 0.999 and closes[j] > level
                       for j in range(swg_lows[i][0], n))
            eql_levels.append({
                "level": level, "swept": swept,
                "signal": f"EQL liquidity pool ₹{level}" + (" — already swept" if swept else " — target for sweep")
            })

    # ── Step 5: Fair Value Gaps (with fill tracking) ──────────────────────
    fvg_list = []
    for i in range(1, n-1):
        prev, c, nxt = candles[i-1], candles[i], candles[i+1]
        # Bullish FVG: gap between prev high and next low
        if prev["h"] < nxt["l"]:
            sz = (nxt["l"] - prev["h"]) / c["c"] * 100
            if sz > 0.05:
                # Check if filled
                filled = any(candles[j]["l"] <= prev["h"] for j in range(i+1, n))
                if not filled:
                    fvg_list.append({
                        "type": "BULLISH", "top": round(nxt["l"], 2),
                        "bot": round(prev["h"], 2),
                        "mid": round((nxt["l"] + prev["h"]) / 2, 2),
                        "size_pct": round(sz, 2), "filled": False,
                        "idx": i
                    })
        # Bearish FVG: gap between next high and prev low
        if prev["l"] > nxt["h"]:
            sz = (prev["l"] - nxt["h"]) / c["c"] * 100
            if sz > 0.05:
                filled = any(candles[j]["h"] >= prev["l"] for j in range(i+1, n))
                if not filled:
                    fvg_list.append({
                        "type": "BEARISH", "top": round(prev["l"], 2),
                        "bot": round(nxt["h"], 2),
                        "mid": round((prev["l"] + nxt["h"]) / 2, 2),
                        "size_pct": round(sz, 2), "filled": False,
                        "idx": i
                    })

    bull_fvg = [f for f in fvg_list if f["type"]=="BULLISH"][-3:]
    bear_fvg = [f for f in fvg_list if f["type"]=="BEARISH"][-3:]
    fvg_result = bull_fvg + bear_fvg

    # ── Step 6: Order Blocks — with volatility filter + mitigation ────────
    # High volatility bars (spike bars) are handled differently
    # OB is mitigated (invalid) when price crosses through it
    def vol_filter(i):
        """For high-volatility bars, use low as parsedHigh and vice versa."""
        bar_range = highs[i] - lows[i]
        if bar_range >= 2 * atr_val:
            return lows[i], highs[i]   # spike bar: invert
        return highs[i], lows[i]       # normal bar

    ob_list = []
    for i in range(2, n-3):
        c, cn = candles[i], candles[i+1]
        body_c  = abs(c["c"] - c["o"])
        body_cn = abs(cn["c"] - cn["o"])
        if body_c == 0: continue

        parsed_h, parsed_l = vol_filter(i)

        # Bullish OB: bearish candle followed by strong bullish candle
        if c["c"] < c["o"] and cn["c"] > cn["o"] and body_cn > body_c * 1.3:
            hi = round(max(c["o"], c["c"]), 2)
            lo = round(min(c["o"], c["c"]), 2)
            # Mitigation check: has price since come back below the OB low?
            mitigated = any(candles[j]["l"] < lo for j in range(i+2, n))
            if not mitigated:
                ob_list.append({
                    "type": "BULLISH", "high": hi, "low": lo,
                    "mid": round((hi+lo)/2, 2),
                    "strength": round(body_cn/body_c, 1),
                    "mitigated": False, "idx": i
                })

        # Bearish OB: bullish candle followed by strong bearish candle
        if c["c"] > c["o"] and cn["c"] < cn["o"] and body_cn > body_c * 1.3:
            hi = round(max(c["o"], c["c"]), 2)
            lo = round(min(c["o"], c["c"]), 2)
            # Mitigation check: has price since come back above the OB high?
            mitigated = any(candles[j]["h"] > hi for j in range(i+2, n))
            if not mitigated:
                ob_list.append({
                    "type": "BEARISH", "high": hi, "low": lo,
                    "mid": round((hi+lo)/2, 2),
                    "strength": round(body_cn/body_c, 1),
                    "mitigated": False, "idx": i
                })

    # Keep most recent non-mitigated OBs only
    bull_obs = [o for o in ob_list if o["type"]=="BULLISH"][-3:]
    bear_obs = [o for o in ob_list if o["type"]=="BEARISH"][-3:]
    ob_result = bull_obs + bear_obs

    # ── Step 7: Liquidity Sweeps (enhanced with EQH/EQL context) ─────────
    sweeps = []
    # Bull sweep: price wick above EQH or swing high then closes below
    for lvl_info in eqh_levels:
        if lvl_info["swept"]:
            sweeps.append({
                "type": "BULL_SWEEP", "level": lvl_info["level"],
                "signal": f"EQH swept ₹{lvl_info['level']} — smart money reversal down likely"
            })
    for lvl_info in eql_levels:
        if lvl_info["swept"]:
            sweeps.append({
                "type": "BEAR_SWEEP", "level": lvl_info["level"],
                "signal": f"EQL swept ₹{lvl_info['level']} — smart money reversal up likely"
            })

    # ── Step 8: Premium / Discount / Equilibrium zones ───────────────────
    swing_high_val = max((h for _,h in swg_highs), default=max(highs))
    swing_low_val  = min((l for _,l in swg_lows),  default=min(lows))
    equilibrium    = (swing_high_val + swing_low_val) / 2
    zone = "EQUILIBRIUM"
    if curr > equilibrium * 1.005:
        zone = "PREMIUM"    # expensive — institutions sell here
    elif curr < equilibrium * 0.995:
        zone = "DISCOUNT"   # cheap — institutions buy here

    # ── Assemble result ───────────────────────────────────────────────────
    # Most significant structure event: prefer CHoCH over BOS, swing over internal
    primary_event = None
    for ev in sorted(all_structure_events, key=lambda e: e["significance"], reverse=True):
        primary_event = ev
        break

    return {
        # Structure
        "structure":     structure,          # BULLISH / BEARISH / NEUTRAL
        "int_trend":     int_trend,          # Internal (5-bar) trend
        "swg_trend":     swg_trend,          # Swing (20-bar) trend
        "structure_event": primary_event,    # Most significant BOS/CHoCH
        "all_events":    all_structure_events[:5],  # All recent structure events

        # Levels
        "strong_high":   strong_high,        # Key resistance (CHoCH formed here)
        "weak_high":     weak_high,          # Minor resistance
        "strong_low":    strong_low,         # Key support (CHoCH formed here)
        "weak_low":      weak_low,           # Minor support

        # EQH/EQL liquidity pools
        "eqh":           eqh_levels[-3:],    # Equal highs (sell-side liquidity)
        "eql":           eql_levels[-3:],    # Equal lows (buy-side liquidity)

        # Order blocks (non-mitigated only)
        "ob":            ob_result,
        "orderBlocks":   ob_result,          # alias for frontend

        # FVG (unfilled only)
        "fvg":           fvg_result,
        "fvgZones":      fvg_result,         # alias for frontend

        # Sweeps
        "sweep":         sweeps,

        # Premium/Discount zone
        "zone":          zone,               # PREMIUM / DISCOUNT / EQUILIBRIUM
        "equilibrium":   round(equilibrium, 2),
        "swing_high":    round(swing_high_val, 2),
        "swing_low":     round(swing_low_val, 2),
    }

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

    # ── Trend15, PDH, PDL — from daily candle fetch ──
    # Include these for snapshot capture (not available in /market when Zerodha is connected)
    if d5:
        result["trend15"]         = d5.get("trend15","UNKNOWN")
        result["trend_strength"]  = d5.get("trend_strength",0)
        result["hh_hl"]           = d5.get("hh_hl",False)
        result["lh_ll"]           = d5.get("lh_ll",False)
        result["pdh"]             = d5.get("prev_day_high",0)
        result["pdl"]             = d5.get("prev_day_low",0)
        result["above_vwap"]      = (d5.get("px",0) > vwap) if vwap else None

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

    # Cache credentials for background stock OI thread
    if key and token:
        CACHE.set("_kite_key", key)
        CACHE.set("_kite_token", token)

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

    # Merge technicals + stock OI from cache
    result = {}
    for sym, p in prices.items():
        d = dict(p)
        inst = INSTRUMENTS.get(sym,{})

        # ── Technicals (SMA/RSI/trend) — for all symbols including MCX ──
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
            if not inst.get("mcx"):
                # ── 15D trend + PDH/PDL + CPR + VWAP — NSE/BSE only ──
                d["trend15"]         = tech.get("trend15","UNKNOWN")
                d["trend_strength"]  = tech.get("trend_strength",0)
                d["trend_up_count"]  = tech.get("trend_up_count",0)
                d["trend_sessions"]  = tech.get("trend_sessions",0)
                d["hh_hl"]           = tech.get("hh_hl",False)
                d["lh_ll"]           = tech.get("lh_ll",False)
                d["prev_day_high"]   = tech.get("prev_day_high",0)
                d["prev_day_low"]    = tech.get("prev_day_low",0)
                d["prev_close"]      = tech.get("prev_close",0)
                pdh = tech.get("prev_day_high",0)
                pdl = tech.get("prev_day_low",0)
                pdc = tech.get("prev_close",0)
                if pdh and pdl and pdc:
                    pivot = (pdh+pdl+pdc)/3
                    tc    = (pdh+pdl)/2
                    bc    = 2*pivot - tc
                    width = round(abs(tc-bc)/pivot*100,3) if pivot else 0
                    d["cpr"] = {
                        "pivot": round(pivot,2), "tc": round(tc,2), "bc": round(bc,2),
                        "type": "NARROW" if width<0.3 else "WIDE",
                        "width_pct": width,
                        "bias": "BULLISH" if pdc>pivot else "BEARISH"
                    }
                px  = d.get("px",0) or 0
                hi  = d.get("high",0) or 0
                lo  = d.get("low",0) or 0
                if px and hi and lo:
                    op  = d.get("open",0) or px
                    d["vwap"] = round((op+hi+lo+px)/4, 2)
                    d["above_vwap"] = px > d["vwap"]
            # Merge cached stock OI — liquid stocks (5-min) AND extended stocks (15-min)
            if sym in OI_STOCKS or sym in OI_STOCKS_EXT:
                oi_data = CACHE.get_val(f"oi_{sym}")
                if oi_data:
                    d["oi"] = {
                        "pcr":       oi_data.get("pcr",0),
                        "max_pain":  oi_data.get("max_pain",0),
                        "ce_wall":   oi_data.get("ce_wall",0),
                        "pe_wall":   oi_data.get("pe_wall",0),
                        "ce_oi":     oi_data.get("ce_oi",0),
                        "pe_oi":     oi_data.get("pe_oi",0),
                        "buildup":   oi_data.get("buildup","UNKNOWN"),
                        "pcr_interp":oi_data.get("pcr_interp","NEUTRAL"),
                        "expiry":    oi_data.get("expiry",""),
                        "atm":       oi_data.get("atm",0),
                        "cached":    True,
                        "refresh":   "5min" if sym in OI_STOCKS else "15min",
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

@app.route("/place_order", methods=["POST"])
def place_order():
    """Place a real Zerodha order. Resolves correct tradingsymbol from instruments CSV."""
    try:
        data = request.get_json(force=True) or {}
        sym   = data.get("sym","").upper()
        strike= int(data.get("strike",0))
        opt   = data.get("option_type","CE").upper()  # CE or PE
        action= data.get("action","BUY").upper()       # BUY or SELL
        key   = data.get("key","") or CACHE.get_val("_kite_key") or ""
        token = data.get("token","") or CACHE.get_val("_kite_token") or ""
        qty   = int(data.get("qty",0))

        if not key or not token:
            return jsonify({"ok":False,"error":"Not connected to Zerodha — reconnect first"})
        if not sym or not strike:
            return jsonify({"ok":False,"error":"Missing symbol or strike"})

        hdrs = _kite_headers(key, token)

        # Step 1: Get nearest expiry and find exact tradingsymbol
        if sym == "SENSEX":
            csv = fetch_kite_instruments_bfo(key, token)
            exchange = "BFO"
        elif sym in OI_MCX:
            csv = fetch_kite_instruments_mcx(key, token)
            exchange = "MCX"
        else:
            csv = fetch_kite_instruments_nfo(key, token)
            exchange = "NFO"

        if not csv:
            return jsonify({"ok":False,"error":"Could not fetch instruments — check token"})

        # Find closest expiry
        today_n = datetime.now(IST).replace(tzinfo=None)
        best_exp = None; best_days = 999
        for line in csv.strip().split("\n")[1:]:
            cols = line.split(",")
            if len(cols)<10: continue
            if not cols[2].startswith(sym): continue
            if cols[9] not in ["CE","PE"]: continue
            try:
                exp = datetime.strptime(cols[5],"%Y-%m-%d")
                d2 = (exp-today_n).days
                if 0<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue

        if not best_exp:
            return jsonify({"ok":False,"error":f"No valid expiry found for {sym}"})

        # Find exact tradingsymbol for this strike and option type
        tradingsymbol = None; instrument_token = None
        for line in csv.strip().split("\n")[1:]:
            cols = line.split(",")
            if len(cols)<10: continue
            if not cols[2].startswith(sym): continue
            if cols[9] != opt: continue
            if cols[5] != best_exp: continue
            try:
                if int(float(cols[6])) == strike:
                    tradingsymbol = cols[2]
                    instrument_token = cols[0]
                    break
            except: continue

        if not tradingsymbol:
            return jsonify({"ok":False,"error":f"Tradingsymbol not found for {sym} {strike} {opt} exp:{best_exp}"})

        # Step 2: Default lot size if not provided
        default_lots = {"NIFTY":75,"BANKNIFTY":15,"FINNIFTY":40,"SENSEX":10,
                        "CRUDEOIL":100,"GOLD":100}
        if not qty:
            qty = default_lots.get(sym, 50)

        # Step 3: Place order via Kite
        order_payload = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": action,
            "order_type": "MARKET",
            "quantity": qty,
            "product": "NRML",
            "validity": "DAY"
        }
        r = requests.post("https://api.kite.trade/orders/regular",
            data=order_payload, headers=hdrs, timeout=15)
        resp = r.json()

        if r.status_code == 200 and resp.get("status") == "success":
            order_id = resp.get("data",{}).get("order_id","")
            print(f"[Order ✅] {action} {tradingsymbol} qty:{qty} order_id:{order_id}")
            return jsonify({"ok":True,"order_id":order_id,"tradingsymbol":tradingsymbol,
                           "exchange":exchange,"qty":qty,"expiry":best_exp})
        else:
            err = resp.get("message","Unknown error")
            print(f"[Order ❌] {tradingsymbol}: {err}")
            return jsonify({"ok":False,"error":err,"tradingsymbol":tradingsymbol})

    except Exception as e:
        print(f"[Order] Exception: {e}")
        return jsonify({"ok":False,"error":str(e)})

@app.route("/oi_debug")
def oi_debug():
    """Debug endpoint — shows OI cache status for all symbols."""
    result = {}
    for sym in sorted(OI_ALL):
        cached = CACHE.get_val(f"oi_{sym}")
        disk_file = f"/tmp/oi_cache/{sym}.json"
        import os, json as _j
        disk_age = None
        try:
            with open(disk_file) as f:
                c = _j.load(f)
                disk_age = int((time.time()-c["ts"])/60)
        except: pass
        result[sym] = {
            "in_memory": bool(cached),
            "mem_pcr": cached.get("pcr") if cached else None,
            "disk_age_min": disk_age,
            "mcx_sym": CACHE.get_val(f"mcx_sym_{sym}") if sym in OI_MCX else None
        }
    key = CACHE.get_val("_kite_key") or ""
    token = CACHE.get_val("_kite_token") or ""
    return jsonify({
        "ok": True,
        "has_credentials": bool(key and token),
        "key_preview": key[:8]+"..." if key else None,
        "symbols": result,
        "time": ist_str()
    })

@app.route("/zerodha_oi")
def zerodha_oi():
    """Live OI from Zerodha — ATM ±10 strikes. Works for indices AND stocks."""
    sym   = request.args.get("sym","NIFTY").upper()
    key   = request.args.get("key","")
    token = request.args.get("token","")

    if sym not in OI_ALL:
        return jsonify({"ok":False,"error":f"{sym} not in F&O list"}),400

    # Cache credentials for background stock OI thread
    if key and token:
        CACHE.set("_kite_key", key)
        CACHE.set("_kite_token", token)

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


def _bg_stock_oi():
    """
    Background thread: refresh OI for 18 liquid F&O stocks + MCX every 5 min.
    These have the most active option chains — need frequent updates.
    """
    time.sleep(30)  # wait for server boot
    print(f"[StockOI] Started — {len(OI_STOCKS)} liquid stocks + {len(OI_MCX)} MCX (5-min cycle)")
    while True:
        try:
            key   = CACHE.get_val("_kite_key") or ""
            token = CACHE.get_val("_kite_token") or ""
            if not key or not token:
                print("[StockOI] No credentials yet — waiting...")
                time.sleep(60); continue
            prices = CACHE.get_val("all_prices") or {}
            fetched = 0
            for sym in sorted(OI_STOCKS | OI_MCX):
                try:
                    result = get_oi(sym, key, token, prices.get(sym,{}).get("px",0))
                    if result:
                        fetched += 1
                        print(f"[StockOI ✅] {sym} PCR:{result.get('pcr','?')} MP:{result.get('max_pain','?')}")
                    time.sleep(3)
                except Exception as e:
                    print(f"[StockOI ❌] {sym}: {e}")
                    time.sleep(3)
            total = len(OI_STOCKS | OI_MCX)
            print(f"[StockOI] Cycle done — {fetched}/{total}. Sleeping 5min.")
            time.sleep(300)
        except Exception as e:
            print(f"[StockOI] Error: {e}")
            time.sleep(60)


def _bg_stock_oi_extended():
    """
    Background thread: refresh OI for 32 extended Nifty50 stocks every 15 min.
    Less liquid options — 15-min refresh is sufficient to catch signals.
    96s cycle (32 stocks × 3s) fits well inside 900s (15 min).
    Only runs during NSE market hours 9:15 AM - 3:30 PM IST.
    """
    time.sleep(90)  # stagger: start 90s after primary thread
    print(f"[ExtOI] Started — {len(OI_STOCKS_EXT)} extended stocks (15-min cycle)")
    while True:
        try:
            key   = CACHE.get_val("_kite_key") or ""
            token = CACHE.get_val("_kite_token") or ""
            if not key or not token:
                time.sleep(120); continue
            now_ist = datetime.now(IST)
            # Only during NSE market hours
            nse_open = now_ist.weekday() < 5 and (
                (now_ist.hour == 9 and now_ist.minute >= 15) or
                (10 <= now_ist.hour <= 14) or
                (now_ist.hour == 15 and now_ist.minute <= 30)
            )
            if not nse_open:
                time.sleep(300); continue
            prices  = CACHE.get_val("all_prices") or {}
            fetched = 0
            for sym in sorted(OI_STOCKS_EXT):
                try:
                    result = get_oi(sym, key, token, prices.get(sym,{}).get("px",0))
                    if result:
                        fetched += 1
                        print(f"[ExtOI ✅] {sym} PCR:{result.get('pcr','?')}")
                    time.sleep(3)
                except Exception as e:
                    print(f"[ExtOI ❌] {sym}: {e}")
                    time.sleep(3)
            print(f"[ExtOI] Cycle done — {fetched}/{len(OI_STOCKS_EXT)}. Sleeping 15min.")
            time.sleep(900)
        except Exception as e:
            print(f"[ExtOI] Error: {e}")
            time.sleep(120)


# Start warmup for both direct run AND gunicorn
_warmup_thread = threading.Thread(target=_warmup, daemon=True)
_warmup_thread.start()

# Start background stock OI refresh thread — liquid stocks every 5 min
_stock_oi_thread = threading.Thread(target=_bg_stock_oi, daemon=True)
_stock_oi_thread.start()

# Start extended OI thread — remaining Nifty50 every 15 min
_ext_oi_thread = threading.Thread(target=_bg_stock_oi_extended, daemon=True)
_ext_oi_thread.start()
print(f"[OI] Total coverage: {len(OI_ALL)} instruments ({len(OI_INDICES)} indices + {len(OI_STOCKS)} liquid + {len(OI_STOCKS_EXT)} extended + {len(OI_MCX)} MCX)")
print(f"[StockOI] Background thread started for {len(OI_STOCKS)} liquid F&O stocks")


# ═══════════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# Technical layers only: SMA, RSI, VWAP, ORB, CPR, PDH/PDL, 15D Trend
# OI excluded — requires intraday data (use snapshot-based backtest for OI validation)
# ═══════════════════════════════════════════════════════════════════
import uuid as _uuid
_bt_jobs = {}   # job_id → {status, progress, message, result}

def _bt_fetch_candles(ticker, interval="5m", days=61):
    """Fetch historical OHLCV using proven fetch_yahoo_candles. Reuses proxy + parsing."""
    rng = f"{min(days,59)}d" if interval=="5m" else f"{days}d"
    d = fetch_yahoo_candles(ticker, interval=interval, rng=rng)
    if not d or not d.get("candles"):
        return []
    # Rename "t" → "ts" to match backtest engine expectations
    return [{"ts":c["t"],"o":c["o"],"h":c["h"],"l":c["l"],"c":c["c"],"v":c["v"]}
            for c in d["candles"]]

# ── Zerodha instrument token cache ───────────────────────────────────────────
_kite_nse_tokens = {}   # sym → instrument_token

def _bt_get_kite_token(sym, key, token):
    """Look up NSE equity instrument token for a symbol. Cached per session."""
    if sym in _kite_nse_tokens:
        return _kite_nse_tokens[sym]
    # Known indices — hardcoded tokens (stable, don't change)
    _static = {
        "NIFTY":256265, "BANKNIFTY":260105, "FINNIFTY":257801,
        "SENSEX":265, "MIDCPNIFTY":288009,
    }
    if sym in _static:
        _kite_nse_tokens[sym] = _static[sym]
        return _static[sym]
    # Fetch NSE equity instruments CSV and parse
    cached_csv = CACHE.get_val("instruments_nse_eq")
    if not cached_csv:
        try:
            r = requests.get("https://api.kite.trade/instruments/NSE",
                headers=_kite_headers(key, token), timeout=20)
            if r.status_code == 200:
                cached_csv = r.text
                CACHE.set("instruments_nse_eq", cached_csv)
        except Exception as e:
            print(f"[BT] NSE instruments fetch failed: {e}")
            return None
    if not cached_csv:
        return None
    # Parse CSV for our symbol
    import csv as _csv, io as _io
    for row in _csv.DictReader(_io.StringIO(cached_csv)):
        ts = (row.get("tradingsymbol") or "").strip()
        seg = (row.get("segment") or "").strip()
        tok = (row.get("instrument_token") or "").strip()
        if ts == sym and seg in ("NSE", "NSE-EQ") and tok:
            _kite_nse_tokens[sym] = int(tok)
            return int(tok)
    print(f"[BT] Token not found for {sym}")
    return None

def _bt_kite_candles(sym, key, api_token, days=60, interval="5minute"):
    """
    Fetch historical candles from Zerodha Kite API.
    This is the authoritative source — accurate NSE data, proper market hours.
    interval: "5minute" | "day"
    Returns list of {ts, o, h, l, c, v} dicts.
    """
    instr_token = _bt_get_kite_token(sym, key, api_token)
    if not instr_token:
        # Fallback to Yahoo Finance for symbols without Kite token
        inst = INSTRUMENTS.get(sym, {})
        yahoo_ticker = inst.get("yahoo", "")
        if yahoo_ticker:
            print(f"[BT] Falling back to Yahoo for {sym}")
            return _bt_fetch_candles(yahoo_ticker, "5m" if interval=="5minute" else "1d", days)
        return []

    from_dt = (datetime.now(IST) - timedelta(days=days)).strftime("%Y-%m-%d")
    to_dt   =  datetime.now(IST).strftime("%Y-%m-%d")

    url = (f"https://api.kite.trade/instruments/historical/"
           f"{instr_token}/{interval}"
           f"?from={from_dt}&to={to_dt}")

    try:
        r = requests.get(url, headers=_kite_headers(key, api_token), timeout=30)
        if r.status_code in [401, 403]:
            print(f"[BT] Kite auth failed for {sym} — token expired?")
            return []
        d = r.json()
        if d.get("status") != "success":
            print(f"[BT] Kite error for {sym}: {d.get('message','')}")
            return []
        bars = []
        for c in d.get("data", {}).get("candles", []):
            # Kite format: ["2026-05-01 09:15:00", open, high, low, close, volume]
            # Sometimes: "2026-05-01T09:15:00+0530" — handle both
            ts_str = c[0]
            try:
                # Normalise: replace space with T, handle +0530 → +05:30
                ts_norm = ts_str.replace(" ", "T")
                ts_norm = re.sub(r'\+(\d{2})(\d{2})$', r'+\1:\2', ts_norm)
                if "+" in ts_norm or "Z" in ts_norm:
                    dt = datetime.fromisoformat(ts_norm.replace("Z","+00:00"))
                else:
                    # No timezone info — Kite returns IST implicitly
                    dt = datetime.strptime(ts_norm, "%Y-%m-%dT%H:%M:%S")
                    dt = dt.replace(tzinfo=IST)
            except Exception as pe:
                continue
            bars.append({
                "ts": int(dt.timestamp()),
                "o": round(float(c[1]),2), "h": round(float(c[2]),2),
                "l": round(float(c[3]),2), "c": round(float(c[4]),2),
                "v": int(c[5]) if len(c)>5 else 0
            })
        print(f"[BT] {sym}: {len(bars)} {interval} bars via Kite ✅")
        if not bars and d.get("data", {}).get("candles"):
            raw_count = len(d["data"]["candles"])
            sample = d["data"]["candles"][0][0] if raw_count else "none"
            print(f"[BT] ⚠ {sym}: {raw_count} raw candles but 0 parsed — sample ts: {sample}")
        return bars
    except Exception as e:
        print(f"[BT] Kite historical failed {sym}: {e}")
        return []


def _bt_score(px, sma20, sma50, rsi, vwap, pdh, pdl, cpr_tc, cpr_bc, cpr_narrow,
              orb_h, orb_l, mins, trend15):
    """Replicate signal scoring engine for one bar. Returns (bull, bear)."""
    bull=0; bear=0
    aboveVwap = px>vwap if vwap else px>sma20
    # SMA layer
    if px>sma20 and px>sma50: bull+=1
    if px<sma20 and px<sma50: bear+=1
    if sma20>sma50: bull+=1
    elif sma20<sma50: bear+=1
    # RSI layer
    if rsi<32 and px>sma20: bull+=2
    elif 35<=rsi<=55 and px>sma20: bull+=1
    if rsi>68 and px<sma20: bear+=2
    elif 45<=rsi<=65 and px<sma20: bear+=1
    # VWAP layer
    if aboveVwap: bull+=2
    else: bear+=2
    # PDH/PDL
    if pdh and px>pdh*1.001: bull+=(2 if True else 1)
    elif pdh and abs(px-pdh)/pdh<0.002: bear+=1
    if pdl and px<pdl*0.999: bear+=2
    elif pdl and abs(px-pdl)/pdl<0.002: bull+=1
    # CPR
    if cpr_narrow and cpr_tc and px>cpr_tc: bull+=2
    if cpr_narrow and cpr_bc and px<cpr_bc: bear+=2
    # ORB
    if orb_h and orb_l:
        tier = 3 if mins<11*60 else 2 if mins<14*60 else 1
        if px>orb_h*1.001: bull+=tier
        elif px<orb_l*0.999: bear+=tier
    # 15D trend
    tmap = {"STRONG_BULL":(3,0),"BULL":(2,0),"NEUTRAL":(0,0),"BEAR":(0,2),"STRONG_BEAR":(0,3)}
    bt,be = tmap.get(trend15,(0,0)); bull+=bt; bear+=be
    # Counter-trend penalty
    if trend15 in ["STRONG_BULL","BULL"] and bear>bull: bear=max(0,bear-2)
    if trend15 in ["STRONG_BEAR","BEAR"] and bull>bear: bull=max(0,bull-2)
    # OI excluded — intraday OI not available for historical dates
    # Use snapshot-based backtest once 30 days of snapshots are captured
    return bull, bear

def _bt_simulate(bias, entry, sl, t1, t2, future_bars):
    """Simulate trade outcome. Returns status + pnl_pct."""
    effective_sl = sl; t1_hit = False
    for i,bar in enumerate(future_bars):
        mins = (bar["ts"]//60)%1440 if isinstance(bar["ts"],int) else 0
        if mins >= 15*60+15:
            pct = (bar["c"]-entry)/entry*100 if bias=="BULLISH" else (entry-bar["c"])/entry*100
            return {"status":"EOD","exit":round(bar["c"],2),"pnl_pct":round(pct,2),"bars":i+1}
        if bias=="BULLISH":
            if bar["l"]<=effective_sl:
                pct=(effective_sl-entry)/entry*100
                return {"status":"T1_BE_SL" if t1_hit else "SL_HIT","exit":round(effective_sl,2),"pnl_pct":round(pct,2),"bars":i+1}
            if not t1_hit and bar["h"]>=t1: t1_hit=True; effective_sl=entry
            if bar["h"]>=t2:
                return {"status":"T2_HIT","exit":round(t2,2),"pnl_pct":round((t2-entry)/entry*100,2),"bars":i+1}
        else:
            if bar["h"]>=effective_sl:
                pct=(entry-effective_sl)/entry*100
                return {"status":"T1_BE_SL" if t1_hit else "SL_HIT","exit":round(effective_sl,2),"pnl_pct":round(pct,2),"bars":i+1}
            if not t1_hit and bar["l"]<=t1: t1_hit=True; effective_sl=entry
            if bar["l"]<=t2:
                return {"status":"T2_HIT","exit":round(t2,2),"pnl_pct":round((entry-t2)/entry*100,2),"bars":i+1}
    return {"status":"OPEN","exit":0,"pnl_pct":0,"bars":len(future_bars)}

def _bt_run_job(job_id, params):
    """Background backtest computation."""
    try:
        days     = int(params.get("days",60))
        min_conf = int(params.get("min_conf",82))
        sym_filter = params.get("syms","all")
        # Which symbols to test
        if sym_filter=="idx":   test_syms = list(OI_INDICES)
        elif sym_filter=="stk": test_syms = [s for s in INSTRUMENTS if not INSTRUMENTS[s].get("mcx") and s not in OI_INDICES]
        else:                   test_syms = [s for s in INSTRUMENTS if not INSTRUMENTS[s].get("mcx")]  # all = indices + stocks

        total  = len(test_syms)
        signals_all = []

        _bt_jobs[job_id].update({"message":"Fetching candles from Zerodha…","progress":2})

        # Read Zerodha credentials from cache (set when Kite is connected in app)
        kite_key   = CACHE.get_val("_kite_key")   or ""
        kite_token = CACHE.get_val("_kite_token") or ""
        if not kite_key or not kite_token:
            raise Exception("Zerodha not connected — open Settings and connect Kite first")

        # Fetch all candles via Zerodha Kite historical API
        # Kite gives accurate NSE data, proper market hours, no rate limiting issues
        candle_data = {}

        def _fetch_sym(sym):
            bars5m = _bt_kite_candles(sym, kite_key, kite_token, days, "5minute")
            bars1d = _bt_kite_candles(sym, kite_key, kite_token, days+30, "day")
            return sym, bars5m, bars1d

        done=0
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(_fetch_sym, sym): sym for sym in test_syms}
            for fut in as_completed(futs, timeout=300):
                try:
                    sym, b5, b1d = fut.result()
                except Exception as fe:
                    sym = futs[fut]; b5=[]; b1d=[]
                    print(f"[BT] Fetch error {sym}: {fe}")
                candle_data[sym] = {"5m":b5,"1d":b1d}
                done+=1
                loaded = sum(1 for v in candle_data.values() if v.get("5m"))
                _bt_jobs[job_id].update({"progress":round(5+done/total*40),
                    "message":f"Loaded {done}/{total} symbols ({loaded} with data)…"})

        _bt_jobs[job_id].update({"message":"Running signal engine…","progress":50})

        # Process each symbol
        for si, sym in enumerate(test_syms):
            _bt_jobs[job_id].update({"progress":round(50+si/total*40),"message":f"Scoring {sym}…"})
            bars5 = candle_data.get(sym,{}).get("5m",[])
            bars1d= candle_data.get(sym,{}).get("1d",[])
            if not bars5 or not bars1d: continue

            # Group 5m bars by calendar date
            from collections import defaultdict as _dd
            by_date = _dd(list)
            for b in bars5:
                dt = datetime.fromtimestamp(b["ts"], tz=IST).date()
                by_date[dt].append(b)

            # Build daily bar lookup (Kite 'day' interval data)
            daily_by_date = {}
            for b in bars1d:
                dt_key = datetime.fromtimestamp(b["ts"], tz=IST).date()
                daily_by_date[dt_key] = b
            sorted_daily = sorted(daily_by_date.keys())

            if not daily_by_date:
                print(f"[BT] {sym}: no daily data, skipping")
                continue

            last_signal_bias = None
            last_signal_date = None

            for dt in sorted(by_date.keys()):
                day_bars = sorted(by_date[dt], key=lambda b:b["ts"])
                if len(day_bars) < 5: continue

                # PDH / PDL / CPR from previous day
                di = sorted_daily.index(dt) if dt in sorted_daily else -1
                prev_dt = sorted_daily[di-1] if di>0 else None
                prev_d = daily_by_date.get(prev_dt) if prev_dt else None
                pdh = prev_d["h"] if prev_d else 0
                pdl = prev_d["l"] if prev_d else 0
                pdc = prev_d["c"] if prev_d else 0
                pivot = (pdh+pdl+pdc)/3 if pdh else 0
                cpr_tc = (pdh+pdl)/2 if pdh else 0
                cpr_bc = 2*pivot-cpr_tc if pivot else 0
                cpr_narrow = (abs(cpr_tc-cpr_bc)/pivot*100 < 0.3) if pivot else False

                # 15D trend from daily bars
                trend15 = "NEUTRAL"
                if di >= 12:
                    d15 = [daily_by_date[sorted_daily[j]] for j in range(max(0,di-15),di) if sorted_daily[j] in daily_by_date]
                    if len(d15)>=10:
                        cls = [b["c"] for b in d15]
                        s5  = sum(cls[-5:])/5; s10 = sum(cls[-10:])/10
                        hh  = d15[-1]["h"] > d15[-4]["h"]; hl = d15[-1]["l"] > d15[-4]["l"]
                        lh  = d15[-1]["h"] < d15[-4]["h"]; ll = d15[-1]["l"] < d15[-4]["l"]
                        if s5>s10 and hh and hl:   trend15="STRONG_BULL"
                        elif s5>s10:               trend15="BULL"
                        elif s5<s10 and lh and ll: trend15="STRONG_BEAR"
                        elif s5<s10:               trend15="BEAR"

                # OI not used in technical-only backtest
                pcr = 0

                # ── ORB (9:15–9:30 AM IST) — must use IST datetime, not ts%86400 ──
                # Bug was: ts%86400 gives UTC seconds → 9:15 IST = 3:45 UTC → never matched
                orb_bars = []
                for b in day_bars:
                    b_ist = datetime.fromtimestamp(b["ts"], tz=IST)
                    b_mins = b_ist.hour*60 + b_ist.minute
                    if 9*60+15 <= b_mins < 9*60+30:
                        orb_bars.append(b)
                orb_h = max(b["h"] for b in orb_bars) if orb_bars else 0
                orb_l = min(b["l"] for b in orb_bars) if orb_bars else 0

                # VWAP accumulation
                cum_tv=0; cum_v=0
                close_buf=[]

                for bi, bar in enumerate(day_bars):
                    bar_time = datetime.fromtimestamp(bar["ts"], tz=IST)
                    mins = bar_time.hour*60 + bar_time.minute
                    if mins < 9*60+30: continue
                    if mins > 15*60+30: break

                    tp = (bar["h"]+bar["l"]+bar["c"])/3
                    cum_tv += tp*bar["v"]; cum_v += bar["v"]
                    vwap = cum_tv/cum_v if cum_v else bar["c"]
                    close_buf.append(bar["c"])

                    if len(close_buf) < 5: continue

                    sma20 = sum(close_buf[-20:])/min(20,len(close_buf))
                    sma50 = sum(close_buf[-50:])/min(50,len(close_buf))
                    if len(close_buf)>=15:
                        diffs=[close_buf[i]-close_buf[i-1] for i in range(max(1,len(close_buf)-14),len(close_buf))]
                        g=[d for d in diffs if d>0]; lo=[abs(d) for d in diffs if d<0]
                        ag=sum(g)/14 if g else 0.001; al=sum(lo)/14 if lo else 0.001
                        rsi = round(100-100/(1+ag/al),1)
                    else: rsi=50

                    px = bar["c"]
                    bull, bear = _bt_score(px,sma20,sma50,rsi,vwap,pdh,pdl,
                                           cpr_tc,cpr_bc,cpr_narrow,orb_h,orb_l,
                                           mins,trend15)

                    # Hard trend blocks
                    if trend15=="STRONG_BULL" and bear>bull: continue
                    if trend15=="STRONG_BEAR" and bull>bear: continue

                    if bull<2 and bear<2: continue
                    bias = "BULLISH" if bull>bear else "BEARISH" if bear>bull else None
                    if not bias: continue

                    # Confidence — do NOT cap for missing OI in backtest
                    # (pcr=0 means NSE archive unavailable, not that OI is bearish)
                    w=bull if bias=="BULLISH" else bear
                    lo2=bear if bias=="BULLISH" else bull
                    conf = min(94,max(62,68+w*3-lo2*2))

                    # Morning thresholds — relax ORB requirement since ORB
                    # is now correctly calculated from Kite 5-min data
                    early=(9*60+30<=mins<9*60+45); late=(9*60+45<=mins<10*60)
                    if early and conf<92: continue
                    elif late and conf<89: continue
                    elif not early and not late and conf<min_conf: continue

                    if last_signal_bias==bias and last_signal_date==dt: continue
                    last_signal_bias=bias; last_signal_date=dt

                    sl_d = px*0.005
                    if bias=="BULLISH": sl=px-sl_d; t1=px+sl_d*1.5; t2=px+sl_d*2.5
                    else:               sl=px+sl_d; t1=px-sl_d*1.5; t2=px-sl_d*2.5

                    outcome = _bt_simulate(bias,px,sl,t1,t2,day_bars[bi+1:])

                    session = ("morning" if mins<10*60 else
                               "midday"  if mins<14*60 else "afternoon")
                    signals_all.append({
                        "sym":sym,"date":str(dt),"time":bar_time.strftime("%H:%M"),
                        "bias":bias,"conf":conf,"bull":bull,"bear":bear,
                        "trend15":trend15,"cpr_narrow":cpr_narrow,
                        "pcr":round(pcr,2),"rsi":round(rsi,1),
                        "vwap":round(vwap,2),"entry":round(px,2),
                        "sl":round(sl,2),"t1":round(t1,2),"t2":round(t2,2),
                        "session":session,
                        **outcome
                    })

        # Aggregate results
        _bt_jobs[job_id].update({"progress":95,"message":"Aggregating results…"})
        closed = [s for s in signals_all if s["status"]!="OPEN"]
        wins   = [s for s in closed if s["pnl_pct"]>0]
        losses = [s for s in closed if s["pnl_pct"]<0]

        def breakdown(fn):
            from collections import defaultdict as _dd2
            grps=_dd2(list)
            for s in closed: grps[fn(s)].append(s)
            out=[]
            for k,g in sorted(grps.items()):
                w=sum(1 for s in g if s["pnl_pct"]>0)
                l=sum(1 for s in g if s["pnl_pct"]<0)
                wr=round(w/(w+l)*100) if w+l else 0
                avg=round(sum(s["pnl_pct"] for s in g)/len(g),2)
                out.append({"k":str(k),"w":w,"l":l,"wr":wr,"avg":avg,"n":len(g)})
            return sorted(out,key=lambda x:-x["wr"])

        aw=round(sum(s["pnl_pct"] for s in wins)/len(wins),2) if wins else 0
        al=round(sum(s["pnl_pct"] for s in losses)/len(losses),2) if losses else 0
        result = {
            "total":len(signals_all), "closed":len(closed),
            "wins":len(wins), "losses":len(losses),
            "win_rate":round(len(wins)/len(closed)*100) if closed else 0,
            "avg_win":aw, "avg_loss":al,
            "profit_factor":round(abs(aw/al),2) if al else 0,
            "by_session": breakdown(lambda s:s["session"]),
            "by_trend":   breakdown(lambda s:s["trend15"]),
            "by_bias":    breakdown(lambda s:s["bias"]),
            "by_conf":    breakdown(lambda s:"94%" if s["conf"]>=94 else "88-93%" if s["conf"]>=88 else "82-87%" if s["conf"]>=82 else "<82%"),
            "by_cpr":     breakdown(lambda s:"NARROW" if s["cpr_narrow"] else "WIDE"),
            "by_sym":     breakdown(lambda s:s["sym"]),
            "signals":    sorted(signals_all,key=lambda s:s["date"]+s["time"])[-300:],
        }
        _bt_jobs[job_id].update({"status":"done","progress":100,"message":"Complete","result":result})
    except Exception as e:
        import traceback
        _bt_jobs[job_id].update({"status":"error","message":str(e),"trace":traceback.format_exc()})
        print(f"[BT] Job {job_id} failed: {e}")

@app.route("/backtest", methods=["POST"])
def backtest_start():
    """Start a backtest job. Returns job_id."""
    params = request.get_json() or {}
    job_id = str(_uuid.uuid4())[:8]
    _bt_jobs[job_id] = {"status":"running","progress":0,"message":"Starting…","result":None}
    t = threading.Thread(target=_bt_run_job, args=(job_id, params), daemon=True)
    t.start()
    return jsonify({"ok":True,"job_id":job_id})

@app.route("/backtest_status/<job_id>")
def backtest_status(job_id):
    """Poll backtest job status."""
    job = _bt_jobs.get(job_id)
    if not job: return jsonify({"ok":False,"error":"Job not found"}),404
    # Include trace in error state for frontend debugging
    resp = {"ok":True, "status":job.get("status"), "progress":job.get("progress",0),
            "message":job.get("message",""), "result":job.get("result")}
    if job.get("status")=="error": resp["trace"] = job.get("trace","")
    return jsonify(resp)

@app.route("/backtest_jobs")
def backtest_jobs():
    """List all backtest jobs — for debugging."""
    return jsonify({"ok":True,"jobs":{jid:{"status":j.get("status"),"progress":j.get("progress"),
        "message":j.get("message"),"has_result":j.get("result") is not None,
        "error":j.get("trace","")[:300] if j.get("status")=="error" else ""}
        for jid,j in _bt_jobs.items()}})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=False)

