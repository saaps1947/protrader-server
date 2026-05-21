from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, json, time
from datetime import datetime, timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)

app = Flask(__name__)
CORS(app)

# NSE Session
nse = requests.Session()
nse.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"*/*","Referer":"https://www.nseindia.com/"})
_nse_cookie_t = 0

def nse_cookies():
    global _nse_cookie_t
    if time.time()-_nse_cookie_t > 300:
        try:
            nse.get("https://www.nseindia.com",timeout=8)
            nse.get("https://www.nseindia.com/market-data/live-equity-market",timeout=8)
            _nse_cookie_t = time.time()
        except: pass

def nse_get(url):
    nse_cookies()
    try: return nse.get(url,timeout=10).json()
    except: return None

YAHOO = {"NIFTY":"^NSEI","BANKNIFTY":"^NSEBANK","SENSEX":"^BSESN","FINNIFTY":"NIFTY_FIN_SERVICE.NS","RELIANCE":"RELIANCE.NS","HDFCBANK":"HDFCBANK.NS","INDIGO":"INDIGO.NS","HAL":"HAL.NS","CRUDEOIL":"CL=F","GOLD":"GC=F"}
MCX_SYMBOLS = {"CRUDEOIL", "GOLD"}

# Cache USD/INR rate — fetch live from Yahoo
_usd_inr_cache = {"rate": 96.5, "ts": 0}

def get_usd_inr():
    """Fetch live USD/INR rate from Yahoo Finance"""
    import time
    if time.time() - _usd_inr_cache["ts"] < 300:  # cache 5 min
        return _usd_inr_cache["rate"]
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDINR=X?interval=1m&range=1d",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=5
        ).json()
        rate = r["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if rate and 80 < rate < 110:  # sanity check
            _usd_inr_cache["rate"] = rate
            _usd_inr_cache["ts"] = time.time()
            return rate
    except:
        pass
    return _usd_inr_cache["rate"]  # fallback to cached

def yahoo(ticker, interval="5m", rng="1d"):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={rng}&includePrePost=false"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10).json()
        res = r["chart"]["result"][0]
        meta = res["meta"]
        ts = res.get("timestamp",[])
        q = res["indicators"]["quote"][0]
        candles = [{"t":ts[i],"o":round(q["open"][i] or 0,2),"h":round(q["high"][i] or 0,2),"l":round(q["low"][i] or 0,2),"c":round(q["close"][i] or 0,2)} for i in range(len(ts)) if q["close"][i]]
        closes = [c["c"] for c in candles]
        def sma(n): return round(sum(closes[-n:])/n,2) if len(closes)>=n else None
        def rsi():
            if len(closes)<15: return None
            g=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
            l=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
            ag=sum(g[-14:])/14; al=sum(l[-14:])/14
            return round(100-100/(1+ag/al),1) if al else 100
        s20=sma(20); s50=sma(50); s200=sma(200)
        px=meta.get("regularMarketPrice",0)
        pc=meta.get("chartPreviousClose",0)
        cross="NONE"
        if len(closes)>=22 and s20 and s50:
            ps20=sum(closes[-21:-1])/20
            if ps20<s50 and s20>s50: cross="GOLDEN_CROSS"
            elif ps20>s50 and s20<s50: cross="DEATH_CROSS"
        highs=[c["h"] for c in candles]; lows=[c["l"] for c in candles]
        return {"px":px,"chg":round(px-pc,2),"pct":round((px-pc)/pc*100,2) if pc else 0,
                "high":meta.get("regularMarketDayHigh",0),"low":meta.get("regularMarketDayLow",0),
                "open":meta.get("regularMarketOpen",0),"prev_close":pc,
                "sma20":s20,"sma50":s50,"sma200":s200,"rsi":rsi(),
                "crossover":cross,
                "trend":"BULLISH" if (s20 and s50 and s20>s50) else "BEARISH" if (s20 and s50 and s20<s50) else "NEUTRAL",
                "breakout": px>=(max(highs)*0.998) if highs else False,
                "breakdown": px<=(min(lows)*1.002) if lows else False,
                "candles":candles[-20:]}
    except Exception as e: return {"error":str(e),"px":0}

def calc_cpr(candles_daily):
    """
    Central Pivot Range from previous day's candles
    CPR = (H + L + C) / 3
    TC = (H + L) / 2 - CPR (top central)
    BC = CPR - (H + L) / 2 (bottom central)
    Narrow CPR = trending day, Wide CPR = sideways
    """
    if not candles_daily or len(candles_daily) < 2:
        return None
    # Use previous day's candle
    prev = candles_daily[-2] if len(candles_daily) >= 2 else candles_daily[-1]
    h, l, c = prev["h"], prev["l"], prev["c"]
    pivot = (h + l + c) / 3
    tc = (h + l) / 2
    bc = 2 * pivot - tc
    width = abs(tc - bc)
    width_pct = width / pivot * 100
    # Narrow = < 0.3% of price = trending day expected
    is_narrow = width_pct < 0.3
    today = candles_daily[-1]
    px = today["c"]
    return {
        "pivot": round(pivot, 2),
        "tc": round(tc, 2),
        "bc": round(bc, 2),
        "width": round(width, 2),
        "width_pct": round(width_pct, 3),
        "is_narrow": is_narrow,
        "type": "NARROW" if is_narrow else "WIDE",
        "bias": "BULLISH" if px > pivot else "BEARISH",
        "px_vs_pivot": round(px - pivot, 2)
    }

def get_multi_tf(ticker):
    """Get SMA data across 5min, 15min, 1hour timeframes"""
    result = {}
    try:
        # 5-min (intraday)
        d5 = yahoo(ticker, "5m", "1d")
        if d5 and d5.get("sma20"):
            result["tf5"] = {
                "sma20": d5["sma20"], "sma50": d5["sma50"],
                "rsi": d5["rsi"], "trend": d5["trend"],
                "crossover": d5["crossover"],
                "breakout": d5["breakout"], "breakdown": d5["breakdown"]
            }
        # 15-min (session)
        d15 = yahoo(ticker, "15m", "5d")
        if d15 and d15.get("sma20"):
            result["tf15"] = {
                "sma20": d15["sma20"], "sma50": d15["sma50"],
                "rsi": d15["rsi"], "trend": d15["trend"],
                "crossover": d15["crossover"]
            }
        # 1-hour (daily bias)
        d1h = yahoo(ticker, "1h", "1mo")
        if d1h and d1h.get("sma20"):
            result["tf1h"] = {
                "sma20": d1h["sma20"], "sma50": d1h["sma50"],
                "sma200": d1h["sma200"], "rsi": d1h["rsi"],
                "trend": d1h["trend"], "crossover": d1h["crossover"]
            }
        # CPR from daily candles
        d1d = yahoo(ticker, "1d", "1mo")
        if d1d and d1d.get("candles"):
            cpr = calc_cpr(d1d["candles"])
            if cpr:
                result["cpr"] = cpr
        # Alignment score
        trends = [result.get("tf5",{}).get("trend",""),
                  result.get("tf15",{}).get("trend",""),
                  result.get("tf1h",{}).get("trend","")]
        bulls = trends.count("BULLISH")
        bears = trends.count("BEARISH")
        if bulls == 3: result["alignment"] = "STRONG_BULL"
        elif bulls == 2: result["alignment"] = "BULL"
        elif bears == 3: result["alignment"] = "STRONG_BEAR"
        elif bears == 2: result["alignment"] = "BEAR"
        else: result["alignment"] = "MIXED"
    except Exception as e:
        result["error"] = str(e)
    return result

def calc_oi(sym):
    if sym in ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","SENSEX"]:
        url=f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
    else:
        url=f"https://www.nseindia.com/api/option-chain-equities?symbol={sym}"
    d=nse_get(url)
    if not d: return None
    try:
        f=d["filtered"]["data"]
        spot=d["filtered"].get("underlyingValue",0) or d["records"].get("underlyingValue",0)
        ce_oi=sum(x.get("CE",{}).get("openInterest",0) or 0 for x in f)
        pe_oi=sum(x.get("PE",{}).get("openInterest",0) or 0 for x in f)
        ce_chg=sum(x.get("CE",{}).get("changeinOpenInterest",0) or 0 for x in f)
        pe_chg=sum(x.get("PE",{}).get("changeinOpenInterest",0) or 0 for x in f)
        pcr=round(pe_oi/ce_oi,2) if ce_oi else 0
        # Max Pain
        strikes=[{"s":x.get("strikePrice",0),"ce":x.get("CE",{}).get("openInterest",0) or 0,"pe":x.get("PE",{}).get("openInterest",0) or 0} for x in f if x.get("strikePrice")]
        mp=0; mpv=float("inf")
        for s in strikes:
            pain=sum(max(0,x["s"]-s["s"])*x["ce"]+max(0,s["s"]-x["s"])*x["pe"] for x in strikes)
            if pain<mpv: mpv=pain; mp=s["s"]
        # ATM IV
        iv=0
        for x in f:
            if abs((x.get("strikePrice",0) or 0)-spot)<200:
                iv=x.get("CE",{}).get("impliedVolatility",0) or 0
                if iv: break
        ce_wall=max(strikes,key=lambda x:x["ce"])["s"] if strikes else 0
        pe_wall=max(strikes,key=lambda x:x["pe"])["s"] if strikes else 0
        return {"ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,"pcr":pcr,"max_pain":mp,"iv":iv,"ce_wall":ce_wall,"pe_wall":pe_wall,"spot":spot,"interp":"Bullish" if pcr>1.1 else "Bearish" if pcr<0.9 else "Neutral"}
    except: return None

@app.route("/")
def home():
    return jsonify({"status":"PRO Trader Server","time":now_ist().strftime("%d %b %Y %H:%M IST")})

@app.route("/market")
def market():
    result={}
    usd_inr = get_usd_inr()
    for sym in ["NIFTY","BANKNIFTY","SENSEX","FINNIFTY","RELIANCE","HDFCBANK","CRUDEOIL","GOLD"]:
        ticker=YAHOO.get(sym)
        if ticker:
            d=yahoo(ticker)
            if d and d.get("px"):
                # Convert MCX commodity prices from USD to INR
                if sym == "CRUDEOIL" and d["px"] < 500:
                    # WTI price in USD -> MCX price in INR (per barrel)
                    factor = usd_inr
                    d["px"] = round(d["px"] * factor, 2)
                    d["chg"] = round(d["chg"] * factor, 2)
                    d["high"] = round(d["high"] * factor, 2) if d.get("high") else 0
                    d["low"] = round(d["low"] * factor, 2) if d.get("low") else 0
                    d["currency"] = "INR"
                    d["note"] = f"MCX approx (WTI x {usd_inr:.1f})"
                elif sym == "GOLD" and d["px"] < 5000:
                    # Gold USD/oz -> MCX INR per 10g
                    # 1 oz = 31.1g, MCX lot = 1kg = 1000g = per 10g unit
                    factor = usd_inr / 31.1 * 10
                    d["px"] = round(d["px"] * factor, 2)
                    d["chg"] = round(d["chg"] * factor, 2)
                    d["high"] = round(d["high"] * factor, 2) if d.get("high") else 0
                    d["low"] = round(d["low"] * factor, 2) if d.get("low") else 0
                    d["currency"] = "INR"
                    d["note"] = f"MCX approx (COMEX x {factor:.1f})"
                result[sym]=d
    vix_val=17.5
    vd=nse_get("https://www.nseindia.com/api/allIndices")
    if vd:
        for item in vd.get("data",[]):
            if "VIX" in (item.get("indexSymbol","") or ""):
                vix_val=item.get("last",17.5); break
    for sym in ["NIFTY","BANKNIFTY"]:
        if sym in result:
            oi=calc_oi(sym)
            if oi: result[sym]["oi"]=oi
    return jsonify({"ok":True,"data":result,"vix":vix_val,"time":now_ist().strftime("%H:%M:%S")})

@app.route("/oi/<sym>")
def oi(sym):
    d=calc_oi(sym.upper())
    if d: return jsonify({"ok":True,"sym":sym.upper(),"data":d})
    return jsonify({"ok":False,"error":"NSE unavailable"}),503

@app.route("/price/<sym>")
def price(sym):
    t=YAHOO.get(sym.upper())
    if not t: return jsonify({"error":"Unknown"}),400
    return jsonify(yahoo(t,request.args.get("interval","5m"),request.args.get("range","1d")))

if __name__=="__main__":
    nse_cookies()
    app.run(host="0.0.0.0",port=10000,debug=False)

# ══════════════════════════════════════════
# SMART MONEY CONCEPTS ENGINE
# Calculates FVG, Order Blocks, Liquidity Sweeps from candle data
# ══════════════════════════════════════════

def calc_smc(candles):
    """
    Calculate Smart Money Concepts from OHLC candles
    Returns: FVGs, Order Blocks, Liquidity Sweeps, Market Structure
    """
    if not candles or len(candles) < 10:
        return {}

    n = len(candles)
    result = {
        "fvg": [],           # Fair Value Gaps
        "ob": [],            # Order Blocks
        "liquidity": [],     # Liquidity pools (equal highs/lows)
        "sweep": [],         # Liquidity sweeps
        "bos": [],           # Break of Structure
        "structure": "UNKNOWN"
    }

    # ── FAIR VALUE GAPS ──
    # Bullish FVG: candle[i-1].high < candle[i+1].low (gap between wicks)
    # Bearish FVG: candle[i-1].low > candle[i+1].high
    for i in range(1, n-1):
        c_prev = candles[i-1]
        c_curr = candles[i]
        c_next = candles[i+1]

        # Bullish FVG: fast up move, gap between prev high and next low
        if c_prev["h"] < c_next["l"]:
            size = c_next["l"] - c_prev["h"]
            size_pct = size / c_curr["c"] * 100
            if size_pct > 0.1:  # meaningful gap
                result["fvg"].append({
                    "type": "BULLISH",
                    "top": round(c_next["l"], 2),
                    "bottom": round(c_prev["h"], 2),
                    "mid": round((c_next["l"] + c_prev["h"]) / 2, 2),
                    "size_pct": round(size_pct, 2),
                    "t": c_curr["t"],
                    "filled": c_curr["l"] <= c_prev["h"]  # if price came back to fill
                })

        # Bearish FVG: fast down move, gap between prev low and next high
        if c_prev["l"] > c_next["h"]:
            size = c_prev["l"] - c_next["h"]
            size_pct = size / c_curr["c"] * 100
            if size_pct > 0.1:
                result["fvg"].append({
                    "type": "BEARISH",
                    "top": round(c_prev["l"], 2),
                    "bottom": round(c_next["h"], 2),
                    "mid": round((c_prev["l"] + c_next["h"]) / 2, 2),
                    "size_pct": round(size_pct, 2),
                    "t": c_curr["t"],
                    "filled": c_curr["h"] >= c_prev["l"]
                })

    # Keep last 5 unfilled FVGs (most recent = most relevant)
    unfilled = [f for f in result["fvg"] if not f["filled"]]
    result["fvg"] = unfilled[-5:] if unfilled else result["fvg"][-3:]

    # ── ORDER BLOCKS ──
    # Bullish OB: last BEARISH candle before a strong up move (institutional buy zone)
    # Bearish OB: last BULLISH candle before a strong down move (institutional sell zone)
    for i in range(2, n-2):
        c = candles[i]
        c_next = candles[i+1]
        c_next2 = candles[i+2] if i+2 < n else c_next

        body = abs(c["c"] - c["o"])
        next_move = abs(c_next["c"] - c_next["o"])

        # Bullish OB: bearish candle followed by strong bull move with displacement
        if c["c"] < c["o"] and c_next["c"] > c_next["o"] and next_move > body * 1.5:
            # Check for displacement (FVG after)
            has_displacement = c_next["h"] > c["h"] * 1.002  # breaks previous high
            if has_displacement:
                result["ob"].append({
                    "type": "BULLISH",
                    "top": round(max(c["o"], c["c"]), 2),
                    "bottom": round(min(c["o"], c["c"]), 2),
                    "mid": round((c["o"] + c["c"]) / 2, 2),
                    "t": c["t"],
                    "strength": round(next_move / body, 1),
                    "valid": True
                })

        # Bearish OB: bullish candle followed by strong bear move with displacement
        if c["c"] > c["o"] and c_next["c"] < c_next["o"] and next_move > body * 1.5:
            has_displacement = c_next["l"] < c["l"] * 0.998
            if has_displacement:
                result["ob"].append({
                    "type": "BEARISH",
                    "top": round(max(c["o"], c["c"]), 2),
                    "bottom": round(min(c["o"], c["c"]), 2),
                    "mid": round((c["o"] + c["c"]) / 2, 2),
                    "t": c["t"],
                    "strength": round(next_move / body, 1),
                    "valid": True
                })

    # Keep last 3 OBs of each type
    bull_obs = [o for o in result["ob"] if o["type"] == "BULLISH"][-2:]
    bear_obs = [o for o in result["ob"] if o["type"] == "BEARISH"][-2:]
    result["ob"] = bull_obs + bear_obs

    # ── LIQUIDITY POOLS & SWEEPS ──
    # Equal highs/lows within 0.15% = liquidity pool (stop hunts cluster here)
    highs = [(i, candles[i]["h"]) for i in range(n)]
    lows = [(i, candles[i]["l"]) for i in range(n)]
    curr_px = candles[-1]["c"]

    # Find clusters of equal highs (buy-side liquidity)
    for i in range(len(highs)-3, max(0, len(highs)-20), -1):
        h1 = highs[i][1]
        similar = [h for h in highs[i+1:i+10] if abs(h[1]-h1)/h1 < 0.0015]
        if len(similar) >= 2:
            level = round(max([h1]+[h[1] for h in similar]), 2)
            # Check if recently swept (price went above then came back)
            swept = any(candles[j]["h"] > level * 1.001 and candles[j]["c"] < level for j in range(i+1, n))
            result["liquidity"].append({
                "type": "BUY_SIDE",
                "level": level,
                "swept": swept,
                "above_current": level > curr_px
            })
            if swept:
                result["sweep"].append({
                    "type": "BULL_SWEEP",  # swept highs = bullish trap = bearish reversal likely
                    "level": level,
                    "signal": "Price swept buy-side liquidity above " + str(level) + " — reversal possible"
                })
            break

    # Find clusters of equal lows (sell-side liquidity)
    for i in range(len(lows)-3, max(0, len(lows)-20), -1):
        l1 = lows[i][1]
        similar = [l for l in lows[i+1:i+10] if abs(l[1]-l1)/l1 < 0.0015]
        if len(similar) >= 2:
            level = round(min([l1]+[l[1] for l in similar]), 2)
            swept = any(candles[j]["l"] < level * 0.999 and candles[j]["c"] > level for j in range(i+1, n))
            result["liquidity"].append({
                "type": "SELL_SIDE",
                "level": level,
                "swept": swept,
                "below_current": level < curr_px
            })
            if swept:
                result["sweep"].append({
                    "type": "BEAR_SWEEP",  # swept lows = bearish trap = bullish reversal likely
                    "level": level,
                    "signal": "Price swept sell-side liquidity below " + str(level) + " — reversal possible"
                })
            break

    # ── MARKET STRUCTURE ──
    # Simple: compare recent swing highs/lows
    if n >= 10:
        recent = candles[-10:]
        mid = candles[-5:]
        recent_h = max(c["h"] for c in recent[:5])
        current_h = max(c["h"] for c in recent[5:])
        recent_l = min(c["l"] for c in recent[:5])
        current_l = min(c["l"] for c in recent[5:])
        if current_h > recent_h and current_l > recent_l:
            result["structure"] = "BULLISH"  # HH + HL
        elif current_h < recent_h and current_l < recent_l:
            result["structure"] = "BEARISH"  # LH + LL
        else:
            result["structure"] = "RANGING"

    return result


@app.route("/smc/<sym>")
def smc_data(sym):
    """Get Smart Money Concepts data for a symbol"""
    ticker = YAHOO.get(sym.upper())
    if not ticker:
        return jsonify({"error": "Unknown symbol"}), 400
    # Use 15-min candles for better SMC detection
    data = yahoo(ticker, "15m", "5d")
    if not data or "candles" not in data:
        return jsonify({"error": "No data"}), 503
    smc = calc_smc(data.get("candles", []))
    return jsonify({
        "ok": True, "sym": sym.upper(),
        "px": data.get("px", 0),
        "smc": smc,
        "candle_count": len(data.get("candles", [])),
        "time": now_ist().strftime("%H:%M:%S")
    })


@app.route("/full/<sym>")
def full_data(sym):
    """Full data: price + technicals + SMC + OI all in one call"""
    sym = sym.upper()
    ticker = YAHOO.get(sym)
    if not ticker:
        return jsonify({"error": "Unknown"}), 400
    # Get candles
    data = yahoo(ticker, "5m", "1d")
    data15 = yahoo(ticker, "15m", "5d")  # for SMC (needs more history)
    smc = calc_smc(data15.get("candles", []) if data15 else [])
    # Get OI if index
    oi = None
    if sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
        try: oi = calc_oi(sym)
        except: pass
    return jsonify({
        "ok": True, "sym": sym,
        "price": data,
        "smc": smc,
        "oi": oi,
        "time": now_ist().strftime("%H:%M:%S")
    })


# ── Keep-alive endpoint ──
@app.route("/ping")
def ping():
    return jsonify({"ok": True, "time": now_ist().strftime("%H:%M:%S IST")})

@app.route("/usdinr")
def usdinr():
    rate = get_usd_inr()
    return jsonify({"rate": rate, "time": now_ist().strftime("%H:%M:%S IST")})

@app.route("/mtf/<sym>")
def multi_tf(sym):
    """Multi-timeframe SMA + CPR for a symbol"""
    ticker = YAHOO.get(sym.upper())
    if not ticker:
        return jsonify({"error": "Unknown symbol"}), 400
    data = get_multi_tf(ticker)
    data["sym"] = sym.upper()
    data["time"] = now_ist().strftime("%H:%M:%S IST")
    return jsonify({"ok": True, "data": data})
