from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)

app = Flask(__name__)
CORS(app)

# NSE Session
nse = requests.Session()
nse.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin"
})
_nse_cookie_t = 0

def nse_cookies():
    global _nse_cookie_t
    if time.time()-_nse_cookie_t > 240:
        try:
            nse.get("https://www.nseindia.com", timeout=8)
            time.sleep(0.5)
            nse.get("https://www.nseindia.com/option-chain", timeout=8)
            time.sleep(0.3)
            _nse_cookie_t = time.time()
        except: pass

def nse_get(url):
    nse_cookies()
    try: return nse.get(url,timeout=10).json()
    except: return None

YAHOO = {
    # Indices
    "NIFTY":"^NSEI","BANKNIFTY":"^NSEBANK","SENSEX":"^BSESN","FINNIFTY":"NIFTY_FIN_SERVICE.NS",
    # Commodities
    "CRUDEOIL":"CL=F","GOLD":"GC=F",
    # Banking & Finance
    "HDFCBANK":"HDFCBANK.NS","ICICIBANK":"ICICIBANK.NS","KOTAKBANK":"KOTAKBANK.NS",
    "AXISBANK":"AXISBANK.NS","SBIN":"SBIN.NS","INDUSINDBK":"INDUSINDBK.NS",
    "BAJFINANCE":"BAJFINANCE.NS","BAJAJFINSV":"BAJAJFINSV.NS",
    # IT
    "TCS":"TCS.NS","INFY":"INFY.NS","WIPRO":"WIPRO.NS","HCLTECH":"HCLTECH.NS",
    "TECHM":"TECHM.NS","LTIM":"LTIM.NS",
    # Energy
    "RELIANCE":"RELIANCE.NS","ONGC":"ONGC.NS","BPCL":"BPCL.NS",
    "POWERGRID":"POWERGRID.NS","NTPC":"NTPC.NS","COALINDIA":"COALINDIA.NS",
    # Auto
    "MARUTI":"MARUTI.NS","TATAMOTORS":"TATAMOTORS.NS","M&M":"M&M.NS",
    "BAJAJ-AUTO":"BAJAJ-AUTO.NS","EICHERMOT":"EICHERMOT.NS","HEROMOTOCO":"HEROMOTOCO.NS",
    # Pharma
    "SUNPHARMA":"SUNPHARMA.NS","DRREDDY":"DRREDDY.NS","CIPLA":"CIPLA.NS",
    "DIVISLAB":"DIVISLAB.NS","APOLLOHOSP":"APOLLOHOSP.NS",
    # FMCG
    "HINDUNILVR":"HINDUNILVR.NS","NESTLEIND":"NESTLEIND.NS","ITC":"ITC.NS",
    "BRITANNIA":"BRITANNIA.NS","TITAN":"TITAN.NS","ASIANPAINT":"ASIANPAINT.NS",
    "TATACONSUM":"TATACONSUM.NS","TRENT":"TRENT.NS",
    # Metal
    "TATASTEEL":"TATASTEEL.NS","HINDALCO":"HINDALCO.NS","JSWSTEEL":"JSWSTEEL.NS",
    # Infra & Others
    "LT":"LT.NS","ADANIPORTS":"ADANIPORTS.NS","ULTRACEMCO":"ULTRACEMCO.NS",
    "GRASIM":"GRASIM.NS","BHARTIARTL":"BHARTIARTL.NS","SHRIRAMFIN":"SHRIRAMFIN.NS",
    "BEL":"BEL.NS","INDIGO":"INDIGO.NS","HAL":"HAL.NS"
}
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

def yahoo(ticker, interval="5m", rng="2d"):
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
        # OI Buildup Analysis (most important for F&O)
        buildup = "UNKNOWN"
        if ce_chg > 0 and pe_chg > 0:
            if pe_chg > ce_chg: buildup = "LONG_BUILDUP"      # Put writers dominant = bullish
            else: buildup = "SHORT_BUILDUP"                     # Call writers dominant = bearish
        elif ce_chg < 0 and pe_chg < 0:
            if ce_chg < pe_chg: buildup = "SHORT_COVERING"    # Call longs exiting = bullish
            else: buildup = "LONG_UNWINDING"                   # Put longs exiting = bearish
        elif ce_chg > 0 and pe_chg < 0: buildup = "SHORT_BUILDUP"
        elif ce_chg < 0 and pe_chg > 0: buildup = "LONG_BUILDUP"

        # Top 3 CE and PE strikes by OI change
        ce_strikes = sorted([{"s":x.get("strikePrice",0),"chg":x.get("CE",{}).get("changeinOpenInterest",0) or 0,"oi":x.get("CE",{}).get("openInterest",0) or 0,"iv":x.get("CE",{}).get("impliedVolatility",0) or 0} for x in f if x.get("CE")], key=lambda x:-x["chg"])[:3]
        pe_strikes = sorted([{"s":x.get("strikePrice",0),"chg":x.get("PE",{}).get("changeinOpenInterest",0) or 0,"oi":x.get("PE",{}).get("openInterest",0) or 0,"iv":x.get("PE",{}).get("impliedVolatility",0) or 0} for x in f if x.get("PE")], key=lambda x:-x["chg"])[:3]

        # PCR interpretation
        if pcr > 1.4: pcr_interp = "EXTREME_BULL (contrarian — watch for reversal)"
        elif pcr > 1.1: pcr_interp = "BULLISH — put writers active"
        elif pcr > 0.9: pcr_interp = "NEUTRAL — watch for breakout"
        elif pcr > 0.7: pcr_interp = "BEARISH — call writers active"
        else: pcr_interp = "EXTREME_BEAR (contrarian — watch for bounce)"

        return {"ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,
                "pcr":pcr,"max_pain":mp,"iv":iv,"ce_wall":ce_wall,"pe_wall":pe_wall,
                "spot":spot,"buildup":buildup,
                "ce_top":ce_strikes,"pe_top":pe_strikes,
                "pcr_interp":pcr_interp,
                "mp_dist":round(spot-mp,2) if spot and mp else 0,
                "interp":"Bullish" if pcr>1.1 else "Bearish" if pcr<0.9 else "Neutral"}
    except Exception as e:
        return None

@app.route("/")
def home():
    return jsonify({"status":"PRO Trader Server","time":now_ist().strftime("%d %b %Y %H:%M IST")})

# Cache for stock data (updated every 5 min in background)
_stock_cache = {}
_stock_cache_ts = 0

def update_stock_cache():
    """Background update of all 50 stocks - called every 5 min"""
    global _stock_cache, _stock_cache_ts
    usd_inr = get_usd_inr()
    stock_syms = [s for s in YAHOO.keys() if s not in ["NIFTY","BANKNIFTY","SENSEX","FINNIFTY","CRUDEOIL","GOLD"]]
    result = {}
    for sym in stock_syms:
        ticker = YAHOO.get(sym)
        if ticker:
            d = yahoo(ticker)
            if d and d.get("px"):
                result[sym] = d
    _stock_cache = result
    _stock_cache_ts = time.time()
    return result

@app.route("/stocks")
def stocks():
    """Returns all Nifty 50 stock data - cached, updated every 5 min"""
    global _stock_cache, _stock_cache_ts
    if not _stock_cache or (time.time() - _stock_cache_ts) > 300:
        update_stock_cache()
    return jsonify({"ok":True, "data":_stock_cache, "ts":_stock_cache_ts,
                    "age_s": int(time.time()-_stock_cache_ts)})

# Zerodha NSE symbol mapping (exchange:tradingsymbol)
KITE_SYMS = {
    # Indices
    "NIFTY":     "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "FINNIFTY":  "NSE:NIFTY FIN SERVICE",
    "SENSEX":    "BSE:SENSEX",
    # Banking & Finance
    "HDFCBANK":"NSE:HDFCBANK","ICICIBANK":"NSE:ICICIBANK","KOTAKBANK":"NSE:KOTAKBANK",
    "AXISBANK":"NSE:AXISBANK","SBIN":"NSE:SBIN","INDUSINDBK":"NSE:INDUSINDBK",
    "BAJFINANCE":"NSE:BAJFINANCE","BAJAJFINSV":"NSE:BAJAJFINSV",
    # IT
    "TCS":"NSE:TCS","INFY":"NSE:INFY","WIPRO":"NSE:WIPRO","HCLTECH":"NSE:HCLTECH",
    "TECHM":"NSE:TECHM","LTIM":"NSE:LTIM",
    # Energy
    "RELIANCE":"NSE:RELIANCE","ONGC":"NSE:ONGC","BPCL":"NSE:BPCL",
    "POWERGRID":"NSE:POWERGRID","NTPC":"NSE:NTPC","COALINDIA":"NSE:COALINDIA",
    # Auto
    "MARUTI":"NSE:MARUTI","TATAMOTORS":"NSE:TATAMOTORS","M&M":"NSE:M&M",
    "BAJAJ-AUTO":"NSE:BAJAJ-AUTO","EICHERMOT":"NSE:EICHERMOT","HEROMOTOCO":"NSE:HEROMOTOCO",
    # Pharma
    "SUNPHARMA":"NSE:SUNPHARMA","DRREDDY":"NSE:DRREDDY","CIPLA":"NSE:CIPLA",
    "DIVISLAB":"NSE:DIVISLAB","APOLLOHOSP":"NSE:APOLLOHOSP",
    # FMCG
    "HINDUNILVR":"NSE:HINDUNILVR","NESTLEIND":"NSE:NESTLEIND","ITC":"NSE:ITC",
    "BRITANNIA":"NSE:BRITANNIA","TITAN":"NSE:TITAN","ASIANPAINT":"NSE:ASIANPAINT",
    "TATACONSUM":"NSE:TATACONSUM","TRENT":"NSE:TRENT",
    # Metal
    "TATASTEEL":"NSE:TATASTEEL","HINDALCO":"NSE:HINDALCO","JSWSTEEL":"NSE:JSWSTEEL",
    # Infra & Others
    "LT":"NSE:LT","ADANIPORTS":"NSE:ADANIPORTS","ULTRACEMCO":"NSE:ULTRACEMCO",
    "GRASIM":"NSE:GRASIM","BHARTIARTL":"NSE:BHARTIARTL","SHRIRAMFIN":"NSE:SHRIRAMFIN",
    "BEL":"NSE:BEL","INDIGO":"NSE:INDIGO","HAL":"NSE:HAL",
}
# Commodities stay on Yahoo (MCX data not in Zerodha equity segment)
MCX_YAHOO = {"CRUDEOIL":"CL=F","GOLD":"GC=F"}

# Technicals cache — Yahoo candle data, refreshed every 5 min
_tech_cache = {}
_tech_cache_ts = {}

def get_technicals(sym):
    """Get SMA20/50, RSI, trend from Yahoo candles. Cached 5 min."""
    now = time.time()
    if sym in _tech_cache and now - _tech_cache_ts.get(sym,0) < 300:
        return _tech_cache[sym]
    ticker = YAHOO.get(sym) or (KITE_SYMS.get(sym,"").replace("NSE:","") + ".NS")
    try:
        d = yahoo(ticker)
        if d and d.get("sma20"):
            tech = {
                "sma20": d["sma20"], "sma50": d["sma50"],
                "rsi": d.get("rsi"), "trend": d.get("trend","NEUTRAL"),
                "crossover": d.get("crossover","NONE"),
                "breakout": d.get("breakout",False),
                "breakdown": d.get("breakdown",False),
                "above_sma20": (d["px"] > d["sma20"]) if d.get("px") and d.get("sma20") else None,
                "high": d.get("high",0), "low": d.get("low",0),
                "open": d.get("open",0), "prev_close": d.get("prev_close",0),
            }
            _tech_cache[sym] = tech
            _tech_cache_ts[sym] = now
            return tech
    except: pass
    return _tech_cache.get(sym, {})  # return stale if fresh fetch failed

@app.route("/market")
def market():
    """
    Live prices from Zerodha Kite (real-time, no delay).
    Technicals (SMA/RSI) from Yahoo candles (cached 5 min).
    Commodities (Crude/Gold) from Yahoo MCX.
    VIX from Yahoo.
    Requires: ?key=API_KEY&token=ACCESS_TOKEN
    Falls back to Yahoo if no Zerodha credentials.
    """
    key   = request.args.get("key","")
    token = request.args.get("token","")

    result = {}
    usd_inr = get_usd_inr()

    # ── Live prices from Zerodha (if credentials provided) ──
    kite_data = {}
    if key and token:
        hdrs = {"X-Kite-Version":"3","Authorization":f"token {key}:{token}","User-Agent":"Mozilla/5.0"}
        try:
            # Bulk quote — all stocks + indices in ONE call
            all_kite = list(KITE_SYMS.values())
            # Kite allows max 500 instruments per call
            qs = "&".join(f"i={s}" for s in all_kite)
            r = requests.get(f"https://api.kite.trade/quote?{qs}", headers=hdrs, timeout=20)
            if r.status_code == 200:
                kdata = r.json().get("data",{})
                for sym, kite_sym in KITE_SYMS.items():
                    q = kdata.get(kite_sym,{})
                    if not q or not q.get("last_price"): continue
                    px  = q["last_price"]
                    pc  = q.get("ohlc",{}).get("close", px)  # prev close
                    chg = round(px - pc, 2)
                    pct = round(chg/pc*100, 2) if pc else 0
                    kite_data[sym] = {
                        "px": px, "chg": chg, "pct": pct,
                        "high": q.get("ohlc",{}).get("high",0),
                        "low":  q.get("ohlc",{}).get("low",0),
                        "open": q.get("ohlc",{}).get("open",0),
                        "prev_close": pc,
                        "volume": q.get("volume",0),
                        "source": "zerodha"
                    }
                print(f"[Kite] Got prices for {len(kite_data)} symbols")
            elif r.status_code in [401,403]:
                print(f"[Kite] Token expired: {r.status_code}")
        except Exception as e:
            print(f"[Kite] Quote error: {e}")

    use_kite = len(kite_data) > 10  # only use Kite if we got meaningful data

    # ── Build result for each symbol ──
    def build_sym(sym):
        # Live price
        if use_kite and sym in kite_data:
            d = dict(kite_data[sym])
        elif sym in MCX_YAHOO:
            # Commodities — Yahoo only
            yd = yahoo(MCX_YAHOO[sym])
            if not yd or not yd.get("px"): return sym, None
            d = {"px":yd["px"],"chg":yd["chg"],"pct":yd.get("pct",0),
                 "high":yd.get("high",0),"low":yd.get("low",0),
                 "open":yd.get("open",0),"prev_close":yd.get("prev_close",0),"source":"yahoo_mcx"}
            # Convert USD to INR
            if sym == "CRUDEOIL" and d["px"] < 500:
                d["px"]=round(d["px"]*usd_inr,2); d["chg"]=round(d["chg"]*usd_inr,2)
            elif sym == "GOLD" and d["px"] < 5000:
                f=usd_inr/31.1*10
                d["px"]=round(d["px"]*f,2); d["chg"]=round(d["chg"]*f,2)
        else:
            # Fallback to Yahoo if no Kite
            ticker = YAHOO.get(sym)
            if not ticker: return sym, None
            yd = yahoo(ticker)
            if not yd or not yd.get("px"): return sym, None
            d = {"px":yd["px"],"chg":yd["chg"],"pct":yd.get("pct",0),
                 "high":yd.get("high",0),"low":yd.get("low",0),
                 "open":yd.get("open",0),"prev_close":yd.get("prev_close",0),"source":"yahoo"}

        if not d.get("px"): return sym, None

        # Add technicals (SMA/RSI) — from Yahoo candles cache
        if sym not in MCX_YAHOO:
            tech = get_technicals(sym)
            if tech:
                d["sma20"]     = tech.get("sma20")
                d["sma50"]     = tech.get("sma50")
                d["technicals"] = {
                    "rsi14":    tech.get("rsi"),
                    "trend":    tech.get("trend","NEUTRAL"),
                    "crossover":tech.get("crossover","NONE"),
                    "breakout": tech.get("breakout",False),
                    "breakdown":tech.get("breakdown",False),
                    "above_sma20": tech.get("above_sma20"),
                }
        return sym, d

    # All symbols = KITE stocks + commodities
    all_syms = list(KITE_SYMS.keys()) + list(MCX_YAHOO.keys())

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(build_sym, sym): sym for sym in all_syms}
        try:
            for fut in as_completed(futures, timeout=40):
                try:
                    sym, d = fut.result()
                    if d: result[sym] = d
                except: pass
        except:
            for fut in futures:
                if fut.done():
                    try:
                        sym, d = fut.result()
                        if d: result[sym] = d
                    except: pass

    if not result:
        return jsonify({"ok":False,"error":"No market data"}),503

    # VIX — Yahoo only (NSE VIX not in Zerodha)
    vix_val = 17.5
    try:
        vd = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX?interval=1d&range=1d",
            headers={"User-Agent":"Mozilla/5.0"}, timeout=5).json()
        px = vd["chart"]["result"][0]["meta"]["regularMarketPrice"]
        if px: vix_val = round(px,2)
    except: pass

    return jsonify({
        "ok":True,"data":result,"vix":vix_val,
        "source": "zerodha" if use_kite else "yahoo",
        "time":now_ist().strftime("%H:%M:%S")
    })

# OI data cache - serves last known data when NSE is unavailable
_oi_cache = {}

@app.route("/oi/<sym>")
def oi(sym):
    sym = sym.upper()
    d = calc_oi(sym)
    if d:
        _oi_cache[sym] = {"data": d, "ts": time.time()}
        return jsonify({"ok": True, "sym": sym, "data": d, "source": "live"})
    # Return cached data when NSE unavailable (market closed etc)
    if sym in _oi_cache:
        cached = _oi_cache[sym]
        age = int((time.time() - cached["ts"]) / 60)
        d2 = dict(cached["data"])
        d2["note"] = "Cached ({} min ago)".format(age)
        return jsonify({"ok": True, "sym": sym, "data": d2, "source": "cache", "age_min": age})
    return jsonify({"ok": False, "error": "NSE unavailable and no cache yet"}), 503

@app.route("/price/<sym>")
def price(sym):
    t=YAHOO.get(sym.upper())
    if not t: return jsonify({"error":"Unknown"}),400
    return jsonify(yahoo(t,request.args.get("interval","5m"),request.args.get("range","1d")))

# Background stock updater
import threading
def bg_stock_updater():
    while True:
        try:
            update_stock_cache()
        except: pass
        time.sleep(300) # every 5 min

if __name__=="__main__":
    nse_cookies()
    # Start background stock updater
    t = threading.Thread(target=bg_stock_updater, daemon=True)
    t.start()
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
@app.route("/kite_proxy")
def kite_proxy():
    """Proxy Kite API calls to avoid browser CORS restrictions"""
    from flask import request as req
    url = req.args.get("url","")
    key = req.args.get("key","")
    token = req.args.get("token","")
    if not url or "kite.trade" not in url:
        return jsonify({"error":"invalid url"}),400
    if not key or not token:
        return jsonify({"error":"missing auth"}),400
    try:
        r = requests.get(url,
            headers={
                "X-Kite-Version":"3",
                "Authorization":"token "+key+":"+token,
                "User-Agent":"Mozilla/5.0"
            },
            timeout=15
        )
        data = r.json()
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),503


@app.route("/zerodha_oi")
def zerodha_oi():
    """
    Fetch live OI from Zerodha Kite API.
    Persists last good OI to disk so it survives server restarts.
    """
    import os, json as _json
    from flask import request as req
    sym   = req.args.get("sym","NIFTY").upper()
    key   = req.args.get("key","")
    token = req.args.get("token","")

    # ── Persistent disk cache (survives Render sleep/restart) ──
    CACHE_DIR = "/tmp/oi_cache"
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = f"{CACHE_DIR}/{sym}.json"

    def save_cache(data):
        try:
            with open(cache_file,"w") as f:
                _json.dump({"data":data,"ts":time.time()},f)
        except: pass

    def load_cache():
        try:
            with open(cache_file) as f:
                c = _json.load(f)
                age = int((time.time()-c['ts'])/60)
                d = dict(c['data'])
                d['note'] = f"Last known OI — {age}min ago (market closed or token expired)"
                d['cached'] = True
                return jsonify({"ok":True,"sym":sym,"data":d,"source":"cache","age_min":age})
        except: return None

    if not key or not token:
        return load_cache() or jsonify({"ok":False,"error":"No Zerodha credentials"}),400

    hdrs = {"X-Kite-Version":"3","Authorization":f"token {key}:{token}","User-Agent":"Mozilla/5.0"}

    # In-memory instruments CSV cache (reset on restart — that's fine, re-downloads once)
    if not hasattr(zerodha_oi,'_csv'): zerodha_oi._csv={}
    csv_cache = zerodha_oi._csv

    try:
        # Step 1: spot price
        idx_map={"NIFTY":"NSE:NIFTY 50","BANKNIFTY":"NSE:NIFTY BANK",
                 "FINNIFTY":"NSE:NIFTY FIN SERVICE","SENSEX":"BSE:SENSEX"}
        if sym not in idx_map:
            return jsonify({"ok":False,"error":f"{sym} not supported"}),400

        r=requests.get("https://api.kite.trade/quote",params={"i":idx_map[sym]},headers=hdrs,timeout=10)
        if r.status_code in [401,403]:
            # Token expired — return last cached OI so app still shows data
            return load_cache() or jsonify({"ok":False,"error":"Zerodha token expired — reconnect in Settings","code":403}),403

        spot=0
        for v in (r.json().get("data") or {}).values():
            spot=v.get("last_price",0); break
        if not spot:
            return load_cache() or jsonify({"ok":False,"error":"Cannot get spot price"}),503

        step=50 if sym in ["NIFTY","FINNIFTY"] else 100
        atm=int(round(spot/step)*step)
        strikes=[atm+step*i for i in range(-10,11)]

        # Step 2: instruments CSV (in-memory cache 4h)
        csv_key=f"csv_{sym}"
        if csv_key not in csv_cache or (time.time()-csv_cache[csv_key]['ts'])>14400:
            rc=requests.get("https://api.kite.trade/instruments/NFO",headers=hdrs,timeout=30)
            if rc.status_code in [401,403]:
                return load_cache() or jsonify({"ok":False,"error":"Token expired","code":403}),403
            if rc.status_code!=200:
                return load_cache() or jsonify({"ok":False,"error":f"Instruments error {rc.status_code}"}),503
            csv_cache[csv_key]={'data':rc.text,'ts':time.time()}
            print(f"[Kite] Instruments CSV cached for {sym}")

        lines=csv_cache[csv_key]['data'].strip().split("\n")

        # Find nearest expiry
        today_n=datetime.now(IST).replace(tzinfo=None)
        best_exp=None; best_days=999
        for line in lines[1:3000]:
            cols=line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym): continue
            if cols[9] not in ["CE","PE"]: continue
            try:
                exp=datetime.strptime(cols[5],"%Y-%m-%d")
                d2=(exp-today_n).days
                if 0<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue
        if not best_exp:
            return load_cache() or jsonify({"ok":False,"error":"No expiry found"}),503

        # Collect ATM±10 instruments
        target=set(strikes)
        instruments=[]
        for line in lines[1:]:
            cols=line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym): continue
            if cols[9] not in ["CE","PE"] or cols[5]!=best_exp: continue
            try:
                sk=float(cols[6])
                if sk in target:
                    instruments.append({"sym":f"NFO:{cols[2]}","strike":int(sk),"type":cols[9]})
            except: continue
        if not instruments:
            return load_cache() or jsonify({"ok":False,"error":"No instruments found"}),503

        # Step 3: Bulk OI quote
        qs="&".join(f"i={i['sym']}" for i in instruments)
        r2=requests.get(f"https://api.kite.trade/quote?{qs}",headers=hdrs,timeout=20)
        if r2.status_code in [401,403]:
            return load_cache() or jsonify({"ok":False,"error":"Token expired","code":403}),403
        qdata=r2.json().get("data",{})

        # Step 4: Aggregate
        ce_oi=0;pe_oi=0;ce_chg=0;pe_chg=0
        strikes_data={}
        for inst in instruments:
            q=qdata.get(inst['sym'],{})
            if not q: continue
            oi=q.get("oi",0) or 0
            oi_low=q.get("oi_day_low",0) or 0
            chg=oi-oi_low if oi_low else 0
            s=inst['strike']
            if s not in strikes_data: strikes_data[s]={"ce":0,"pe":0,"ce_chg":0,"pe_chg":0}
            if inst['type']=="CE":
                ce_oi+=oi;ce_chg+=chg
                strikes_data[s]["ce"]=oi;strikes_data[s]["ce_chg"]=chg
            else:
                pe_oi+=oi;pe_chg+=chg
                strikes_data[s]["pe"]=oi;strikes_data[s]["pe_chg"]=chg

        # If OI is zero (market closed), return last cached data
        if not ce_oi and not pe_oi:
            return load_cache() or jsonify({"ok":False,"error":"OI is zero — market closed, no cache yet"}),503

        # Step 5: Calculate metrics
        pcr=round(pe_oi/ce_oi,2) if ce_oi else 0
        mp=0;mpv=float("inf")
        for s in strikes_data:
            pain=sum(max(0,x-s)*strikes_data[x]["ce"]+max(0,s-x)*strikes_data[x]["pe"] for x in strikes_data)
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

        result={"ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,
                "pcr":pcr,"max_pain":int(mp),"iv":0,
                "ce_wall":int(ce_wall),"pe_wall":int(pe_wall),
                "spot":round(spot,1),"buildup":buildup,"pcr_interp":pcr_interp,
                "mp_dist":round(spot-mp,0) if mp else 0,
                "source":"zerodha_kite","expiry":best_exp,
                "instruments_count":len(instruments)}

        # Save to disk — persists across restarts
        save_cache(result)
        print(f"[Kite OI ✅] {sym} PCR:{pcr} MP:{mp} CE:{ce_oi} PE:{pe_oi} Exp:{best_exp}")
        return jsonify({"ok":True,"sym":sym,"data":result,"time":now_ist().strftime("%H:%M:%S IST")})

    except Exception as e:
        import traceback
        print(f"[Kite OI ERROR] {sym}: {e}\n{traceback.format_exc()[-300:]}")
        return load_cache() or jsonify({"ok":False,"error":str(e)}),503

@app.route("/debug_oi")
def debug_oi():
    """Debug endpoint - shows exactly what OI fetch returns"""
    from flask import request as req
    sym = req.args.get("sym","NIFTY").upper()
    key = req.args.get("key","")
    token = req.args.get("token","")
    result = {"sym":sym,"has_key":bool(key),"has_token":bool(token),
              "key_prefix":key[:8]+"..." if key else "EMPTY",
              "nse_test":None,"kite_test":None}
    # Test Kite token validity
    if key and token:
        try:
            h = {"X-Kite-Version":"3","Authorization":f"token {key}:{token}"}
            r = requests.get("https://api.kite.trade/user/profile",headers=h,timeout=10)
            result["kite_test"] = {"status":r.status_code,"ok":r.status_code==200,
                                    "response":r.json() if r.status_code==200 else r.text[:200]}
        except Exception as e:
            result["kite_test"] = {"error":str(e)}
    # Test NSE directly
    try:
        nse_cookies()
        r2 = nse.get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",timeout=10)
        result["nse_test"] = {"status":r2.status_code,"ok":r2.status_code==200,
                               "has_data":bool(r2.status_code==200 and r2.json().get("filtered"))}
    except Exception as e:
        result["nse_test"] = {"error":str(e)}
    return jsonify(result)

@app.route("/test_oi")
def test_oi():
    """Quick diagnostic - test if Zerodha token works"""
    from flask import request as req
    key = req.args.get("key","")
    token = req.args.get("token","")
    if not key or not token:
        return jsonify({"error":"pass ?key=...&token=..."})
    headers={"X-Kite-Version":"3","Authorization":f"token {key}:{token}","User-Agent":"Mozilla/5.0"}
    try:
        r = requests.get("https://api.kite.trade/user/profile",headers=headers,timeout=10)
        profile = r.json() if r.status_code==200 else {"status_code":r.status_code,"text":r.text[:200]}
        r2 = requests.get("https://api.kite.trade/quote?i=NSE%3ANIFTY+50",headers=headers,timeout=10)
        quote = r2.json() if r2.status_code==200 else {"status_code":r2.status_code}
        return jsonify({"profile_ok":r.status_code==200,"profile":profile,"quote_ok":r2.status_code==200,"quote":quote,"time":now_ist().strftime("%H:%M:%S IST")})
    except Exception as e:
        return jsonify({"error":str(e)})

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
