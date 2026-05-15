from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, json, time
from datetime import datetime

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
# MCX commodities are USD-priced on Yahoo - multiply by INR rate for Indian prices
MCX_SYMBOLS = {"CRUDEOIL", "GOLD"}
INR_RATE = 84  # approximate USD/INR

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

def calc_oi(sym):
    if sym in ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]:
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
    return jsonify({"status":"PRO Trader Server","time":datetime.now().strftime("%d %b %Y %H:%M IST")})

@app.route("/market")
def market():
    result={}
    for sym in ["NIFTY","BANKNIFTY","SENSEX","FINNIFTY","RELIANCE","HDFCBANK","CRUDEOIL","GOLD"]:
        ticker=YAHOO.get(sym)
        if ticker:
            d=yahoo(ticker)
            if d and d.get("px"): result[sym]=d
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
    return jsonify({"ok":True,"data":result,"vix":vix_val,"time":datetime.now().strftime("%H:%M:%S")})

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
