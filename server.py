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
import requests, time, threading, re, os, hmac
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ═══════════════════════════════════════════════════════════════
# LAYER 0 — SETUP & CONSTANTS
# ═══════════════════════════════════════════════════════════════

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def ist_str(): return now_ist().strftime("%H:%M:%S IST")

app = Flask(__name__)

# ── QUOTAGUARD STATIC IP — Kite Connect calls only ─────────────────────────
# SEBI mandated static-IP whitelisting for all broker API order requests,
# enforced since April 1, 2026 (SEBI/HO/MIRSD-PoD/P/CIR/2025/0000013).
# Render's default IPs are shared and change on every deploy/restart, so
# every Kite Connect call — not just order placement, to be safe — now
# routes through this session. When QUOTAGUARDSTATIC_URL isn't set (local
# dev, or before QuotaGuard is provisioned), it silently falls back to a
# normal direct connection so nothing breaks.
#
# Deliberately NOT applied to Yahoo Finance calls (query1.finance.yahoo.com)
# — Yahoo has no whitelist requirement, and routing that traffic through a
# paid proxy would just burn QuotaGuard's bandwidth allowance for no reason.
KITE_SESSION = requests.Session()
_quotaguard_url = os.environ.get("QUOTAGUARDSTATIC_URL", "").strip()
if _quotaguard_url:
    KITE_SESSION.proxies = {"http": _quotaguard_url, "https": _quotaguard_url}
    print("[QuotaGuard] Kite API calls routing through static IP proxy")
else:
    print("[QuotaGuard] QUOTAGUARDSTATIC_URL not set — Kite calls use Render's "
          "default (non-static) IP. Order placement will be rejected by "
          "Zerodha until this is configured — see setup notes.")

# ── SUPABASE — persistent signal + journal storage ─────────────────────────
# Uses the new sb_secret_... key (replaces the old JWT service_role key —
# same permissions, drop-in compatible with the client library). This key
# bypasses Row Level Security by design, which is why it must only ever be
# set here, as a Render environment variable, never in index.html.
#
# Falls back to None when unconfigured, same pattern as QUOTAGUARDSTATIC_URL —
# every write function below checks for that and no-ops with a log line
# rather than crashing, so the app keeps working exactly as before if
# Supabase isn't set up yet.
from supabase import create_client, Client as _SupabaseClient

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
SB: "_SupabaseClient | None" = None
if SUPABASE_URL and SUPABASE_SECRET_KEY:
    try:
        SB = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
        print("[Supabase] Connected — signals/journal persistence active")
    except Exception as _sbe:
        print(f"[Supabase] Failed to connect: {_sbe}")
else:
    print("[Supabase] SUPABASE_URL / SUPABASE_SECRET_KEY not set — "
          "signal/journal persistence disabled, app runs as before")


def write_signal(sig: dict, fired: bool, source: str = "worker"):
    """
    Persist one generated signal (fired into the feed, or filtered out below
    threshold — both are logged, see schema notes). Never raises — a
    Supabase hiccup should never take down a live scan.
    """
    if not SB: return
    try:
        SB.table("signals").insert({
            "sym": sig.get("sym"), "bias": sig.get("bias"), "fired": fired,
            "confidence": sig.get("confidence"), "urgency": sig.get("urgency"),
            "bull_score": sig.get("bullScore"), "bear_score": sig.get("bearScore"),
            "neutral_score": sig.get("neutralScore"), "layers": sig.get("layers"),
            "has_oi": sig.get("hasOI"), "setup_key": sig.get("setupKey"),
            "regime": sig.get("regime"),
            "entry_px": sig.get("entry"), "sl_px": sig.get("sl"),
            "t1_px": sig.get("target1"), "t2_px": sig.get("target2"),
            "rr": sig.get("rr"), "why": sig.get("why"), "vix": sig.get("vix"),
            "source": source,
        }).execute()
    except Exception as e:
        print(f"[Supabase] write_signal failed for {sig.get('sym')}: {e}")


def get_last_session_signals(limit_symbols: int = 40):
    """
    Read-only: most recent signal per symbol, for display when the market
    is closed and the live feed is empty. Returns [] on any failure or if
    Supabase isn't configured — this must never block the app from loading.

    Only HIGH/MEDIUM urgency — LOW-urgency signals aren't meaningful as a
    "here's what was happening" reference and would just add noise.

    Dedup to one-per-symbol happens here in Python rather than in the query
    itself: PostgREST's fluent query builder (what supabase-py wraps) doesn't
    expose SQL's DISTINCT ON, so the simplest correct approach is fetching a
    reasonably large recent batch (already ordered newest-first) and keeping
    only the first occurrence of each symbol.
    """
    if not SB: return []
    try:
        r = (SB.table("signals")
             .select("sym,bias,confidence,urgency,entry_px,sl_px,t1_px,t2_px,rr,why,created_at,setup_key,regime")
             .in_("urgency", ["HIGH", "MEDIUM"])
             .order("created_at", desc=True)
             .limit(300)
             .execute())
        rows = r.data or []
        seen = set()
        out = []
        for row in rows:
            sym = row.get("sym")
            if not sym or sym in seen: continue
            seen.add(sym)
            out.append(row)
            if len(out) >= limit_symbols: break
        return out
    except Exception as e:
        print(f"[Supabase] get_last_session_signals failed: {e}")
        return []


@app.route("/last_session_signals")
def last_session_signals():
    """
    Read-only endpoint for the closed-market reference view. Explicitly
    NOT meant to be treated as live/tradeable data by the client — that
    distinction is enforced client-side (separate render path from allSigs,
    no execute action available on these cards), this endpoint just returns
    whatever the most recent real signals were.
    """
    return jsonify({"ok": True, "signals": get_last_session_signals()})


def _normalize_setup_key_for_backtest(live_setup_key: str):
    """
    The backtest's setup_key taxonomy is NOT identical to the live engine's —
    see _bt_setup_key()'s own docstring. It never includes "OI" (no reliable
    intraday OI history when that function was written), and uses "ORB"
    where live uses "Structure"/"CHoCH". A live setup_key like
    "OI+Price+Momentum+CHoCH+Regime" cannot exist verbatim in backtest data —
    an exact-string match would silently fail for most real signals.

    This strips what the backtest can't produce and maps live's structure
    tag onto its closest backtest proxy, so an approximate match is still
    possible. Callers must treat this as a DIFFERENT, LABELED tier from an
    exact match — never present it as the same setup, since it isn't.
    Returns None if nothing meaningful survives the strip (backtest has no
    equivalent at all for this setup).
    """
    parts = (live_setup_key or "").split("+")
    mapped = set()
    for p in parts:
        if p == "OI": continue          # backtest never produces this
        if p in ("CHoCH", "Structure"): mapped.add("ORB"); continue
        if p in ("Price", "Momentum", "Regime"): mapped.add(p); continue
        # "Unconfirmed" or anything else unrecognized — skip
    order = ["Price", "Momentum", "ORB", "Regime"]
    normalized = [p for p in order if p in mapped]
    return "+".join(normalized) if normalized else None


def get_latest_backtest_summary():
    """
    Simple headline summary for the Backtest tab's default view — latest
    run's key numbers plus a trend comparison against the previous run, so
    "is my strategy improving or decaying" is answerable at a glance
    without digging into the 13 detailed breakdown tabs.
    """
    if not SB:
        return {"ok": True, "available": False, "reason": "supabase_not_configured"}
    try:
        r = (SB.table("backtest_runs")
             .select("win_rate,closed,total,profit_factor,created_at")
             .order("created_at", desc=True)
             .limit(2)
             .execute())
        rows = r.data or []
        if not rows:
            return {"ok": True, "available": False, "reason": "no_backtest_run_yet"}
        latest = rows[0]
        prev = rows[1] if len(rows) > 1 else None
        trend = None
        if prev and prev.get("win_rate") is not None and latest.get("win_rate") is not None:
            diff = latest["win_rate"] - prev["win_rate"]
            trend = {"direction": "up" if diff > 0 else "down" if diff < 0 else "flat",
                      "change": diff, "prev_win_rate": prev["win_rate"], "prev_date": prev.get("created_at")}
        return {
            "ok": True, "available": True,
            "win_rate": latest.get("win_rate"), "closed": latest.get("closed"),
            "total": latest.get("total"), "profit_factor": latest.get("profit_factor"),
            "created_at": latest.get("created_at"),
            "reliable": (latest.get("closed") or 0) >= 30,
            "trend": trend,
        }
    except Exception as e:
        print(f"[Supabase] get_latest_backtest_summary failed: {e}")
        return {"ok": False, "available": False, "error": str(e)}


@app.route("/latest_backtest_summary")
def latest_backtest_summary():
    return jsonify(get_latest_backtest_summary())


def get_setup_winrate(setup_key: str, regime: str = ""):
    """
    Historical win-rate lookup for the "confidence" badge on new signal
    cards — reads the by_setup/by_setup_regime breakdown from the most
    recent completed backtest run in Supabase. A fast read, not a live
    simulation — running a full backtest per new signal would be far too
    slow for real-time scanning.

    Tries progressively looser matches, always labeling which tier matched:
      1. setup_key + regime, EXACT match — most specific, most trustworthy
      2. setup_key alone, EXACT match
      3. normalized (backtest-compatible) setup_key — approximate, labeled
    Returns available=False rather than guessing when nothing reasonable
    matches — a missing badge is honest; a misleading one isn't.
    """
    if not setup_key or not SB:
        return {"ok": True, "available": False, "reason": "no_setup_key" if not setup_key else "supabase_not_configured"}
    try:
        r = (SB.table("backtest_runs")
             .select("by_setup,by_setup_regime,created_at")
             .order("created_at", desc=True)
             .limit(1)
             .execute())
        rows = r.data or []
        if not rows:
            return {"ok": True, "available": False, "reason": "no_backtest_run_yet"}
        run = rows[0]
        by_setup = run.get("by_setup") or []
        by_setup_regime = run.get("by_setup_regime") or []

        def find(lst, key):
            return next((x for x in lst if x.get("k") == key), None)

        match, match_type = None, None
        if regime:
            match = find(by_setup_regime, setup_key + " / " + regime)
            if match: match_type = "exact_setup_and_regime"
        if not match:
            match = find(by_setup, setup_key)
            if match: match_type = "exact_setup"
        if not match:
            norm = _normalize_setup_key_for_backtest(setup_key)
            if norm and norm != setup_key:
                match = find(by_setup, norm)
                if match: match_type = "approximate"

        if not match:
            return {"ok": True, "available": False, "reason": "no_historical_match",
                    "backtest_date": run.get("created_at")}

        return {
            "ok": True, "available": True,
            "win_rate": match.get("wr"), "n": match.get("n"),
            "reliable": match.get("reliable"), "avg_pnl": match.get("avg"),
            "match_type": match_type, "backtest_date": run.get("created_at"),
        }
    except Exception as e:
        print(f"[Supabase] get_setup_winrate failed for {setup_key}: {e}")
        return {"ok": False, "available": False, "error": str(e)}


@app.route("/setup_winrate")
def setup_winrate():
    setup_key = request.args.get("setup_key", "")
    regime = request.args.get("regime", "")
    return jsonify(get_setup_winrate(setup_key, regime))


def upsert_journal_entry(entry: dict):
    """
    Insert or update one journal/trade entry, keyed by its existing client-
    generated id — same id scheme as SIGNAL_LOG uses today, so this can sync
    both directions without inventing a second id system.

    CROSS-INSTANCE DEDUP: the phone app and the always-on worker are two
    separate browser instances, each with their own local SIGNAL_LOG and
    dedup guards — neither knows what the other has already logged. Without
    a check here, both could independently decide "this is a new trade" for
    the same symbol+bias and each write their own row, corrupting the one
    thing this whole pipeline exists to get right: an authoritative signal
    history. This is the single place every write from any instance passes
    through, so it's the only place a cross-instance check can actually work
    without a much larger client-side refactor (the client's own
    _writeJournal() is synchronous and used immediately by its callers —
    making it async to pre-check Supabase first would mean touching every
    call site, real risk for real headline gain).

    Only checked for entry.get("status")=="OPEN" — a genuinely new position.
    Status transitions (SL_HIT/T1_HIT/T2_HIT/MANUAL) update an EXISTING id
    that's already the authoritative row, so they're never blocked here.
    """
    if not SB: return
    try:
        if entry.get("status") == "OPEN":
            existing = (SB.table("journal")
                        .select("id,status")
                        .eq("sym", entry.get("sym"))
                        .eq("bias", entry.get("bias"))
                        .in_("status", ["OPEN", "T1_HIT"])
                        .execute())
            dupes = [r for r in (existing.data or []) if r.get("id") != entry.get("id")]
            if dupes:
                print(f"[Supabase] Duplicate journal entry blocked: {entry.get('sym')} "
                      f"{entry.get('bias')} already open as {dupes[0]['id']} "
                      f"(a different instance sent {entry.get('id')}) — not creating a second live row")
                return

        SB.table("journal").upsert({
            "id": entry.get("id"), "sym": entry.get("sym"), "bias": entry.get("bias"),
            "trade": entry.get("trade"), "strategy": entry.get("strategy"),
            "setup_key": entry.get("setupKey"), "time_bucket": entry.get("timeBucket"),
            "regime": entry.get("regime"), "layers": entry.get("layers"),
            "has_oi": entry.get("hasOI"), "confidence": entry.get("confidence"),
            "urgency": entry.get("urgency"),
            "entry_px": entry.get("entry_px"), "sl_px": entry.get("sl_px"),
            "t1_px": entry.get("t1_px"), "t2_px": entry.get("t2_px"),
            "rr": entry.get("rr"), "why": entry.get("why"), "vix": entry.get("vix"),
            "timeframe": entry.get("timeframe"), "status": entry.get("status"),
            "exit_px": entry.get("exit_px"), "pnl": entry.get("pnl"),
            "outcome": entry.get("outcome"), "exit_time": entry.get("exitTime"),
            "notes": entry.get("notes"), "source": entry.get("source"),
            "updated_at": datetime.now(IST).isoformat(),
        }).execute()
    except Exception as e:
        print(f"[Supabase] upsert_journal_entry failed for {entry.get('id')}: {e}")


def write_candles_bulk(sym: str, interval: str, bars: list):
    """
    One-shot bulk write for the 60-day backfill — writes the ENTIRE batch
    given, unlike write_candles() which only sends the new tail since a
    watermark. Safe to re-run: the (sym, interval, t) unique constraint
    means a repeat backfill just re-upserts the same rows, not duplicates.
    """
    if not SB or not bars: return 0
    rows = [{
        "sym": sym, "interval": interval, "t": b["t"],
        "ts": datetime.fromtimestamp(b["t"], tz=IST).isoformat(),
        "o": b.get("o"), "h": b.get("h"), "l": b.get("l"), "c": b.get("c"),
        "v": b.get("v"),
    } for b in bars]
    # Batch in chunks — a 60-day/5-min backfill can be ~3200+ rows for a
    # single NSE symbol, and sending that as one giant request risks a
    # payload-size or timeout issue. 500 rows per call is comfortably small.
    #
    # FIX: this used to catch exceptions around the WHOLE chunk loop, so a
    # failure on, say, chunk 7 of 7 discarded credit for chunks 1-6, which
    # had already committed successfully to Supabase — reporting "wrote 0"
    # for what was actually a mostly-successful write. Each chunk is now
    # independent: a later failure doesn't erase earlier progress, and the
    # specific failing chunk's error gets logged, not swallowed into a
    # generic top-level exception message.
    CHUNK = 500
    written = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        try:
            SB.table("candles").upsert(chunk, on_conflict="sym,interval,t").execute()
            written += len(chunk)
        except Exception as e:
            print(f"[Supabase] write_candles_bulk: {sym} chunk {i}-{i+len(chunk)} "
                  f"failed ({written} rows already written before this) — {e}")
    return written


def write_candles(sym: str, interval: str, bars: list):
    """
    Persist candle bars — appending only NEW ones since the last write,
    tracked via a watermark in CACHE (the app's existing cache utility,
    not a new dependency), rather than resending the whole window every
    refresh. get_technicals() re-fetches up to ~750 bars every 5 min per
    symbol via its own TTL, and the vast majority already exist in
    Supabase on every refresh — sending only the new tail avoids ~98%
    wasted payload on every single call.
    First call for a symbol+interval has no watermark yet, so it backfills
    whatever history it was given (up to 10 days for 5m, 35 days for 1d —
    however much get_technicals() fetched).
    """
    if not SB or not bars: return
    try:
        hwm_key = f"candles_hwm_{sym}_{interval}"
        hwm = CACHE.get_val(hwm_key) or 0
        new_bars = [b for b in bars if b.get("t", 0) > hwm]
        if not new_bars: return
        rows = [{
            "sym": sym, "interval": interval, "t": b["t"],
            "ts": datetime.fromtimestamp(b["t"], tz=IST).isoformat(),
            "o": b.get("o"), "h": b.get("h"), "l": b.get("l"), "c": b.get("c"),
            "v": b.get("v"),
        } for b in new_bars]
        SB.table("candles").upsert(rows, on_conflict="sym,interval,t").execute()
        CACHE.set(hwm_key, max(b["t"] for b in new_bars))
    except Exception as e:
        print(f"[Supabase] write_candles failed for {sym} {interval}: {e}")


def write_oi_chain(sym: str, expiry: str, atm: int, spot: float, strikes_data: dict):
    """
    Persist the full per-strike OI chain (ATM ±10 strikes) — richer than the
    aggregate PCR/wall numbers already in `snapshots`, useful for any future
    strategy built on per-strike OI concentration/buildup rather than just
    the pre-computed summary metrics this app currently scores on.

    Throttled to at most once per 15 min per symbol via a CACHE watermark —
    get_oi() itself refreshes every 2 min (TTL["oi"]), but writing the full
    ~21-strike chain every 2 min across ~54 OI-tracked symbols would run
    ~221K rows/day; 15 min cuts that to ~29K/day with little real loss for
    strategy work, which typically looks at buildup over 15min+ windows
    anyway. Independent of get_oi()'s own cache — this only gates the write,
    never the OI computation itself.
    """
    if not SB or not strikes_data: return
    try:
        wm_key = f"oi_chain_last_write_{sym}"
        last = CACHE.get_val(wm_key) or 0
        if time.time() - last < 900: return
        ts_now = datetime.now(IST).isoformat()
        rows = [{
            "sym": sym, "expiry": expiry, "ts": ts_now,
            "atm": atm, "spot": spot, "strike": strike,
            "ce_oi": d.get("ce", 0), "pe_oi": d.get("pe", 0),
            "ce_chg": d.get("ce_chg", 0), "pe_chg": d.get("pe_chg", 0),
        } for strike, d in strikes_data.items()]
        SB.table("oi_chain").insert(rows).execute()
        CACHE.set(wm_key, time.time())
    except Exception as e:
        print(f"[Supabase] write_oi_chain failed for {sym}: {e}")


def write_snapshot(row: dict, source: str = "client"):
    """
    Persist one per-symbol snapshot row — mirrors captureSnapshot() in
    index.html field-for-field. Called from /snapshot (client posts here in
    addition to its existing IndexedDB save — this is purely additive, the
    local IndexedDB behavior is untouched) and, later, from the always-on
    worker once it exists.
    """
    if not SB: return
    try:
        payload = dict(row)
        payload["source"] = source
        payload.pop("id", None)   # let Supabase assign its own bigserial id
        SB.table("snapshots").insert(payload).execute()
    except Exception as e:
        print(f"[Supabase] write_snapshot failed for {row.get('sym')}: {e}")


def write_backtest_result(job_id: str, params: dict, result: dict):
    """
    Persist a completed backtest run — the aggregate summary into
    backtest_runs, and every individual simulated trade from result["signals"]
    into backtest_trades. Called automatically at the end of every backtest,
    from _bt_run_job — no extra step needed on your end, every run you
    kick off from the app is saved from now on.
    """
    if not SB: return
    try:
        run = SB.table("backtest_runs").insert({
            "job_id": job_id, "params": params,
            "total": result.get("total"), "closed": result.get("closed"),
            "wins": result.get("wins"), "losses": result.get("losses"),
            "win_rate": result.get("win_rate"), "avg_win": result.get("avg_win"),
            "avg_loss": result.get("avg_loss"), "profit_factor": result.get("profit_factor"),
            "mfe_stats": result.get("mfe_stats"),
            "by_session": result.get("by_session"), "by_trend": result.get("by_trend"),
            "by_bias": result.get("by_bias"), "by_conf": result.get("by_conf"),
            "by_cpr": result.get("by_cpr"), "by_sym": result.get("by_sym"),
            "by_setup": result.get("by_setup"), "by_regime": result.get("by_regime"),
            "by_setup_regime": result.get("by_setup_regime"), "by_score": result.get("by_score"),
            "by_layer": result.get("by_layer"), "by_time": result.get("by_time"),
            "by_mfe": result.get("by_mfe"), "by_combo": result.get("by_combo"),
        }).execute()
        run_id = run.data[0]["id"]

        trades = result.get("signals", [])
        rows = []
        for s in trades:
            rows.append({
                "run_id": run_id, "date": s.get("date"), "time": s.get("time"),
                "sym": s.get("sym"), "bias": s.get("bias"), "conf": s.get("conf"),
                "bull": s.get("bull"), "bear": s.get("bear"), "score": s.get("score"),
                "trend15": s.get("trend15"), "regime": s.get("regime"),
                "cpr_narrow": s.get("cpr_narrow"), "pcr": s.get("pcr"), "rsi": s.get("rsi"),
                "setup_key": s.get("setup_key"), "entry": s.get("entry"), "sl": s.get("sl"),
                "t1": s.get("t1"), "t2": s.get("t2"), "status": s.get("status"),
                "exit": s.get("exit"), "pnl_pct": s.get("pnl_pct"), "mfe": s.get("mfe"),
                "mae": s.get("mae"), "bars_to_t1": s.get("bars_to_t1"), "bars": s.get("bars"),
                "session": s.get("session"), "layers": s.get("layers"),
            })
        # Batch insert — Supabase/PostgREST handles this in one round trip
        # rather than one request per trade (a run can have 300 trades).
        if rows:
            SB.table("backtest_trades").insert(rows).execute()
        print(f"[Supabase] Backtest run {job_id} saved — {len(rows)} trades")
    except Exception as e:
        print(f"[Supabase] write_backtest_result failed for job {job_id}: {e}")


@app.route("/snapshot", methods=["POST"])
def snapshot_ingest():
    """
    Client posts one snapshot row here in addition to its own IndexedDB save.
    Fire-and-forget from the client's perspective — this never blocks or
    changes the existing local snapshot behavior, it's a pure addition.
    """
    try:
        row = request.get_json(force=True) or {}
        if not row.get("sym"):
            return jsonify({"ok": False, "error": "missing sym"}), 400
        write_snapshot(row, source="client")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/ingest_signal", methods=["POST"])
def ingest_signal():
    """
    Any client posts a generated signal here — the phone app, or the
    always-on worker. Fire-and-forget, same pattern as /snapshot: never
    blocks the caller, a failed write here never surfaces as an error to
    whoever's scanning.

    NOTE on `fired`: every signal reaching this endpoint has already cleared
    the engine's admission threshold (bullScore/bearScore >= minScore) — the
    scoring loop doesn't currently retain a record for symbols that scored
    but never cleared that bar, so this is "signals that made it into the
    live feed" (any urgency: HIGH/MEDIUM/LOW), not literally every candidate
    the engine ever considered. Always writes fired=true for now.
    """
    try:
        sig = request.get_json(force=True) or {}
        if not sig.get("sym") or not sig.get("bias"):
            return jsonify({"ok": False, "error": "missing sym or bias"}), 400
        source = sig.pop("_source", "worker")
        write_signal(sig, fired=True, source=source)
        # Was completely silent on success — meant no real-time confirmation
        # anywhere that a signal write actually happened, only discoverable
        # later via a separate SQL query. Now visible immediately in
        # Render's own logs, the moment it fires.
        print(f"[Signal ✅] {sig.get('sym')} {sig.get('bias')} {sig.get('urgency')} "
              f"{sig.get('confidence')}% from {source}")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/ingest_journal", methods=["POST"])
def ingest_journal():
    """
    Any client posts a journal entry here (new OPEN entry, or a status
    update — SL_HIT/T1_HIT/T2_HIT/MANUAL) — the phone app's _writeJournal()
    and checkSignalLevels(), or the worker's equivalent. upsert_journal_entry
    keys on `id`, so a status change just updates the existing row rather
    than duplicating it.
    """
    try:
        entry = request.get_json(force=True) or {}
        if not entry.get("id") or not entry.get("sym"):
            return jsonify({"ok": False, "error": "missing id or sym"}), 400
        upsert_journal_entry(entry)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



def supabase_health():
    """Quick connectivity check — hit this once after setup to confirm the
    keys and schema are both working before building anything on top."""
    if not SB:
        return jsonify({"ok": False, "connected": False,
                         "reason": "SUPABASE_URL / SUPABASE_SECRET_KEY not set"})
    try:
        r = SB.table("signals").select("id", count="exact").limit(1).execute()
        return jsonify({"ok": True, "connected": True, "signals_row_count": r.count})
    except Exception as e:
        return jsonify({"ok": False, "connected": False, "reason": str(e)})

# ── CORS ──────────────────────────────────────────────────────────────────────
# Configurable via ALLOWED_ORIGINS env var (comma-separated), e.g.
#   ALLOWED_ORIGINS=https://yourname.github.io,http://localhost:8080
# Defaults to "*" so existing deployments keep working. NOTE: CORS is a browser
# convenience, NOT a security boundary — any non-browser client ignores it.
# The real protection on the order route is the credential + secret check below.
_ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
CORS(app, origins=_ALLOWED_ORIGINS)

# ── TIER-0 SECURITY: order-route authentication ───────────────────────────────
# Optional shared secret. If PROTRADER_API_SECRET is set in the environment,
# every order-placing request must present it in the X-PT-Secret header.
# If it is NOT set, order routes still require live Kite credentials to be sent
# explicitly in the request body (they no longer fall back to the server's
# cached credentials), so an anonymous caller cannot trade someone else's account.
API_SECRET = os.environ.get("PROTRADER_API_SECRET", "").strip()

def _check_order_auth():
    """Returns None if authorised, else an (json, status) error tuple."""
    if API_SECRET:
        sent = request.headers.get("X-PT-Secret", "")
        if not sent or not hmac.compare_digest(sent, API_SECRET):
            return jsonify({"ok": False, "error": "Unauthorised — bad or missing X-PT-Secret"}), 401
    return None

def _creds(data=None):
    """
    Resolve Kite credentials for READ-ONLY routes.
    Priority: headers → JSON body → query string → server cache.
    Headers are preferred so tokens stop appearing in URLs (and therefore in
    Render request logs and browser history).
    """
    key = request.headers.get("X-Kite-Key", "") or ""
    tok = request.headers.get("X-Kite-Token", "") or ""
    if data:
        key = key or data.get("key", "") or ""
        tok = tok or data.get("token", "") or ""
    key = key or request.args.get("key", "") or CACHE.get_val("_kite_key") or ""
    tok = tok or request.args.get("token", "") or CACHE.get_val("_kite_token") or ""
    return key, tok

def _creds_strict(data):
    """
    Resolve Kite credentials for ORDER routes. NO server-cache fallback —
    the caller must supply their own credentials. This is what stops an
    anonymous POST from trading against the last-seen user's account.
    """
    key = request.headers.get("X-Kite-Key", "") or (data.get("key", "") if data else "") or ""
    tok = request.headers.get("X-Kite-Token", "") or (data.get("token", "") if data else "") or ""
    return key.strip(), tok.strip()

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
    "SILVER":    {"kite":None, "yahoo":"SI=F",  "step":100, "sector":"COMMODITY", "mcx":True},
    "NATURALGAS":{"kite":None, "yahoo":"NG=F",  "step":5,   "sector":"COMMODITY", "mcx":True},
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
    "LTM":    {"kite":"NSE:LTM",    "yahoo":"LTM.NS",    "step":100, "sector":"IT"},  # was LTIM until 27 Feb 2026 rebrand
    # ── Energy ──
    "RELIANCE": {"kite":"NSE:RELIANCE","yahoo":"RELIANCE.NS","step":50, "sector":"ENERGY"},
    "ONGC":     {"kite":"NSE:ONGC",   "yahoo":"ONGC.NS",    "step":5,  "sector":"ENERGY"},
    "BPCL":     {"kite":"NSE:BPCL",   "yahoo":"BPCL.NS",    "step":10, "sector":"ENERGY"},
    "POWERGRID":{"kite":"NSE:POWERGRID","yahoo":"POWERGRID.NS","step":10,"sector":"ENERGY"},
    "NTPC":     {"kite":"NSE:NTPC",   "yahoo":"NTPC.NS",    "step":5,  "sector":"ENERGY"},
    "COALINDIA":{"kite":"NSE:COALINDIA","yahoo":"COALINDIA.NS","step":10,"sector":"ENERGY"},
    # ── Auto ──
    "MARUTI":    {"kite":"NSE:MARUTI",    "yahoo":"MARUTI.NS",    "step":100, "sector":"AUTO"},
    "TMPV":   {"kite":"NSE:TMPV",   "yahoo":"TMPV.NS",   "step":10,  "sector":"AUTO"},  # was TATAMOTORS — renamed after the Oct 2025 CV demerger (passenger-vehicle entity, incl. JLR/EVs — the direct continuation of the original business, not the spun-off commercial-vehicle entity now trading as TMCV)
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
    # Exchange operators — real NSE-listed equities (BSE Ltd, MCX Ltd), not
    # to be confused with the "mcx" boolean flag used elsewhere for MCX
    # commodity contracts (CRUDEOIL/GOLD/SILVER/NATURALGAS). Deliberately no
    # "mcx" key here, same as every other NSE stock — that flag is only ever
    # set on the actual commodity entries. step:50 matches what both the
    # server's own get_oi() fallback (default 50) and the client's price-
    # tiered fallback (>1000 -> 50) would already produce for this price
    # range — not a fresh guess, both existing safety nets agree.
    "BSE":        {"kite":"NSE:BSE",       "yahoo":"BSE.NS",       "step":50, "sector":"EXCHANGE"},
    "MCX":        {"kite":"NSE:MCX",       "yahoo":"MCX.NS",       "step":50, "sector":"EXCHANGE"},
}

OI_INDICES = {"NIFTY","BANKNIFTY","FINNIFTY","SENSEX"}  # Index option chains
OI_MCX     = set()  # MCX commodity option OI excluded — back-tested 48% direction-match (coin flip), thin chains. Price/technicals drive MCX signals instead.

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
    "TMPV","MARUTI",
    # Infra
    "LT","ADANIPORTS",
    # Telecom
    "BHARTIARTL",
    # Exchange operators — heavily traded, added on request
    "BSE","MCX",
}

# Extended Nifty50 stocks — OI fetched every 15 min (less liquid options)
OI_STOCKS_EXT = {
    "APOLLOHOSP","ASIANPAINT","BEL","BPCL","BRITANNIA","CIPLA","COALINDIA",
    "DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HAL","HEROMOTOCO","HINDALCO",
    "HINDUNILVR","INDIGO","ITC","JSWSTEEL","LTM","M&M","NESTLEIND","NTPC",
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
        """Returns True if key exists, is not None, and is within TTL seconds."""
        val, age = self.get(key)
        return val is not None and age is not None and age < ttl

    def get_val(self, key):
        val, _ = self.get(key)
        return val

    def age(self, key):
        _, age = self.get(key)
        return age

    def set_error(self, key):
        """Mark a key as errored WITHOUT overwriting a valid stale value.
        Prevents a temporary API failure from evicting good cached data."""
        with self._lock:
            entry = self._store.get(key)
            # Only set error sentinel if there is no value (or existing is also None)
            if not entry or entry.get("val") is None:
                self._store[key] = {"val": None, "ts": time.time()}

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
        r = KITE_SESSION.get("https://api.kite.trade/quote",
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
        trend_up_count = 0; trend_sessions = 0; avg_range_pct = 0
        prev_day_high = 0; prev_day_low = 0  # PDH / PDL
        try:
            url_d = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1mo&includePrePost=false"
            r_d = requests.get(url_d, headers={"User-Agent":"Mozilla/5.0"}, timeout=8).json()
            res_d = r_d["chart"]["result"][0]
            q_d = res_d["indicators"]["quote"][0]
            # FIX: build dc/dh/dl from ONE aligned loop, bounded by the SHORTEST
            # of the three arrays. The old code used range(len(timestamp)) as the
            # bound for all three independently — but Yahoo can return close/high/low
            # arrays of DIFFERENT lengths (data gaps, partial sessions). Indexing
            # past a shorter array's end threw an uncaught IndexError, which the
            # outer except swallowed — silently wiping out prev_day_high/low too,
            # even though those were already computed correctly moments earlier.
            # This also fixes a subtler correctness bug: the old independent
            # comprehensions could drop a day from dh but not dc, so dh[-2] and
            # dc[-2] no longer referred to the same calendar day.
            qc, qh, ql = q_d.get("close",[]), q_d.get("high",[]), q_d.get("low",[])
            n_bound = min(len(res_d.get("timestamp",[])), len(qc), len(qh), len(ql))
            dc=[]; dh=[]; dl=[]
            for i in range(n_bound):
                if qc[i] and qh[i] and ql[i]:
                    dc.append(qc[i]); dh.append(qh[i]); dl.append(ql[i])
            if len(dc) >= 2:
                # Previous day high/low (index -2 = yesterday's completed session)
                prev_day_high = round(dh[-2], 2) if len(dh)>=2 else 0
                prev_day_low  = round(dl[-2], 2) if len(dl)>=2 else 0
            if len(dc) >= 10:
              try:
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

                # ── 15-DAY AVERAGE DAILY RANGE % — for ATR setup filter ──
                # Measures a symbol's typical intraday range. Used to filter out
                # low-volatility symbols (PSUs, defensives) that rarely move enough
                # to reach targets. A stock that averages <1.2% daily range can't
                # reliably hit a 0.75% T1 + give room for SL.
                rng_window = min(15, len(completed_h), len(completed_l))
                if rng_window >= 5:
                    ranges = [(completed_h[-i]-completed_l[-i])/completed_l[-i]*100
                              for i in range(1, rng_window+1)
                              if completed_l[-i] > 0]
                    avg_range_pct = round(sum(ranges)/len(ranges), 2) if ranges else 0
                else:
                    avg_range_pct = 0
              except Exception:
                # DEFENSE IN DEPTH: if anything in trend15/avg_range_pct calc
                # fails unexpectedly, fail ONLY this sub-block. prev_day_high/
                # prev_day_low were already computed above and must survive —
                # CPR depends on them and has nothing to do with this section.
                pass
        except Exception as te:
            pass

        # MTF intraday trend override (same as Kite path). Yahoo provides less
        # 5m history so the 1h leg may be weak, but 5m/15m still work and the
        # function degrades gracefully when a timeframe lacks bars.
        try:
            _mtf = compute_mtf_trend(candles)
            if _mtf["trend15"] != "UNKNOWN":
                trend15 = _mtf["trend15"]
                hh_hl = _mtf["hh_hl"]; lh_ll = _mtf["lh_ll"]
                trend_strength = abs(_mtf["mtf_score"]) * 16
        except Exception:
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
            "avg_range_pct":avg_range_pct,
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

# ── Shared Kite instrument token cache (live + backtest) ─────────────────────
_kite_token_cache = {}   # sym → instrument_token  (in-memory, survives restarts poorly — CACHE handles persistence)

_KITE_STATIC_TOKENS = {
    "NIFTY":     256265,
    "BANKNIFTY": 260105,
    "FINNIFTY":  257801,
    "SENSEX":    265,
    "MIDCPNIFTY":288009,
}

def _get_kite_instr_token(sym, key, token):
    """
    Resolve a symbol to its Kite instrument_token.
    Priority: in-memory cache → static index tokens → NSE instruments CSV.
    Returns int token or None.
    """
    if sym in _kite_token_cache:
        return _kite_token_cache[sym]
    if sym in _KITE_STATIC_TOKENS:
        _kite_token_cache[sym] = _KITE_STATIC_TOKENS[sym]
        return _KITE_STATIC_TOKENS[sym]
    # Try persistent cache first
    cache_key = f"kite_tok_{sym}"
    cached = CACHE.get_val(cache_key)
    if cached:
        _kite_token_cache[sym] = cached
        return cached
    # Fetch NSE instruments CSV
    csv_key = "instruments_nse_eq"
    csv_text = CACHE.get_val(csv_key)
    if not csv_text:
        try:
            r = KITE_SESSION.get("https://api.kite.trade/instruments/NSE",
                headers=_kite_headers(key, token), timeout=20)
            if r.status_code == 200:
                csv_text = r.text
                CACHE.set(csv_key, csv_text)
            else:
                print(f"[KiteToken] NSE instruments HTTP {r.status_code}")
                return None
        except Exception as e:
            print(f"[KiteToken] NSE instruments fetch failed: {e}")
            return None
    import csv as _csv, io as _io
    for row in _csv.DictReader(_io.StringIO(csv_text)):
        ts  = (row.get("tradingsymbol") or "").strip()
        seg = (row.get("segment") or "").strip()
        tok = (row.get("instrument_token") or "").strip()
        if ts == sym and seg in ("NSE", "NSE-EQ") and tok:
            int_tok = int(tok)
            _kite_token_cache[sym] = int_tok
            CACHE.set(cache_key, int_tok)
            return int_tok
    print(f"[KiteToken] Token not found for {sym}")
    return None


# Yahoo → Kite interval mapping
_KITE_INTERVAL = {
    "5m":  "5minute",
    "15m": "15minute",
    "1h":  "60minute",
    "1d":  "day",
}

def fetch_kite_live_candles(sym, key, token, interval="5m", days=2):
    """
    Fetch intraday/daily candles from Zerodha Kite historical API.
    Returns list of {t, o, h, l, c, v} — same format as fetch_yahoo_candles candles.
    Falls back to Yahoo if token missing or Kite fails.

    interval: "5m" | "15m" | "1h" | "1d"
    days: how many calendar days of history to request
    """
    inst = INSTRUMENTS.get(sym, {})

    # MCX commodities — Kite historical not supported, always use Yahoo
    if inst.get("mcx"):
        ticker = inst.get("yahoo", "")
        if not ticker: return []
        d = fetch_yahoo_candles(ticker, interval, f"{days}d")
        return d.get("candles", []) if d else []

    if not key or not token:
        # No Kite credentials — fall back to Yahoo
        ticker = inst.get("yahoo", "")
        if not ticker: return []
        d = fetch_yahoo_candles(ticker, interval, f"{days}d")
        return d.get("candles", []) if d else []

    instr_token = _get_kite_instr_token(sym, key, token)
    if not instr_token:
        ticker = inst.get("yahoo", "")
        if not ticker: return []
        print(f"[KiteLive] No token for {sym} — Yahoo fallback ({interval})")
        d = fetch_yahoo_candles(ticker, interval, f"{days}d")
        return d.get("candles", []) if d else []

    kite_interval = _KITE_INTERVAL.get(interval, "5minute")
    from_dt = (datetime.now(IST) - timedelta(days=days + 1)).strftime("%Y-%m-%d")
    to_dt   =  datetime.now(IST).strftime("%Y-%m-%d")
    url = (f"https://api.kite.trade/instruments/historical/"
           f"{instr_token}/{kite_interval}"
           f"?from={from_dt}&to={to_dt}")

    try:
        r = KITE_SESSION.get(url, headers=_kite_headers(key, token), timeout=15)
        if r.status_code in [401, 403]:
            print(f"[KiteLive] Auth failed for {sym} — Yahoo fallback")
            ticker = inst.get("yahoo", "")
            if not ticker: return []
            d = fetch_yahoo_candles(ticker, interval, f"{days}d")
            return d.get("candles", []) if d else []
        if r.status_code != 200:
            print(f"[KiteLive] HTTP {r.status_code} for {sym} {interval}")
            return []
        d = r.json()
        if d.get("status") != "success":
            print(f"[KiteLive] {sym} {interval}: {d.get('message','')}")
            return []
        bars = []
        for c in d.get("data", {}).get("candles", []):
            ts_str = c[0]
            try:
                ts_norm = ts_str.replace(" ", "T")
                ts_norm = re.sub(r'\+(\d{2})(\d{2})$', r'+\1:\2', ts_norm)
                if "+" in ts_norm or "Z" in ts_norm:
                    dt = datetime.fromisoformat(ts_norm.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(ts_norm, "%Y-%m-%dT%H:%M:%S")
                    dt = dt.replace(tzinfo=IST)
            except Exception:
                continue
            bars.append({
                "t": int(dt.timestamp()),
                "o": round(float(c[1]), 2),
                "h": round(float(c[2]), 2),
                "l": round(float(c[3]), 2),
                "c": round(float(c[4]), 2),
                "v": int(c[5]) if len(c) > 5 else 0,
            })
        print(f"[KiteLive] {sym} {interval}: {len(bars)} bars ✅")
        return bars
    except Exception as e:
        print(f"[KiteLive] {sym} {interval} failed: {e}")
        ticker = inst.get("yahoo", "")
        if not ticker: return []
        d = fetch_yahoo_candles(ticker, interval, f"{days}d")
        return d.get("candles", []) if d else []


def fetch_kite_live_candles_computed(sym, key, token, interval="5m", days=2):
    """
    Like fetch_kite_live_candles but also computes SMA/RSI/trend/PDH/PDL/trend15
    from the returned bars — matching the output shape of fetch_yahoo_candles().
    Used by get_technicals() to replace Yahoo entirely for NSE symbols.
    Returns a dict matching fetch_yahoo_candles output, or None on failure.
    """
    # 5min candles — fetch 10 days so the MTF trend can build a valid 1-hour
    # timeframe (needs ~20 hourly candles = ~5 sessions of 5m bars).
    candles5 = fetch_kite_live_candles(sym, key, token, "5m", 10)
    if not candles5:
        return None

    closes  = [c["c"] for c in candles5]
    highs   = [c["h"] for c in candles5]
    lows    = [c["l"] for c in candles5]
    volumes = [c["v"] for c in candles5]

    def sma(n):
        return round(sum(closes[-n:]) / n, 2) if len(closes) >= n else None

    def rsi14():
        if len(closes) < 15: return None
        g = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
        l = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
        ag = sum(g[-14:]) / 14; al = sum(l[-14:]) / 14
        return round(100 - 100 / (1 + ag / al), 1) if al else 100.0

    s20 = sma(20); s50 = sma(50); s200 = sma(200)

    # Use Kite /quote for live price (already fetched in get_all_prices)
    # Fallback: last close from candles
    px = closes[-1] if closes else 0

    # Prices from Kite quote (get_all_prices caches these)
    prices_cache = CACHE.get_val("all_prices") or {}
    sym_price = prices_cache.get(sym, {})
    if sym_price.get("px"):
        px       = sym_price["px"]
        pc       = sym_price.get("prev_close", px)
        day_high = sym_price.get("high", max(highs) if highs else 0)
        day_low  = sym_price.get("low",  min(lows)  if lows  else 0)
        day_open = sym_price.get("open", 0)
        chg      = sym_price.get("chg", 0)
        pct      = sym_price.get("pct", 0)
        volume   = sym_price.get("volume", volumes[-1] if volumes else 0)
    else:
        pc       = closes[-2] if len(closes) > 1 else px
        day_high = max(highs) if highs else 0
        day_low  = min(lows)  if lows  else 0
        day_open = candles5[0]["o"] if candles5 else 0
        chg      = round(px - pc, 2)
        pct      = round((px - pc) / pc * 100, 2) if pc else 0
        volume   = volumes[-1] if volumes else 0

    cross = "NONE"
    if len(closes) >= 22 and s20 and s50:
        ps20 = sum(closes[-21:-1]) / 20
        if ps20 < s50 and s20 > s50: cross = "GOLDEN_CROSS"
        elif ps20 > s50 and s20 < s50: cross = "DEATH_CROSS"

    avg_vol = int(sum(volumes[-20:]) / 20) if len(volumes) >= 20 else 0
    cur_vol = volumes[-1] if volumes else 0
    vol_ratio = round(cur_vol / avg_vol, 2) if avg_vol else 0

    # Daily candles for trend15, PDH/PDL, avg_range_pct
    trend15 = "UNKNOWN"; trend_strength = 0; trend_up_count = 0
    trend_sessions = 0; avg_range_pct = 0
    hh_hl = False; lh_ll = False
    prev_day_high = 0; prev_day_low = 0

    candles_daily = fetch_kite_live_candles(sym, key, token, "1d", 35)
    if candles_daily and len(candles_daily) >= 2:
        dc = [c["c"] for c in candles_daily]
        dh = [c["h"] for c in candles_daily]
        dl = [c["l"] for c in candles_daily]
        if len(dc) >= 2:
            prev_day_high = round(dh[-2], 2)
            prev_day_low  = round(dl[-2], 2)
        if len(dc) >= 10:
            try:
                completed   = dc[:-1]
                completed_h = dh[:-1]
                completed_l = dl[:-1]
                d_s5  = sum(completed[-5:])  / min(5,  len(completed))
                d_s10 = sum(completed[-10:]) / min(10, len(completed))
                days_w = completed[-15:] if len(completed) >= 15 else completed
                n_comp = len(days_w) - 1
                up_days = sum(1 for i in range(1, len(days_w)) if days_w[i] > days_w[i-1])
                dn_days = n_comp - up_days
                trend_strength = round(up_days / n_comp * 100) if n_comp > 0 else 0
                trend_up_count = up_days; trend_sessions = n_comp
                if len(completed_h) >= 3 and len(completed_l) >= 3:
                    hh_hl = completed_h[-1] > completed_h[-3] and completed_l[-1] > completed_l[-3]
                    lh_ll = completed_h[-1] < completed_h[-3] and completed_l[-1] < completed_l[-3]
                # NOTE: daily-based trend15 kept below but OVERRIDDEN by the MTF
                # intraday trend after this block (daily trend proved to have
                # negative predictive value on indices; intraday 5m/15m/1h works).
                # We still compute trend_strength / hh_hl / lh_ll from daily for
                # any downstream consumers, then replace trend15 itself.
                if d_s5 > d_s10 and hh_hl:   trend15 = "STRONG_BULL"
                elif d_s5 > d_s10:            trend15 = "BULL"
                elif d_s5 < d_s10 and lh_ll:  trend15 = "STRONG_BEAR"
                elif d_s5 < d_s10:           trend15 = "BEAR"
                else:                          trend15 = "NEUTRAL"
                rng_window = min(15, len(completed_h), len(completed_l))
                if rng_window >= 5:
                    ranges = [(completed_h[-i] - completed_l[-i]) / completed_l[-i] * 100
                              for i in range(1, rng_window + 1) if completed_l[-i] > 0]
                    avg_range_pct = round(sum(ranges) / len(ranges), 2) if ranges else 0
            except Exception:
                pass

    # ── MTF INTRADAY TREND — replaces daily-15d trend15 ──────────────────────
    # Data (60-day index study) showed daily-15d trend15 was -0.228R (harmful),
    # while a 5m-primary intraday trend was the best single directional signal.
    # We keep 15m and 1h as lighter confirming inputs (5m x3, 15m x2, 1h x1).
    _mtf = compute_mtf_trend(candles5)
    if _mtf["trend15"] != "UNKNOWN":
        trend15 = _mtf["trend15"]
        hh_hl = _mtf["hh_hl"]; lh_ll = _mtf["lh_ll"]
        trend_strength = abs(_mtf["mtf_score"]) * 16  # 0..96 rough scale

    return {
        "px": px, "chg": chg, "pct": pct,
        "high": day_high, "low": day_low, "open": day_open, "prev_close": pc,
        "sma20": s20, "sma50": s50, "sma200": s200,
        "rsi": rsi14(),
        "crossover": cross,
        "trend": ("BULLISH" if (s20 and s50 and s20 > s50)
                  else "BEARISH" if (s20 and s50 and s20 < s50) else "NEUTRAL"),
        "trend15": trend15, "trend_strength": trend_strength,
        "mtf_score": _mtf["mtf_score"], "trend_5m": _mtf["t5"],
        "trend_15m": _mtf["t15"], "trend_1h": _mtf["t60"],
        "trend_up_count": trend_up_count, "trend_sessions": trend_sessions,
        "avg_range_pct": avg_range_pct,
        "hh_hl": hh_hl, "lh_ll": lh_ll,
        "prev_day_high": prev_day_high,
        "prev_day_low":  prev_day_low,
        "breakout":  bool(highs and px >= max(highs) * 0.998),
        "breakdown": bool(lows  and px <= min(lows)  * 1.002),
        "volume": cur_vol, "avg_volume": avg_vol, "vol_ratio": vol_ratio,
        # WIDENED from last 78 bars (~1 day) to last 300 bars (~4 days).
        # calc_smc's swing-structure detection (find_pivots with a 20-bar
        # lookback) needs enough history for a SECOND swing pivot to form
        # near the first one (equal highs/lows -> liquidity sweep signal).
        # With only 78 bars there was room for ~1 swing pivot, so EQH/EQL/
        # sweep almost never had a pair to compare — that's why "sweeps"
        # was stuck at ~1% while ORB/CHoCH/OB/FVG (which only need recent
        # bars) worked fine. candles5 is already fetched with 10 days of
        # history (for the MTF trend fix), so this reuses data already in
        # memory — no extra network calls, no slowdown.
        "candles": candles5[-300:],
        "candles_daily": candles_daily,   # passed through so get_smc_cpr can reuse
        "_source": "kite",
    }


def fetch_kite_instruments_nfo(key, token):
    """Fetch NFO instruments CSV from Zerodha. Cached 4 hours."""
    cache_key = "instruments_nfo"
    if CACHE.fresh(cache_key, TTL["instruments"]):
        return CACHE.get_val(cache_key)
    try:
        r = KITE_SESSION.get("https://api.kite.trade/instruments/NFO",
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
        r = KITE_SESSION.get("https://api.kite.trade/instruments/BFO",
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
        r = KITE_SESSION.get("https://api.kite.trade/instruments/MCX",
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

def _ema(vals, n):
    """Exponential moving average of the last values. Returns None if insufficient."""
    if len(vals) < n: return None
    k = 2/(n+1); e = sum(vals[:n])/n
    for v in vals[n:]: e = v*k + e*(1-k)
    return e

def _resample(bars, span):
    """Aggregate 5-min bars into higher timeframe. span = number of 5m bars per new bar."""
    if span <= 1: return bars
    out=[]; bucket=[]
    for b in bars:
        bucket.append(b)
        if len(bucket)==span:
            out.append({"h":max(x["h"] for x in bucket),"l":min(x["l"] for x in bucket),
                        "c":bucket[-1]["c"]})
            bucket=[]
    if bucket:
        out.append({"h":max(x["h"] for x in bucket),"l":min(x["l"] for x in bucket),
                    "c":bucket[-1]["c"]})
    return out

def _tf_trend(bars, need=20):
    """
    Trend on one timeframe using EMA alignment + EMA slope + market structure (HH/HL).
    Returns +1 (bull), -1 (bear), 0 (neutral). Majority vote of the three methods.
    """
    if not bars or len(bars) < need: return 0
    c = [b["c"] for b in bars]
    e9, e20 = _ema(c,9), _ema(c,20)
    align = 1 if (e9 and e20 and e9>e20) else -1 if (e9 and e20 and e9<e20) else 0
    e9now, e9prev = _ema(c,9), _ema(c[:-3],9)
    slope = 1 if (e9now and e9prev and e9now>e9prev) else -1 if (e9now and e9prev and e9now<e9prev) else 0
    w = bars[-12:] if len(bars)>=12 else bars; mid=len(w)//2
    struct=0
    if mid>=2:
        h1=max(b["h"] for b in w[:mid]); h2=max(b["h"] for b in w[mid:])
        l1=min(b["l"] for b in w[:mid]); l2=min(b["l"] for b in w[mid:])
        struct = 1 if (h2>h1 and l2>l1) else -1 if (h2<h1 and l2<l1) else 0
    s = align + slope + struct
    return 1 if s>0 else -1 if s<0 else 0

def compute_mtf_trend(candles5):
    """
    Multi-timeframe intraday trend replacing the old daily-15d trend15.
    Analyzes 5-minute, 15-minute, and 1-hour trends together, weighting the
    SMALLER timeframes more heavily (data shows 5m carries the most intraday edge
    on indices; 1h is kept as a lighter confirming input).

    Weights: 5m x3, 15m x2, 1h x1. Score in [-6,+6].
    Maps to the same labels the rest of the engine expects so nothing downstream
    breaks: STRONG_BULL / BULL / NEUTRAL / BEAR / STRONG_BEAR.

    Returns dict: {trend15, mtf_score, t5, t15, t60, hh_hl, lh_ll}.
    """
    if not candles5 or len(candles5) < 30:
        return {"trend15":"UNKNOWN","mtf_score":0,"t5":0,"t15":0,"t60":0,
                "hh_hl":False,"lh_ll":False}
    t5  = _tf_trend(candles5[-30:], need=20)          # ~last 30 five-min bars
    t15 = _tf_trend(_resample(candles5[-300:], 3)[-20:], need=15)   # 15-min bars
    t60 = _tf_trend(_resample(candles5[-800:], 12)[-20:], need=10)  # 1-hour bars
    # Weighted: smaller timeframe dominates (5m x3, 15m x2, 1h x1)
    score = t5*3 + t15*2 + t60*1
    # Map score -> label. STRONG when smaller TFs strongly align.
    if score >= 5:   trend15 = "STRONG_BULL"
    elif score >= 2: trend15 = "BULL"
    elif score <= -5: trend15 = "STRONG_BEAR"
    elif score <= -2: trend15 = "BEAR"
    else:             trend15 = "NEUTRAL"
    # hh_hl / lh_ll derived from 5m structure for downstream consumers
    hh_hl = (t5==1 and t15==1)
    lh_ll = (t5==-1 and t15==-1)
    return {"trend15":trend15,"mtf_score":score,"t5":t5,"t15":t15,"t60":t60,
            "hh_hl":hh_hl,"lh_ll":lh_ll}

def calc_orb(candles):
    """
    Opening Range Breakout — first 15 minutes (3 × 5min candles after 9:15).
    Tracks failed breakouts: if price breaks out then returns inside range,
    the breakout is invalidated. A second breakout requires volume confirmation.
    """
    if not candles: return None
    # Derive "today" from the LATEST candle's date (in IST), not the server clock.
    # This is robust to server timezone drift and to the candle set ending
    # on a prior session (e.g. fetched after hours).
    last_ts = candles[-1].get("t", 0)
    if not last_ts: return None
    last_dt = datetime.fromtimestamp(last_ts, tz=IST)
    today = last_dt.date()
    # ORB window = 9:15-9:30 IST of the latest candle's date, expressed in UTC epoch
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
    # NSE changed NIFTY weekly expiry from Thursday → Tuesday (effective Sep 2024).
    # BANKNIFTY = Wednesday, FINNIFTY = Tuesday, MIDCPNIFTY = Monday, SENSEX = Friday.
    from datetime import datetime, timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist)
    weekday = now.weekday()  # 0=Mon,1=Tue,2=Wed,3=Thu,4=Fri
    is_tuesday   = weekday == 1   # NIFTY, FINNIFTY
    is_wednesday = weekday == 2   # BANKNIFTY
    is_thursday  = weekday == 3   # legacy fallback / stocks
    is_friday    = weekday == 4   # SENSEX
    is_expiry_day = is_tuesday or is_wednesday or is_thursday or is_friday
    is_expiry_time = now.hour >= 13  # afternoon on expiry

    # ── REGIME CLASSIFICATION ──
    # Priority order matters — more specific wins

    # 1. EXPIRY PINNING — expiry day + price near max pain + tight walls
    if is_expiry_day and pinned and wall_width_pct < 2.0:
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
    """India VIX — primary source Zerodha Kite (NSE:INDIA VIX). Cached 1 min.
    Yahoo kept only as a last-resort fallback if Kite credentials are unavailable
    or the Kite call fails, so VIX (used by risk gates) never goes blank."""
    if CACHE.fresh("vix", TTL["vix"]):
        return CACHE.get_val("vix")
    # 1) Kite — the authoritative source now that we standardize on Zerodha.
    key   = CACHE.get_val("_kite_key")
    token = CACHE.get_val("_kite_token")
    if key and token:
        try:
            data = fetch_kite_quotes(key, token, ["NSE:INDIA VIX"])
            if data and not data.get("_token_expired"):
                q = data.get("NSE:INDIA VIX", {})
                px = q.get("last_price", 0)
                if px:
                    val = round(px, 2)
                    CACHE.set("vix", val)
                    return val
        except Exception as e:
            print(f"[VIX] Kite fetch failed, falling back: {e}")
    # 2) Yahoo fallback — only if Kite unavailable/failed.
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
    """
    SMA/RSI/crossover from candle data. Cached 5 min.
    Source priority: Zerodha Kite historical (accurate, real-time) -> Yahoo Finance (MCX/fallback).
    Always returns stale cache on failure rather than empty dict.
    """
    cache_key = f"tech_{sym}"
    if CACHE.fresh(cache_key, TTL["technicals"]):
        return CACHE.get_val(cache_key)

    inst = INSTRUMENTS.get(sym, {})

    kite_key   = CACHE.get_val("_kite_key")   or ""
    kite_token = CACHE.get_val("_kite_token") or ""

    d = None

    if kite_key and kite_token and not inst.get("mcx"):
        d = fetch_kite_live_candles_computed(sym, kite_key, kite_token)
        if d:
            print(f"[Tech] {sym}: Kite candles OK")

    if not d:
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
            elif sym == "SILVER":
                mcx_scale = usdinr * 1000 / 31.1035  # $/troy_oz → ₹/kg (MCX quotes per kg)
            elif sym == "NATURALGAS":
                mcx_scale = usdinr          # $/mmBtu → ₹/mmBtu (MCX quotes per mmBtu directly)
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
            "avg_range_pct":   d.get("avg_range_pct",0),
            "hh_hl":           d.get("hh_hl",False),
            "lh_ll":           d.get("lh_ll",False),
            "_source":         d.get("_source","yahoo"),
        }
        # ORB computed here so it flows to /market for ALL symbols (not just /smc).
        # This is the fix for ORB always being 0% in snapshots: previously ORB
        # only came from /smc which runs for a few symbols; now every symbol in
        # the market response carries ORB via the same reliable path as CPR.
        if d.get("candles"):
            orb_calc = calc_orb(d["candles"])
            if orb_calc:
                tech["orb"] = orb_calc
            # SMC computed here too — same fix as ORB. calc_smc is heavier but
            # get_technicals is cached 5 min per symbol, so it runs at most once
            # per symbol per 5 min. This fixes SMC always being ~1% in snapshots
            # (previously only came from /smc for a handful of viewed symbols).
            try:
                smc_calc = calc_smc(d["candles"])
                if smc_calc:
                    tech["smc"] = smc_calc
            except Exception as e:
                print(f"[Tech] {sym} SMC calc failed: {e}")
        CACHE.set(cache_key, tech)
        if d.get("candles"):
            CACHE.set(f"candles5_{sym}", d["candles"])
            write_candles(sym, "5m", d["candles"])
        if d.get("candles_daily"):
            CACHE.set(f"candles1d_{sym}", d["candles_daily"])
            write_candles(sym, "1d", d["candles_daily"])
        return tech
    return CACHE.get_val(cache_key) or {}

def resolve_mcx_front_month(sym, key, token):
    """
    Returns the current front-month MCX futures Kite symbol for `sym`
    (e.g. "MCX:SILVER25AUGFUT"), re-resolving from Zerodha's instrument
    list whenever the cached contract has expired. Shared by both the
    price-fetch path and the OI-fetch path so they can never disagree
    about which contract is "current" — previously each had its own
    separate, inconsistent caching logic (one never re-validated expiry
    at all; the other only happened to re-fetch on a cache miss, not on
    a genuinely expired-but-still-cached string).
    """
    cache_key = f"mcx_sym_{sym}"
    cached = CACHE.get_val(cache_key)  # {"sym":..., "expiry":"YYYY-MM-DD"} or None
    today_d = datetime.now(IST).date()
    if cached and isinstance(cached, dict):
        try:
            exp_d = datetime.strptime(cached["expiry"], "%Y-%m-%d").date()
            if exp_d >= today_d:
                return cached["sym"]  # still valid, not yet expired
        except Exception:
            pass  # malformed cache entry — fall through and re-resolve

    r_inst = KITE_SESSION.get("https://api.kite.trade/instruments/MCX",
        headers=_kite_headers(key, token), timeout=10)
    if r_inst.status_code != 200:
        return None
    from io import StringIO
    import csv as _csv
    reader = _csv.DictReader(StringIO(r_inst.text))
    contracts = []
    for row in reader:
        if row.get("name","").upper() != sym or row.get("instrument_type","") != "FUT":
            continue
        exp_str = row.get("expiry","")
        try:
            exp_d = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if exp_d < today_d: continue  # skip already-expired contracts
        contracts.append((exp_str, f"MCX:{row.get('tradingsymbol','')}"))
    if not contracts:
        # No contracts matched this exact name — likely a naming mismatch
        # vs Zerodha's MCX instrument master (e.g. if NATURALGAS is listed
        # as "NATGASMINI" or similar). Log clearly so this doesn't fail
        # silently.
        print(f"[MCX] WARNING: no valid FUT contracts found for name='{sym}' — check exact Zerodha MCX instrument name")
        return None
    contracts.sort()  # earliest still-valid expiry first = front month
    exp_str, mcx_kite_sym = contracts[0]
    CACHE.set(cache_key, {"sym": mcx_kite_sym, "expiry": exp_str})
    print(f"[MCX] {sym} front-month resolved: {mcx_kite_sym} (expires {exp_str})")
    return mcx_kite_sym



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
    for sym in ["CRUDEOIL","GOLD","SILVER","NATURALGAS"]:
        try:
            mcx_kite_sym = resolve_mcx_front_month(sym, key, token)

            if mcx_kite_sym:
                r_q = KITE_SESSION.get("https://api.kite.trade/quote",
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
                elif sym=="SILVER" and px<500: f=usd_inr*1000/31.1035; px=round(d["px"]*f,2); chg=round(d["chg"]*f,2)
                elif sym=="NATURALGAS" and px<50: px=round(px*usd_inr,2); chg=round(d["chg"]*usd_inr,2)
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

    def best_effort_cache(reason=""):
        """Try disk first, then stale in-memory cache. Never return None if we have ANY data.
        This prevents OI freezing when Zerodha is temporarily unreachable (Render wake-up,
        network hiccup, token issue). Stale data is better than no data — signals still
        score correctly and the user sees something rather than a frozen strip."""
        disk = load_disk()
        if disk:
            return disk
        stale = CACHE.get_val(cache_key)
        if stale:
            age_min = int((CACHE.age(cache_key) or 0) / 60)
            print(f"[OI] {sym}: serving stale in-memory cache ({age_min}min old). Reason: {reason}")
            d = dict(stale); d["cached"]=True; d["cache_age_min"]=age_min; d["stale"]=True
            return d
        return None

    def save_disk(data):
        try:
            with open(disk_file,"w") as f:
                _json.dump({"data":data,"ts":time.time()},f)
        except: pass

    if not key or not token:
        return best_effort_cache("no credentials")

    try:
        hdrs = _kite_headers(key, token)

        # Step 1 — spot price
        if not spot:
            if sym in OI_INDICES:
                idx_map = {"NIFTY":"NSE:NIFTY 50","BANKNIFTY":"NSE:NIFTY BANK",
                           "FINNIFTY":"NSE:NIFTY FIN SERVICE","SENSEX":"BSE:SENSEX"}
                kite_sym = idx_map.get(sym)
            elif sym in OI_MCX:
                # Shared, expiry-aware resolver — see resolve_mcx_front_month()
                # definition for why this used to be a separate, inconsistent
                # implementation here.
                kite_sym = resolve_mcx_front_month(sym, key, token)
            else:
                kite_sym = INSTRUMENTS.get(sym,{}).get("kite","")

            if kite_sym:
                r = KITE_SESSION.get("https://api.kite.trade/quote",
                    params={"i": kite_sym}, headers=hdrs, timeout=10)
                if r.status_code in [401,403]:
                    print(f"[OI] Auth failed for {sym} — token expired?")
                    return best_effort_cache("token expired/invalid (401/403)")
                if r.status_code == 200:
                    for v in (r.json().get("data") or {}).values():
                        spot = v.get("last_price",0); break
                else:
                    print(f"[OI] Spot fetch HTTP {r.status_code} for {sym}")
            else:
                print(f"[OI] No kite_sym found for {sym}")

        if not spot: return best_effort_cache("spot fetch failed")

        # Step 2 — instruments CSV (cached 4h)
        # Route to correct exchange instruments file
        if sym == "SENSEX":
            csv = fetch_kite_instruments_bfo(key, token)   # BSE/BFO
        elif sym in OI_MCX:
            csv = fetch_kite_instruments_mcx(key, token)   # MCX commodities
        else:
            csv = fetch_kite_instruments_nfo(key, token)   # NSE/NFO (default)
        if not csv: return best_effort_cache("instruments CSV unavailable")

        lines = csv.strip().split("\n")
        step  = INSTRUMENTS.get(sym,{}).get("step",50)
        atm   = int(round(spot/step)*step)
        # NO strike range filter — fetch ALL active strikes for this expiry.
        # ATM ±30 still only captures ~52% of full-chain OI (far OTM calls at
        # 25,500-28,000 hold 10+ Cr CE OI, making PCR look too high vs Sensibull).
        # Removing the filter and using POST (no URL length limit) gives full-chain
        # PCR matching Sensibull. The NFO CSV only lists ~80-150 active strikes for
        # a given NIFTY weekly expiry, so the quote call stays within Zerodha limits.

        # Find nearest expiry.
        # FIX: require digit immediately after sym prefix — "NIFTY" matches
        # "NIFTY25JUN..." (next char is '2') but NOT "NIFTYNXT50..." or "NIFTYIT..."
        # NIFTYNXT50 and MIDCPNIFTY options were contaminating NIFTY OI totals.
        sym_len = len(sym)
        def sym_match_fn(ts):
            return (ts.startswith(sym) and len(ts) > sym_len and
                    (ts[sym_len].isdigit() or sym in ("SENSEX","CRUDEOIL","GOLD")))

        now_ist = datetime.now(IST)
        today_n = now_ist.replace(tzinfo=None)
        # EXPIRY DAY HANDLING:
        # Keep showing the CURRENT (expiring) contract all day so intraday expiry
        # moves — short covering, unwinding, pinning — remain visible and tradeable.
        # Only roll to the NEXT expiry after 3 PM, when the expiring contract is
        # effectively settled (last 30 min) and its OI is meaningless.
        # min_days_allowed=0 normally; =1 after 3PM (skip today's dead contract).
        after_3pm = now_ist.hour >= 15
        min_days_allowed = 1 if after_3pm else 0

        best_exp=None; best_days=999
        all_exps = set()
        for line in lines[1:]:
            cols=line.split(",")
            if len(cols)<10: continue
            ts = cols[2] if len(cols)>2 else ""
            if not sym_match_fn(ts): continue
            if cols[9] not in ["CE","PE"]: continue
            try:
                exp=datetime.strptime(cols[5],"%Y-%m-%d")
                d2=(exp-today_n).days
                all_exps.add(cols[5])
                if min_days_allowed<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue

        if not best_exp: return best_effort_cache("no valid expiry found")
        is_expiry_today = (best_days == 0)  # current contract expires today
        best_days = best_days  # days to expiry — used for IV calculation

        # Collect instruments for ATM ±10
        # Each exchange uses its own prefix for Kite quote API
        # ATM ±20 strikes = ±1000pts for NIFTY (step=50), ±2000pts for BANKNIFTY (step=100)
        # This matches Sensibull's visible range. Full chain inflated both CE and PE ~10%
        # because far-OTM strikes (22000PE, 26500CE etc.) are included but Sensibull cuts them.
        # Since both sides inflate equally PCR stays right, but absolute values and walls are wrong.
        atm = int(round(spot/step)*step)
        # Strike range: ATM ±10 (original calibration).
        # 21 strikes × 2 = 42 instruments. PCR thresholds are set for this range.
        # Changing range without changing thresholds breaks signal scoring — they
        # must stay in sync. If you need a wider range, recalibrate thresholds too.
        #
        # EXPIRY-DAY WIDENING: on expiry day OI genuinely concentrates further
        # from ATM than a normal day — heavy far-strike writing as premium
        # collapses, last-minute pinning/unwinding, plus positions built up
        # over the whole prior week can sit at strikes outside a narrow
        # same-day window. Confirmed against live Kite/Sensibull comparison
        # screenshots that a flat ±10 window mismatches PCR specifically on
        # expiry day. Widen to ±30 strikes on expiry day ONLY, so every other
        # day keeps the exact calibration the PCR thresholds were tuned for.
        strike_range = step * (30 if is_expiry_today else 10)
        lo_strike = atm - strike_range
        hi_strike = atm + strike_range

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
            ts = cols[2] if len(cols)>2 else ""
            if not sym_match_fn(ts): continue
            if cols[9] not in ["CE","PE"] or cols[5]!=best_exp: continue
            try:
                sk=int(float(cols[6]))
                if sk < lo_strike or sk > hi_strike: continue  # ATM ±10 strikes
                instruments.append({"sym":f"{exchange_prefix}:{cols[2]}","strike":sk,"type":cols[9]})
            except: continue

        if not instruments: return best_effort_cache("no option instruments found")

        # Step 3 — Batched GET (Zerodha /quote is GET-only, POST returns 405)
        syms = [i["sym"] for i in instruments]
        qdata = {}
        for batch_start in range(0, len(syms), 100):
            batch = syms[batch_start:batch_start+100]
            try:
                rb = KITE_SESSION.get("https://api.kite.trade/quote",
                    params=[("i",s) for s in batch], headers=hdrs, timeout=15)
                if rb.status_code in [401,403]:
                    return best_effort_cache("auth failed 401/403 on batch quote")
                if rb.status_code == 200:
                    qdata.update(rb.json().get("data",{}))
            except Exception as be:
                print(f"[OI] batch GET error: {be}")
        strikes_used = [i['strike'] for i in instruments]
        print(f"[OI] {sym}: expiry={best_exp} ATM={atm} range={min(strikes_used) if strikes_used else 0}-{max(strikes_used) if strikes_used else 0} count={len(strikes_used)} quoted={len(qdata)}")

        # Step 4 — Aggregate OI with correct change calculation
        # FIX: oi_day_low is the MINIMUM OI seen today (a level, not a delta).
        # Using (current_oi - oi_day_low) gives a meaningless number — e.g. if OI
        # dropped from 200k → 100k → 150k, day_low=100k but actual change is +50k.
        # Correct approach: compare to previous fetch (~2-5 min ago).
        prev_snap_key = f"oi_snap_{sym}_{best_exp}"
        prev_snap = CACHE.get_val(prev_snap_key) or {}  # {strike_type: oi}

        ce_oi=0;pe_oi=0;ce_chg=0;pe_chg=0
        strikes_data={}
        new_snap = {}
        for inst in instruments:
            q=qdata.get(inst["sym"],{})
            if not q: continue
            oi = q.get("oi",0) or 0
            s  = inst["strike"]
            snap_key = f"{s}_{inst['type']}"
            prev_oi  = prev_snap.get(snap_key, oi)  # first call: delta=0
            chg = oi - prev_oi
            new_snap[snap_key] = oi
            if s not in strikes_data:
                strikes_data[s]={"ce":0,"pe":0,"ce_chg":0,"pe_chg":0}
            if inst["type"]=="CE":
                ce_oi+=oi; ce_chg+=chg
                strikes_data[s]["ce"]=oi; strikes_data[s]["ce_chg"]=chg
            else:
                pe_oi+=oi; pe_chg+=chg
                strikes_data[s]["pe"]=oi; strikes_data[s]["pe_chg"]=chg

        # Persist snapshot for next OI fetch (used to compute accurate delta)
        if new_snap:
            CACHE.set(prev_snap_key, new_snap)

        # 15-MINUTE OI CHANGE TRACKING: separate from the fetch-to-fetch
        # delta above (~2 min, mostly noise). Keeps a rolling history of
        # (timestamp, total_ce_oi, total_pe_oi) samples and finds the one
        # closest to 15 minutes ago for a meaningful medium-term OI
        # build-up/unwind reading (e.g. "PE OI +1.2Cr in last 15min" =
        # fresh put writing / support building).
        hist_key = f"oi_hist_{sym}_{best_exp}"
        oi_hist = CACHE.get_val(hist_key) or []
        now_ts = time.time()
        oi_hist.append({"t": now_ts, "ce": ce_oi, "pe": pe_oi})
        oi_hist = [h for h in oi_hist if now_ts - h["t"] <= 2400]
        CACHE.set(hist_key, oi_hist)

        ce_chg_15m = 0; pe_chg_15m = 0; oi_15m_available = False
        target_ts = now_ts - 900
        if len(oi_hist) >= 2:
            closest = min(oi_hist[:-1], key=lambda h: abs(h["t"]-target_ts))
            if abs(closest["t"]-target_ts) <= 300:
                ce_chg_15m = ce_oi - closest["ce"]
                pe_chg_15m = pe_oi - closest["pe"]
                oi_15m_available = True

        if not ce_oi and not pe_oi:
            return best_effort_cache("option chain response error")
        # Sanity check: if total OI is suspiciously low (< 0.05 Cr per side),
        # the BFO/NFO quote likely returned empty OI (market not yet open, or
        # BFO OI arrives with a ~5 min delay at market open). Don't cache this
        # near-zero result — fall back to last good value so the app shows
        # valid walls/PCR rather than misleading 0.10 Cr readings.
        # On expiry day, OI legitimately declines all day as the contract settles.
        # Lower the threshold so real declining OI isn't mistaken for bad data and
        # frozen to stale cache — otherwise expiry-day OI shows wrong (stale) values.
        min_oi_threshold = 100_000 if is_expiry_today else 500_000
        if ce_oi < min_oi_threshold or pe_oi < min_oi_threshold:
            stale = CACHE.get_val(cache_key)
            if stale and stale.get("ce_oi",0) >= min_oi_threshold:
                print(f"[OI] {sym}: suspiciously low OI ({ce_oi}/{pe_oi}) — using stale cache")
                return stale
            # No good stale either — return current (better than nothing)
            print(f"[OI] {sym}: low OI warning ({ce_oi}/{pe_oi}) — no valid stale available")

        # Step 5 — Compute metrics
        pcr = round(pe_oi/ce_oi,2) if ce_oi else 0

        mp=0;mpv=float("inf")
        for s in strikes_data:
            pain=sum(max(0,x-s)*strikes_data[x]["ce"]+max(0,s-x)*strikes_data[x]["pe"]
                     for x in strikes_data)
            if pain<mpv: mpv=pain;mp=s

        # Walls — exclude the 2 outermost strikes on each side of ATM.
        # Edge strikes accumulate legacy/hedging OI from prior expiries that
        # got rolled here. This caused SENSEX walls to show 76,500 (edge)
        # instead of 76,900 (actual highest concentration near ATM).
        # Trimming 2 strikes per side (step×2 pts) ensures wall = active
        # resistance/support, not far-OTM legacy positions.
        all_strikes = sorted(strikes_data.keys())
        trim = 2  # exclude 2 outermost strikes on each side
        wall_strikes = all_strikes[trim:-trim] if len(all_strikes) > trim*2 else all_strikes
        wall_nearby  = {s: strikes_data[s] for s in wall_strikes}
        # CE wall = strike with most call OI (resistance above current price)
        # PE wall = strike with most put OI (support below current price)
        ce_wall_strikes = {s:d for s,d in wall_nearby.items() if s > atm}
        pe_wall_strikes = {s:d for s,d in wall_nearby.items() if s < atm}
        ce_wall = max(ce_wall_strikes, key=lambda x:ce_wall_strikes[x]["ce"], default=0) or                   max(wall_nearby, key=lambda x:wall_nearby[x]["ce"], default=0)
        pe_wall = max(pe_wall_strikes, key=lambda x:pe_wall_strikes[x]["pe"], default=0) or                   max(wall_nearby, key=lambda x:wall_nearby[x]["pe"], default=0)

        buildup="UNKNOWN"
        if ce_chg>0 and pe_chg>0: buildup="LONG_BUILDUP" if pe_chg>ce_chg else "SHORT_BUILDUP"
        elif ce_chg<0 and pe_chg<0: buildup="SHORT_COVERING" if ce_chg<pe_chg else "LONG_UNWINDING"
        elif ce_chg>0: buildup="SHORT_BUILDUP"
        elif pe_chg>0: buildup="LONG_BUILDUP"

        # PCR thresholds calibrated for ATM ±30 range — full-range PCR runs higher
        # than ATM-only due to far-OTM hedging puts. Sensibull-aligned thresholds.
        # PCR thresholds calibrated for ATM ±10 strikes (original baseline).
        # These must stay in sync with pcrBull/pcrBear thresholds in index.html.
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
                # On expiry day, DTE→0 makes the sqrt(dte/365) term collapse and
                # IV explode to meaningless values. Floor DTE at 1 day, and on
                # expiry day itself use a fractional day (remaining hours/24) so
                # the IV proxy stays in a sane range rather than spiking.
                import math
                if is_expiry_today:
                    hrs_left = max(0.5, 15.5 - now_ist.hour - now_ist.minute/60)
                    dte = hrs_left / 24  # fraction of a day remaining
                else:
                    dte = max(best_days, 1)
                iv_est = round(straddle / (spot * math.sqrt(dte/365)) * 100, 1)
                iv_est = min(iv_est, 80)  # cap at realistic max
        except Exception as iv_err:
            print(f"[OI] IV calc error: {iv_err}")

        write_oi_chain(sym, best_exp, atm, spot, strikes_data)

        result = {
            "ce_oi":ce_oi,"pe_oi":pe_oi,"ce_chg":ce_chg,"pe_chg":pe_chg,
            "ce_chg_15m":ce_chg_15m,"pe_chg_15m":pe_chg_15m,"oi_15m_available":oi_15m_available,
            "pcr":pcr,"max_pain":int(mp),"iv":iv_est,
            "ce_wall":int(ce_wall),"pe_wall":int(pe_wall),
            "spot":round(spot,1),"buildup":buildup,"pcr_interp":pcr_interp,
            "mp_dist":round(spot-mp,0) if mp else 0,
            "source":"zerodha_kite","expiry":best_exp,
            "strikes_count":len(strikes_data),
            "is_expiry_day":is_expiry_today,   # today = expiry for this contract
            "days_to_expiry":best_days,
            "atm":atm
        }
        CACHE.set(cache_key, result)
        save_disk(result)
        print(f"[OI ✅] {sym} PCR:{pcr} MP:{mp} CE:{ce_oi} PE:{pe_oi} Exp:{best_exp} Strikes:{len(strikes_data)}")
        return result

    except Exception as e:
        import traceback
        print(f"[OI ERROR] {sym}: {e}\n{traceback.format_exc()[-300:]}")
        result = best_effort_cache("exception in OI fetch")
        if not result:
            CACHE.set_error(cache_key)
        return result


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

    # FIX: structure can show "UNKNOWN" even when a real CHoCH/BOS event just
    # fired. This happens because swg_trend=NEUTRAL (ambiguous HH/HL pattern,
    # which is NORMAL during a reversal) falls through to int_trend, and if
    # internal pivots are also sparse, int_trend is "UNKNOWN" too — even though
    # all_structure_events already contains a valid CHoCH/BOS break.
    # Fix: if structure is NEUTRAL or UNKNOWN, derive it from the most recent
    # (highest-index) structure event instead of the ambiguous trend label.
    # A CHoCH/BOS event existing means the market just told us a direction —
    # that's more reliable than a stale HH/HL/LH/LL trend classification.
    if structure in ("NEUTRAL", "UNKNOWN") and all_structure_events:
        most_recent = max(all_structure_events, key=lambda e: e["index"])
        structure = most_recent["bias"]  # BULLISH or BEARISH from the latest break

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
    """Get full market intelligence for a symbol. Cached 5 min.
    Source priority: Zerodha Kite historical -> Yahoo Finance fallback."""
    cache_key = f"smc_{sym}"
    if CACHE.fresh(cache_key, TTL["smc"]):
        return CACHE.get_val(cache_key)

    inst   = INSTRUMENTS.get(sym,{})
    ticker = inst.get("yahoo","")

    kite_key   = CACHE.get_val("_kite_key")   or ""
    kite_token = CACHE.get_val("_kite_token") or ""
    use_kite   = bool(kite_key and kite_token and not inst.get("mcx"))

    result = {}

    # ── 5min candles ─────────────────────────────────────────────────────────
    if use_kite:
        candles5 = fetch_kite_live_candles(sym, kite_key, kite_token, "5m", 2)
    else:
        d5_yh = fetch_yahoo_candles(ticker, "5m", "2d") if ticker else None
        candles5 = (d5_yh.get("candles",[]) if d5_yh else [])

    # Fallback to candles5 cache (set by get_technicals) if fresh fetch empty
    if not candles5:
        candles5 = CACHE.get_val(f"candles5_{sym}") or []

    # SMC, VWAP, ORB, Volume — all from 5min candles
    if candles5:
        result["smc"] = calc_smc(candles5)
    vwap = calc_vwap(candles5)
    if vwap: result["vwap"] = vwap
    orb = calc_orb(candles5)
    if orb: result["orb"] = orb
    vol = calc_volume_analysis(candles5)
    if vol: result["volume"] = vol

    # ── Daily candles (CPR) ──────────────────────────────────────────────────
    # Reuse if get_technicals already fetched them this cycle
    candles_daily = CACHE.get_val(f"candles1d_{sym}") or []
    if not candles_daily:
        if use_kite:
            candles_daily = fetch_kite_live_candles(sym, kite_key, kite_token, "1d", 35)
        elif ticker:
            d1d_yh = fetch_yahoo_candles(ticker, "1d", "1mo")
            candles_daily = d1d_yh.get("candles",[]) if d1d_yh else []
        if candles_daily:
            CACHE.set(f"candles1d_{sym}", candles_daily)

    if candles_daily:
        cpr = calc_cpr(candles_daily)
        if cpr: result["cpr"] = cpr

    # ── MTF alignment (15min + 1hr) ──────────────────────────────────────────
    def _trend_from_candles(bars):
        """SMA20 vs SMA50 trend from a candle list."""
        if not bars or len(bars) < 20: return ""
        cl = [c["c"] for c in bars]
        s20 = sum(cl[-20:]) / 20
        s50 = sum(cl[-50:]) / 50 if len(cl) >= 50 else None
        if not s50: return ""
        return "BULLISH" if s20 > s50 else "BEARISH"

    if use_kite:
        candles15 = fetch_kite_live_candles(sym, kite_key, kite_token, "15m", 5)
        candles1h = fetch_kite_live_candles(sym, kite_key, kite_token, "1h",  30)
        tf5  = _trend_from_candles(candles5)
        tf15 = _trend_from_candles(candles15)
        tf1h = _trend_from_candles(candles1h)
    else:
        d15_yh = fetch_yahoo_candles(ticker, "15m", "5d") if ticker else None
        d1h_yh = fetch_yahoo_candles(ticker, "1h", "1mo") if ticker else None
        tf5  = d5_yh.get("trend","")  if not use_kite and d5_yh  else _trend_from_candles(candles5)
        tf15 = d15_yh.get("trend","") if d15_yh else ""
        tf1h = d1h_yh.get("trend","") if d1h_yh else ""

    trends = [tf5, tf15, tf1h]
    bulls = trends.count("BULLISH"); bears = trends.count("BEARISH")
    result["mtf"] = {
        "tf5": tf5, "tf15": tf15, "tf1h": tf1h,
        "alignment": ("STRONG_BULL" if bulls==3 else "BULL" if bulls==2
                      else "STRONG_BEAR" if bears==3 else "BEAR" if bears==2 else "MIXED")
    }

    # ── OI writer behavior ────────────────────────────────────────────────────
    writer = calc_oi_writer_behavior(oi_data)
    if writer: result["writer"] = writer

    # ── Market regime ─────────────────────────────────────────────────────────
    tech = get_technicals(sym)
    regime = detect_market_regime(candles5, oi_data, vwap, orb, vol)
    result["regime"] = regime

    # ── Trend15, PDH, PDL ─────────────────────────────────────────────────────
    result["trend15"]        = tech.get("trend15","UNKNOWN")
    result["trend_strength"] = tech.get("trend_strength",0)
    result["hh_hl"]          = tech.get("hh_hl",False)
    result["lh_ll"]          = tech.get("lh_ll",False)
    result["pdh"]            = tech.get("prev_day_high",0)
    result["pdl"]            = tech.get("prev_day_low",0)

    # Live price for above_vwap check
    prices_cache = CACHE.get_val("all_prices") or {}
    px = prices_cache.get(sym,{}).get("px",0) or (candles5[-1]["c"] if candles5 else 0)
    result["above_vwap"] = (px > vwap) if vwap else None

    # ── AI narrative ──────────────────────────────────────────────────────────
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
    """Keepalive endpoint — called every 10 min by frontend to prevent Render free-tier sleep."""
    return jsonify({
        "ok":    True,
        "time":  ist_str(),
        "cache_keys": len(CACHE._store) if hasattr(CACHE,'_store') else -1,
    })

@app.route("/debug_orb/<sym>")
def debug_orb(sym):
    """Debug endpoint: shows raw 5m candles and ORB calculation for a symbol.
    Call: /debug_orb/NIFTY?key=xxx&token=yyy
    Shows exactly what timestamps Kite returns and whether ORB window is found.
    """
    sym = sym.upper()
    key, token = _creds()
    if key and token:
        CACHE.set("_kite_key", key)
        CACHE.set("_kite_token", token)

    bars = fetch_kite_live_candles(sym, key, token, "5m", 2)
    orb  = calc_orb(bars) if bars else None

    # Find today's candles only
    from datetime import datetime, timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)
    today = now_ist.date()
    orb_start = int(datetime(today.year, today.month, today.day, 3, 45, tzinfo=timezone.utc).timestamp())
    orb_end   = int(datetime(today.year, today.month, today.day, 4,  0, tzinfo=timezone.utc).timestamp())
    orb_bars  = [b for b in bars if orb_start <= b["t"] < orb_end]
    today_bars= [b for b in bars if b["t"] >= orb_start]

    return jsonify({
        "sym": sym,
        "total_bars": len(bars),
        "today_bars": len(today_bars),
        "orb_window": {"start": orb_start, "end": orb_end,
                       "start_ist": datetime.fromtimestamp(orb_start, tz=ist).strftime("%H:%M"),
                       "end_ist":   datetime.fromtimestamp(orb_end,   tz=ist).strftime("%H:%M")},
        "orb_bars_found": len(orb_bars),
        "orb_bars": orb_bars,
        "orb_result": orb,
        "first_bar": bars[0]  if bars else None,
        "last_bar":  bars[-1] if bars else None,
        "kite_creds": "present" if (key and token) else "MISSING",
    })


@app.route("/export_candles")
def export_candles():
    """
    Export 5-min candles for all watchlist symbols as CSV.
    One-time use — download this and upload to the Local Replay tab.
    Format: timestamp,open,high,low,close,volume,symbol
    """
    import csv as _csv, io as _io
    key   = CACHE.get_val("_kite_key")   or ""
    token = CACHE.get_val("_kite_token") or ""

    # All NSE symbols (no MCX — Yahoo only, not useful for replay)
    syms = [s for s,v in INSTRUMENTS.items() if not v.get("mcx")]

    output = _io.StringIO()
    writer = _csv.writer(output)
    writer.writerow(["timestamp","open","high","low","close","volume","symbol"])

    fetched = 0
    for sym in syms:
        try:
            bars = fetch_kite_live_candles(sym, key, token, "5m", 60)
            if not bars:
                print(f"[Export] {sym}: 0 bars — skipping")
                continue
            for b in bars:
                writer.writerow([b["t"],b["o"],b["h"],b["l"],b["c"],b["v"],sym])
            fetched += 1
            print(f"[Export] {sym}: {len(bars)} bars ✅")
        except Exception as e:
            print(f"[Export] {sym}: ERROR {e}")

    csv_data = output.getvalue()
    print(f"[Export] Done — {fetched}/{len(syms)} symbols exported")
    from flask import Response
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=protrader_candles_60d.csv"}
    )


@app.route("/usdinr")
def usdinr():
    return jsonify({"ok":True,"rate":get_usdinr(),"time":ist_str()})

@app.route("/lot_sizes")
def lot_sizes():
    """Get current lot sizes from Zerodha NFO instruments CSV."""
    key, token = _creds()

    # Fallback lot sizes — updated to current SEBI values (Nov 2024 revision)
    # NIFTY: 75 → 65, BANKNIFTY: 30 → 35, FINNIFTY: 40 → 65, SENSEX: 20 → 20
    # MIDCPNIFTY: 120 → 120 (unchanged)
    # Source: NSE circular Nov 2024. Update this whenever SEBI revises.
    fallback = {"NIFTY":65,"BANKNIFTY":35,"FINNIFTY":65,"SENSEX":20,
                "MIDCPNIFTY":120,"CRUDEOIL":100,"GOLD":100,"SILVER":30,"NATURALGAS":1250}

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
    key, token = _creds()
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
            r = KITE_SESSION.get("https://api.kite.trade/quote", params=params_h, headers=hdrs, timeout=8)
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
    key, token = _creds()
    
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
    key, token = _creds()
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
    key, token = _creds()

    # Cache credentials — stored for entire server lifetime (no TTL on CACHE.set)
    # This ensures /smc calls after this point can use Kite candles
    if key and token:
        CACHE.set("_kite_key",   key)
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
            # FIX (commodity signals): this whole block — trend15, PDH/PDL, CPR,
            # VWAP, ORB, and SMC — used to be wrapped in `if not inst.get("mcx")`,
            # so it silently never ran for CRUDEOIL/GOLD/SILVER/NATURALGAS. But
            # get_technicals() above computes ALL of this for MCX from the exact
            # same candle pipeline as NSE/BSE (see the mcx_scale conversion loop
            # in get_technicals) — the data already exists in `tech`, it was just
            # being thrown away here before reaching the client.
            #
            # Downstream effect this caused: the client scoring engine treats a
            # missing vwap/orb/smc/cpr as "this layer doesn't apply" (vwap>0
            # checks fail, orb/smc objects are undefined) rather than "this
            # layer is genuinely absent" — so for MCX, 4 of the 5 independent-
            # layer categories (price action, SMC structure, and effectively
            # regime, since regime derivation also leans on vwap/trend15) were
            # PERMANENTLY zero. Only the OI category is meant to be zero for
            # MCX (that exclusion is intentional and stays — see OI_MCX). With
            # price/SMC/ORB also gone, bullLayers/bearLayers could basically
            # never exceed 1, which hard-caps confidence at 74% and locks
            # urgency at LOW forever (urg requires conf>=75 or layers>=2) — so
            # commodity signals weren't weak, they were structurally silenced.
            #
            # OI itself stays excluded for MCX (client still gates that on
            # !isMCX) — this fix only restores the price-action layers, which
            # have no NSE-specific dependency at all.
            d["trend15"]         = tech.get("trend15","UNKNOWN")
            d["trend_strength"]  = tech.get("trend_strength",0)
            d["trend_up_count"]  = tech.get("trend_up_count",0)
            d["trend_sessions"]  = tech.get("trend_sessions",0)
            d["avg_range_pct"]   = tech.get("avg_range_pct",0)
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
            # FIX: MCX price entries use short keys "h"/"l" (see the MCX fetch
            # loop above) and never carry "open" at all — only NSE/BSE entries
            # use "high"/"low"/"open". Without these fallbacks, hi/lo silently
            # evaluated to 0 for every commodity and the VWAP block below never
            # ran even after un-gating it. tech's own high/low/open (computed
            # from real candles, INR-scaled for MCX) is the fallback for open
            # specifically, since no live "open" exists anywhere in the raw
            # MCX price dict.
            hi  = d.get("high",0) or d.get("h",0) or 0
            lo  = d.get("low",0)  or d.get("l",0) or 0
            if px and hi and lo:
                op  = d.get("open",0) or tech.get("open",0) or px
                d["vwap"] = round((op+hi+lo+px)/4, 2)
                d["above_vwap"] = px > d["vwap"]
            # ORB from technicals (computed in get_technicals for all symbols)
            if tech.get("orb"):
                d["orb"] = tech["orb"]
            # SMC from technicals (same all-symbols fix as ORB).
            # Trim redundant aliases (orderBlocks/fvgZones duplicate ob/fvg)
            # to keep the 55-symbol market payload lean.
            if tech.get("smc"):
                _smc = tech["smc"]
                d["smc"] = {k:v for k,v in _smc.items()
                            if k not in ("orderBlocks","fvgZones")}
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

def _poll_fill_price(order_id, hdrs, tries=6, delay=0.7):
    """
    Read back the average fill price of a just-placed MARKET order.
    Kite fills market orders in well under a second, but the order history can
    lag briefly, so we poll a few times. Returns float or None.
    """
    for _ in range(tries):
        try:
            r = KITE_SESSION.get(f"https://api.kite.trade/orders/{order_id}",
                             headers=hdrs, timeout=10)
            j = r.json()
            if j.get("status") == "success":
                legs = j.get("data") or []
                if legs:
                    last = legs[-1]
                    avg  = float(last.get("average_price") or 0)
                    st   = (last.get("status") or "").upper()
                    if st == "COMPLETE" and avg > 0:
                        return avg
                    if st in ("REJECTED","CANCELLED"):
                        return None
        except Exception:
            pass
        time.sleep(delay)
    return None


@app.route("/exit_order", methods=["POST"])
def exit_order():
    """
    Square off an open option position at market, and cancel its resting stop.
    The app previously had no exit path of any kind — this is it.

    Body: sym, strike, option_type, qty, key, token,
          [product=MIS], [cancel_order_id] (the SL-M order to pull first)
    """
    auth_err = _check_order_auth()
    if auth_err: return auth_err
    try:
        data = request.get_json(force=True) or {}
        sym    = data.get("sym","").upper()
        strike = int(data.get("strike",0))
        opt    = data.get("option_type","CE").upper()
        qty    = int(data.get("qty",0))
        product= (data.get("product","MIS") or "MIS").upper()
        cancel_id = data.get("cancel_order_id","")
        key, token = _creds_strict(data)

        if not key or not token:
            return jsonify({"ok":False,"error":"Missing Kite credentials"}),401
        if not sym or not strike or not qty:
            return jsonify({"ok":False,"error":"Need sym, strike and qty"}),400

        hdrs = _kite_headers(key, token)

        # Pull the resting stop first, otherwise it becomes a naked short once
        # the long leg is gone.
        cancelled = False
        if cancel_id:
            try:
                rc = KITE_SESSION.delete(f"https://api.kite.trade/orders/regular/{cancel_id}",
                                     headers=hdrs, timeout=10)
                cancelled = (rc.status_code == 200)
            except Exception:
                cancelled = False

        if sym == "SENSEX":
            csv = fetch_kite_instruments_bfo(key, token); exchange = "BFO"
        elif sym in OI_MCX:
            csv = fetch_kite_instruments_mcx(key, token); exchange = "MCX"
        else:
            csv = fetch_kite_instruments_nfo(key, token); exchange = "NFO"
        if not csv:
            return jsonify({"ok":False,"error":"Could not fetch instruments"})

        today_n = datetime.now(IST).replace(tzinfo=None)
        best_exp = None; best_days = 999
        for line in csv.strip().split("\n")[1:]:
            cols = line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym): continue
            if cols[9] not in ["CE","PE"]: continue
            try:
                d2 = (datetime.strptime(cols[5],"%Y-%m-%d")-today_n).days
                if 0<=d2<best_days: best_days=d2; best_exp=cols[5]
            except: continue

        tradingsymbol = None
        for line in csv.strip().split("\n")[1:]:
            cols = line.split(",")
            if len(cols)<10 or not cols[2].startswith(sym): continue
            if cols[9]!=opt or cols[5]!=best_exp: continue
            try:
                if int(float(cols[6]))==strike:
                    tradingsymbol = cols[2]; break
            except: continue
        if not tradingsymbol:
            return jsonify({"ok":False,"error":f"Tradingsymbol not found for {sym} {strike} {opt}"})

        r = KITE_SESSION.post("https://api.kite.trade/orders/regular", headers=hdrs, timeout=15,
            data={"tradingsymbol":tradingsymbol,"exchange":exchange,
                  "transaction_type":"SELL","order_type":"MARKET",
                  "quantity":qty,"product":product,"validity":"DAY"})
        resp = r.json()
        if r.status_code==200 and resp.get("status")=="success":
            oid = resp.get("data",{}).get("order_id","")
            print(f"[Exit ✅] {tradingsymbol} qty:{qty} order_id:{oid}")
            return jsonify({"ok":True,"order_id":oid,"tradingsymbol":tradingsymbol,
                            "stop_cancelled":cancelled})
        return jsonify({"ok":False,"error":resp.get("message","Exit failed"),
                        "stop_cancelled":cancelled})
    except Exception as e:
        print(f"[Exit] Exception: {e}")
        return jsonify({"ok":False,"error":str(e)})


@app.route("/place_order", methods=["POST"])
def place_order():
    """
    Place a real Zerodha order. Resolves correct tradingsymbol from instruments CSV.

    TIER-0 HARDENING (see notes below):
      1. Requires caller-supplied Kite credentials — no server-cache fallback.
      2. Optional X-PT-Secret shared-secret gate.
      3. Defaults to MIS (intraday, broker auto-square-off) instead of NRML.
      4. Rejects naked option selling unless explicitly opted into, so a
         directional signal can never be routed as a short option by accident.
      5. Automatically attaches a protective SL-M stop leg on option buys.
    """
    auth_err = _check_order_auth()
    if auth_err: return auth_err
    try:
        data = request.get_json(force=True) or {}
        sym   = data.get("sym","").upper()
        strike= int(data.get("strike",0))
        opt   = data.get("option_type","CE").upper()  # CE or PE
        action= data.get("action","BUY").upper()       # BUY or SELL
        key, token = _creds_strict(data)
        qty   = int(data.get("qty",0))
        # Product: MIS = intraday with broker auto-square-off. This app is an
        # intraday engine, so NRML (carry-forward, full margin, no auto exit)
        # is the wrong default and was the previous behaviour.
        product = (data.get("product","MIS") or "MIS").upper()
        if product not in ("MIS","NRML"): product = "MIS"
        # Protective stop on the option premium, as a % of the fill price.
        # 0 or None disables the stop leg.
        sl_pct  = float(data.get("sl_pct", 30) or 0)
        allow_short = bool(data.get("allow_short", False))

        if not key or not token:
            return jsonify({"ok":False,"error":"Missing Kite credentials — send key/token in the request body or X-Kite-Key / X-Kite-Token headers"}),401
        if not sym or not strike:
            return jsonify({"ok":False,"error":"Missing symbol or strike"})
        if action not in ("BUY","SELL"):
            return jsonify({"ok":False,"error":f"Invalid action '{action}' — must be BUY or SELL"}),400
        if opt not in ("CE","PE"):
            return jsonify({"ok":False,"error":f"Invalid option_type '{opt}' — must be CE or PE"}),400

        # ── DIRECTION SAFETY GATE ─────────────────────────────────────────────
        # A SELL on a CE/PE opens a NAKED SHORT OPTION: undefined risk, large
        # margin, and — critically — the OPPOSITE market view to the one a
        # "buy a put" bear signal intends. The old client sent SELL for every
        # bearish signal, which silently converted "buy 24000 PE" into "short
        # 24000 PE" (a bullish position). Refuse unless explicitly requested.
        if action == "SELL" and not allow_short:
            return jsonify({"ok":False,
                "error":("Refused: SELL on an option is a naked short (undefined risk) and is the "
                         "opposite view to a directional signal. To express a BEARISH view, send "
                         "action=BUY with option_type=PE. If you really intend to write options, "
                         "resend with allow_short=true.")}),400

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
        # Fallback only — the client normally sends qty from /lot_sizes, which
        # reads live lot sizes from the Zerodha NFO CSV. NIFTY was stale at 65.
        default_lots = {"NIFTY":75,"BANKNIFTY":35,"FINNIFTY":65,"SENSEX":20,
                        "CRUDEOIL":100,"GOLD":100,"SILVER":30,"NATURALGAS":1250,
                        "BSE":200}  # confirmed. MCX Ltd deliberately omitted — lot
                        # size figures I found for it weren't confident enough to
                        # hardcode; falls back to the generic 50 default below
                        # only if the live lookup ever fails, same as before.
        if not qty:
            qty = default_lots.get(sym, 50)

        # Step 3: Place entry order via Kite
        # FIX: SEBI's algo-trading compliance rules (enforced since April 1,
        # 2026) reject any market order missing market_protection — orders
        # with it at 0, or simply absent, get rejected outright. -1 tells
        # Zerodha to auto-calculate the protection band per their own
        # guidelines — this is Kite's own documented default, matches their
        # official API example exactly, not a guess.
        order_payload = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": action,
            "order_type": "MARKET",
            "quantity": qty,
            "product": product,
            "validity": "DAY",
            "market_protection": "-1"
        }
        r = KITE_SESSION.post("https://api.kite.trade/orders/regular",
            data=order_payload, headers=hdrs, timeout=15)
        resp = r.json()

        if not (r.status_code == 200 and resp.get("status") == "success"):
            err = resp.get("message","Unknown error")
            print(f"[Order ❌] {tradingsymbol}: {err}")
            return jsonify({"ok":False,"error":err,"tradingsymbol":tradingsymbol})

        order_id = resp.get("data",{}).get("order_id","")
        print(f"[Order ✅] {action} {tradingsymbol} qty:{qty} product:{product} order_id:{order_id}")

        # ── Step 4: PROTECTIVE STOP LEG ───────────────────────────────────────
        # Previously the app placed entries and never placed any exit at all —
        # live positions sat in the market with no stop attached. We now read
        # back the actual fill price and put a real SL-M order on the exchange.
        stop = {"placed": False, "reason": "", "order_id": "", "trigger": 0}
        if action == "BUY" and sl_pct > 0:
            fill_px = _poll_fill_price(order_id, hdrs)
            if not fill_px:
                stop["reason"] = "Could not read fill price — NO STOP IS ATTACHED, exit manually"
            else:
                trigger = round(fill_px * (1 - sl_pct/100.0), 1)
                if trigger < 0.05:
                    stop["reason"] = "Computed trigger below tick size — no stop placed"
                else:
                    sl_payload = {
                        "tradingsymbol": tradingsymbol,
                        "exchange": exchange,
                        "transaction_type": "SELL",   # exiting a long option
                        "order_type": "SL-M",
                        "quantity": qty,
                        "product": product,
                        "validity": "DAY",
                        "trigger_price": trigger,
                        # Genuinely unclear whether SL-M requires this — one
                        # source explicitly says SL/Limit orders don't need
                        # it, only true MARKET orders do. Included anyway:
                        # Kite's API is generally tolerant of extra optional
                        # params, and the cost of a wrongly-omitted required
                        # field (order rejected, position left unprotected)
                        # is far worse than an unneeded field being present.
                        "market_protection": "-1"
                    }
                    try:
                        r2 = KITE_SESSION.post("https://api.kite.trade/orders/regular",
                            data=sl_payload, headers=hdrs, timeout=15)
                        d2 = r2.json()
                        if r2.status_code == 200 and d2.get("status") == "success":
                            stop = {"placed": True, "reason": "",
                                    "order_id": d2.get("data",{}).get("order_id",""),
                                    "trigger": trigger, "fill_px": fill_px}
                            print(f"[Stop ✅] SL-M {tradingsymbol} trigger:{trigger} (fill {fill_px})")
                        else:
                            stop["reason"] = d2.get("message","SL order rejected")
                            print(f"[Stop ❌] {tradingsymbol}: {stop['reason']}")
                    except Exception as se:
                        stop["reason"] = str(se)
                        print(f"[Stop ❌] {tradingsymbol}: {se}")

        return jsonify({"ok":True,"order_id":order_id,"tradingsymbol":tradingsymbol,
                       "exchange":exchange,"qty":qty,"expiry":best_exp,
                       "product":product,"action":action,"stop":stop})

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
    key, token = _creds()

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
    """Full market intelligence: SMC + CPR + MTF + VWAP + ORB + Volume + Regime + Narrative.
    Accepts optional ?key=&token= to prime Kite credentials without requiring /market first.
    This ensures ORB/SMC/VWAP use live Kite candles even on cold-start Render instances.
    """
    sym = sym.upper()
    if sym not in INSTRUMENTS:
        return jsonify({"ok":False,"error":"Unknown symbol"}),400
    # Accept and cache Kite credentials if passed — allows /smc to use Kite
    # without depending on /market having been called first this session.
    key, token = _creds()
    if key and token:
        CACHE.set("_kite_key",   key)
        CACHE.set("_kite_token", token)
        print(f"[SMC] Kite credentials updated from /smc request for {sym}")
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
    key, token = _creds()
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
        r = KITE_SESSION.post("https://api.kite.trade/session/token",
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
    key, token = _creds()
    if not key or not token:
        return jsonify({"error":"Pass ?key=YOUR_API_KEY&token=YOUR_ACCESS_TOKEN"})
    hdrs = _kite_headers(key,token)
    result = {"key_provided":bool(key),"token_provided":bool(token)}
    try:
        # Test 1: Profile
        r1 = KITE_SESSION.get("https://api.kite.trade/user/profile",headers=hdrs,timeout=10)
        result["profile_status"] = r1.status_code
        result["profile_ok"] = r1.status_code==200
        result["profile_response"] = r1.json() if r1.status_code!=200 else {"name": r1.json().get("data",{}).get("user_name","")}

        # Test 2: Spot price
        r2 = KITE_SESSION.get("https://api.kite.trade/quote?i=NSE%3ANIFTY+50",headers=hdrs,timeout=10)
        result["quote_status"] = r2.status_code
        result["quote_ok"] = r2.status_code==200
        result["quote_response"] = r2.json() if r2.status_code!=200 else {"price": list(r2.json().get("data",{}).values())[0].get("last_price",0) if r2.json().get("data") else 0}

        # Test 3: NFO instruments (just count lines)
        r3 = KITE_SESSION.get("https://api.kite.trade/instruments/NFO",headers=hdrs,timeout=20)
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
    time.sleep(30)  # wait for server boot and first client /market call with credentials
    print(f"[StockOI] Started — {len(OI_STOCKS)} liquid stocks + {len(OI_MCX)} MCX (5-min cycle)")
    while True:
        try:
            key   = CACHE.get_val("_kite_key") or ""
            token = CACHE.get_val("_kite_token") or ""
            if not key or not token:
                print("[StockOI] No credentials yet — waiting...")
                time.sleep(60); continue
            prices = CACHE.get_val("all_prices") or {}
            fetched = 0; auth_failed = 0
            for sym in sorted(OI_STOCKS | OI_MCX):
                try:
                    result = get_oi(sym, key, token, prices.get(sym,{}).get("px",0))
                    if result and result.get("source") == "zerodha_kite":
                        fetched += 1
                        print(f"[StockOI ✅] {sym} PCR:{result.get('pcr','?')} MP:{result.get('max_pain','?')}")
                    elif result:
                        print(f"[StockOI 📋] {sym} from cache/disk")
                        fetched += 1
                    else:
                        auth_failed += 1
                    time.sleep(3)
                except Exception as e:
                    print(f"[StockOI ❌] {sym}: {e}")
                    time.sleep(3)
            total = len(OI_STOCKS | OI_MCX)
            print(f"[StockOI] Cycle done — {fetched}/{total} ok, {auth_failed} failed. Sleeping 5min.")
            # If ALL fetches failed, token is likely expired — back off longer
            # to avoid hammering Zerodha with 401s every 5 minutes
            if auth_failed == total:
                print("[StockOI] All fetches failed — token likely expired. Backing off 10min.")
                time.sleep(600)
            else:
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
_backfill_jobs = {}   # job_id → {status, progress, message, done, total, results}


def _backfill_run_job(job_id):
    """
    One-time (re-runnable) 60-day candle backfill across every tracked
    symbol. Runs in a background thread — a full pass across ~58 symbols,
    rate-limited, realistically takes a couple of minutes, too long for a
    single blocking request.

    Kite's per-request limit for 5-minute candles is 100 days (confirmed
    against Zerodha's own developer forum) — 60 days fits in ONE call per
    symbol, no pagination needed. Rate-limited to ~1 req/sec across symbols,
    since Zerodha's historical API throttles rapid-fire requests across many
    instruments.

    MCX commodities are included but will NOT get a real 60-day backfill —
    Kite's historical API doesn't cover MCX at all, so these fall back to
    Yahoo, which only offers a ~2-day window. That's a real, known ceiling,
    not a bug in this job — flagged clearly in the results so it's visible
    rather than silently incomplete.
    """
    try:
        key = CACHE.get_val("_kite_key") or ""
        token = CACHE.get_val("_kite_token") or ""
        syms = sorted(INSTRUMENTS.keys())
        total = len(syms)
        _backfill_jobs[job_id] = {"status": "running", "progress": 0,
                                   "message": "Starting…", "done": 0, "total": total,
                                   "results": []}
        for i, sym in enumerate(syms):
            is_mcx = INSTRUMENTS.get(sym, {}).get("mcx", False)
            _backfill_jobs[job_id]["message"] = f"Fetching {sym} ({i+1}/{total})"
            _backfill_jobs[job_id]["progress"] = round((i / total) * 100, 1)
            try:
                bars = fetch_kite_live_candles(sym, key, token, "5m", days=60)
                written = write_candles_bulk(sym, "5m", bars)
                _backfill_jobs[job_id]["results"].append({
                    "sym": sym, "bars": len(bars), "written": written,
                    "mcx_limited": is_mcx,
                })
                # FIX: previously the per-symbol failure reason only lived in
                # this in-memory results list, retrievable only via
                # /backfill_status/<job_id> — useless once the job_id isn't
                # in hand anymore (e.g. triggered from the phone button,
                # which doesn't surface it prominently) or the process has
                # restarted since. Now every outcome prints straight to
                # Render's logs as it happens — the actual reason is always
                # right there, no job_id needed.
                if written == 0:
                    print(f"[Backfill] {sym}: fetched {len(bars)} bars but wrote 0 "
                          f"— {'expected for MCX (Yahoo fallback only)' if is_mcx else 'unexpected, worth checking Supabase connectivity'}")
            except Exception as e:
                _backfill_jobs[job_id]["results"].append({
                    "sym": sym, "bars": 0, "written": 0, "error": str(e),
                })
                print(f"[Backfill] {sym}: FAILED — {e}")
            _backfill_jobs[job_id]["done"] = i + 1
            time.sleep(1)  # pace requests — avoid Zerodha's rate limit on rapid historical calls

        results = _backfill_jobs[job_id]["results"]
        total_bars = sum(r.get("written", 0) for r in results)
        failed = [r for r in results if r.get("error") or r.get("written", 0) == 0]
        real_failures = [r for r in failed if not r.get("mcx_limited")]
        _backfill_jobs[job_id].update({
            "status": "done", "progress": 100,
            "message": f"Complete — {total_bars} candles written across {total} symbols"
                       + (f", {len(real_failures)} unexpected failures (MCX shortfall excluded)" if real_failures else ""),
        })
        # Distinguish "expected MCX shortfall" from "genuinely failed, worth
        # investigating" right in the summary — a wall of symbol names with
        # no reason attached (the old behaviour) isn't actionable on its own.
        fail_detail = ", ".join(f"{r['sym']} ({r.get('error','wrote 0 bars')})" for r in real_failures)
        print(f"[Backfill] Done: {total_bars} candles across {total} symbols. "
              f"Real failures: {fail_detail or 'none'}")
    except Exception as e:
        _backfill_jobs[job_id] = {"status": "error", "progress": 0, "message": str(e)}
        print(f"[Backfill] Job {job_id} crashed: {e}")


@app.route("/backfill_candles", methods=["POST"])
def backfill_candles_start():
    """Kick off the 60-day candle backfill. Returns immediately with a job_id
    to poll — the actual fetch runs in a background thread."""
    job_id = str(_uuid.uuid4())[:8]
    t = threading.Thread(target=_backfill_run_job, args=(job_id,), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/backfill_status/<job_id>")
def backfill_status(job_id):
    job = _backfill_jobs.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Unknown job_id"}), 404
    return jsonify({"ok": True, **job})



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
def _bt_get_kite_token(sym, key, token):
    """Thin backtest wrapper — delegates to shared _get_kite_instr_token."""
    return _get_kite_instr_token(sym, key, token)

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
        r = KITE_SESSION.get(url, headers=_kite_headers(key, api_token), timeout=30)
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
              orb_h, orb_l, mins, trend15, prev_sma20=None, prev_sma50=None, vol_ratio=1.0):
    """
    Replicate signal scoring engine for one bar. Returns (bull, bear, layer_map).
    Mirrors live engine EXACTLY including all recent fixes:
      - VWAP above: +1 (was +2, FIX 3)
      - Golden/Death Cross: +2 (was +4, FIX 2)
      - Layer map: dict of every component contribution for attribution analysis
    """
    bull=0; bear=0
    aboveVwap = vwap>0 and px>vwap
    belowVwap = vwap>0 and px<vwap
    layer = {}  # layer attribution: key → signed contribution (+bull/-bear)

    # ── SMA / crossover layer ─────────────────────────────────────────────
    if px>sma20 and px>sma50:    bull+=1; layer["above_both_sma"]=+1
    elif px<sma20 and px<sma50:  bear+=1; layer["above_both_sma"]=-1
    else: layer["above_both_sma"]=0
    if sma20>sma50:              bull+=1; layer["sma_trend"]=+1
    elif sma20<sma50:            bear+=1; layer["sma_trend"]=-1
    else: layer["sma_trend"]=0
    # Golden/Death Cross +2 (FIX 2 — was +4, too much weight on lagging indicator)
    golden = prev_sma20 and prev_sma50 and prev_sma20<prev_sma50 and sma20>sma50
    death  = prev_sma20 and prev_sma50 and prev_sma20>prev_sma50 and sma20<sma50
    if golden: bull+=2; layer["golden_cross"]=+2
    elif death: bear+=2; layer["golden_cross"]=-2
    else: layer["golden_cross"]=0

    # ── RSI layer ─────────────────────────────────────────────────────────
    if rsi<32 and px>sma20:          bull+=2; layer["rsi"]=+2
    elif 35<=rsi<=55 and px>sma20:   bull+=1; layer["rsi"]=+1
    elif rsi>68 and px<sma20:        bear+=2; layer["rsi"]=-2
    elif 45<=rsi<=65 and px<sma20:   bear+=1; layer["rsi"]=-1
    else: layer["rsi"]=0

    # ── VWAP layer (FIX 3 — above/below +1 not +2; reclaim is the +2 event) ──
    vwap_reclaim = aboveVwap and rsi>0 and rsi<55   # proxy: above VWAP + momentum not extended
    vwap_reject  = belowVwap and rsi>45
    if vwap_reclaim:        bull+=2; layer["vwap"]=+2
    elif aboveVwap:         bull+=1; layer["vwap"]=+1
    elif vwap_reject:       bear+=2; layer["vwap"]=-2
    elif belowVwap:         bear+=1; layer["vwap"]=-1
    else: layer["vwap"]=0

    # ── PDH/PDL layer ─────────────────────────────────────────────────────
    vol_ok = vol_ratio > 1.2
    if pdh and px>pdh*1.001:       bull+=(2 if vol_ok else 1); layer["pdh"]=+(2 if vol_ok else 1)
    elif pdh and abs(px-pdh)/pdh<0.002: bear+=1; layer["pdh"]=-1
    else: layer["pdh"]=0
    if pdl and px<pdl*0.999:       bear+=(2 if vol_ok else 1); layer["pdl"]=-(2 if vol_ok else 1)
    elif pdl and abs(px-pdl)/pdl<0.002: bull+=1; layer["pdl"]=+1
    else: layer["pdl"]=0

    # ── CPR layer ─────────────────────────────────────────────────────────
    if cpr_narrow and cpr_tc and px>cpr_tc: bull+=2; layer["cpr"]=+2
    elif cpr_narrow and cpr_bc and px<cpr_bc: bear+=2; layer["cpr"]=-2
    else: layer["cpr"]=0

    # ── ORB layer (tiered by session) ─────────────────────────────────────
    if orb_h and orb_l:
        tier = 3 if mins<11*60 else 2 if mins<14*60 else 1
        vol_mult = 1 if not vol_ok else 0  # +1 bonus with volume (max tier already covers it)
        if px>orb_h*1.001:   bull+=tier; layer["orb"]=+tier
        elif px<orb_l*0.999: bear+=tier;  layer["orb"]=-tier
        else: layer["orb"]=0
    else: layer["orb"]=0

    # ── 15D trend layer ───────────────────────────────────────────────────
    tmap = {"STRONG_BULL":(3,0),"BULL":(2,0),"NEUTRAL":(0,0),"BEAR":(0,2),"STRONG_BEAR":(0,3)}
    bt,be = tmap.get(trend15,(0,0))
    bull+=bt; bear+=be
    layer["trend15"] = +bt if bt else -be

    # ── Volume layer ──────────────────────────────────────────────────────
    if vol_ok and px>sma20:  bull+=1; layer["volume"]=+1
    elif vol_ok and px<sma20: bear+=1; layer["volume"]=-1
    else: layer["volume"]=0
    # Contradiction: breakout/breakdown on drying volume
    breakout_dry = (px>orb_h*1.001 if orb_h else False) and vol_ratio<0.5
    if breakout_dry: bull=max(0,bull-3); layer["vol_penalty"]=-3
    else: layer["vol_penalty"]=0

    # ── Counter-trend penalty ─────────────────────────────────────────────
    if trend15 in ["STRONG_BULL","BULL"] and bear>bull:
        bear=max(0,bear-2); layer["counter_trend"]=-2
    elif trend15 in ["STRONG_BEAR","BEAR"] and bull>bear:
        bull=max(0,bull-2); layer["counter_trend"]=-2
    else: layer["counter_trend"]=0

    return bull, bear, layer

def _bt_derive_regime(px, sma20, sma50, vwap, trend15, rsi, pct):
    """
    Mirrors the LIVE engine's client-side deriveRegime() exactly — same
    thresholds, same category names. This is the technical-only fallback
    regime detector the live app already uses when full OI-based regime
    data isn't available, so this is genuine parity, not an approximation
    invented for the backtest.

    Categories: TRENDING_UP, TRENDING_DOWN, CHOPPY, DEVELOPING.
    NOTE: the live engine's SERVER-side regime classifier (detect_market_regime
    in server.py) has additional categories — BREAKOUT_DAY, EXPIRY_PINNING,
    MEAN_REVERT — that require OI/IV data and can't be replicated here for
    the same reason setup_key excludes "OI": no reliable intraday OI in
    historical backtests. Once the snapshot-based backtest exists, swap this
    for the full classifier.
    """
    if not px or not sma20:
        return "DEVELOPING"
    aboveVwap = px>vwap if vwap else px>sma20
    smaUp     = sma20>sma50
    strongBull = trend15=="STRONG_BULL"
    bull15     = trend15 in ("BULL","STRONG_BULL")
    bear15     = trend15 in ("BEAR","STRONG_BEAR")
    strongBear = trend15=="STRONG_BEAR"

    if strongBull and aboveVwap and smaUp:   return "TRENDING_UP"
    if strongBear and not aboveVwap and not smaUp: return "TRENDING_DOWN"
    if bull15 and aboveVwap and smaUp:       return "TRENDING_UP"
    if bear15 and not aboveVwap and not smaUp: return "TRENDING_DOWN"
    if abs(pct)<0.25 and 42<rsi<58 and (smaUp!=aboveVwap): return "CHOPPY"
    return "DEVELOPING"

def _bt_setup_key(px, sma20, sma50, rsi, vwap, pdh, pdl, cpr_tc, cpr_bc, cpr_narrow,
                   orb_h, orb_l, trend15, bias):
    """
    Tags a backtest signal with the setup components that fired — mirrors the
    live engine's setup_key taxonomy (e.g. "OI+Price+Momentum+Structure+Regime")
    as closely as possible using only backtest-available data.

    Two intentional differences from the live engine's setup_key:
      1. "OI" is NEVER included — the backtest has no reliable intraday OI
         (NSE end-of-day archives would be look-ahead bias). This is the ONE
         layer this taxonomy is waiting on. Once the snapshot-based backtest
         has 30+ days of real intraday OI, "OI" can be added as a 5th category
         here without changing the aggregation/leaderboard code at all.
      2. "ORB" replaces the live engine's "Structure"/"CHoCH" tag — backtest
         has no SMC order-block/CHoCH/BOS data. Opening Range Breakout is the
         closest available proxy for "price cleared a meaningful structural
         level," but it is NOT the same signal as live's SMC structure tag.
         Treat "ORB" here and "Structure"/"CHoCH" in live signals as related
         but distinct categories — don't merge them when comparing data later.
    """
    parts = []
    if bias == "BULLISH":
        price_fired  = bool((pdh and px > pdh*1.001) or (cpr_narrow and cpr_tc and px > cpr_tc))
        mom_fired    = bool((rsi < 32 and px > sma20) or (35 <= rsi <= 55 and px > sma20) or (sma20 > sma50))
        orb_fired    = bool(orb_h and orb_l and px > orb_h*1.001)
        regime_fired = (trend15 == "STRONG_BULL")
    else:
        price_fired  = bool((pdl and px < pdl*0.999) or (cpr_narrow and cpr_bc and px < cpr_bc))
        mom_fired    = bool((rsi > 68 and px < sma20) or (45 <= rsi <= 65 and px < sma20) or (sma20 < sma50))
        orb_fired    = bool(orb_h and orb_l and px < orb_l*0.999)
        regime_fired = (trend15 == "STRONG_BEAR")

    if price_fired:  parts.append("Price")
    if mom_fired:    parts.append("Momentum")
    if orb_fired:    parts.append("ORB")
    if regime_fired: parts.append("Regime")
    return "+".join(parts) if parts else "Unconfirmed"

def _bt_simulate(bias, entry, sl, t1, t2, future_bars):
    """
    Simulate trade outcome. Returns status + pnl_pct + MFE + MAE + bars_to_t1.
    MFE = Maximum Favorable Excursion (best price reached in our direction)
    MAE = Maximum Adverse Excursion (worst price reached against us)
    Both expressed as % from entry. Used to diagnose SL placement and target sizing.
    """
    effective_sl = sl; t1_hit = False
    mfe = 0.0   # best % move in our favour (always positive)
    mae = 0.0   # worst % move against us (always positive, i.e. max drawdown)
    bars_to_t1 = None

    for i, bar in enumerate(future_bars):
        b_ist = datetime.fromtimestamp(bar["ts"], tz=IST) if bar.get("ts") else None
        mins  = (b_ist.hour*60 + b_ist.minute) if b_ist else 0

        # Track MFE and MAE on every bar
        if bias == "BULLISH":
            bar_gain = (bar["h"] - entry) / entry * 100
            bar_loss = (entry - bar["l"]) / entry * 100
        else:
            bar_gain = (entry - bar["l"]) / entry * 100
            bar_loss = (bar["h"] - entry) / entry * 100
        mfe = max(mfe, bar_gain)
        mae = max(mae, bar_loss)

        # EOD cut
        if mins >= 15*60+15:
            pct = (bar["c"]-entry)/entry*100 if bias=="BULLISH" else (entry-bar["c"])/entry*100
            return {"status":"EOD","exit":round(bar["c"],2),"pnl_pct":round(pct,2),"bars":i+1,
                    "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":bars_to_t1}

        if bias == "BULLISH":
            if bar["l"] <= effective_sl:
                pct = (effective_sl-entry)/entry*100
                return {"status":"T1_BE_SL" if t1_hit else "SL_HIT",
                        "exit":round(effective_sl,2),"pnl_pct":round(pct,2),"bars":i+1,
                        "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":bars_to_t1}
            if not t1_hit and bar["h"] >= t1:
                t1_hit = True; effective_sl = entry; bars_to_t1 = i+1
            if bar["h"] >= t2:
                return {"status":"T2_HIT","exit":round(t2,2),
                        "pnl_pct":round((t2-entry)/entry*100,2),"bars":i+1,
                        "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":bars_to_t1}
        else:
            if bar["h"] >= effective_sl:
                pct = (entry-effective_sl)/entry*100
                return {"status":"T1_BE_SL" if t1_hit else "SL_HIT",
                        "exit":round(effective_sl,2),"pnl_pct":round(pct,2),"bars":i+1,
                        "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":bars_to_t1}
            if not t1_hit and bar["l"] <= t1:
                t1_hit = True; effective_sl = entry; bars_to_t1 = i+1
            if bar["l"] <= t2:
                return {"status":"T2_HIT","exit":round(t2,2),
                        "pnl_pct":round((entry-t2)/entry*100,2),"bars":i+1,
                        "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":bars_to_t1}

    return {"status":"OPEN","exit":0,"pnl_pct":0,"bars":len(future_bars),
            "mfe":round(mfe,3),"mae":round(mae,3),"bars_to_t1":None}

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
                    vol_ratio = bar["v"] / (sum(b["v"] for b in day_bars[max(0,bi-20):bi])/min(20,bi) or 1) if bi>0 else 1.0
                    prev_c   = close_buf[-2] if len(close_buf)>=2 else None
                    prev_sma20 = sum(close_buf[-21:-1])/20 if len(close_buf)>=21 else None
                    prev_sma50 = sum(close_buf[-51:-1])/50 if len(close_buf)>=51 else None
                    bull, bear, layer_map = _bt_score(px,sma20,sma50,rsi,vwap,pdh,pdl,
                                           cpr_tc,cpr_bc,cpr_narrow,orb_h,orb_l,
                                           mins,trend15,prev_sma20,prev_sma50,vol_ratio)

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

                    setup_key = _bt_setup_key(px,sma20,sma50,rsi,vwap,pdh,pdl,
                                               cpr_tc,cpr_bc,cpr_narrow,
                                               orb_h,orb_l,trend15,bias)

                    # % move vs prev close — same definition as live engine's s.pct
                    pct_move = round((px-pdc)/pdc*100, 2) if pdc else 0
                    regime = _bt_derive_regime(px,sma20,sma50,vwap,trend15,rsi,pct_move)

                    session = ("morning" if mins<10*60 else
                               "midday"  if mins<14*60 else "afternoon")
                    signals_all.append({
                        "sym":sym,"date":str(dt),"time":bar_time.strftime("%H:%M"),
                        "bias":bias,"conf":conf,"bull":bull,"bear":bear,
                        "trend15":trend15,"cpr_narrow":cpr_narrow,
                        "pcr":round(pcr,2),"rsi":round(rsi,1),
                        "vwap":round(vwap,2),"entry":round(px,2),
                        "sl":round(sl,2),"t1":round(t1,2),"t2":round(t2,2),
                        "session":session,"setup_key":setup_key,"regime":regime,
                        "layers": layer_map,   # per-layer contribution map
                        "score":  max(bull, bear),  # winning side raw score
                        "vol_ratio": round(vol_ratio, 2),
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
                # n>=30 is the threshold we use everywhere else (suppression
                # rule discussed for the future live feedback loop) — flagging
                # it here too so small samples aren't mistaken for signal.
                out.append({"k":str(k),"w":w,"l":l,"wr":wr,"avg":avg,"n":len(g),"reliable":len(g)>=30})
            return sorted(out,key=lambda x:-x["wr"])

        aw=round(sum(s["pnl_pct"] for s in wins)/len(wins),2) if wins else 0
        al=round(sum(s["pnl_pct"] for s in losses)/len(losses),2) if losses else 0

        # ── MFE/MAE summary stats ────────────────────────────────────────────
        mfe_wins   = [s["mfe"] for s in wins   if s.get("mfe") is not None]
        mae_losses = [s["mae"] for s in losses  if s.get("mae") is not None]
        mae_wins   = [s["mae"] for s in wins    if s.get("mae") is not None]
        mfe_losses = [s["mfe"] for s in losses  if s.get("mfe") is not None]
        avg_mfe_win  = round(sum(mfe_wins)/len(mfe_wins),3)   if mfe_wins   else 0
        avg_mae_loss = round(sum(mae_losses)/len(mae_losses),3) if mae_losses else 0
        avg_mae_win  = round(sum(mae_wins)/len(mae_wins),3)    if mae_wins   else 0
        avg_mfe_loss = round(sum(mfe_losses)/len(mfe_losses),3) if mfe_losses else 0
        # Distribution: % of losses that never went MFE > 0.3% (entered wrong)
        losses_never_positive = sum(1 for s in losses if (s.get("mfe") or 0) < 0.3)
        # Distribution: % of wins that reached T1 within 30min (6 bars)
        wins_fast = sum(1 for s in wins if (s.get("bars_to_t1") or 999) <= 6)

        # ── Layer attribution analysis ────────────────────────────────────────
        # For each layer: compute win rate when layer fired vs when it didn't
        # "Fired" = |contribution| > 0
        layer_keys = ["above_both_sma","sma_trend","golden_cross","rsi","vwap",
                      "pdh","pdl","cpr","orb","trend15","volume","vol_penalty","counter_trend"]
        layer_stats = []
        for lk in layer_keys:
            with_layer    = [s for s in closed if abs(s.get("layers",{}).get(lk,0))>0]
            without_layer = [s for s in closed if abs(s.get("layers",{}).get(lk,0))==0]
            if not with_layer: continue
            wr_with    = round(sum(1 for s in with_layer    if s["pnl_pct"]>0)/len(with_layer)*100)    if with_layer    else 0
            wr_without = round(sum(1 for s in without_layer if s["pnl_pct"]>0)/len(without_layer)*100) if without_layer else 0
            edge = wr_with - wr_without
            avg_pnl = round(sum(s["pnl_pct"] for s in with_layer)/len(with_layer),2)
            layer_stats.append({
                "k": lk, "n": len(with_layer), "n_without": len(without_layer),
                "wr_with": wr_with, "wr_without": wr_without,
                "edge": edge, "avg_pnl": avg_pnl,
                "reliable": len(with_layer) >= 30,
                "verdict": ("KEEP" if edge >= 5 else "MONITOR" if edge >= 0 else "CONSIDER_REMOVING")
            })
        layer_stats.sort(key=lambda x: -x["edge"])

        # ── Score bucket analysis ─────────────────────────────────────────────
        def score_bucket(s):
            sc = s.get("score", max(s["bull"],s["bear"]))
            if sc <= 3:   return "3"
            elif sc <= 4: return "4"
            elif sc <= 5: return "5"
            elif sc <= 6: return "6"
            elif sc <= 7: return "7"
            else:         return "8+"
        by_score_raw = breakdown(score_bucket)
        # Sort numerically
        score_order = ["3","4","5","6","7","8+"]
        by_score_sorted = sorted(by_score_raw, key=lambda x: score_order.index(x["k"]) if x["k"] in score_order else 99)

        # ── Time-of-day bucket ─────────────────────────────────────────────────
        def time_bucket(s):
            t = s.get("time","")
            try:
                h,m = int(t.split(":")[0]),int(t.split(":")[1])
                mins_t = h*60+m
            except: return "Unknown"
            if   mins_t < 9*60+45:  return "9:30–9:44 (early)"
            elif mins_t < 10*60:    return "9:45–9:59 (late open)"
            elif mins_t < 11*60:    return "10:00–10:59"
            elif mins_t < 12*60:    return "11:00–11:59"
            elif mins_t < 13*60:    return "12:00–12:59"
            elif mins_t < 14*60:    return "13:00–13:59"
            else:                   return "14:00–15:00 (close)"
        by_time = sorted(breakdown(time_bucket), key=lambda x: x["k"])

        # ── MFE bucket breakdown ───────────────────────────────────────────────
        def mfe_bucket(s):
            mfe = s.get("mfe", 0) or 0
            if mfe < 0.2:   return "MFE <0.2% (never moved)"
            elif mfe < 0.5: return "MFE 0.2–0.5%"
            elif mfe < 0.8: return "MFE 0.5–0.8% (near T1)"
            elif mfe < 1.2: return "MFE 0.8–1.2% (hit T1)"
            else:           return "MFE >1.2% (hit T2 zone)"
        by_mfe = breakdown(mfe_bucket)

        # ── Interaction analysis — category grouped, not exact layer match ─────
        # Exact layer matching creates 2^N combos (9 layers = 512 combinations).
        # Group SMC sub-layers into one label, split VWAP state vs event.
        def combo_key(s):
            layers = s.get("layers",{})
            fires = []
            has_choch = abs(layers.get("golden_cross",0)) > 0  # reused as structure proxy
            has_ob    = False  # OB not tracked in server backtest (no SMC)
            # Server backtest layers available: above_both_sma, sma_trend, golden_cross,
            # rsi, vwap, pdh, pdl, cpr, orb, trend15, volume
            if abs(layers.get("trend15",0)) > 0:     fires.append("TREND")
            if abs(layers.get("orb",0)) > 0:          fires.append("ORB")
            if abs(layers.get("cpr",0)) > 0:          fires.append("CPR")
            if abs(layers.get("rsi",0)) > 0:          fires.append("RSI")
            if abs(layers.get("vwap",0)) > 0:
                # vwap layer: +2 = reclaim (event), +1 = state (above)
                fires.append("VWAP✦" if abs(layers.get("vwap",0)) >= 2 else "VWAP")
            if abs(layers.get("pdh",0)) > 0:          fires.append("PDH/PDL")
            if abs(layers.get("golden_cross",0)) > 0: fires.append("SMAcross")
            if abs(layers.get("volume",0)) > 0:       fires.append("VOL")
            return "+".join(sorted(fires)) if fires else "none"
        by_combo = sorted([r for r in breakdown(combo_key) if r["n"]>=20], key=lambda x:-x["wr"])[:25]

        result = {
            "total":len(signals_all), "closed":len(closed),
            "wins":len(wins), "losses":len(losses),
            "win_rate":round(len(wins)/len(closed)*100) if closed else 0,
            "avg_win":aw, "avg_loss":al,
            "profit_factor":round(abs(aw/al),2) if al else 0,
            # ── MFE/MAE stats ──
            "mfe_stats": {
                "avg_mfe_win":   avg_mfe_win,
                "avg_mae_win":   avg_mae_win,
                "avg_mfe_loss":  avg_mfe_loss,
                "avg_mae_loss":  avg_mae_loss,
                "losses_never_positive_pct": round(losses_never_positive/len(losses)*100) if losses else 0,
                "wins_fast_pct": round(wins_fast/len(wins)*100) if wins else 0,
            },
            # ── Existing breakdowns ──
            "by_session": breakdown(lambda s:s["session"]),
            "by_trend":   breakdown(lambda s:s["trend15"]),
            "by_bias":    breakdown(lambda s:s["bias"]),
            "by_conf":    breakdown(lambda s:"94%+" if s["conf"]>=94 else "88-93%" if s["conf"]>=88 else "82-87%" if s["conf"]>=82 else "<82%"),
            "by_cpr":     breakdown(lambda s:"NARROW" if s["cpr_narrow"] else "WIDE"),
            "by_sym":     breakdown(lambda s:s["sym"]),
            "by_setup":   breakdown(lambda s:s["setup_key"]),
            "by_regime":  breakdown(lambda s:s["regime"]),
            "by_setup_regime": breakdown(lambda s:s["setup_key"]+" / "+s["regime"]),
            # ── New breakdowns (5 points) ──
            "by_score":   by_score_sorted,    # Point 2: score bucket analysis
            "by_layer":   layer_stats,         # Point 3: layer attribution (which are wasted)
            "by_time":    by_time,             # Point 5: time-of-day win rate
            "by_mfe":     by_mfe,              # Point 1: MFE distribution
            "by_combo":   by_combo,            # Point 4: interaction analysis
            "signals":    sorted(signals_all, key=lambda s:s["date"]+s["time"])[-300:],
        }
        _bt_jobs[job_id].update({"status":"done","progress":100,"message":"Complete","result":result})
        write_backtest_result(job_id, params, result)
    except Exception as e:
        import traceback
        _bt_jobs[job_id].update({"status":"error","message":str(e),"trace":traceback.format_exc()})
        print(f"[BT] Job {job_id} failed: {e}")


_last_auto_backtest_date = None  # tracks which trading day we last auto-ran

def _bg_daily_backtest():
    """
    Runs the SAME backtest logic the manual "Run Backtest" button triggers
    (_bt_run_job, identical defaults: 60 days, 82% min conf, all symbols),
    automatically once per trading day shortly after NSE close. This is
    what keeps the win-rate confidence badge on signal cards fresh without
    needing you to remember to trigger it — every day gets a new
    backtest_runs row in Supabase, referable later.

    Deliberately NOT rewiring the candle source here — this still fetches
    fresh from Zerodha's historical API each run, same as the manual
    trigger always has. Migrating that to read from the Supabase candle
    archive instead is a separate, larger change (removes the daily Zerodha
    API load and the same-day Kite-token dependency below) — worth doing,
    but scoped out of this change to keep it low-risk.

    Known limitation: `_last_auto_backtest_date` is in-memory only, so a
    server restart mid-day could reset it and trigger one redundant re-run
    later that same day. Harmless (just a wasted ~5 min of Zerodha calls),
    not worth the added complexity of a Supabase-backed check for something
    this low-stakes.
    """
    global _last_auto_backtest_date
    time.sleep(60)  # let the server finish booting before the first check
    print("[AutoBacktest] Scheduler started — runs once daily after market close")
    while True:
        try:
            now = datetime.now(IST)
            today_str = now.strftime("%Y-%m-%d")
            is_weekday = now.weekday() < 5
            # 15:45 IST — NSE closes 15:30, small buffer for the day's final
            # candles to actually be available via Kite's historical API.
            past_close = (now.hour > 15) or (now.hour == 15 and now.minute >= 45)
            already_ran_today = (_last_auto_backtest_date == today_str)

            if is_weekday and past_close and not already_ran_today:
                key = CACHE.get_val("_kite_key") or ""
                token = CACHE.get_val("_kite_token") or ""
                if not key or not token:
                    print(f"[AutoBacktest] Skipping {today_str} — no cached Kite credentials "
                          f"(phone hasn't connected today). Will retry on next check.")
                else:
                    print(f"[AutoBacktest] Starting scheduled run for {today_str}")
                    job_id = str(_uuid.uuid4())[:8]
                    _bt_jobs[job_id] = {"status":"running","progress":0,"message":"Starting…","result":None}
                    _bt_run_job(job_id, {"days":60,"min_conf":82,"syms":"all"})
                    _last_auto_backtest_date = today_str
                    final_status = _bt_jobs.get(job_id,{}).get("status")
                    print(f"[AutoBacktest] Run for {today_str} finished — status: {final_status}")

            time.sleep(600)  # check every 10 min — cheap, no harm checking often
        except Exception as e:
            print(f"[AutoBacktest] Scheduler error: {e}")
            time.sleep(600)


_auto_backtest_thread = threading.Thread(target=_bg_daily_backtest, daemon=True)
_auto_backtest_thread.start()


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

def _warmup_token_cache():
    """
    Pre-warm the Kite instrument token cache for all 55 watchlist symbols.
    Runs in background 90s after startup (gives Kite credentials time to arrive
    via first /market call). Without warmup, Batch3 symbols hit cold CSV parse
    during first scan → 14 symbols missing SMA until 10:13 AM.
    """
    import time as _time
    _time.sleep(90)  # wait for first /market call to store credentials
    key   = CACHE.get_val("_kite_key")   or ""
    token = CACHE.get_val("_kite_token") or ""
    if not key or not token:
        print("[Warmup] No Kite credentials yet — skipping token warmup")
        return
    nsyms = [s for s,v in INSTRUMENTS.items() if not v.get("mcx")]
    print(f"[Warmup] Pre-warming instrument tokens for {len(nsyms)} symbols...")
    warmed = 0
    for sym in nsyms:
        try:
            tok = _get_kite_instr_token(sym, key, token)
            if tok: warmed += 1
        except Exception as e:
            print(f"[Warmup] {sym}: {e}")
    print(f"[Warmup] Done — {warmed}/{len(nsyms)} tokens cached")

import threading as _threading
_warmup_thread = _threading.Thread(target=_warmup_token_cache, daemon=True)
_warmup_thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=False)

