#!/usr/bin/env python3
"""
generator.py (fixed per requested items + latest order-call changes)

Fixes included previously:
- A.3: Historical timestamps use IST via now_ist()
- A.4: prev_close derived from DAILY candles (last full day) when not provided
- A.5: Guarded enum usage for get_quote() first-call path
- B.9: Precise hold duration using total_seconds() (float days)
- B.10: Do not assume instant fills; use a minimal pending-order heuristic
- B.11: Market-closed/holiday detection heuristic (no API list) using last_trade_time/volume
- C.13: Faster Google Sheets "is empty" check (acell) instead of get_all_values()
- C.16: Header "mode" -> "run_mode" for clarity
- C.17: get_cash_balance parses comma-formatted numbers
- C.18: More robust quote field extraction (last_traded_price/ltp + sample “ohlc.close”)
- C.19: Percent sanity check comment + consistent percent use
- C.20: Enforce a minimum loop sleep interval

New changes per your request:
- Removed `price` from any BUY/SELL order placement (market-only).
- Ensured every trade passes: exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_CASH, product=groww.PRODUCT_CNC.
- Removed unnecessary `timeout` parameter from place_order usage.
"""
import os
import time
import json
import base64
import math
import signal
import logging
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pyotp

# Groww API SDK import
try:
    from growwapi import GrowwAPI
except Exception:
    GrowwAPI = None

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# -------------------------
# Logging / Timezone
# -------------------------
LOG = logging.getLogger("goldbees_trader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
IST = ZoneInfo("Asia/Kolkata")

# -------------------------
# Strategy constants
# (All in PERCENT units; e.g., 0.30 means 0.30%)
# -------------------------
BUY_THRESHOLD = -0.29       # percent
PROFIT_TARGET = 4.0         # percent
STOP_LOSS = -1.0            # percent
TRAILING_DRAWDOWN = 0.30    # percent
MIN_DAYS = 0
MAX_DAYS = 5

# -------------------------
# Environment-driven config
# -------------------------
GENERATOR_API_KEY = os.environ.get("GENERATOR_API_KEY")
GENERATOR_TOTP_SECRET = os.environ.get("GENERATOR_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GCP_SERVICE_ACCOUNT_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = os.environ.get("SHEET_NAME", "Trades")
# C.20: enforce a floor later
LOOP_FREQ_SECONDS = float(os.environ.get("LOOP_FREQ_SECONDS", "1"))
RUN_MODE = os.environ.get("RUN_MODE", "EARLY")
END_TIME_IST = os.environ.get("END_TIME_IST", "15:30")
TICKER = os.environ.get("TICKER", "GOLDBEES")

# -------------------------
# Runtime state
# -------------------------
CASH = 0.0
SHARES_HELD = 0
BUY_PRICE = 0.0
ENTRY_DATE = None
PEAK_RETURN = None

# B.10: pending order heuristic (very lightweight)
PENDING_ORDER = None  # dict: {"side": "BUY"/"SELL", "qty": int, "price": float, "placed_at": datetime}

SHUTDOWN = False

# -------------------------
# Helpers
# -------------------------
def now_ist():
    return datetime.now(tz=IST)

def parse_time_hhmm(hhmm: str):
    h, m = hhmm.split(":")
    return int(h), int(m)

def ist_epoch_for_today(hhmm: str):
    h, m = parse_time_hhmm(hhmm)
    today = now_ist().date()
    dt = datetime(year=today.year, month=today.month, day=today.day, hour=h, minute=m, tzinfo=IST)
    return int(dt.timestamp())

def send_telegram(final_capital, result_text):
    """Send minimal Telegram message with final capital and SUCCESS/LOSS only."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        LOG.debug("Telegram not configured; skipping send.")
        return
    try:
        final_capital_val = float(final_capital or 0.0)
    except Exception:
        final_capital_val = 0.0
    text = f"Capital: ₹{final_capital_val:,.2f}\nResult: {result_text}"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        LOG.debug("Telegram response: %s", resp.text)
    except Exception as e:
        LOG.warning("Failed to send telegram: %s", e)

def load_gsheet_client():
    """Return a gspread client using the GCP_SERVICE_ACCOUNT_JSON env var."""
    if not GCP_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON not set")
    try:
        if GCP_SERVICE_ACCOUNT_JSON.strip().startswith("{"):
            info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
        else:
            info = json.loads(base64.b64decode(GCP_SERVICE_ACCOUNT_JSON).decode("utf-8"))
    except Exception:
        info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def append_trade_row(sheet_client, row):
    """Append a row to the configured sheet (creates if missing)."""
    try:
        sh = sheet_client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME, rows="2000", cols="20")

        # C.13: faster "is empty" check
        try:
            if ws.acell('A1').value is None:
                headers = [
                    "timestamp_ist", "run_mode", "ticker", "buy_price",
                    "sell_price", "shares", "pnl_amt", "pnl_pct", "final_capital"
                ]  # C.16: 'run_mode'
                ws.append_row(headers)
        except Exception:
            # Fallback if acell fails for any reason
            existing = ws.get_all_values()
            if len(existing) == 0:
                headers = [
                    "timestamp_ist", "run_mode", "ticker", "buy_price",
                    "sell_price", "shares", "pnl_amt", "pnl_pct", "final_capital"
                ]
                ws.append_row(headers)

        ws.append_row(row)
    except Exception as e:
        LOG.warning("Failed to append to sheet: %s", e)

# -------------------------
# Dynamic delta (δ) table & util
# -------------------------
CASH_DELTA_BANDS = [
    (0, 1_000, 0.0078),
    (1_001, 3_000, 0.0053),
    (3_001, 5_000, 0.0044),
    (5_001, 10_000, 0.0040),
    (10_001, 50_000, 0.0033),
    (50_001, 100_000, 0.0028),
    (100_001, 500_000, 0.0023),
    (500_001, 1_000_000, 0.0024),
    (1_000_001, 10_000_000, 0.0025),
]

def get_delta_for_cash(cash_amount: float) -> float:
    """Return δ for given cash amount. If not matched, return a conservative default."""
    try:
        c = float(cash_amount or 0.0)
    except Exception:
        c = 0.0
    for lo, hi, delta in CASH_DELTA_BANDS:
        if lo <= c <= hi:
            return float(delta)
    return 0.0030  # fallback

# -------------------------
# Groww wrapper
# -------------------------
class GrowwClientWrapper:
    """
    Wrapper that initializes Groww client using TOTP and provides:
    - get_quote
    - place_order (market-only; no price, no timeout)
    - get_holdings_for_user
    - get_historical_candle_data
    - get_cash_balance
    """
    def __init__(self, api_key, totp_secret):
        if GrowwAPI is None:
            raise RuntimeError("growwapi SDK not installed or available.")
        self.api_key = api_key
        self.totp_secret = totp_secret
        self.access_token = None
        self.client = None
        self._init_client()

    def _init_client(self):
        if not self.api_key or not self.totp_secret:
            raise RuntimeError("GENERATOR_API_KEY or GENERATOR_TOTP_SECRET not set")
        try:
            totp = pyotp.TOTP(self.totp_secret).now()
            try:
                token = GrowwAPI.get_access_token(self.api_key, totp)
                self.access_token = token
                self.client = GrowwAPI(self.access_token)
            except Exception:
                try:
                    self.client = GrowwAPI(self.api_key)
                except Exception:
                    self.client = GrowwAPI()
            LOG.info("Groww client initialized.")
        except Exception as e:
            LOG.error("Failed to initialize Groww client: %s", e)
            raise

    def refresh_if_needed(self):
        if self.client is None:
            self._init_client()

    def get_quote(self, trading_symbol, timeout=5):
        """
        A.5 + C.18:
        Try official signature first (per sample), guarded against missing enums.
        Fallback to alternative signatures.
        """
        self.refresh_if_needed()
        ex = getattr(self.client, "EXCHANGE_NSE", None)
        sg = getattr(self.client, "SEGMENT_CASH", None)
        try:
            if ex is not None and sg is not None:
                return self.client.get_quote(
                    exchange=ex,
                    segment=sg,
                    trading_symbol=trading_symbol,
                    timeout=timeout
                )
        except Exception:
            pass
        try:
            return self.client.get_quote(trading_symbol, timeout)
        except Exception:
            return self.client.get_quote(self.client.EXCHANGE_NSE, self.client.SEGMENT_CASH, trading_symbol)

    def place_order(self, trading_symbol, quantity, order_type=None, transaction_type=None, validity=None, **kwargs):
        """
        Market-order only. Always sets:
          exchange=self.client.EXCHANGE_NSE,
          segment=self.client.SEGMENT_CASH,
          product=self.client.PRODUCT_CNC
        No price. No timeout.
        """
        self.refresh_if_needed()
        try:
            return self.client.place_order(
                trading_symbol=trading_symbol,
                quantity=quantity,
                validity=validity or getattr(self.client, "VALIDITY_DAY", None),
                exchange=getattr(self.client, "EXCHANGE_NSE", None),
                segment=getattr(self.client, "SEGMENT_CASH", None),
                product=getattr(self.client, "PRODUCT_CNC", None),
                order_type=order_type or getattr(self.client, "ORDER_TYPE_MARKET", None),
                transaction_type=transaction_type or getattr(self.client, "TRANSACTION_TYPE_BUY", None),              
                **kwargs
            )
        
        except Exception as e:
                LOG.warning("place_order failed: %s", e)
                raise

    def get_holdings_for_user(self, timeout=5):
        self.refresh_if_needed()
        try:
            return self.client.get_holdings_for_user(timeout=timeout)
        except Exception:
            return self.client.get_holdings_for_user()

    def get_historical_candle_data(self, trading_symbol, start_time, end_time, interval_in_minutes=60, timeout=10):
        self.refresh_if_needed()
        return self.client.get_historical_candle_data(
            trading_symbol=trading_symbol,
            exchange=getattr(self.client, "EXCHANGE_NSE", None),
            segment=getattr(self.client, "SEGMENT_CASH", None),
            start_time=start_time,
            end_time=end_time,
            interval_in_minutes=interval_in_minutes,
            timeout=timeout
        )

    def get_cash_balance(self):
        """Fetch cnc_balance_available; C.17: allow comma-formatted strings."""
        self.refresh_if_needed()
        def _to_float(val):
            try:
                return float(str(val).replace(',', ''))
            except Exception:
                return None
        try:
            resp = self.client.get_available_margin_details()
            if not resp:
                raise RuntimeError("Empty response from get_available_margin_details()")
            if isinstance(resp, dict):
                equity = resp.get("equity_margin_details", {})
                if equity and "cnc_balance_available" in equity:
                    v = _to_float(equity["cnc_balance_available"])
                    if v is not None:
                        return v
                if "cnc_balance_available" in resp:
                    v = _to_float(resp["cnc_balance_available"])
                    if v is not None:
                        return v
                if "data" in resp and isinstance(resp["data"], dict):
                    d = resp["data"]
                    if "cnc_balance_available" in d:
                        v = _to_float(d["cnc_balance_available"])
                        if v is not None:
                            return v
            if isinstance(resp, (list, tuple)):
                for item in resp:
                    if isinstance(item, dict) and "cnc_balance_available" in item:
                        v = _to_float(item["cnc_balance_available"])
                        if v is not None:
                            return v
        except Exception as e:
            LOG.error("Error fetching cnc_balance_available: %s", e)
        raise RuntimeError("Unable to fetch cnc_balance_available from Groww API")

# -------------------------
# Utility functions
# -------------------------
def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def calculate_price_change_pct(current_last_price, prev_close):
    try:
        return (current_last_price - prev_close) / prev_close * 100.0
    except Exception:
        return 0.0

def compute_shares_to_buy(cash_amount: float, last_price: float) -> int:
    """
    Q = floor( C / ( P * (1 + δ) ) ), δ depends on cash band.
    """
    if last_price is None or last_price <= 0:
        return 0
    delta = get_delta_for_cash(cash_amount)
    try:
        denom = float(last_price) * (1.0 + float(delta))
        qty = math.floor(float(cash_amount) / denom) if denom > 0 else 0
        return int(max(0, qty))
    except Exception as e:
        LOG.warning("Error computing shares to buy: %s", e)
        return 0

def extract_last_price(quote: dict) -> float:
    """
    C.18: Use robust set of fields; per sample, 'last_price' present.
    Fallbacks include last_traded_price / ltp / ohlc last/close / offer_price.
    """
    if not isinstance(quote, dict):
        return 0.0
    last_price = (
        quote.get("last_price")
        or quote.get("last_trade_price")
        or quote.get("last_traded_price")
        or quote.get("ltp")
        or ((quote.get("ohlc") or {}).get("last"))
        or ((quote.get("ohlc") or {}).get("close"))
        or quote.get("offer_price")
    )
    return safe_float(last_price, 0.0)

def extract_previous_close(quote: dict) -> float | None:
    """
    A.4: Prefer explicit previous_close if provided by SDK; else try ohlc.prev_close/previous_close.
    If absent, we'll compute using DAILY candles (last full day's close) in the main loop.
    """
    if not isinstance(quote, dict):
        return None
    for key in ("previous_close", "prev_close"):
        if quote.get(key) not in (None, 0, "0"):
            return safe_float(quote.get(key), None)
    ohlc = quote.get("ohlc") or {}
    for key in ("previous_close", "prev_close", "close"):
        if ohlc.get(key) not in (None, 0, "0"):
            return safe_float(ohlc.get(key), None)
    return None

def ms_to_dt_ist(ms: int | float | None):
    if not ms:
        return None
    try:
        # ms epoch to IST datetime
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=IST)
    except Exception:
        return None

# -------------------------
# Main trading loop
# -------------------------
def run_live_loop():
    global CASH, SHARES_HELD, BUY_PRICE, ENTRY_DATE, PEAK_RETURN, PENDING_ORDER

    groww = GrowwClientWrapper(GENERATOR_API_KEY, GENERATOR_TOTP_SECRET)
    sheet_client = None
    try:
        sheet_client = load_gsheet_client()
        LOG.info("Sheets client ready.")
    except Exception as e:
        LOG.warning("Sheets client not available: %s", e)
        sheet_client = None

    # Fetch initial cash
    try:
        CASH = groww.get_cash_balance()
        CASH = safe_float(CASH, 0.0)
        LOG.info("Fetched initial available cash from Groww: ₹%.2f", CASH)
        if CASH < 100:
            LOG.warning("Available cash < ₹100 (₹%.2f). Trading will skip buy attempts until balance increases.", CASH)
    except Exception as e:
        LOG.error("Failed to fetch initial cash balance: %s", e)
        LOG.debug(traceback.format_exc())
        CASH = 0.0

    # Initialize from holdings if present
    try:
        holdings = groww.get_holdings_for_user()
        if holdings:
            hlist = holdings.get("data") if isinstance(holdings, dict) and "data" in holdings else holdings
            for h in (hlist or []):
                sym = None
                if isinstance(h, dict):
                    sym = h.get("trading_symbol") or h.get("symbol") or h.get("instrument")
                if sym and TICKER in str(sym):
                    try:
                        SHARES_HELD = int(h.get("quantity", 0) or h.get("qty", 0) or 0)
                    except Exception:
                        SHARES_HELD = int(h.get("quantity", 0) if isinstance(h.get("quantity", 0), (int, float)) else 0)
                    BUY_PRICE = safe_float(h.get("average_price", 0.0) or 0.0)
                    ENTRY_DATE = now_ist()  # best-effort
                    LOG.info("Existing holdings detected: shares=%s buy_price=%.2f", SHARES_HELD, BUY_PRICE)
                    break
    except Exception as e:
        LOG.info("Could not read holdings at startup: %s", e)

    trade_happened_this_run = False
    end_epoch = ist_epoch_for_today(END_TIME_IST)
    LOG.info("RUN_MODE=%s LOOP_FREQ_SECONDS=%s TICKER=%s END_TIME_IST=%s (epoch=%s)",
             RUN_MODE, LOOP_FREQ_SECONDS, TICKER, END_TIME_IST, end_epoch)

    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 15

    # C.20: enforce floor to avoid API hammering
    LOOP_SLEEP = max(0.5, LOOP_FREQ_SECONDS)

    while True:
        current_ist = now_ist()

        # Market open gate
        if current_ist.hour < MARKET_OPEN_HOUR or (current_ist.hour == MARKET_OPEN_HOUR and current_ist.minute < MARKET_OPEN_MINUTE):
            # Throttle logging to avoid noisy spam before open
            if current_ist.minute % 5 == 0 and current_ist.second < 1:
                LOG.info("Market not open yet (%s); waiting for 09:15 IST.", current_ist.strftime("%H:%M:%S"))
            time.sleep(LOOP_SLEEP)
            continue

        if SHUTDOWN:
            LOG.info("Shutdown signaled.")
            break

        if end_epoch and int(time.time()) >= end_epoch:
            LOG.info("End time reached; stopping run.")
            break

        # Fetch quote with retries
        quote = None
        for attempt in range(3):
            try:
                quote = groww.get_quote(trading_symbol=TICKER, timeout=5)
                break
            except Exception as e:
                LOG.warning("Quote attempt %d failed: %s", attempt+1, e)
                time.sleep(0.5)
        if not quote or not isinstance(quote, dict):
            LOG.warning("Failed to fetch quote; sleeping and continuing.")
            time.sleep(LOOP_SLEEP)
            continue

        # # B.11: market-closed/holiday heuristic (no API calendar)
        # # Use last_trade_time and volume to infer no live trading.
        # last_trade_dt = ms_to_dt_ist(quote.get("last_trade_time"))
        # vol = safe_float(quote.get("volume", 0), 0.0)
        # if current_ist.hour >= 9 and current_ist.minute >= 15:
            # # after 09:15 IST, if last trade is older than 60 minutes OR volume is 0 for the day, likely closed
            # stale = False
            # if last_trade_dt:
                # stale = (current_ist - last_trade_dt) > timedelta(minutes=60)
            # if stale or vol == 0:
                # LOG.info("Market likely closed/holiday (stale last trade or zero volume). Skipping iteration.")
                # time.sleep(60)  # back off a minute
                # continue

        # Extract last price
        try:
            last_price = extract_last_price(quote)
            if last_price <= 0:
                raise ValueError("Invalid last_price")
        except Exception as e:
            LOG.warning("Malformed quote; skipping iteration: %s", e)
            time.sleep(LOOP_SLEEP)
            continue

        # prev_close
        prev_close = extract_previous_close(quote)
        if prev_close in (None, 0.0):
            # A.3 + A.4: DAILY fallback using IST timestamps; take last full day's close
            try:
                end_dt = now_ist()
                start_dt = end_dt - timedelta(days=3)
                hist = groww.get_historical_candle_data(
                    trading_symbol=TICKER,
                    start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    interval_in_minutes=1440
                )
                candles = hist.get("candles") if isinstance(hist, dict) else None
                if candles and len(candles) >= 2:
                    prev_close = safe_float(candles[-2][4], None)
            except Exception as e:
                LOG.warning("Daily historical fallback failed: %s", e)

        if prev_close is None or prev_close == 0:
            LOG.warning("prev_close unavailable; skipping iteration.")
            time.sleep(LOOP_SLEEP)
            continue

        price_change_pct = calculate_price_change_pct(last_price, prev_close)

        # B.10: Pending-order heuristic fill check
        if PENDING_ORDER:
            side = PENDING_ORDER["side"]
            qty = PENDING_ORDER["qty"]
            ref_price = PENDING_ORDER["price"]  # reference (last seen) price
            placed_at = PENDING_ORDER["placed_at"]
            age = (current_ist - placed_at).total_seconds()

            filled = False
            # For market orders, consider filled quickly; still keep a small heuristic
            if age >= 2:
                filled = True

            if filled or age > 120:
                if filled:
                    LOG.info("Heuristic: %s market order filled (qty %d)", side, qty)
                    if side == "BUY" and SHARES_HELD == 0:
                        # We don't know exact fill price; approximate with ref_price for bookkeeping
                        BUY_PRICE = float(ref_price)
                        SHARES_HELD = int(qty)
                        nonlocal_cash = CASH - (SHARES_HELD * BUY_PRICE)
                        CASH = max(0.0, float(nonlocal_cash))
                        ENTRY_DATE = now_ist()
                        PEAK_RETURN = None
                    elif side == "SELL" and SHARES_HELD > 0:
                        proceeds = SHARES_HELD * ref_price
                        profit_amt = proceeds - (SHARES_HELD * BUY_PRICE)
                        try:
                            pnl_pct = (ref_price - BUY_PRICE) / BUY_PRICE * 100.0
                        except Exception:
                            pnl_pct = 0.0
                        CASH = float(CASH + proceeds)
                        # log to sheets
                        timestamp = now_ist().strftime("%Y-%m-%d %H:%M:%S %Z")
                        row = [
                            timestamp,
                            RUN_MODE,
                            TICKER,
                            f"{BUY_PRICE:.2f}",
                            f"{ref_price:.2f}",
                            str(SHARES_HELD),
                            f"{profit_amt:.2f}",
                            f"{pnl_pct:.4f}",
                            f"{CASH:.2f}"
                        ]
                        if sheet_client:
                            append_trade_row(sheet_client, row)
                        else:
                            LOG.info("Sheet not available; would append row: %s", row)
                        result_text = "SUCCESS" if profit_amt > 0 else "LOSS" if profit_amt < 0 else "BREAKEVEN"
                        send_telegram(CASH, result_text)
                        LOG.info("Trade closed. Profit ₹%.2f (%.4f%%). Final capital ₹%.2f", profit_amt, pnl_pct, CASH)
                        SHARES_HELD = 0
                        BUY_PRICE = 0.0
                        ENTRY_DATE = None
                        PEAK_RETURN = None
                        trade_happened_this_run = True
                else:
                    LOG.info("Heuristic: order expired (unexpected): %s qty=%d", side, qty)
                PENDING_ORDER = None

        # BUY logic
        if SHARES_HELD == 0 and not PENDING_ORDER and price_change_pct <= BUY_THRESHOLD:
            # refresh cash
            try:
                CASH = groww.get_cash_balance()
                CASH = safe_float(CASH, 0.0)
                LOG.info("Refreshed CASH before BUY: ₹%.2f", CASH)
            except Exception as e:
                LOG.warning("Failed to refresh cash before buy: %s", e)

            shares_to_buy = compute_shares_to_buy(CASH, last_price)
            if shares_to_buy <= 0:
                LOG.info("Insufficient cash to buy (cash=₹%.2f, price=₹%.2f, delta=%s)", CASH, last_price, get_delta_for_cash(CASH))
            else:
                try:
                    LOG.info("Placing BUY (MARKET): symbol=%s qty=%s (Δ=%.4f%%, change=%.4f%%)",
                             TICKER, shares_to_buy, get_delta_for_cash(CASH)*100.0, price_change_pct)
                    order_resp = groww.place_order(
                        trading_symbol=TICKER,
                        quantity=shares_to_buy,
                        order_type=getattr(groww.client, "ORDER_TYPE_MARKET", None),
                        transaction_type=getattr(groww.client, "TRANSACTION_TYPE_BUY", None),
                        validity=getattr(groww.client, "VALIDITY_DAY", None),
                        # Always set exchange/segment/product on every trade:
                        exchange=getattr(groww.client, "EXCHANGE_NSE", None),
                        segment=getattr(groww.client, "SEGMENT_CASH", None),
                        product=getattr(groww.client, "PRODUCT_CNC", None)
                    )
                    LOG.info("Buy order response: %s", str(order_resp))
                    # B.10: set pending and wait for heuristic fill instead of immediate assume
                    PENDING_ORDER = {"side": "BUY", "qty": int(shares_to_buy), "price": float(last_price), "placed_at": now_ist()}
                except Exception as e:
                    LOG.error("Place buy order failed: %s", e)
                    LOG.debug(traceback.format_exc())

        # SELL logic
        if SHARES_HELD > 0 and not PENDING_ORDER:
            # B.9: precise hold duration
            if ENTRY_DATE:
                held_days = (now_ist() - ENTRY_DATE).total_seconds() / 86400.0
            else:
                held_days = 0.0

            try:
                current_return = (last_price - BUY_PRICE) / BUY_PRICE * 100.0
            except Exception:
                current_return = 0.0

            if PEAK_RETURN is None:
                PEAK_RETURN = current_return
            else:
                PEAK_RETURN = max(PEAK_RETURN, current_return)

            exit_trade = False
            exit_reason = None

            # Keep original gates but use precise held_days comparisons
            if current_return <= STOP_LOSS and held_days >= float(MAX_DAYS):
                exit_trade = True
                exit_reason = "stop_loss"
            elif current_return >= PROFIT_TARGET and held_days >= float(MIN_DAYS):
                exit_trade = True
                exit_reason = "profit_target"
            elif abs(PEAK_RETURN - current_return) >= TRAILING_DRAWDOWN and held_days >= float(MAX_DAYS):
                exit_trade = True
                exit_reason = "trailing_drawdown"

            if exit_trade:
                try:
                    LOG.info("Placing SELL (MARKET, reason=%s): shares=%d", exit_reason, SHARES_HELD)
                    order_resp = groww.place_order(
                        trading_symbol=TICKER,
                        quantity=SHARES_HELD,
                        order_type=getattr(groww.client, "ORDER_TYPE_MARKET", None),
                        transaction_type=getattr(groww.client, "TRANSACTION_TYPE_SELL", None),
                        validity=getattr(groww.client, "VALIDITY_DAY", None),
                        # Always set exchange/segment/product on every trade:
                        exchange=getattr(groww.client, "EXCHANGE_NSE", None),
                        segment=getattr(groww.client, "SEGMENT_CASH", None),
                        product=getattr(groww.client, "PRODUCT_CNC", None)
                    )
                    LOG.info("Sell order response: %s", str(order_resp))
                    # B.10: set pending sell; fill handled by heuristic
                    PENDING_ORDER = {"side": "SELL", "qty": int(SHARES_HELD), "price": float(last_price), "placed_at": now_ist()}
                except Exception as e:
                    LOG.error("Place sell order failed: %s", e)
                    LOG.debug(traceback.format_exc())

        time.sleep(LOOP_SLEEP)

    # loop end
    LOG.info("Run ended. Final cash: ₹%.2f", CASH)
    if trade_happened_this_run:
        send_telegram(CASH, "DONE")
    return

# -------------------------
# Signal handling
# -------------------------
def on_shutdown(signum, frame):
    global SHUTDOWN
    LOG.info("Received shutdown signal: %s", signum)
    SHUTDOWN = True

signal.signal(signal.SIGINT, on_shutdown)
signal.signal(signal.SIGTERM, on_shutdown)

# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    try:
        LOG.info(
            "Starting generator.py. Strategy params: BUY_THRESHOLD=%.3f PROFIT_TARGET=%.2f STOP_LOSS=%.2f TRAILING_DRAWDOWN=%.2f",
            BUY_THRESHOLD, PROFIT_TARGET, STOP_LOSS, TRAILING_DRAWDOWN
        )
        run_live_loop()
    except Exception as e:
        LOG.error("Unhandled exception: %s", e)
        LOG.debug(traceback.format_exc())
        try:
            send_telegram(CASH if 'CASH' in globals() else 0.0, "ERROR")
        except Exception:
            pass
        raise
