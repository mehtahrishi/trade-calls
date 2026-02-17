import pandas as pd
import yfinance as yf
import time
from cache import market_cache

# ---------- LOAD SYMBOLS FROM LOCAL CSV ----------
def load_symbols():
    try:
        df = pd.read_csv("sp500.csv")
        symbols = [s.replace(".", "-") for s in df["Symbol"].tolist()]
        print(f"Loaded {len(symbols)} US symbols")
        return symbols
    except Exception as e:
        print("Failed to load sp500.csv:", e)
        return []

symbols = load_symbols()


# ---------- FETCH ONE STOCK ----------
def fetch_stock(sym):
    try:
        info = yf.Ticker(sym).info

        current = info.get("currentPrice")
        prev_close = info.get("previousClose")

        change = (current - prev_close) if current and prev_close else None
        percent = (change / prev_close * 100) if change and prev_close else None

        volume = info.get("volume")
        avg_vol = info.get("averageVolume")
        vol_percent = (volume / avg_vol * 100) if volume and avg_vol else None

        w52_high = info.get("fiftyTwoWeekHigh")
        dist_52_high = ((current - w52_high) / w52_high * 100) if current and w52_high else None

        return {
            "symbol": sym,
            "name": info.get("shortName"),
            "price": current,
            "change": change,
            "percent": percent,
            "open": info.get("open"),
            "high": info.get("dayHigh"),
            "low": info.get("dayLow"),
            "prevClose": prev_close,
            "volume": volume,
            "volumePercent": vol_percent,
            "marketCap": info.get("marketCap"),
            "pe": info.get("trailingPE"),
            "beta": info.get("beta"),
            "week52Distance": dist_52_high,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "exchange": info.get("exchange"),
        }

    except Exception:
        return None


# ---------- BACKGROUND LOOP ----------
def run():
    batch_size = 5
    start = 0

    while True:
        if not symbols:
            print("No US symbols loaded")
            time.sleep(20)
            continue

        batch = symbols[start:start + batch_size]
        snapshot = {}

        for sym in batch:
            data = fetch_stock(sym)
            if data:
                snapshot[sym] = data
            time.sleep(0.5)  # prevent Yahoo rate limit

        market_cache["us"].update(snapshot)

        print(f"US updated {start}-{start+batch_size}")

        start += batch_size
        if start >= len(symbols):
            start = 0
            print("US full cycle complete\n")
            time.sleep(20)
        else:
            time.sleep(2)
