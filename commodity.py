import yfinance as yf
import time
from cache import market_cache

# Futures symbols
COMMODITIES = {
    "GC=F": "GOLD",
    "SI=F": "SILVER",
    "HG=F": "COPPER"
}

USDINR = "USDINR=X"

# cache 52w highs (loaded once)
static_highs = {}


# ---------- LOAD STATIC DATA ----------
def load_static():
    print("Loading commodity static data...")
    for sym in COMMODITIES:
        try:
            info = yf.Ticker(sym).info
            static_highs[sym] = info.get("fiftyTwoWeekHigh", 0)
        except:
            static_highs[sym] = 0
    print("Commodity static loaded")


# ---------- FETCH LIVE ----------
def fetch_live():
    try:
        tickers = list(COMMODITIES.keys()) + [USDINR]

        df = yf.download(tickers, period="1d", interval="1m", progress=False)

        if df.empty:
            return None

        usdinr = df["Close"][USDINR].dropna().iloc[-1]

        snapshot = {}

        for sym, name in COMMODITIES.items():
            series = df["Close"][sym].dropna()
            if len(series) < 2:
                continue

            curr = series.iloc[-1]
            prev = series.iloc[-2]

            # MCX conversion
            if sym == "GC=F":
                multiplier = (usdinr / 31.1035) * 10
                unit = "10g"
            elif sym == "SI=F":
                multiplier = (usdinr / 31.1035) * 32.1507
                unit = "1kg"
            else:
                multiplier = usdinr * 2.20462
                unit = "1kg"

            price = curr * multiplier
            prev_price = prev * multiplier

            change = price - prev_price
            percent = (change / prev_price * 100) if prev_price else None

            snapshot[name] = {
                "symbol": name,
                "price": price,
                "change": change,
                "percent": percent,
                "unit": unit,
                "usdInr": usdinr,
                "week52High": static_highs.get(sym, 0) * multiplier
            }

        return snapshot

    except Exception as e:
        print("Commodity error:", e)
        return None


# ---------- LOOP ----------
def run():
    load_static()

    while True:
        data = fetch_live()
        if data:
            market_cache["commodity"].update(data)
            print("COMMODITY updated")

        time.sleep(6)   # safe refresh
