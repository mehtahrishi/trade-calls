import requests
import pandas as pd
import time
from cache import market_cache

# ---------- LOAD SYMBOL MASTER ----------
csv_file = "NSE_EQUITY_L.csv"
df_static = pd.read_csv(csv_file)
df_static.columns = df_static.columns.str.strip().str.replace('\ufeff', '', regex=True)

symbols = df_static["SYMBOL"].tolist()

# build static lookup map (SYMBOL → static info)
static_map = {}
for _, row in df_static.iterrows():
    static_map[row["SYMBOL"]] = {
        "name": row["NAME OF COMPANY"],
        "series": row["SERIES"],
        "listed": row["DATE OF LISTING"],
        "isin": row["ISIN NUMBER"],
        "faceValue": row["FACE VALUE"],
    }

# ---------- NSE SESSION ----------
session = requests.Session()
headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
session.get("https://www.nseindia.com", headers=headers)


# ---------- FETCH ONE STOCK ----------
def fetch_live(symbol):
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        r = session.get(url, headers=headers, timeout=5)
        data = r.json()

        price = data.get("priceInfo", {})
        intra = price.get("intraDayHighLow", {})
        week = price.get("weekHighLow", {})
        preopen = data.get("preOpenMarket", {})
        industry = data.get("industryInfo", {})
        meta = data.get("metadata", {})
        sec = data.get("securityInfo", {})

        last = price.get("lastPrice")
        prev = price.get("previousClose")
        vwap = price.get("vwap")

        static = static_map.get(symbol, {})

        return {
            # -------- STATIC --------
            "symbol": symbol,
            "name": static.get("name"),
            "series": static.get("series"),
            "listed": static.get("listed"),
            "isin": static.get("isin"),
            "faceValue": static.get("faceValue"),

            # -------- PRICE --------
            "price": last,
            "change": price.get("change"),
            "percent": price.get("pChange"),
            "open": price.get("open"),
            "high": intra.get("max"),
            "low": intra.get("min"),
            "prevClose": prev,
            "volume": preopen.get("totalTradedVolume"),

            # -------- VWAP --------
            "vwap": vwap,
            "vwapPercent": ((last - vwap) / vwap * 100) if last and vwap else None,

            # -------- CIRCUITS --------
            "upperCircuit": price.get("upperCP"),
            "lowerCircuit": price.get("lowerCP"),
            "priceBand": price.get("pPriceBand"),

            # -------- WEEK --------
            "weekHigh": week.get("max"),
            "weekLow": week.get("min"),

            # -------- ORDER BOOK --------
            "buyQty": preopen.get("totalBuyQuantity"),
            "sellQty": preopen.get("totalSellQuantity"),
            "imbalance": (preopen.get("totalBuyQuantity", 0) - preopen.get("totalSellQuantity", 0)),

            # -------- CLASSIFICATION --------
            "sector": industry.get("sector"),
            "industry": industry.get("industry"),
            "index": meta.get("pdSectorInd"),
            "status": sec.get("tradingStatus"),
        }

    except Exception:
        return None


# ---------- BACKGROUND LOOP ----------
def run():
    batch_size = 10
    start = 0

    while True:
        batch = symbols[start:start+batch_size]
        snapshot = {}

        for sym in batch:
            live = fetch_live(sym)
            if live:
                snapshot[sym] = live

        # update cache (no deletion, rolling update)
        market_cache["nse"].update(snapshot)

        print(f"NSE updated batch {start}-{start+batch_size}")

        start += batch_size
        if start >= len(symbols):
            start = 0
            print("Full NSE cycle complete\n")
            time.sleep(10)   # pause after full sweep
        else:
            time.sleep(1)    # gap between batches
