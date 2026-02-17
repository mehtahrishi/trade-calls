from bsedata.bse import BSE
import time
from cache import market_cache

bse = BSE()


# ---------- LOAD ALL BSE SCRIP CODES ----------
def load_codes():
    print("Loading BSE scrip list...")
    codes = bse.getScripCodes()  # dict {code: name}
    code_list = list(codes.keys())
    print(f"Loaded {len(code_list)} BSE stocks")
    return code_list


codes = load_codes()


# ---------- FETCH ONE STOCK ----------
def fetch_stock(code):
    try:
        data = bse.getQuote(code)

        return {
            "code": code,
            "symbol": data.get("securityID"),
            "price": data.get("currentValue"),
            "change": data.get("change"),
            "percent": data.get("pChange"),
            "open": data.get("previousOpen"),
            "high": data.get("dayHigh"),
            "low": data.get("dayLow"),
            "prevClose": data.get("previousClose"),
            "vwap": data.get("weightedAvgPrice"),
            "volume": data.get("totalTradedQuantity"),
            "weekHigh": data.get("52weekHigh"),
            "weekLow": data.get("52weekLow"),
            "marketCap": data.get("marketCapFull"),
            "industry": data.get("industry"),
        }

    except Exception:
        return None


# ---------- BACKGROUND LOOP ----------
def run():
    batch_size = 20
    start = 0

    while True:
        if not codes:
            print("No BSE codes loaded")
            time.sleep(20)
            continue

        batch = codes[start:start + batch_size]
        snapshot = {}

        for code in batch:
            stock = fetch_stock(code)
            if stock:
                snapshot[str(code)] = stock
            time.sleep(0.15)  # avoid blocking BSE

        market_cache["bse"].update(snapshot)

        print(f"BSE updated {start}-{start+batch_size}")

        start += batch_size
        if start >= len(codes):
            start = 0
            print("BSE full cycle complete\n")
            time.sleep(25)
        else:
            time.sleep(2)
