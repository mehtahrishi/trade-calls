import requests
import time
from cache import market_cache

COIN_IDS = "bitcoin,ethereum,solana,tron"

URL = "https://api.coingecko.com/api/v3/coins/markets"


def fetch_crypto():
    try:
        params = {
            "vs_currency": "usd",
            "ids": COIN_IDS,
            "order": "market_cap_desc",
            "sparkline": "false"
        }

        r = requests.get(URL, params=params, timeout=10)
        data = r.json()

        snapshot = {}

        for coin in data:
            curr = coin.get("current_price", 0)
            chg = coin.get("price_change_24h", 0)
            pct = coin.get("price_change_percentage_24h", 0)

            snapshot[coin["symbol"].upper()] = {
                "name": coin.get("name"),
                "symbol": coin["symbol"].upper(),
                "price": curr,
                "change24h": chg,
                "percent24h": pct,
                "prevClose": curr - chg if curr and chg else None,
                "low24h": coin.get("low_24h"),
                "high24h": coin.get("high_24h"),
                "marketCap": coin.get("market_cap"),
                "fdv": coin.get("fully_diluted_valuation"),
                "circulatingSupply": coin.get("circulating_supply"),
                "ath": coin.get("ath"),
            }

        return snapshot

    except Exception as e:
        print("Crypto fetch error:", e)
        return None


def run():
    while True:
        data = fetch_crypto()

        if data:
            market_cache["crypto"].update(data)
            print("CRYPTO updated")

        time.sleep(45)  # respect coingecko rate limits
