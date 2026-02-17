from fastapi import FastAPI
import threading
import nse
import us
import crypto
import bse
import commodity

from cache import market_cache

app = FastAPI()

# Strategy: Use a startup event so threads start only when the app is ready
@app.on_event("startup")
def startup_event():
    # daemon=True is good, it ensures threads die when the main process dies
    threading.Thread(target=nse.run, daemon=True).start()
    threading.Thread(target=us.run, daemon=True).start()
    threading.Thread(target=bse.run, daemon=True).start()
    threading.Thread(target=crypto.run, daemon=True).start()
    threading.Thread(target=commodity.run, daemon=True).start()


@app.get("/")
def root():
    return {"status": "online", "markets": ["nse", "us"]}

@app.get("/nse")
def nse_data():
    # Add a .get() to prevent KeyErrors if the scraper hasn't finished its first run
    data = market_cache.get("nse", {"error": "Data not yet available"})
    return data

@app.get("/us")
def us_data():
    data = market_cache.get("us", {})
    if not data:
        return {"status": "warming_up", "message": "US market loading"}
    return data

@app.get("/bse")
def bse_data():
    return market_cache["bse"]
@app.get("/crypto")
def crypto_data():
    return market_cache["crypto"]

@app.get("/commodity")
def commodity_data():
    return market_cache["commodity"]