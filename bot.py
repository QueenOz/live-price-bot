import asyncio
import httpx
import json
import websockets
import os
from datetime import datetime

SUPABASE_URL = "https://auth.play.apestox.app"
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")  # must be set in env
WEBSOCKET_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# Globals
tracked_symbols = set()
latest_prices = {}

async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(f"{SUPABASE_URL}/functions/v1/fetch-symbols", headers={
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "apikey": SUPABASE_ANON_KEY
            })
            symbols = res.json().get("symbols", [])
            return list(set(symbols))  # unique
        except Exception as e:
            print("❌ Error fetching symbols:", e)
            return []

async def insert_prices():
    if not latest_prices:
        print("⏳ No prices to insert")
        return

    prices_payload = [
        {
            "symbol": p["symbol"],
            "standardized_symbol": p.get("standardized_symbol", p["symbol"]),
            "price": p["price"],
            "updated_at": datetime.utcnow().isoformat(),
            "market_type": "forex" if "/" in p["symbol"] else "stock_or_crypto",
            "asset_name": p.get("asset_name", p["symbol"])
        }
        for p in latest_prices.values()
    ]

    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(
                f"{SUPABASE_URL}/functions/v1/insert-live-price",
                headers={
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                    "apikey": SUPABASE_ANON_KEY,
                    "Content-Type": "application/json"
                },
                json={"prices": prices_payload}
            )
            print("📡 Inserted prices:", len(prices_payload), "→", res.status_code)
        except Exception as e:
            print("❌ Error inserting prices:", e)

async def websocket_loop():
    global tracked_symbols
    async for websocket in websockets.connect(WEBSOCKET_URL):
        try:
            print("🔗 Connected to TwelveData WebSocket")
            await websocket.send(json.dumps({
                "action": "subscribe",
                "params": {
                    "symbols": ",".join(tracked_symbols),
                    "apikey": os.getenv("TWELVE_DATA_API_KEY")
                }
            }))
            async for message in websocket:
                data = json.loads(message)
                if "symbol" in data and "price" in data:
                    latest_prices[data["symbol"]] = {
                        "symbol": data["symbol"],
                        "price": float(data["price"])
                    }
        except websockets.ConnectionClosed:
            print("🔌 WebSocket disconnected, reconnecting...")
            continue
        except Exception as e:
            print("⚠️ WebSocket error:", e)
            await asyncio.sleep(5)

async def scheduler():
    global tracked_symbols
    while True:
        symbols = await fetch_symbols()
        if set(symbols) != tracked_symbols:
            print("🔄 Updating tracked symbols:", symbols)
            tracked_symbols = set(symbols)
        await asyncio.sleep(60)  # refresh every 60s

async def inserter():
    while True:
        await insert_prices()
        await asyncio.sleep(2)

async def main():
    await asyncio.gather(
        websocket_loop(),
        scheduler(),
        inserter()
    )

if __name__ == "__main__":
    asyncio.run(main())







