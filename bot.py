import os
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
import httpx
import websockets
import logging

load_dotenv()

# === Config ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
LIVE_PRICES_ENDPOINT = f"{SUPABASE_URL}/rest/v1/live_prices"
FETCH_SYMBOLS_URL = f"{SUPABASE_URL}/functions/v1/fetch-symbols"
INSERT_LIVE_PRICE_URL = f"{SUPABASE_URL}/functions/v1/insert-live-price"
WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}
SYMBOL_REFRESH_INTERVAL = 60
PRICE_REFRESH_INTERVAL = 2

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")


async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(FETCH_SYMBOLS_URL, headers=SUPABASE_HEADERS)
            data = res.json()
            symbols = data.get("symbols", [])
            logger.info(f"🧠 Fetched symbols: {symbols}")
            return symbols
        except Exception as e:
            logger.error(f"❌ Failed to fetch symbols: {e}")
            return []


async def insert_live_prices(prices):
    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(
                INSERT_LIVE_PRICE_URL,
                headers=SUPABASE_HEADERS,
                json=prices
            )
            if res.status_code == 200:
                logger.info(f"✅ Inserted {len(prices)} prices")
            else:
                logger.warning(f"⚠️ Failed to insert prices: {res.status_code} {res.text}")
        except Exception as e:
            logger.error(f"❌ Insert error: {e}")


async def poll_live_prices():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                res = await client.get(LIVE_PRICES_ENDPOINT, headers=SUPABASE_HEADERS, params={
                    "select": "symbol,price,updated_at"
                })
                data = res.json()
                logger.info(f"📊 Live prices count: {len(data)}")
            except Exception as e:
                logger.error(f"❌ Poll error: {e}")
            await asyncio.sleep(PRICE_REFRESH_INTERVAL)


async def websocket_loop():
    symbols = await fetch_symbols()
    if not symbols:
        logger.error("🚫 No symbols to subscribe to")
        return

    async with websockets.connect(WEBSOCKET_URL) as ws:
        await ws.send(json.dumps({
            "action": "subscribe",
            "params": {
                "symbols": ",".join(symbols)
            }
        }))
        logger.info(f"📤 Subscribed to: {symbols}")

        last_symbol_refresh = datetime.utcnow()

        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("event") == "price":
                    symbol = data.get("symbol")
                    price = data.get("price")
                    mtype = data.get("type")
                    ts = datetime.utcnow().isoformat()

                    await insert_live_prices([{
                        "symbol": symbol,
                        "standardized_symbol": symbol.replace("/", "_"),
                        "price": price,
                        "market_type": mtype,
                        "asset_name": symbol.split("/")[0],
                        "updated_at": ts
                    }])

                # refresh symbols every 60s
                if (datetime.utcnow() - last_symbol_refresh).total_seconds() > SYMBOL_REFRESH_INTERVAL:
                    new_symbols = await fetch_symbols()
                    if set(new_symbols) != set(symbols):
                        await ws.send(json.dumps({
                            "action": "unsubscribe",
                            "params": { "symbols": ",".join(symbols) }
                        }))
                        await ws.send(json.dumps({
                            "action": "subscribe",
                            "params": { "symbols": ",".join(new_symbols) }
                        }))
                        logger.info(f"🔁 Updated subscription: {new_symbols}")
                        symbols = new_symbols
                    last_symbol_refresh = datetime.utcnow()

            except websockets.exceptions.ConnectionClosedError:
                logger.warning("❌ WebSocket connection closed. Reconnecting in 2s...")
                await asyncio.sleep(2)
                return await websocket_loop()
            except Exception as e:
                logger.error(f"❌ WebSocket error: {e}")
                await asyncio.sleep(2)


async def main():
    await asyncio.gather(
        websocket_loop(),
        poll_live_prices()
    )

if __name__ == "__main__":
    asyncio.run(main())







