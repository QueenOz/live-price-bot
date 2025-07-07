import os
import json
import logging
import asyncio
import httpx
import websockets
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# Constants
WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"
INSERT_LIVE_PRICE_ENDPOINT = f"{SUPABASE_URL}/functions/v1/insert-live-price"
LIVE_PRICES_ENDPOINT = f"{SUPABASE_URL}/rest/v1/live_prices"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

symbol_list = []
price_buffer = []

# Fetch symbol list from Edge Function
async def fetch_symbols():
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(FETCH_SYMBOLS_ENDPOINT, headers=HEADERS)
            data = resp.json()
            logger.info(f"🧠 Fetched symbols: {data}")
            return data.get("symbols", [])
    except Exception as e:
        logger.error(f"❌ Error fetching symbols: {e}")
        return []

# Send price buffer to Supabase via Edge Function
async def flush_price_buffer():
    global price_buffer
    if not price_buffer:
        return
    try:
        async with httpx.AsyncClient() as client:
            payload = {"prices": price_buffer}
            await client.post(INSERT_LIVE_PRICE_ENDPOINT, headers=HEADERS, json=payload)
            logger.info(f"📡 Flushed {len(price_buffer)} prices")
            price_buffer = []
    except Exception as e:
        logger.error(f"❌ Failed to flush price buffer: {e}")

# WebSocket heartbeat
async def send_heartbeat(ws):
    while True:
        try:
            await asyncio.sleep(10)
            await ws.send(json.dumps({"action": "heartbeat"}))
        except:
            break

# WebSocket price streaming
async def websocket_loop():
    global symbol_list
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                if not symbol_list:
                    symbol_list = await fetch_symbols()

                if not symbol_list:
                    logger.info("⏳ No symbols to subscribe. Retrying...")
                    await asyncio.sleep(10)
                    continue

                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": ",".join(symbol_list)}
                }))
                logger.info(f"📤 Subscribed to: {symbol_list}")

                heartbeat = asyncio.create_task(send_heartbeat(ws))

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=15)
                        data = json.loads(msg)
                        if data.get("event") == "price":
                            symbol = data.get("symbol")
                            price = float(data.get("price", 0))
                            std_symbol = symbol.replace("/", "_")
                            market_type = "forex" if "/" in symbol else "stock_or_crypto"

                            price_buffer.append({
                                "symbol": symbol,
                                "standardized_symbol": std_symbol,
                                "price": price,
                                "updated_at": datetime.utcnow().isoformat(),
                                "market_type": market_type,
                                "asset_name": std_symbol
                            })
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ WebSocket timeout")
                        break
                    except Exception as e:
                        logger.error(f"❌ WebSocket error: {e}")
                        break

                heartbeat.cancel()
        except Exception as e:
            logger.error(f"🔌 WebSocket failed: {e}")

        logger.info("🔁 Reconnecting WebSocket in 2s...")
        await asyncio.sleep(2)

# Refresh symbols every 60s
async def symbol_refresher():
    global symbol_list
    while True:
        symbol_list = await fetch_symbols()
        await asyncio.sleep(60)

# Flush buffer every 5s
async def buffer_flusher():
    while True:
        await flush_price_buffer()
        await asyncio.sleep(5)

# Optional: Pull latest from live_prices every 2s
async def pull_live_prices():
    while True:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(LIVE_PRICES_ENDPOINT, headers=HEADERS, params={
                    "select": "symbol,price,updated_at"
                })
                logger.info(f"📊 Live prices count: {len(resp.json())}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch live prices: {e}")
        await asyncio.sleep(2)

# Main loop
async def main():
    await asyncio.gather(
        websocket_loop(),
        symbol_refresher(),
        buffer_flusher(),
        pull_live_prices()  # optional
    )

if __name__ == "__main__":
    asyncio.run(main())





