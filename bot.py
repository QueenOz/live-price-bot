import os
import json
import logging
import asyncio
import httpx
import websockets
from dotenv import load_dotenv
from datetime import datetime
from supabase import create_client

# Load .env variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}"
}

WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"

GAMES_URL = f"{SUPABASE_URL}/rest/v1/games"
ASSETS_URL = f"{SUPABASE_URL}/rest/v1/game_assets"
SYMBOL_LIMIT = 8

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            games_resp = await client.get(GAMES_URL, headers=SUPABASE_HEADERS, params={
                "select": "id",
                "status": "in.(upcoming,active)"
            })
            game_ids = [g["id"] for g in games_resp.json()]
            if not game_ids:
                return []

            assets_resp = await client.get(ASSETS_URL, headers=SUPABASE_HEADERS, params={
                "select": "standardized_symbol,market_type,game_status",
                "game_id": f"in.({','.join(game_ids)})"
            })
            rows = assets_resp.json()
            symbols = set()

            for row in rows:
                std_symbol = row.get("standardized_symbol")
                market_type = row.get("market_type")

                if not std_symbol:
                    continue

                if market_type == "forex":
                    symbols.add(std_symbol.replace("_", "/"))
                else:
                    symbols.add(std_symbol)

            return list(symbols)[:SYMBOL_LIMIT]

        except Exception as e:
            logger.error(f"❌ Error fetching symbols: {e}")
            return []

async def upsert_price(data):
    try:
        symbol = data["symbol"]
        price = float(data["price"])
        std_symbol = symbol.replace("/", "_")

        supabase.table("live_prices").upsert({
            "symbol": symbol,
            "standardized_symbol": std_symbol,
            "price": price,
            "updated_at": datetime.utcnow().isoformat()
        }).execute()

        logger.info(f"✅ {symbol} → {price}")
    except Exception as e:
        logger.error(f"❌ Failed to upsert price: {e}")

async def price_bot_loop():
    reconnect_delay = 2

    while True:
        try:
            symbols = await fetch_symbols()
            if not symbols:
                logger.info("⏳ No symbols found. Retrying in 5s.")
                await asyncio.sleep(5)
                continue

            logger.info(f"🧠 Subscribing to: {symbols}")
            async with websockets.connect(WEBSOCKET_URL) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": ",".join(symbols)}
                }))
                logger.info(f"📤 Sent subscribe: {','.join(symbols)}")

                async def send_heartbeat():
                    while True:
                        try:
                            await asyncio.sleep(10)
                            await ws.send(json.dumps({"action": "heartbeat"}))
                        except:
                            break

                heartbeat_task = asyncio.create_task(send_heartbeat())

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=15)
                        data = json.loads(msg)
                        if data.get("event") == "price" and "symbol" in data:
                            await upsert_price(data)
                    except asyncio.TimeoutError:
                        logger.warning("⏱️ Timeout. Reconnecting.")
                        break
                    except Exception as e:
                        logger.error(f"❌ Receive error: {e}")
                        break

                heartbeat_task.cancel()

        except Exception as e:
            logger.error(f"🔌 WebSocket connection error: {e}")

        logger.info(f"🔁 Reconnecting in {reconnect_delay} seconds...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)

if __name__ == "__main__":
    asyncio.run(price_bot_loop())
