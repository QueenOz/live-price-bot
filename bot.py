import os
import json
import logging
import asyncio
import httpx
import websockets
from dotenv import load_dotenv
from datetime import datetime
from supabase import create_client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# Init Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}"
}

# Constants
WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
SYMBOL_LIMIT = 8
GAMES_URL = f"{SUPABASE_URL}/rest/v1/games"
ASSETS_URL = f"{SUPABASE_URL}/rest/v1/game_assets"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

# Normalize symbol for Twelve Data
def normalize_symbol(symbol: str, market_type: str) -> str:
    if market_type in ["forex", "crypto", "stock"]:
        return symbol.replace("_", "/")
    return symbol

# Fetch symbols from game_assets
async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            # Get active/upcoming games
            games_resp = await client.get(GAMES_URL, headers=SUPABASE_HEADERS, params={
                "select": "id",
                "status": "in.(upcoming,active)"
            })
            game_ids = [g["id"] for g in games_resp.json()]
            if not game_ids:
                return []

            # Get relevant asset symbols and market types
            assets_resp = await client.get(ASSETS_URL, headers=SUPABASE_HEADERS, params={
                "select": "standardized_symbol,market_type,game_status",
                "game_id": f"in.({','.join(game_ids)})"
            })

            symbols = set()
            for asset in assets_resp.json():
                raw = asset.get("standardized_symbol")
                market = asset.get("market_type")
                if raw and market:
                    symbols.add(normalize_symbol(raw, market))

            return list(symbols)[:SYMBOL_LIMIT]

        except Exception as e:
            logger.error(f"❌ Error fetching symbols: {e}")
            return []

# Upsert into live_prices table
async def upsert_price(data):
    try:
        symbol = data["symbol"]
        price = float(data["price"])
        std_symbol = symbol.replace("/", "_")

        response = supabase.table("live_prices").upsert({
            "symbol": symbol,
            "standardized_symbol": std_symbol,
            "price": price,
            "updated_at": datetime.utcnow().isoformat()
        }).execute()

        logger.info(f"✅ {symbol} → {price}")
        logger.info(f"📝 Upsert response: {response}")
    except Exception as e:
        logger.error(f"❌ Failed to upsert price: {e}")

# Main WebSocket loop
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

                # Heartbeat
                async def send_heartbeat():
                    while True:
                        try:
                            await asyncio.sleep(10)
                            await ws.send(json.dumps({"action": "heartbeat"}))
                            logger.debug("💓 Heartbeat sent")
                        except Exception as e:
                            logger.warning(f"⚠️ Heartbeat failed: {e}")
                            break

                heartbeat_task = asyncio.create_task(send_heartbeat())

                # Listen for price updates
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=12)
                        data = json.loads(msg)
                        logger.debug(f"📩 Received: {data}")

                        if data.get("event") == "price" and "symbol" in data:
                            await upsert_price(data)
                    except asyncio.TimeoutError:
                        logger.warning("⏱️ No messages. Reconnecting.")
                        break
                    except Exception as e:
                        logger.error(f"❌ WebSocket error: {e}")
                        break

                heartbeat_task.cancel()

        except Exception as e:
            logger.error(f"🔌 Connection error: {e}")

        logger.info(f"🔁 Reconnecting in {reconnect_delay} seconds...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)

# Run
if __name__ == "__main__":
    asyncio.run(price_bot_loop())
