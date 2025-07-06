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
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_API_KEY = os.getenv("TWELVE_API_KEY")  # ✅ fixed

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}
SYMBOL_LIMIT = 8
GAMES_URL = f"{SUPABASE_URL}/rest/v1/games"
ASSETS_URL = f"{SUPABASE_URL}/rest/v1/game_assets"
WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_API_KEY}"  # ✅ fixed

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

# Normalize DB symbol to Twelve Data format
def normalize_symbol(symbol: str) -> str:
    return symbol.replace("_", "/") if "_" in symbol else symbol

# Fetch live asset symbols from Supabase
async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            # Get upcoming + active game IDs
            games_resp = await client.get(GAMES_URL, headers=HEADERS, params={
                "select": "id",
                "status": "in.(upcoming,active)"
            })
            game_ids = [g["id"] for g in games_resp.json()]
            if not game_ids:
                return []

            # Get symbols from related game_assets
            assets_resp = await client.get(ASSETS_URL, headers=HEADERS, params={
                "select": "symbol",
                "game_id": f"in.({','.join(game_ids)})"
            })

            symbols = list({
                normalize_symbol(a["symbol"])
                for a in assets_resp.json()
                if a.get("symbol")
            })
            return symbols[:SYMBOL_LIMIT]
        except Exception as e:
            logger.error(f"❌ Error fetching symbols: {e}")
            return []

# Upsert live price to Supabase
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

# Live price loop
async def price_bot_loop():
    while True:
        try:
            symbols = await fetch_symbols()
            if not symbols:
                logger.info("⏳ No active symbols. Retrying in 2 seconds.")
                await asyncio.sleep(2)
                continue

            logger.info(f"🧠 Subscribing to: {symbols}")
            async with websockets.connect(WEBSOCKET_URL) as ws:
                # Subscribe
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": ",".join(symbols)}
                }))

                # Heartbeat
                async def send_heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send(json.dumps({"action": "heartbeat"}))
                            logger.debug("💓 Heartbeat sent")
                        except Exception as e:
                            logger.warning(f"⚠️ Heartbeat failed: {e}")
                            break

                heartbeat_task = asyncio.create_task(send_heartbeat())

                # Listen for updates
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=12)
                        data = json.loads(message)

                        if data.get("event") == "price" and "symbol" in data:
                            await upsert_price(data)
                        else:
                            logger.debug(f"Ignored: {data}")
                    except asyncio.TimeoutError:
                        logger.warning("⏱️ Timeout. Reconnecting...")
                        break
                    except Exception as e:
                        logger.error(f"❌ WebSocket recv error: {e}")
                        break

                heartbeat_task.cancel()

        except Exception as e:
            logger.error(f"⚠️ Connection error: {e}")

        logger.info("🔁 Reconnecting in 2 seconds...")
        await asyncio.sleep(2)

# Run
if __name__ == "__main__":
    asyncio.run(price_bot_loop())






