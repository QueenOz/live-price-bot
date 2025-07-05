import os
import time
import json
import logging
import asyncio
import httpx
import websockets
from supabase import create_client
from dotenv import load_dotenv

# Load .env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

async def get_symbols():
    try:
        async with httpx.AsyncClient() as client:
            games = await client.get(
                f"{SUPABASE_URL}/rest/v1/games",
                params={"select": "id", "status": "in.(upcoming,active)"},
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
            )
            if games.status_code != 200:
                logger.error(f"Failed to fetch games: {games.text}")
                return []

            game_ids = [g["id"] for g in games.json()]
            if not game_ids:
                logger.info("No active/upcoming games.")
                return []

            asset_query = ",".join(f"{gid}" for gid in game_ids)
            assets = await client.get(
                f"{SUPABASE_URL}/rest/v1/game_assets",
                params={"select": "asset_name", "game_id": f"in.({asset_query})"},
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
            )
            if assets.status_code != 200:
                logger.error(f"Failed to fetch assets: {assets.text}")
                return []

            # Remove None and duplicates
            symbols = list({a["asset_name"] for a in assets.json() if a.get("asset_name")})
            return symbols
    except Exception as e:
        logger.error(f"Error getting symbols: {e}")
        return []

async def subscribe_and_stream(symbols):
    url = f"wss://ws.twelvedata.com/v1/price?apikey={TD_API_KEY}"

    try:
        async with websockets.connect(url) as ws:
            logger.info("🟢 Connected to TwelveData")

            # Subscribe
            await ws.send(json.dumps({
                "action": "subscribe",
                "params": {"symbols": ",".join(symbols)}
            }))
            logger.info(f"🧠 Subscribing to: {symbols}")

            async for message in ws:
                try:
                    data = json.loads(message)
                    if "symbol" in data and "price" in data:
                        await upsert_price(data)
                except Exception as e:
                    logger.warning(f"Invalid data: {message} | Error: {e}")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
        logger.warning("⚠️ WebSocket closed: Reconnecting soon...")

async def upsert_price(data):
    try:
        symbol = data["symbol"]  # e.g. EUR/USD
        standardized = symbol.replace("/", "_")
        price = float(data["price"])
        now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        supabase.table("live_prices").upsert({
            "symbol": symbol,
            "standardized_symbol": standardized,
            "price": price,
            "updated_at": now
        }).execute()

        logger.info(f"✅ Price updated: {symbol} = {price}")
    except Exception as e:
        logger.error(f"❌ Failed to upsert price for {data}: {e}")

async def run_bot():
    while True:
        symbols = await get_symbols()
        if symbols:
            await subscribe_and_stream(symbols)
        else:
            logger.info("😴 No symbols to track. Sleeping...")
        await asyncio.sleep(2)  # refresh interval

if __name__ == "__main__":
    asyncio.run(run_bot())
