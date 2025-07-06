# bot.py
import os
import time
import json
import logging
import asyncio
import httpx
import websockets
from dotenv import load_dotenv
from datetime import datetime
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TDWebSocket")

GAMES_URL = f"{SUPABASE_URL}/rest/v1/games"
ASSETS_URL = f"{SUPABASE_URL}/rest/v1/game_assets"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}
WEBSOCKET_URL = "wss://ws.twelvedata.com/v1/ws"
SYMBOL_LIMIT = 8

async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        try:
            games_resp = await client.get(GAMES_URL, headers=HEADERS, params={
                "select": "id",
                "status": "in.(upcoming,active)"
            })
            game_ids = [g["id"] for g in games_resp.json()]
            if not game_ids:
                return []

            assets_resp = await client.get(ASSETS_URL, headers=HEADERS, params={
                "select": "symbol,asset_name,market_type",
                "game_id": f"in.({','.join(game_ids)})"
            })
            raw_assets = assets_resp.json()

            # Use asset_name for forex, symbol for others
            symbols = set()
            for a in raw_assets:
                if a.get("market_type") == "forex" and a.get("asset_name"):
                    symbols.add(a["asset_name"])
                elif a.get("symbol"):
                    symbols.add(a["symbol"])

            return list(symbols)[:SYMBOL_LIMIT]
        except Exception as e:
            logger.error(f"❌ Error fetching symbols: {e}")
            return []

async def upsert_price(data):
    try:
        ws_symbol = data["symbol"]
        price = float(data["price"])
        std_symbol = ws_symbol.replace("/", "_")

        supabase.table("live_prices").upsert({
            "symbol": ws_symbol,  # store exact ws symbol like EUR/USD
            "standardized_symbol": std_symbol,
            "price": price,
            "updated_at": datetime.utcnow().isoformat()
        }).execute()

        logger.info(f"✅ Price updated: {ws_symbol} → {price}")
    except Exception as e:
        logger.error(f"❌ Failed to upsert: {e}")

async def price_bot_loop():
    while True:
        try:
            symbols = await fetch_symbols()
            if not symbols:
                logger.info("⏳ No active games/assets. Retrying in 2 seconds.")
                await asyncio.sleep(2)
                continue

            logger.info(f"🧠 Subscribing to: {symbols}")
            async with websockets.connect(f"{WEBSOCKET_URL}?apikey={TD_API_KEY}") as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": ",".join(symbols)}
                }))
                logger.info("🟢 Connected to TwelveData")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=10)
                        data = json.loads(message)
                        if "symbol" in data and "price" in data:
                            await upsert_price(data)
                        else:
                            logger.debug(f"Ignored: {data}")
                    except asyncio.TimeoutError:
                        logger.info("⏳ No price updates. Refreshing.")
                        break
        except Exception as e:
            logger.error(f"⚠️ WebSocket error: {e}")
        logger.warning("⚠️ Reconnecting in 2 seconds...")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(price_bot_loop())


