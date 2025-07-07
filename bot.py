import asyncio
import websockets
import httpx
import os
import json
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
TD_API_KEY = os.environ["TD_API_KEY"]

SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
TD_WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

async def get_symbols():
    async with httpx.AsyncClient() as client:
        # 1. Fetch active or upcoming game IDs
        games_resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/games",
            params={"select": "id", "status": "in.(upcoming,active)"},
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        )
        game_ids = [g["id"] for g in games_resp.json()]

        if not game_ids:
            return []

        # 2. Fetch game_assets for these games
        asset_resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/game_assets",
            params={
                "select": "symbol,standardized_symbol,market_type,game_status,game_id",
                "game_id": f"in.({','.join(game_ids)})"
            },
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        )
        assets = asset_resp.json()

        symbols = []
        for asset in assets:
            symbol = asset["standardized_symbol"]
            if asset["market_type"] == "forex":
                symbol = symbol.replace("_", "/")
            symbols.append({
                "symbol": symbol,
                "standardized_symbol": asset["standardized_symbol"],
            })

        return symbols

async def handle_price_update(symbol, standardized_symbol, price):
    now = datetime.now(timezone.utc).isoformat()
    SUPABASE.table("live_prices").upsert({
        "symbol": symbol,
        "standardized_symbol": standardized_symbol,
        "price": price,
        "updated_at": now,
    }, on_conflict=["symbol"]).execute()

async def run():
    symbols = await get_symbols()
    if not symbols:
        print("❌ No active symbols found")
        return

    symbol_map = {s["symbol"]: s["standardized_symbol"] for s in symbols}
    subscribe_symbols = list(symbol_map.keys())

    async with websockets.connect(TD_WEBSOCKET_URL) as ws:
        subscribe_payload = {
            "action": "subscribe",
            "symbols": subscribe_symbols
        }
        await ws.send(json.dumps(subscribe_payload))
        print(f"📤 Subscribed to: {subscribe_symbols}")

        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=30)
                data = json.loads(message)

                if "symbol" in data and "price" in data:
                    symbol = data["symbol"]
                    price = float(data["price"])
                    standardized_symbol = symbol_map.get(symbol)

                    print(f"📩 {symbol}: {price}")
                    await handle_price_update(symbol, standardized_symbol, price)

            except asyncio.TimeoutError:
                print("🔁 Timeout — sending ping")
                await ws.send(json.dumps({"action": "ping"}))
            except websockets.ConnectionClosed:
                print("🔌 Connection closed, reconnecting...")
                await run()
            except Exception as e:
                print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(run())

