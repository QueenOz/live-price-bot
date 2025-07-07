import os
import asyncio
import json
import httpx
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
from twelvedata_ws import TDWebSocketClient

# ✅ Load environment variables from .env file
load_dotenv()

# ✅ Get env variables safely
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# ✅ Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ✅ Table and endpoints
SUPABASE_GAME_URL = f"{SUPABASE_URL}/rest/v1"
headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
}

# ✅ Live prices table: upsert rows
async def save_price(symbol, standardized_symbol, price):
    await supabase.table("live_prices").upsert({
        "symbol": symbol,
        "standardized_symbol": standardized_symbol,
        "price": price,
        "updated_at": datetime.utcnow().isoformat()
    }, on_conflict=["symbol"]).execute()

# ✅ Fetch active/upcoming game assets
async def fetch_symbols():
    async with httpx.AsyncClient() as client:
        # Get active game IDs
        game_res = await client.get(
            f"{SUPABASE_GAME_URL}/games?select=id&status=in.(upcoming,active)",
            headers=headers
        )
        game_ids = [g["id"] for g in game_res.json()]
        if not game_ids:
            return []

        # Get all assets for those games
        game_ids_str = ",".join(f'"{gid}"' for gid in game_ids)
        asset_res = await client.get(
            f"{SUPABASE_GAME_URL}/game_assets"
            f"?select=symbol,standardized_symbol,market_type,game_status&game_id=in.({game_ids_str})",
            headers=headers
        )
        assets = asset_res.json()

        # Filter and normalize
        symbols = []
        for asset in assets:
            if asset["game_status"] not in ("active", "upcoming"):
                continue
            market = asset["market_type"]
            std_symbol = asset["standardized_symbol"]
            if market == "forex":
                symbol = std_symbol.replace("_", "/")
            else:
                symbol = std_symbol
            symbols.append((symbol, std_symbol))
        return symbols

# ✅ Start WebSocket connection
async def start_ws(symbols):
    client = TDWebSocketClient(apikey=TWELVE_DATA_API_KEY)

    @client.on_message()
    async def handle_message(msg):
        try:
            data = json.loads(msg)
            if "price" in data and "symbol" in data:
                await save_price(
                    symbol=data["symbol"],
                    standardized_symbol=data["symbol"].replace("/", "_"),
                    price=float(data["price"])
                )
        except Exception as e:
            print(f"⚠️ Error parsing message: {e}")

    await client.connect()
    for symbol, _ in symbols:
        print(f"📤 Subscribing to: {symbol}")
        await client.subscribe(symbol)

# ✅ Main loop
async def main():
    symbols = await fetch_symbols()
    if symbols:
        await start_ws(symbols)
    else:
        print("🛑 No symbols to subscribe to.")

if __name__ == "__main__":
    asyncio.run(main())


