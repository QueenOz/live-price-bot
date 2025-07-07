import asyncio
import json
import os
from datetime import datetime
from dotenv import load_dotenv

import aiohttp
from supabase import create_client, Client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TD_WEBSOCKET_URL = os.getenv("TD_WEBSOCKET_URL") + f"?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Shared sets and maps
symbols = set()
symbol_map = {}  # maps "symbol" → "standardized_symbol"
previous_symbols = set()

async def fetch_symbols_loop():
    global symbols, symbol_map
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(FETCH_SYMBOLS_ENDPOINT, headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                    "Content-Type": "application/json"
                }) as response:
                    data = await response.json()

                    # Expecting: [{ symbol: "ETH/USD", standardized_symbol: "ETHUSD" }]
                    new_symbols = set()
                    new_symbol_map = {}

                    for row in data.get("symbols", []):
                        symbol = row.get("symbol")
                        standardized_symbol = row.get("standardized_symbol")
                        if symbol and standardized_symbol:
                            new_symbols.add(symbol)
                            new_symbol_map[symbol] = standardized_symbol

                    symbols = new_symbols
                    symbol_map = new_symbol_map

                    print(f"🔄 Refreshed symbols: {symbols}")
        except Exception as e:
            print("❌ Failed to fetch symbols:", e)

        await asyncio.sleep(60)

async def insert_price(data):
    try:
        price = data.get("price")
        symbol = data.get("symbol")
        status = "pulled" if price is not None else "failed"
        if price is None:
            price = 0
            print(f"❌ No price for {symbol}, inserting 0")
        else:
            print(f"✅ Price pulled for {symbol}: {price}")

        # Find matching symbol record from the symbols set
        matched = next((s for s in symbols if s["symbol"] == symbol), None)

        row = {
            "symbol": symbol,
            "price": price,
            "status": status,
            "updated_at": datetime.utcfromtimestamp(data["timestamp"]).isoformat() + "Z",
            "market_type": data.get("type", "unknown"),
            "standardized_symbol": matched["standardized_symbol"] if matched else None,
            "name": matched["asset_name"] if matched else None  # ✅ new field
        }

        supabase.table("live_prices").upsert([row]).execute()
        print(f"✅ Upserted price for {symbol}: {price} ({status})")
    except Exception as e:
        print("❌ Error inserting price:", e)


        supabase.table("live_prices").upsert(rows).execute()
        print(f"✅ Upserted price for {symbol}: {price} ({status})")
    except Exception as e:
        print("❌ Error inserting price:", e)

async def send_heartbeat(ws):
    while True:
        try:
            await asyncio.sleep(10)
            await ws.send_str(json.dumps({"action": "heartbeat"}))
        except Exception as e:
            print("💔 Heartbeat failed:", e)
            break

async def maintain_connection():
    global previous_symbols

    while True:
        try:
            if not symbols:
                print("⚠️ No symbols to subscribe.")
                await asyncio.sleep(10)
                continue

            resubscribe = symbols != previous_symbols
            previous_symbols = set(symbols)

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(TD_WEBSOCKET_URL) as ws:
                    if resubscribe:
                        subscribe_payload = json.dumps({
                            "action": "subscribe",
                            "params": {
                                "symbols": ",".join(symbols),
                                "apikey": TWELVE_DATA_API_KEY
                            }
                        })
                        await ws.send_str(subscribe_payload)
                        print(f"📤 Subscribed to: {symbols}")
                    else:
                        print("✅ Symbol list unchanged, skipping re-subscribe")

                    asyncio.create_task(send_heartbeat(ws))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if data.get("event") == "price":
                                    await insert_price(data)
                                elif data.get("event") == "status":
                                    print(f"⚙️ Status event: {data}")
                                else:
                                    print(f"🪵 Other event: {data}")
                            except Exception as e:
                                print("⚠️ Error parsing message:", e)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("❌ WebSocket error:", msg.data)
                            break
        except Exception as e:
            print("🔁 Reconnecting due to error:", e)
            await asyncio.sleep(5)

async def main():
    await asyncio.gather(
        fetch_symbols_loop(),
        maintain_connection()
    )

if __name__ == '__main__':
    asyncio.run(main())







