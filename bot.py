import asyncio
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import aiohttp
from supabase import create_client, Client

# Load env vars
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TD_WEBSOCKET_URL = os.getenv("TD_WEBSOCKET_URL") + f"?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Shared state
symbols = set()
symbol_map = {}
previous_symbols = set()
last_price_time = datetime.now(timezone.utc)

async def fetch_symbols_loop():
    global symbols, symbol_map
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    FETCH_SYMBOLS_ENDPOINT,
                    headers={
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "apikey": SUPABASE_SERVICE_ROLE_KEY,
                        "Content-Type": "application/json"
                    }
                ) as response:
                    data = await response.json()
                    new_symbols = set()
                    new_map = {}

                    for row in data.get("symbols", []):
                        market_type = row.get("market_type")
                        name = row.get("asset_name")
                        standardized = row.get("standardized_symbol")
                        raw_symbol = row.get("symbol")

                        # ✅ Use standardized_symbol for forex, fallback to asset_name
                        if market_type == "forex":
                            resolved_symbol = standardized or name
                        else:
                            resolved_symbol = raw_symbol

                        if not resolved_symbol:
                            continue

                        new_symbols.add(resolved_symbol)
                        new_map[resolved_symbol] = {
                            "standardized_symbol": standardized,
                            "asset_name": name,
                            "market_type": market_type or "unknown"
                        }

                    symbols = new_symbols
                    symbol_map = new_map
                    print(f"🔄 Refreshed symbols: {symbols}")
        except Exception as e:
            print("❌ Failed to fetch symbols:", e)

        await asyncio.sleep(60)

async def insert_price(data):
    global last_price_time
    try:
        price = data.get("price")
        symbol = data.get("symbol")
        ts = datetime.fromtimestamp(data["timestamp"], timezone.utc).isoformat()
        status = "pulled" if price is not None else "failed"
        if price is None:
            price = 0
            print(f"❌ No price for {symbol}, inserting 0")
        else:
            print(f"✅ Price pulled for {symbol}: {price}")
            last_price_time = datetime.now(timezone.utc)

        asset_info = symbol_map.get(symbol, {})
        row = {
            "symbol": symbol,
            "standardized_symbol": asset_info.get("standardized_symbol"),
            "name": asset_info.get("asset_name"),
            "market_type": asset_info.get("market_type"),
            "price": price,
            "status": status,
            "updated_at": ts
        }

        supabase.table("live_prices").upsert([row]).execute()
        print(f"✅ Upserted price for {symbol}: {price} ({status})")
    except Exception as e:
        print("❌ Error inserting price:", e)

async def send_heartbeat(ws):
    while True:
        try:
            await asyncio.sleep(10)
            await ws.send_str(json.dumps({"action": "heartbeat"}))
        except Exception as e:
            print("💔 Heartbeat failed, triggering reconnect:", e)
            raise e

async def check_price_timeout():
    global last_price_time
    while True:
        await asyncio.sleep(5)
        now = datetime.now(timezone.utc)
        delta = (now - last_price_time).total_seconds()
        if delta > 5:
            print(f"⏱️ No price received in {int(delta)}s, triggering reconnect...")
            raise Exception("Timeout: No price update")

async def maintain_connection():
    global previous_symbols
    heartbeat_task = None
    timeout_task = None

    while True:
        try:
            if not symbols:
                print("⚠️ No symbols to subscribe.")
                await asyncio.sleep(5)
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

                    heartbeat_task = asyncio.create_task(send_heartbeat(ws))
                    timeout_task = asyncio.create_task(check_price_timeout())

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
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
            if timeout_task:
                timeout_task.cancel()
            await asyncio.sleep(3)

async def main():
    await asyncio.gather(
        fetch_symbols_loop(),
        maintain_connection()
    )

if __name__ == '__main__':
    asyncio.run(main())







