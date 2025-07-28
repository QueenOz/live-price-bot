import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import aiohttp
from supabase import create_client, Client
import traceback

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

# Error logging with deduplication
async def log_error_with_deduplication(error_type, severity, message, function_name=None, symbol=None, 
                                     connection_state=None, stack_trace=None, request_data=None, 
                                     response_data=None, active_symbols_count=None):
    """Log error with automatic deduplication"""
    try:
        # Check if similar error exists in last hour
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        
        existing_query = supabase.table("bot_errors").select("*").eq(
            "error_message", message
        ).gte("created_at", one_hour_ago)
        
        if function_name:
            existing_query = existing_query.eq("function_name", function_name)
        if symbol:
            existing_query = existing_query.eq("symbol", symbol)
            
        existing = existing_query.execute()
        
        if existing.data:
            # Update existing error count
            error_record = existing.data[0]
            supabase.table("bot_errors").update({
                "occurrence_count": error_record["occurrence_count"] + 1,
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "connection_state": connection_state,
                "active_symbols_count": active_symbols_count
            }).eq("id", error_record["id"]).execute()
            print(f"🔄 Updated existing error #{error_record['id']}: {message}")
        else:
            # Insert new error
            error_data = {
                "error_type": error_type,
                "severity": severity,
                "error_message": message,
                "function_name": function_name,
                "symbol": symbol,
                "connection_state": connection_state,
                "stack_trace": stack_trace,
                "request_data": request_data,
                "response_data": response_data,
                "active_symbols_count": active_symbols_count,
                "first_occurrence": datetime.now(timezone.utc).isoformat(),
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            result = supabase.table("bot_errors").insert(error_data).execute()
            print(f"🆕 Logged new error #{result.data[0]['id']}: {message}")
            
    except Exception as e:
        # Fallback to console if error logging fails
        print(f"❌ Failed to log error to database: {e}")
        print(f"Original error: {message}")

async def clear_live_prices():
    """Clear all rows from live_prices table"""
    try:
        # First, get count of existing rows to report how many we're deleting
        existing_data = supabase.table("live_prices").select("symbol").execute()
        row_count = len(existing_data.data) if existing_data.data else 0
        
        if row_count > 0:
            # ✅ This works - deletes all rows where symbol is not empty string
            result = supabase.table("live_prices").delete().neq("symbol", "").execute()
            print(f"🗑️ Cleared {row_count} rows from live_prices table")
        else:
            print("🗑️ live_prices table was already empty")
        
        await log_error_with_deduplication(
            error_type="symbol_fetch",
            severity="warning", 
            message=f"No symbols returned from fetch-symbols, cleared {row_count} rows from live_prices table",
            function_name="fetch_symbols_loop",
            active_symbols_count=0
        )
    except Exception as e:
        await log_error_with_deduplication(
            error_type="database",
            severity="error",
            message=f"Failed to clear live_prices table: {str(e)}",
            function_name="clear_live_prices",
            stack_trace=traceback.format_exc()
        )

async def fetch_symbols_loop():
    global symbols, symbol_map
    
    # Create session once and reuse it
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(
                    FETCH_SYMBOLS_ENDPOINT,
                    headers={
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "apikey": SUPABASE_SERVICE_ROLE_KEY,
                        "Content-Type": "application/json"
                    }
                ) as response:
                    
                    if not response.ok:
                        error_text = await response.text()
                        await log_error_with_deduplication(
                            error_type="api",
                            severity="error",
                            message=f"fetch-symbols API error: HTTP {response.status}",
                            function_name="fetch_symbols_loop",
                            request_data={"url": FETCH_SYMBOLS_ENDPOINT, "status": response.status},
                            response_data={"error": error_text},
                            active_symbols_count=len(symbols)
                        )
                        await asyncio.sleep(60)
                        continue
                    
                    data = await response.json()
                    fetched_symbols = data.get("symbols", [])
                    
                    # ✅ Check if no symbols returned - clear live_prices table
                    if not fetched_symbols:
                        print("⚠️ No symbols returned from fetch-symbols endpoint")
                        await clear_live_prices()
                        symbols = set()
                        symbol_map = {}
                        await asyncio.sleep(60)
                        continue
                    
                    new_symbols = set()
                    new_map = {}

                    for row in fetched_symbols:
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
                    print(f"🔄 Refreshed symbols: {len(symbols)} symbols loaded")
                    
            except aiohttp.ClientError as e:
                await log_error_with_deduplication(
                    error_type="api",
                    severity="error",
                    message=f"HTTP client error in fetch_symbols_loop: {str(e)}",
                    function_name="fetch_symbols_loop",
                    stack_trace=traceback.format_exc(),
                    active_symbols_count=len(symbols)
                )
            except json.JSONDecodeError as e:
                await log_error_with_deduplication(
                    error_type="parsing",
                    severity="error",
                    message=f"JSON parsing error in fetch_symbols_loop: {str(e)}",
                    function_name="fetch_symbols_loop",
                    stack_trace=traceback.format_exc(),
                    active_symbols_count=len(symbols)
                )
            except Exception as e:
                await log_error_with_deduplication(
                    error_type="symbol_fetch",
                    severity="error",
                    message=f"Unexpected error in fetch_symbols_loop: {str(e)}",
                    function_name="fetch_symbols_loop",
                    stack_trace=traceback.format_exc(),
                    active_symbols_count=len(symbols)
                )

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
            await log_error_with_deduplication(
                error_type="price_insert",
                severity="warning",
                message=f"No price data received for symbol",
                function_name="insert_price",
                symbol=symbol,
                active_symbols_count=len(symbols)
            )
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
        await log_error_with_deduplication(
            error_type="price_insert",
            severity="error",
            message=f"Error inserting price: {str(e)}",
            function_name="insert_price",
            symbol=data.get("symbol") if data else None,
            stack_trace=traceback.format_exc(),
            request_data=data,
            active_symbols_count=len(symbols)
        )

async def send_heartbeat(ws):
    while True:
        try:
            await asyncio.sleep(10)
            await ws.send_str(json.dumps({"action": "heartbeat"}))
        except Exception as e:
            await log_error_with_deduplication(
                error_type="heartbeat",
                severity="error",
                message=f"Heartbeat failed: {str(e)}",
                function_name="send_heartbeat",
                connection_state="failed",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            raise e

async def check_price_timeout():
    global last_price_time
    while True:
        await asyncio.sleep(5)
        now = datetime.now(timezone.utc)
        delta = (now - last_price_time).total_seconds()
        if delta > 5:
            await log_error_with_deduplication(
                error_type="timeout",
                severity="warning",
                message=f"No price received in {int(delta)} seconds, triggering reconnect",
                function_name="check_price_timeout",
                connection_state="timeout",
                active_symbols_count=len(symbols)
            )
            raise Exception("Timeout: No price update")

async def maintain_connection():
    global previous_symbols
    heartbeat_task = None
    timeout_task = None

    # Create session once and reuse it for WebSocket connections
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if not symbols:
                    print("⚠️ No symbols to subscribe.")
                    await asyncio.sleep(5)
                    continue

                resubscribe = symbols != previous_symbols
                previous_symbols = set(symbols)

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
                        print(f"📤 Subscribed to: {len(symbols)} symbols")
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
                            except json.JSONDecodeError as e:
                                await log_error_with_deduplication(
                                    error_type="parsing",
                                    severity="warning",
                                    message=f"Error parsing WebSocket message: {str(e)}",
                                    function_name="maintain_connection",
                                    connection_state="connected",
                                    response_data={"raw_message": msg.data},
                                    active_symbols_count=len(symbols)
                                )
                            except Exception as e:
                                await log_error_with_deduplication(
                                    error_type="websocket",
                                    severity="error",
                                    message=f"Error processing WebSocket message: {str(e)}",
                                    function_name="maintain_connection",
                                    connection_state="connected",
                                    stack_trace=traceback.format_exc(),
                                    active_symbols_count=len(symbols)
                                )
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            await log_error_with_deduplication(
                                error_type="websocket",
                                severity="error",
                                message=f"WebSocket error: {msg.data}",
                                function_name="maintain_connection",
                                connection_state="error",
                                response_data={"error_data": str(msg.data)},
                                active_symbols_count=len(symbols)
                            )
                            break
                            
            except aiohttp.ClientError as e:
                await log_error_with_deduplication(
                    error_type="connection",
                    severity="error",
                    message=f"WebSocket connection error: {str(e)}",
                    function_name="maintain_connection",
                    connection_state="failed",
                    stack_trace=traceback.format_exc(),
                    active_symbols_count=len(symbols)
                )
            except Exception as e:
                await log_error_with_deduplication(
                    error_type="connection",
                    severity="error",
                    message=f"Unexpected error in maintain_connection: {str(e)}",
                    function_name="maintain_connection",
                    connection_state="failed",
                    stack_trace=traceback.format_exc(),
                    active_symbols_count=len(symbols)
                )
            finally:
                if heartbeat_task:
                    heartbeat_task.cancel()
                if timeout_task:
                    timeout_task.cancel()
                await asyncio.sleep(3)

async def main():
    try:
        print("🚀 Starting trading bot...")
        await log_error_with_deduplication(
            error_type="startup",
            severity="info",
            message="Trading bot started successfully",
            function_name="main",
            active_symbols_count=0
        )
        
        await asyncio.gather(
            fetch_symbols_loop(),
            maintain_connection()
        )
    except Exception as e:
        await log_error_with_deduplication(
            error_type="startup",
            severity="fatal",
            message=f"Critical error in main: {str(e)}",
            function_name="main",
            stack_trace=traceback.format_exc(),
            active_symbols_count=len(symbols)
        )
        raise

if __name__ == '__main__':
    asyncio.run(main())
