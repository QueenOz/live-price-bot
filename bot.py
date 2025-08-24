import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import aiohttp
from supabase import create_client, Client
import traceback
import random

# 🚨 REAL MONEY = ZERO DELAY: Immediate insertion
price_buffer = {}
last_insert_time = datetime.now(timezone.utc)
IMMEDIATE_INSERT = True  # 🚨 REAL MONEY MODE: Insert every price immediately

# Load env vars
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
# 🔧 FIXED: Use correct WebSocket URL from documentation
TD_WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

# 🔧 NEW: Binance WebSocket for ETH/USD (free, no API key needed)
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws/ethusdt@ticker"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Shared state with shutdown control
symbols = set()
symbol_map = {}
previous_symbols = set()
last_price_time = datetime.now(timezone.utc)
shutdown_requested = False

# Task monitoring
fetch_task = None
connection_task = None
binance_connection_task = None  # 🔧 NEW: Binance connection task
watchdog_task = None

async def exponential_backoff(attempt, base_delay=1, max_delay=60):
    """Calculate exponential backoff with jitter"""
    delay = min(base_delay * (2 ** min(attempt, 6)), max_delay)
    jitter = random.uniform(0.1, 0.3) * delay
    final_delay = delay + jitter
    print(f"⏳ Backing off for {final_delay:.1f}s (attempt #{attempt + 1})")
    await asyncio.sleep(final_delay)
    return final_delay

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
    """Enhanced fetch loop with exponential backoff and never-give-up attitude"""
    global symbols, symbol_map, shutdown_requested
    consecutive_failures = 0
    
    while not shutdown_requested:
        session = None
        try:
            # Create fresh session for each attempt after failure
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=20)  # 🔧 Increased timeouts
            )
            
            print(f"📡 Fetching symbols from {FETCH_SYMBOLS_ENDPOINT}...")
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
                    consecutive_failures += 1
                    print(f"❌ fetch-symbols API error: HTTP {response.status} - {error_text}")
                    await log_error_with_deduplication(
                        error_type="api",
                        severity="error",
                        message=f"fetch-symbols API error: HTTP {response.status}",
                        function_name="fetch_symbols_loop",
                        request_data={"url": FETCH_SYMBOLS_ENDPOINT, "status": response.status},
                        response_data={"error": error_text},
                        active_symbols_count=len(symbols)
                    )
                    await exponential_backoff(consecutive_failures, base_delay=5)
                    continue
                
                data = await response.json()
                fetched_symbols = data.get("symbols", [])
                
                print(f"🔍 RAW RESPONSE: Found {len(fetched_symbols)} symbols from view")
                if fetched_symbols:
                    print(f"📋 First few symbols: {[s.get('symbol') for s in fetched_symbols[:5]]}")
                
                # ✅ Check if no symbols returned - clear live_prices table
                if not fetched_symbols:
                    print("⚠️ No symbols returned from fetch-symbols endpoint")
                    await clear_live_prices()
                    symbols = set()
                    symbol_map = {}
                    consecutive_failures += 1
                    await exponential_backoff(consecutive_failures, base_delay=10)
                    continue
                
                new_symbols = set()
                new_map = {}
                for row in fetched_symbols:
                    symbol_str = row.get("symbol")
                    if not symbol_str:
                        print(f"⚠️ Skipping row with missing symbol: {row}")
                        continue  # skip empty

                    print(f"🔧 PROCESSING: {symbol_str}")
                    new_symbols.add(symbol_str)
                    new_map[symbol_str] = {
                        "market_id": row.get("market_id"),
                        "symbol": symbol_str,
                        "standardized_symbol": row.get("standardized_symbol"),
                        "asset_name": row.get("asset_name"),
                        "market_type": row.get("market_type") or "unknown",
                        "exchange": row.get("exchange") or None
                    }

                symbols = new_symbols
                symbol_map = new_map
                
                consecutive_failures = 0  # Reset on success
                print(f"✅ SUCCESS: Loaded {len(symbols)} symbols from game_assets_view")
                print(f"📋 ALL SYMBOLS: {sorted(list(symbols))}")  # Show all symbols alphabetically
                print(f"🗺️ Symbol map has {len(symbol_map)} entries")
                
                # 🔍 Debug specific symbols
                if "BTC/USD" in symbol_map:
                    print(f"✅ BTC/USD found: {symbol_map['BTC/USD']}")
                if "ETH/USD" in symbol_map:
                    print(f"✅ ETH/USD found: {symbol_map['ETH/USD']}")
                    print(f"   Will use Binance WebSocket for ETH/USD prices")
                else:
                    print("❌ ETH/USD NOT found in symbol_map")
                
                # Normal interval between successful fetches
                await asyncio.sleep(60)
                
        except asyncio.CancelledError:
            print("🛑 fetch_symbols_loop cancelled")
            break
        except asyncio.TimeoutError as e:
            consecutive_failures += 1
            print(f"⏰ TIMEOUT: fetch_symbols_loop timed out (attempt {consecutive_failures})")
            await log_error_with_deduplication(
                error_type="timeout",
                severity="error",
                message=f"fetch_symbols_loop timeout: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        except aiohttp.ClientError as e:
            consecutive_failures += 1
            print(f"🌐 HTTP CLIENT ERROR: {str(e)}")
            await log_error_with_deduplication(
                error_type="api",
                severity="error",
                message=f"HTTP client error in fetch_symbols_loop: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        except json.JSONDecodeError as e:
            consecutive_failures += 1
            print(f"📄 JSON PARSE ERROR: {str(e)}")
            await log_error_with_deduplication(
                error_type="parsing",
                severity="error",
                message=f"JSON parsing error in fetch_symbols_loop: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        except Exception as e:
            consecutive_failures += 1
            print(f"💥 UNEXPECTED ERROR: {str(e)}")
            await log_error_with_deduplication(
                error_type="symbol_fetch",
                severity="error",
                message=f"Unexpected error in fetch_symbols_loop: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        finally:
            if session and not session.closed:
                await session.close()

async def receive_price(data, source="TwelveData"):
    """🚨 REAL MONEY MODE: IMMEDIATE insertion - consistent symbol handling"""
    global last_price_time

    symbol = data.get("symbol")  # Use exactly what WebSocket sends
    price = data.get("price")
    timestamp = data.get("timestamp")

    # 🔍 DEBUG: Log all incoming symbols
    print(f"🔍 INCOMING ({source}): symbol='{symbol}', price={price}, timestamp={timestamp}")

    # 🔧 TIMESTAMP FIX: Use current time if WebSocket timestamp is invalid
    try:
        # Check if timestamp is reasonable (not too far in future/past)
        ws_time = datetime.fromtimestamp(timestamp, timezone.utc)
        current_time = datetime.now(timezone.utc)
        time_diff = abs((ws_time - current_time).total_seconds())
        
        if time_diff > 3600:  # More than 1 hour difference
            print(f"⚠️ Invalid timestamp {timestamp} ({ws_time}), using current time")
            ts = current_time.isoformat()
        else:
            ts = ws_time.isoformat()
            
    except (ValueError, OSError):
        print(f"⚠️ Failed to parse timestamp {timestamp}, using current time")
        ts = datetime.now(timezone.utc).isoformat()
        
    status = "pulled" if price is not None else "failed"

    if price is None:
        price = 0
        print(f"❌ No price for {symbol}, inserting 0 IMMEDIATELY")
    else:
        print(f"💰 REAL MONEY ({source}): {symbol}: ${price} - INSERTING NOW!")
        last_price_time = datetime.now(timezone.utc)

    # Look up using exact symbol (should work for both BTC/USD and other symbols)
    matched = symbol_map.get(symbol)
    if not matched:
        print(f"❌ No match in symbol_map for symbol '{symbol}' from {source}")
        print(f"🔍 Available symbols in map: {list(symbol_map.keys())}")
        return

    # 🚨 REAL MONEY: Build price data using exact symbol
    price_data = {
        "symbol": symbol,  # Use exact symbol from WebSocket/database
        "standardized_symbol": matched.get("standardized_symbol", symbol),
        "price": price,
        "updated_at": ts,
        "asset_name": matched.get("asset_name", symbol),
        "market_type": matched.get("market_type", "unknown"),
        "status": status,
        "search_symbol": matched.get("standardized_symbol", symbol),
        "exchange": source
    }
    
    # 🚨 CRITICAL: IMMEDIATE INSERT - NO WAITING!
    print(f"🚨 IMMEDIATE INSERT ({source}): {symbol} @ ${price}")
    asyncio.create_task(insert_single_price_immediate(price_data))

async def receive_binance_price(data):
    """🔧 NEW: Handle Binance ETHUSDT data and convert to ETH/USD format"""
    global last_price_time
    
    # Binance ticker format: {"s":"ETHUSDT","c":"4791.99","o":"4778.40","h":"4956.78","l":"4720.00"}
    binance_symbol = data.get("s")  # "ETHUSDT"
    price = data.get("c")  # Current price
    
    # Convert to our database format
    db_symbol = "ETH/USD"
    
    print(f"🔍 INCOMING (Binance): symbol='{binance_symbol}' -> '{db_symbol}', price={price}")
    
    # Check if we have ETH/USD in symbol_map
    matched = symbol_map.get(db_symbol)
    if not matched:
        print(f"❌ No match in symbol_map for '{db_symbol}'")
        print(f"🔍 Available symbols in map: {list(symbol_map.keys())}")
        return
    
    if price is None:
        price = 0
        print(f"❌ No price for {db_symbol}, inserting 0 IMMEDIATELY")
    else:
        print(f"💰 REAL MONEY (Binance): {db_symbol}: ${price} - INSERTING NOW!")
        last_price_time = datetime.now(timezone.utc)
    
    # Build price data
    price_data = {
        "symbol": db_symbol,  # ETH/USD
        "standardized_symbol": matched.get("standardized_symbol", "ETH"),
        "price": float(price) if price else 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "asset_name": matched.get("asset_name", "ETH/USD"),
        "market_type": matched.get("market_type", "crypto"),
        "status": "pulled",
        "search_symbol": matched.get("standardized_symbol", "ETH"),
        "exchange": "Binance"
    }
    
    print(f"🚨 IMMEDIATE INSERT (Binance): {db_symbol} @ ${price}")
    asyncio.create_task(insert_single_price_immediate(price_data))

async def insert_single_price_immediate(price_data):
    """🚨 REAL MONEY: Single price immediate insert - fastest possible"""
    try:
        symbol = price_data["symbol"]
        price = price_data["price"]
        exchange = price_data.get("exchange", "Unknown")
        
        print(f"💰 INSERTING NOW ({exchange}): {symbol} @ ${price}")
        
        # Transform to match live_prices table schema
        row = {
            "symbol": price_data["symbol"],
            "standardized_symbol": price_data["standardized_symbol"],
            "search_symbol": price_data["search_symbol"], 
            "name": price_data["asset_name"],
            "market_type": price_data["market_type"],
            "exchange": price_data["exchange"],
            "price": price_data["price"],
            "status": price_data["status"],
            "updated_at": price_data["updated_at"]
        }
        
        # 🚨 FASTEST: Direct single upsert
        start_time = datetime.now()
        result = supabase.table("live_prices").upsert(
            row,
            on_conflict="symbol"
        ).execute()
        end_time = datetime.now()
        
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        if result.data:
            print(f"💰 INSERTED ({exchange}) {symbol} in {duration_ms:.0f}ms @ ${price}")
        else:
            print(f"⚠️ Insert failed for {symbol} from {exchange}")
            
    except Exception as e:
        await log_error_with_deduplication(
            error_type="database",
            severity="error",
            message=f"IMMEDIATE insert failed for {price_data.get('symbol', 'unknown')} from {price_data.get('exchange', 'Unknown')}: {str(e)}",
            function_name="insert_single_price_immediate",
            stack_trace=traceback.format_exc()
        )

async def insert_prices_loop():
    """🚨 REAL MONEY MODE: No batching - everything is immediate"""
    print("🚨 REAL MONEY MODE: All prices inserted immediately - no batching loop needed")
    
    while not shutdown_requested:
        # Just monitor system health
        await asyncio.sleep(5)
        print(f"💰 REAL MONEY MODE: Active - all prices inserted immediately")

async def send_heartbeat(ws):
    while not shutdown_requested:
        try:
            await asyncio.sleep(10)
            await ws.send_str(json.dumps({"action": "heartbeat"}))
        except asyncio.CancelledError:
            break
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
    global last_price_time, shutdown_requested
    while not shutdown_requested:
        try:
            await asyncio.sleep(5)
            now = datetime.now(timezone.utc)
            delta = (now - last_price_time).total_seconds()
            if delta > 30:  # 🚀 REDUCED: 30 seconds timeout for real-time
                await log_error_with_deduplication(
                    error_type="timeout",
                    severity="warning",
                    message=f"No price received in {int(delta)} seconds, triggering reconnect",
                    function_name="check_price_timeout",
                    connection_state="timeout",
                    active_symbols_count=len(symbols)
                )
                raise Exception("Timeout: No price update")
        except asyncio.CancelledError:
            break

async def maintain_connection():
    """🔧 TwelveData connection - handles BTC/USD and other supported symbols"""
    global previous_symbols, shutdown_requested
    consecutive_failures = 0
    heartbeat_task = None
    timeout_task = None

    while not shutdown_requested:
        session = None
        try:
            if not symbols:
                print("⚠️ No symbols to subscribe.")
                await asyncio.sleep(5)
                continue

            print(f"🔍 TwelveData SYMBOLS CHECK: {len(symbols)} symbols available: {list(symbols)}")
            resubscribe = symbols != previous_symbols
            previous_symbols = set(symbols)

            # Create fresh session for each connection attempt
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )

            print(f"🔗 Connecting to TwelveData WebSocket... (failures: {consecutive_failures})")
            print(f"🔗 WebSocket URL: {TD_WEBSOCKET_URL}")
            async with session.ws_connect(
                TD_WEBSOCKET_URL,
                heartbeat=30
            ) as ws:
                print("✅ TwelveData WebSocket connected successfully!")
                consecutive_failures = 0  # Reset on successful connection
                
                if resubscribe:
                    print(f"🚀 ATTEMPTING TwelveData SUBSCRIPTION with {len(symbols)} symbols...")
                    
                    # 🔧 CONSISTENT: Use all symbols exactly as they come from database
                    final_symbols = list(symbols)  # No symbol modification at all
                    
                    subscribe_payload = json.dumps({
                        "action": "subscribe",
                        "params": {
                            "symbols": ",".join(final_symbols)  # e.g., "BTC/USD,ETH/USD"
                        }
                    })
                    
                    print(f"📤 SENDING TwelveData SUBSCRIPTION...")
                    await ws.send_str(subscribe_payload)
                    print(f"📤 🚀 TwelveData SUBSCRIBED: {final_symbols}")
                    print(f"🔍 EXACT SUBSCRIPTION PAYLOAD: {subscribe_payload}")
                    
                    # 🚨 Wait for subscription confirmation
                    print("⏳ Waiting for TwelveData subscription confirmation...")
                    
                else:
                    print("✅ TwelveData symbol list unchanged, skipping re-subscribe")

                heartbeat_task = asyncio.create_task(send_heartbeat(ws))
                timeout_task = asyncio.create_task(check_price_timeout())

                async for msg in ws:
                    if shutdown_requested:
                        break
                        
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            # 🔍 DEBUG: Log ALL WebSocket messages
                            print(f"🔍 RAW TwelveData WEBSOCKET: {data}")
                            
                            if data.get("event") == "price":
                                # 🚨 CRITICAL: Process price immediately
                                print(f"💰 TwelveData PRICE EVENT: {data}")
                                await receive_price(data, source="TwelveData")
                            elif data.get("event") == "status":
                                print(f"⚙️ TwelveData Status: {data}")
                            elif data.get("event") == "subscribe-status":
                                print(f"📋 TwelveData SUBSCRIPTION STATUS: {data}")
                                
                                # 🔍 Enhanced: Check individual subscription success/failure
                                symbol = data.get("symbol")
                                status = data.get("status")
                                
                                if symbol and status:
                                    if status == "ok":
                                        print(f"✅ TwelveData SUBSCRIPTION SUCCESS: {symbol}")
                                    else:
                                        print(f"❌ TwelveData SUBSCRIPTION FAILED: {symbol} - Status: {status}")
                                        print(f"   Error details: {data}")
                                        
                                        # 🚨 CRITICAL: Check if ETH/USD subscription failed
                                        if symbol == "ETH/USD":
                                            print(f"🚨 ETH/USD REJECTED BY TWELVEDATA!")
                                            print(f"   This confirms ETH/USD not supported by TwelveData")
                                            print(f"   Binance WebSocket will handle ETH/USD instead")
                                            
                            elif data.get("event") == "heartbeat":
                                print(f"💓 TwelveData Heartbeat: {data.get('status', 'unknown')}")
                            else:
                                print(f"🪵 TwelveData Other event: {data}")
                        except json.JSONDecodeError as e:
                            await log_error_with_deduplication(
                                error_type="parsing",
                                severity="warning",
                                message=f"Error parsing TwelveData WebSocket message: {str(e)}",
                                function_name="maintain_connection",
                                connection_state="connected",
                                response_data={"raw_message": msg.data},
                                active_symbols_count=len(symbols)
                            )
                        except Exception as e:
                            await log_error_with_deduplication(
                                error_type="websocket",
                                severity="error",
                                message=f"Error processing TwelveData WebSocket message: {str(e)}",
                                function_name="maintain_connection",
                                connection_state="connected",
                                stack_trace=traceback.format_exc(),
                                active_symbols_count=len(symbols)
                            )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        await log_error_with_deduplication(
                            error_type="websocket",
                            severity="error",
                            message=f"TwelveData WebSocket error: {msg.data}",
                            function_name="maintain_connection",
                            connection_state="error",
                            response_data={"error_data": str(msg.data)},
                            active_symbols_count=len(symbols)
                        )
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("🔌 TwelveData WebSocket closed by server")
                        break
                        
        except asyncio.CancelledError:
            print("🛑 TwelveData maintain_connection cancelled")
            break
        except aiohttp.ClientError as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"TwelveData WebSocket connection error: {str(e)}",
                function_name="maintain_connection",
                connection_state="failed",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
        except Exception as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"Unexpected error in TwelveData maintain_connection: {str(e)}",
                function_name="maintain_connection",
                connection_state="failed",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
        finally:
            # Clean up tasks
            if heartbeat_task and not heartbeat_task.done():
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                    
            if timeout_task and not timeout_task.done():
                timeout_task.cancel()
                try:
                    await timeout_task
                except asyncio.CancelledError:
                    pass
            
            # Clean up session
            if session and not session.closed:
                await session.close()
            
            # Exponential backoff before reconnecting (only if there were failures)
            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)  # Brief pause on normal disconnect

async def maintain_binance_connection():
    """🔧 NEW: Binance connection specifically for ETH/USD"""
    global shutdown_requested
    consecutive_failures = 0
    
    while not shutdown_requested:
        session = None
        try:
            # Only connect if we have ETH/USD in our symbols
            if "ETH/USD" not in symbol_map:
                print("⚠️ No ETH/USD in symbol_map, skipping Binance connection")
                await asyncio.sleep(5)
                continue

            print(f"🔍 BINANCE: Connecting for ETH/USD (ETHUSDT)")
            
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )
            
            print(f"🔗 Connecting to Binance WebSocket... (failures: {consecutive_failures})")
            print(f"🔗 Binance URL: {BINANCE_WEBSOCKET_URL}")
            async with session.ws_connect(BINANCE_WEBSOCKET_URL) as ws:
                print("✅ Binance WebSocket connected for ETH/USD!")
                consecutive_failures = 0  # Reset on successful connection
                
                async for msg in ws:
                    if shutdown_requested:
                        break
                        
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            print(f"🔍 RAW BINANCE WEBSOCKET: {data}")
                            
                            # Binance ticker format: {"s":"ETHUSDT","c":"4791.99",...}
                            if data.get("s") == "ETHUSDT":
                                print(f"💰 BINANCE PRICE EVENT: {data}")
                                await receive_binance_price(data)
                                
                        except json.JSONDecodeError as e:
                            await log_error_with_deduplication(
                                error_type="parsing",
                                severity="warning",
                                message=f"Error parsing Binance WebSocket message: {str(e)}",
                                function_name="maintain_binance_connection",
                                connection_state="connected",
                                response_data={"raw_message": msg.data}
                            )
                        except Exception as e:
                            await log_error_with_deduplication(
                                error_type="websocket",
                                severity="error",
                                message=f"Error processing Binance WebSocket message: {str(e)}",
                                function_name="maintain_binance_connection",
                                connection_state="connected",
                                stack_trace=traceback.format_exc()
                            )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        await log_error_with_deduplication(
                            error_type="websocket",
                            severity="error",
                            message=f"Binance WebSocket error: {msg.data}",
                            function_name="maintain_binance_connection",
                            connection_state="error",
                            response_data={"error_data": str(msg.data)}
                        )
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("🔌 Binance WebSocket closed by server")
                        break
                        
        except asyncio.CancelledError:
            print("🛑 Binance maintain_connection cancelled")
            break
        except aiohttp.ClientError as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"Binance WebSocket connection error: {str(e)}",
                function_name="maintain_binance_connection",
                connection_state="failed",
                stack_trace=traceback.format_exc()
            )
        except Exception as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"Unexpected error in Binance maintain_connection: {str(e)}",
                function_name="maintain_binance_connection",
                connection_state="failed",
                stack_trace=traceback.format_exc()
            )
        finally:
            # Clean up session
            if session and not session.closed:
                await session.close()
            
            # Exponential backoff before reconnecting (only if there were failures)
            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)  # Brief pause on normal disconnect

async def watchdog():
    """Monitor system health and restart tasks if they die"""
    global fetch_task, connection_task, binance_connection_task, shutdown_requested
    print("🐕 Starting HYBRID WEBSOCKET watchdog...")
    
    while not shutdown_requested:
        try:
            await asyncio.sleep(10)  # Check every 10 seconds
            
            # Check if fetch task is still running
            if fetch_task and fetch_task.done():
                exception = fetch_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="startup",
                        severity="critical",
                        message=f"fetch_symbols_loop task died: {str(exception)}",
                        function_name="watchdog",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
                
                print("🚨 fetch_symbols_loop task died, restarting...")
                fetch_task = asyncio.create_task(fetch_symbols_loop())
            
            # Check if TwelveData connection task is still running
            if connection_task and connection_task.done():
                exception = connection_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="connection",
                        severity="critical",
                        message=f"TwelveData maintain_connection task died: {str(exception)}",
                        function_name="watchdog",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
                
                print("🚨 TwelveData connection task died, restarting...")
                connection_task = asyncio.create_task(maintain_connection())
            
            # Check if Binance connection task is still running
            if binance_connection_task and binance_connection_task.done():
                exception = binance_connection_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="connection",
                        severity="critical",
                        message=f"Binance maintain_connection task died: {str(exception)}",
                        function_name="watchdog",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
                
                print("🚨 Binance connection task died, restarting...")
                binance_connection_task = asyncio.create_task(maintain_binance_connection())
            
            # Check system health
            now = datetime.now(timezone.utc)
            time_since_price = (now - last_price_time).total_seconds()
            
            if time_since_price > 60:  # 🚀 REDUCED: 1 minute for real-time
                await log_error_with_deduplication(
                    error_type="timeout",
                    severity="critical",
                    message=f"No price updates for {int(time_since_price)} seconds - system may be stalled",
                    function_name="watchdog",
                    active_symbols_count=len(symbols)
                )
                print(f"💔 Health check failed - no prices for {int(time_since_price)}s")
            else:
                print(f"⚡ HYBRID MODE OK: last price {int(time_since_price)}s ago - TwelveData + Binance active")
            
        except asyncio.CancelledError:
            print("🛑 Watchdog cancelled")
            break
        except Exception as e:
            await log_error_with_deduplication(
                error_type="network",
                severity="error",
                message=f"Watchdog error: {str(e)}",
                function_name="watchdog",
                stack_trace=traceback.format_exc()
            )

def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

async def graceful_shutdown():
    """Cancel all tasks gracefully"""
    global fetch_task, connection_task, binance_connection_task, watchdog_task
    print("🛑 Shutting down gracefully...")
    
    # 🚨 REAL MONEY: No buffered prices to flush - all inserted immediately
    print("🚨 REAL MONEY MODE: No buffered prices to flush - all inserted immediately")
    
    tasks_to_cancel = []
    if fetch_task and not fetch_task.done():
        tasks_to_cancel.append(fetch_task)
    if connection_task and not connection_task.done():
        tasks_to_cancel.append(connection_task)
    if binance_connection_task and not binance_connection_task.done():
        tasks_to_cancel.append(binance_connection_task)
    if watchdog_task and not watchdog_task.done():
        tasks_to_cancel.append(watchdog_task)
    
    for task in tasks_to_cancel:
        task.cancel()
    
    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    
    await log_error_with_deduplication(
        error_type="shutdown",
        severity="info",
        message="HYBRID trading bot shutdown gracefully",
        function_name="graceful_shutdown"
    )
    print("✅ All tasks cancelled, goodbye!")

async def main():
    """🚀 HYBRID main with TwelveData + Binance WebSocket connections"""
    global fetch_task, connection_task, binance_connection_task, watchdog_task, shutdown_requested
    restart_count = 0
    max_restarts = 999999
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    while restart_count < max_restarts and not shutdown_requested:
        try:
            print(f"🚨 HYBRID WEBSOCKET MODE: IMMEDIATE insertion - every price matters!")
            print(f"💰 ZERO DELAYS: All symbols inserted instantly on every update")
            print(f"📡 TwelveData: BTC/USD and other supported symbols")
            print(f"📡 Binance: ETH/USD (ETHUSDT -> ETH/USD)")
            
            await log_error_with_deduplication(
                error_type="startup",
                severity="info",
                message=f"HYBRID trading bot started (restart #{restart_count}) - TwelveData + Binance",
                function_name="main",
                active_symbols_count=0
            )
            
            # Start all core tasks
            fetch_task = asyncio.create_task(fetch_symbols_loop())
            connection_task = asyncio.create_task(maintain_connection())  # TwelveData
            binance_connection_task = asyncio.create_task(maintain_binance_connection())  # Binance
            insert_task = asyncio.create_task(insert_prices_loop())
            watchdog_task = asyncio.create_task(watchdog())
            
            print("✅ HYBRID WEBSOCKET tasks started:")
            print("  📡 fetch_symbols_loop - Gets active symbols")
            print("  🔗 maintain_connection - TwelveData WebSocket (BTC/USD, others)")
            print("  🔗 maintain_binance_connection - Binance WebSocket (ETH/USD)")
            print(f"  💰 insert_prices_loop - IMMEDIATE insertion on every price update")
            print("  🐕 watchdog - Task monitoring and restart")
            
            # Wait for any task to complete (which shouldn't happen unless shutdown)
            done, pending = await asyncio.wait(
                [fetch_task, connection_task, binance_connection_task, insert_task, watchdog_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_requested:
                break
            
            # If we get here, a core task died unexpectedly
            restart_count += 1
            for task in done:
                exception = task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="startup",
                        severity="fatal",
                        message=f"Core task crashed: {str(exception)}",
                        function_name="main",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
            
            print(f"💥 Core task died, restarting entire bot (attempt {restart_count}/{max_restarts})...")
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            
            # Exponential backoff before restart
            if restart_count < max_restarts:
                await exponential_backoff(restart_count, base_delay=5, max_delay=120)
            
        except Exception as e:
            restart_count += 1
            await log_error_with_deduplication(
                error_type="startup",
                severity="fatal",
                message=f"Main function crashed: {str(e)}",
                function_name="main",
                stack_trace=traceback.format_exc()
            )
            
            if restart_count < max_restarts:
                print(f"💀 Main crashed, restarting in 10s... (attempt {restart_count}/{max_restarts})")
                await asyncio.sleep(10)
            else:
                print("💀 Max restarts reached, giving up!")
                raise

if __name__ == '__main__':
    try:
        print("🚀 HYBRID WEBSOCKET TRADING BOT STARTING...")
        print("📡 Data Sources:")
        print(f"   TwelveData: {TD_WEBSOCKET_URL}")
        print(f"   Binance: {BINANCE_WEBSOCKET_URL}")
        print("💰 Mode: IMMEDIATE insertion - every price update inserted instantly")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🔴 Interrupted by user")
    except Exception as e:
        print(f"☠️ Fatal error: {e}")
        sys.exit(1) "
