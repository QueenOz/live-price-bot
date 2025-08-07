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

price_buffer = {}
last_insert_time = datetime.now(timezone.utc)
BATCH_INTERVAL = timedelta(seconds=15)



# Load env vars
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TD_WEBSOCKET_URL = os.getenv("TD_WEBSOCKET_URL") + f"?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

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
                timeout=aiohttp.ClientTimeout(total=30, connect=10)
            )
            
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
                        continue  # skip empty

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
                print(f"🔄 Refreshed symbols: {len(symbols)} symbols loaded")
                
                # Normal interval between successful fetches
                await asyncio.sleep(60)
                
        except asyncio.CancelledError:
            print("🛑 fetch_symbols_loop cancelled")
            break
        except aiohttp.ClientError as e:
            consecutive_failures += 1
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

async def insert_price(data):
    async def receive_price(data):
    global price_buffer, last_price_time
    symbol = data.get("symbol")
    price = data.get("price")
    timestamp = data.get("timestamp")

    ts = datetime.utcfromtimestamp(timestamp).isoformat() + "Z"
    status = "pulled" if price is not None else "failed"

    if price is None:
        price = 0
        print(f"❌ No price for {symbol}, inserting 0")
    else:
        print(f"✅ Price pulled for {symbol}: {price}")
        last_price_time = datetime.now(timezone.utc)

    matched = symbol_map.get(symbol)
    if not matched:
        print(f"⚠️ No match in symbol_map for {symbol}")
        return

    price_buffer[symbol] = {
        "symbol": symbol,
        "standardized_symbol": matched.get("standardized_symbol", symbol),
        "price": price,
        "updated_at": ts,
        "name": matched.get("asset_name", symbol),
        "market_type": matched.get("market_type", "unknown"),
        "status": status,
        "search_symbol": matched.get("standardized_symbol", symbol),
        "exchange": matched.get("exchange", None)
    }


        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{SUPABASE_URL}/functions/v1/insert-live-price",
                    headers={
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={"prices": [row]}
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        await log_error_with_deduplication(
                            error_type="edge_insert",
                            severity="error",
                            message=f"Edge insert-live-price failed: HTTP {resp.status}",
                            function_name="insert_price",
                            symbol=symbol,
                            response_data={"error": error_text},
                            request_data=row
                        )
                    else:
                        print(f"✅ [Edge] Price inserted for {symbol}: {price}")
            except Exception as e:
                await log_error_with_deduplication(
                    error_type="edge_insert",
                    severity="error",
                    message=f"Edge insert-live-price exception: {str(e)}",
                    function_name="insert_price",
                    symbol=symbol,
                    request_data=row,
                    stack_trace=traceback.format_exc()
                )

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
            if delta > 30:  # Increased timeout to 30 seconds
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
    """Enhanced connection with exponential backoff and never-give-up attitude"""
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

            resubscribe = symbols != previous_symbols
            previous_symbols = set(symbols)

            # Create fresh session for each connection attempt
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )

            print(f"🔗 Connecting to WebSocket... (failures: {consecutive_failures})")
            async with session.ws_connect(
                TD_WEBSOCKET_URL,
                heartbeat=30
            ) as ws:
                consecutive_failures = 0  # Reset on successful connection
                
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
                    if shutdown_requested:
                        break
                        
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
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("🔌 WebSocket closed by server")
                        break
                        
        except asyncio.CancelledError:
            print("🛑 maintain_connection cancelled")
            break
        except aiohttp.ClientError as e:
            consecutive_failures += 1
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
            consecutive_failures += 1
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

async def watchdog():
    """Monitor system health and restart tasks if they die"""
    global fetch_task, connection_task, shutdown_requested
    print("🐕 Starting watchdog...")
    
    while not shutdown_requested:
        try:
            await asyncio.sleep(15)  # Check every 15 seconds
            
            # Check if fetch task is still running
            if fetch_task and fetch_task.done():
                exception = fetch_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="task_failure",
                        severity="critical",
                        message=f"fetch_symbols_loop task died: {str(exception)}",
                        function_name="watchdog",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
                
                print("🚨 fetch_symbols_loop task died, restarting...")
                fetch_task = asyncio.create_task(fetch_symbols_loop())
            
            # Check if connection task is still running
            if connection_task and connection_task.done():
                exception = connection_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="task_failure",
                        severity="critical",
                        message=f"maintain_connection task died: {str(exception)}",
                        function_name="watchdog",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
                
                print("🚨 maintain_connection task died, restarting...")
                connection_task = asyncio.create_task(maintain_connection())
            
            # Check system health
            now = datetime.now(timezone.utc)
            time_since_price = (now - last_price_time).total_seconds()
            
            if time_since_price > 300:  # 5 minutes without price
                await log_error_with_deduplication(
                    error_type="health_check",
                    severity="critical",
                    message=f"No price updates for {int(time_since_price)} seconds - system may be stalled",
                    function_name="watchdog",
                    active_symbols_count=len(symbols)
                )
                print(f"💔 Health check failed - no prices for {int(time_since_price)}s")
            else:
                print("💚 Health check passed")
            
        except asyncio.CancelledError:
            print("🛑 Watchdog cancelled")
            break
        except Exception as e:
            await log_error_with_deduplication(
                error_type="watchdog",
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
    global fetch_task, connection_task, watchdog_task
    print("🛑 Shutting down gracefully...")
    
    tasks_to_cancel = []
    if fetch_task and not fetch_task.done():
        tasks_to_cancel.append(fetch_task)
    if connection_task and not connection_task.done():
        tasks_to_cancel.append(connection_task)
    if watchdog_task and not watchdog_task.done():
        tasks_to_cancel.append(watchdog_task)
    
    for task in tasks_to_cancel:
        task.cancel()
    
    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    
    await log_error_with_deduplication(
        error_type="shutdown",
        severity="info",
        message="Trading bot shutdown gracefully",
        function_name="graceful_shutdown"
    )
    print("✅ All tasks cancelled, goodbye!")

async def main():
    await asyncio.gather(
        websocket_price_handler(),
        fetch_symbols_loop(),
        insert_prices_loop()
    )

async def main():
    """Enhanced main with task supervision and auto-restart"""
    global fetch_task, connection_task, watchdog_task, shutdown_requested
    restart_count = 0
    max_restarts = 10
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    while restart_count < max_restarts and not shutdown_requested:
        try:
            print(f"🚀 Starting enhanced trading bot (restart #{restart_count})...")
            
            await log_error_with_deduplication(
                error_type="startup",
                severity="info",
                message=f"Trading bot started (restart #{restart_count})",
                function_name="main",
                active_symbols_count=0
            )
            
            # Start all core tasks
            fetch_task = asyncio.create_task(fetch_symbols_loop())
            connection_task = asyncio.create_task(maintain_connection())
            watchdog_task = asyncio.create_task(watchdog())
            
            # Wait for any task to complete (which shouldn't happen unless shutdown)
            done, pending = await asyncio.wait(
                [fetch_task, connection_task, watchdog_task],
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
                        error_type="task_crash",
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
                error_type="main_crash",
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
    
    # Graceful shutdown
    await graceful_shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"💀 Fatal error: {e}")
        sys.exit(1)
