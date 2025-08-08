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
import logging

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trading_bot.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

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
restart_count = 0

# Task monitoring
fetch_task = None
connection_task = None
watchdog_task = None
insert_task = None
health_task = None

# 24/7 Configuration
MAX_RESTART_COUNT = 999999  # ✅ Essentially infinite restarts
RESTART_DELAY_BASE = 5      # Base delay between restarts
RESTART_DELAY_MAX = 300     # Max delay (5 minutes)
HEALTH_CHECK_INTERVAL = 30  # Health check every 30 seconds
PRICE_TIMEOUT_THRESHOLD = 120  # Restart if no prices for 2 minutes
CONNECTION_RETRY_LIMIT = 50    # Max connection retries before full restart

async def exponential_backoff(attempt, base_delay=1, max_delay=60):
    """Calculate exponential backoff with jitter for 24/7 resilience"""
    delay = min(base_delay * (2 ** min(attempt, 8)), max_delay)
    jitter = random.uniform(0.1, 0.3) * delay
    final_delay = delay + jitter
    logger.info(f"⏳ Backing off for {final_delay:.1f}s (attempt #{attempt + 1})")
    await asyncio.sleep(final_delay)
    return final_delay

async def log_error_with_deduplication(error_type, severity, message, function_name=None, symbol=None, 
                                     connection_state=None, stack_trace=None, request_data=None, 
                                     response_data=None, active_symbols_count=None):
    """Enhanced error logging with deduplication for 24/7 operation"""
    try:
        # Log to console/file immediately
        logger.error(f"[{error_type}] {message}")
        
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
                "active_symbols_count": active_symbols_count,
                "restart_count": restart_count
            }).eq("id", error_record["id"]).execute()
            logger.info(f"🔄 Updated existing error #{error_record['id']}")
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
                "restart_count": restart_count,
                "first_occurrence": datetime.now(timezone.utc).isoformat(),
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            result = supabase.table("bot_errors").insert(error_data).execute()
            logger.info(f"🆕 Logged new error #{result.data[0]['id']}")
            
    except Exception as e:
        # Fallback to console if error logging fails
        logger.error(f"❌ Failed to log error to database: {e}")
        logger.error(f"Original error: {message}")

async def clear_live_prices():
    """Clear all rows from live_prices table"""
    try:
        existing_data = supabase.table("live_prices").select("symbol").execute()
        row_count = len(existing_data.data) if existing_data.data else 0
        
        if row_count > 0:
            result = supabase.table("live_prices").delete().neq("symbol", "").execute()
            logger.info(f"🗑️ Cleared {row_count} rows from live_prices table")
        else:
            logger.info("🗑️ live_prices table was already empty")
        
        await log_error_with_deduplication(
            error_type="symbol_fetch",
            severity="warning", 
            message=f"No symbols returned from fetch-symbols, cleared {row_count} rows from live_prices table",
            function_name="clear_live_prices",
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
    """Enhanced fetch loop with 24/7 resilience"""
    global symbols, symbol_map, shutdown_requested
    consecutive_failures = 0
    
    while not shutdown_requested:
        session = None
        try:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
                connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
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
                    
                    # ✅ Escalate to full restart if too many failures
                    if consecutive_failures >= 10:
                        logger.error("🚨 Too many fetch failures, triggering full restart")
                        raise Exception("Too many consecutive fetch failures")
                    
                    await exponential_backoff(consecutive_failures, base_delay=5)
                    continue
                
                data = await response.json()
                fetched_symbols = data.get("symbols", [])
                
                if not fetched_symbols:
                    logger.warning("⚠️ No symbols returned from fetch-symbols endpoint")
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
                        continue

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
                logger.info(f"🔄 Refreshed symbols: {len(symbols)} symbols loaded")
                
                # Normal interval between successful fetches
                await asyncio.sleep(60)
                
        except asyncio.CancelledError:
            logger.info("🛑 fetch_symbols_loop cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="symbol_fetch",
                severity="error",
                message=f"Error in fetch_symbols_loop: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            
            # ✅ Trigger full restart for critical failures
            if consecutive_failures >= 15:
                logger.error("🚨 Critical fetch failure, triggering system restart")
                raise Exception("Critical fetch failure")
                
            await exponential_backoff(consecutive_failures, base_delay=5)
        finally:
            if session and not session.closed:
                await session.close()

async def receive_price(data):
    global price_buffer, last_price_time

    symbol = data.get("symbol")
    price = data.get("price")
    timestamp = data.get("timestamp")

    ts = datetime.utcfromtimestamp(timestamp).isoformat() + "Z"
    status = "pulled" if price is not None else "failed"

    if price is None:
        price = 0
        logger.warning(f"❌ No price for {symbol}, inserting 0")
    else:
        logger.info(f"✅ Price pulled for {symbol}: {price}")
        last_price_time = datetime.now(timezone.utc)

    matched = symbol_map.get(symbol)
    if not matched:
        logger.warning(f"⚠️ No match in symbol_map for {symbol}")
        return

    price_buffer[symbol] = {
        "symbol": symbol,
        "standardized_symbol": matched.get("standardized_symbol", symbol),
        "price": price,
        "updated_at": ts,
        "asset_name": matched.get("asset_name", symbol),
        "market_type": matched.get("market_type", "unknown"),
        "status": status,
        "search_symbol": matched.get("standardized_symbol", symbol),
        "exchange": matched.get("exchange", None)
    }

async def insert_prices_loop():
    """Enhanced insert loop with 24/7 resilience"""
    global price_buffer, last_insert_time, shutdown_requested

    while not shutdown_requested:
        try:
            now = datetime.now(timezone.utc)
            elapsed = now - last_insert_time

            if elapsed >= BATCH_INTERVAL and price_buffer:
                batch = list(price_buffer.values())
                price_buffer.clear()
                last_insert_time = now
                logger.info(f"📤 Inserting batch of {len(batch)} prices")
                await insert_price_batch(batch)
            
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("🛑 insert_prices_loop cancelled")
            break
        except Exception as e:
            await log_error_with_deduplication(
                error_type="insert_loop",
                severity="error",
                message=f"Error in insert_prices_loop: {str(e)}",
                function_name="insert_prices_loop",
                stack_trace=traceback.format_exc()
            )
            await asyncio.sleep(10)  # Brief pause on error

async def insert_price_batch(batch):
    """Enhanced batch insert with retries"""
    for row in batch:
        symbol = row.get("symbol")
        price = row.get("price")
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as session:
                    async with session.post(
                        f"{SUPABASE_URL}/functions/v1/insert-live-price",
                        headers={
                            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                            "Content-Type": "application/json"
                        },
                        json={"prices": [row]}
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"✅ [Edge] Price inserted for {symbol}: {price}")
                            break
                        else:
                            error_text = await resp.text()
                            raise Exception(f"HTTP {resp.status}: {error_text}")
                            
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    await log_error_with_deduplication(
                        error_type="edge_insert",
                        severity="error",
                        message=f"Edge insert-live-price failed after {max_retries} retries: {str(e)}",
                        function_name="insert_price_batch",
                        symbol=symbol,
                        request_data=row,
                        stack_trace=traceback.format_exc()
                    )
                else:
                    await asyncio.sleep(retry_count * 2)  # Exponential backoff

async def send_heartbeat(ws):
    """Enhanced heartbeat with better error handling"""
    while not shutdown_requested:
        try:
            await asyncio.sleep(10)
            if not ws.closed:
                await ws.send_str(json.dumps({"action": "heartbeat"}))
                logger.debug("💓 Heartbeat sent")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"💔 Heartbeat failed: {str(e)}")
            raise e  # Let the connection handler deal with it

async def maintain_connection():
    """Enhanced connection with unlimited retries for 24/7 operation"""
    global previous_symbols, shutdown_requested
    consecutive_failures = 0
    heartbeat_task = None

    while not shutdown_requested:
        session = None
        try:
            if not symbols:
                logger.warning("⚠️ No symbols to subscribe, waiting...")
                await asyncio.sleep(5)
                continue

            resubscribe = symbols != previous_symbols
            previous_symbols = set(symbols)

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15),
                connector=aiohttp.TCPConnector(
                    limit=10, 
                    ttl_dns_cache=300,
                    use_dns_cache=True,
                    keepalive_timeout=30
                )
            )

            logger.info(f"🔗 Connecting to WebSocket... (failures: {consecutive_failures})")
            async with session.ws_connect(
                TD_WEBSOCKET_URL,
                heartbeat=30,
                compress=0  # Disable compression for stability
            ) as ws:
                consecutive_failures = 0  # Reset on successful connection
                logger.info("🔗 WebSocket connected successfully")
                
                if resubscribe:
                    subscribe_payload = json.dumps({
                        "action": "subscribe",
                        "params": {
                            "symbols": ",".join(symbols),
                            "apikey": TWELVE_DATA_API_KEY
                        }
                    })
                    await ws.send_str(subscribe_payload)
                    logger.info(f"📤 Subscribed to: {len(symbols)} symbols")
                else:
                    logger.info("✅ Symbol list unchanged, skipping re-subscribe")

                heartbeat_task = asyncio.create_task(send_heartbeat(ws))

                async for msg in ws:
                    if shutdown_requested:
                        break
                        
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            if data.get("event") == "price":
                                await receive_price(data)
                            elif data.get("event") == "status":
                                logger.info(f"⚙️ Status event: {data}")
                            else:
                                logger.debug(f"🪵 Other event: {data}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"⚠️ JSON decode error: {str(e)}")
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
                        logger.error(f"🔌 WebSocket error: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.info("🔌 WebSocket closed by server")
                        break
                        
        except asyncio.CancelledError:
            logger.info("🛑 maintain_connection cancelled")
            break
        except Exception as e:
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
            
            # ✅ Trigger full restart if too many connection failures
            if consecutive_failures >= CONNECTION_RETRY_LIMIT:
                logger.error("🚨 Too many connection failures, triggering full restart")
                raise Exception("Too many consecutive connection failures")
                
        finally:
            # Clean up tasks
            if heartbeat_task and not heartbeat_task.done():
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            # Clean up session
            if session and not session.closed:
                await session.close()
            
            # Exponential backoff before reconnecting
            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)

async def health_monitor():
    """24/7 Health monitoring with automatic recovery"""
    global last_price_time, shutdown_requested, fetch_task, connection_task, insert_task
    
    while not shutdown_requested:
        try:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            
            now = datetime.now(timezone.utc)
            time_since_price = (now - last_price_time).total_seconds()
            
            # ✅ Log health status every 10 minutes
            if now.minute % 10 == 0 and now.second < 30:
                logger.info(f"💚 Health check: {len(symbols)} symbols, last price {int(time_since_price)}s ago")
            
            # ✅ Check for stalled price updates
            if time_since_price > PRICE_TIMEOUT_THRESHOLD:
                await log_error_with_deduplication(
                    error_type="health_check",
                    severity="critical",
                    message=f"No price updates for {int(time_since_price)} seconds - triggering restart",
                    function_name="health_monitor",
                    active_symbols_count=len(symbols)
                )
                logger.error(f"💔 Health check failed - no prices for {int(time_since_price)}s")
                raise Exception("Price timeout exceeded")
            
            # ✅ Check task health and restart individual tasks if needed
            tasks_to_check = [
                ("fetch_task", fetch_task, fetch_symbols_loop),
                ("connection_task", connection_task, maintain_connection),
                ("insert_task", insert_task, insert_prices_loop)
            ]
            
            for task_name, task, task_func in tasks_to_check:
                if task and task.done():
                    exception = task.exception()
                    if exception:
                        logger.error(f"🚨 {task_name} died: {str(exception)}")
                        await log_error_with_deduplication(
                            error_type="task_failure",
                            severity="critical",
                            message=f"{task_name} died: {str(exception)}",
                            function_name="health_monitor",
                            stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                        )
                    
                    # Restart the individual task
                    logger.info(f"🔄 Restarting {task_name}...")
                    if task_name == "fetch_task":
                        fetch_task = asyncio.create_task(task_func())
                    elif task_name == "connection_task":
                        connection_task = asyncio.create_task(task_func())
                    elif task_name == "insert_task":
                        insert_task = asyncio.create_task(task_func())
            
        except asyncio.CancelledError:
            logger.info("🛑 Health monitor cancelled")
            break
        except Exception as e:
            # Health monitor failure triggers full restart
            await log_error_with_deduplication(
                error_type="health_monitor",
                severity="fatal",
                message=f"Health monitor failure: {str(e)}",
                function_name="health_monitor",
                stack_trace=traceback.format_exc()
            )
            logger.error("🚨 Health monitor failure, triggering full restart")
            raise

def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

async def graceful_shutdown():
    """Cancel all tasks gracefully"""
    global fetch_task, connection_task, watchdog_task, insert_task, health_task
    logger.info("🛑 Shutting down gracefully...")
    
    tasks_to_cancel = []
    for task in [fetch_task, connection_task, watchdog_task, insert_task, health_task]:
        if task and not task.done():
            tasks_to_cancel.append(task)
    
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
    logger.info("✅ All tasks cancelled, goodbye!")

async def main():
    """Enhanced main with unlimited restarts for 24/7 operation"""
    global fetch_task, connection_task, watchdog_task, insert_task, health_task, shutdown_requested, restart_count
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    logger.info("🚀 Starting 24/7 Enhanced Trading Bot...")
    
    while not shutdown_requested and restart_count < MAX_RESTART_COUNT:
        try:
            restart_count += 1
            logger.info(f"🚀 Starting bot instance #{restart_count}...")
            
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
            insert_task = asyncio.create_task(insert_prices_loop())
            health_task = asyncio.create_task(health_monitor())
            
            # Wait for any task to complete (should not happen unless error/shutdown)
            done, pending = await asyncio.wait(
                [fetch_task, connection_task, insert_task, health_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_requested:
                logger.info("🛑 Shutdown requested, exiting main loop")
                break
            
            # If we get here, a core task died unexpectedly
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
            
            logger.warning(f"💥 Core task died, restarting entire bot (restart #{restart_count + 1})...")
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            
            # ✅ Dynamic restart delay - longer delays for frequent restarts
            restart_delay = min(RESTART_DELAY_BASE * (restart_count // 10 + 1), RESTART_DELAY_MAX)
            logger.info(f"⏳ Waiting {restart_delay}s before restart...")
            await asyncio.sleep(restart_delay)
            
        except KeyboardInterrupt:
            logger.info("🔴 Keyboard interrupt received")
            shutdown_requested = True
            break
        except Exception as e:
            await log_error_with_deduplication(
                error_type="main_crash",
                severity="fatal",
                message=f"Main function crashed: {str(e)}",
                function_name="main",
                stack_trace=traceback.format_exc()
            )
            
            logger.error(f"💀 Main crashed: {str(e)}")
            restart_delay = min(RESTART_DELAY_BASE * (restart_count // 5 + 1), RESTART_DELAY_MAX)
            logger.info(f"⏳ Restarting in {restart_delay}s... (restart #{restart_count + 1})")
            await asyncio.sleep(restart_delay)

    # Final cleanup
    if not shutdown_requested:
        logger.error(f"🔴 Maximum restart count ({MAX_RESTART_COUNT}) reached, shutting down")
    
    await graceful_shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🔴 Interrupted by user")
    except Exception as e:
        print(f"☠️ Fatal error: {e}")
        sys.exit(1)

