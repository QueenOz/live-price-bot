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

# Load env vars
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TD_WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

# Binance WebSocket for crypto pairs
BINANCE_CRYPTO_STREAMS = [
    "ethusdt@ticker",
    "btcusdt@ticker", 
    "adausdt@ticker",
    "solusdt@ticker",
    "xrpusdt@ticker",
    "dogeusdt@ticker",
    "avaxusdt@ticker",
    "maticusdt@ticker"
]
BINANCE_WEBSOCKET_URL = f"wss://stream.binance.com:9443/stream?streams={'/'.join(BINANCE_CRYPTO_STREAMS)}"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Shared state
symbols = set()
symbol_map = {}
unique_symbols = set()  # Deduplicated symbols for actual subscription
successful_subscriptions = set()  # Track which symbols are actually working
last_price_time = datetime.now(timezone.utc)
shutdown_requested = False

# Task monitoring
fetch_task = None
twelvedata_task = None
binance_task = None
watchdog_task = None

# Symbol prioritization for TwelveData (most important first)
PRIORITY_SYMBOLS = [
    "BTC/USD", "AAPL", "TSLA", "NVDA", "MSFT", "AMZN", 
    "GOOGL", "EUR/USD", "GBP/USD", "USD/JPY"
]

# Binance to database symbol mapping
BINANCE_SYMBOL_MAP = {
    "ETHUSDT": "ETH/USD",
    "BTCUSDT": "BTC/USD", 
    "ADAUSDT": "ADA/USD",
    "SOLUSDT": "SOL/USD",
    "XRPUSDT": "XRP/USD",
    "DOGEUSDT": "DOGE/USD",
    "AVAXUSDT": "AVAX/USD",
    "MATICUSDT": "MATIC/USD"
}

async def exponential_backoff(attempt, base_delay=1, max_delay=60):
    """Calculate exponential backoff with jitter"""
    delay = min(base_delay * (2 ** min(attempt, 6)), max_delay)
    jitter = random.uniform(0.1, 0.3) * delay
    final_delay = delay + jitter
    print(f"Backing off for {final_delay:.1f}s (attempt #{attempt + 1})")
    await asyncio.sleep(final_delay)
    return final_delay

async def log_error_with_deduplication(error_type, severity, message, function_name=None, symbol=None, 
                                     connection_state=None, stack_trace=None, request_data=None, 
                                     response_data=None, active_symbols_count=None):
    """Log error with automatic deduplication"""
    try:
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
            error_record = existing.data[0]
            supabase.table("bot_errors").update({
                "occurrence_count": error_record["occurrence_count"] + 1,
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "connection_state": connection_state,
                "active_symbols_count": active_symbols_count
            }).eq("id", error_record["id"]).execute()
            print(f"Updated existing error #{error_record['id']}: {message}")
        else:
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
            print(f"Logged new error #{result.data[0]['id']}: {message}")
            
    except Exception as e:
        print(f"Failed to log error to database: {e}")
        print(f"Original error: {message}")

async def clear_live_prices():
    """Clear all rows from live_prices table"""
    try:
        existing_data = supabase.table("live_prices").select("symbol").execute()
        row_count = len(existing_data.data) if existing_data.data else 0
        
        if row_count > 0:
            result = supabase.table("live_prices").delete().neq("symbol", "").execute()
            print(f"Cleared {row_count} rows from live_prices table")
        else:
            print("live_prices table was already empty")
        
        await log_error_with_deduplication(
            error_type="symbol_fetch",
            severity="warning", 
            message=f"No symbols returned from fetch-symbols, cleared {row_count} rows",
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
    """Fetch symbols from game_assets_view and deduplicate"""
    global symbols, symbol_map, unique_symbols, shutdown_requested
    consecutive_failures = 0
    
    while not shutdown_requested:
        session = None
        try:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=20)
            )
            
            print(f"Fetching symbols from {FETCH_SYMBOLS_ENDPOINT}...")
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
                    print(f"fetch-symbols API error: HTTP {response.status} - {error_text}")
                    await log_error_with_deduplication(
                        error_type="api",
                        severity="error",
                        message=f"fetch-symbols API error: HTTP {response.status}",
                        function_name="fetch_symbols_loop",
                        response_data={"error": error_text},
                        active_symbols_count=len(symbols)
                    )
                    await exponential_backoff(consecutive_failures, base_delay=5)
                    continue
                
                data = await response.json()
                fetched_symbols = data.get("symbols", [])
                
                print(f"Found {len(fetched_symbols)} assets from game_assets_view")
                
                if not fetched_symbols:
                    print("No symbols returned from fetch-symbols endpoint")
                    await clear_live_prices()
                    symbols = set()
                    symbol_map = {}
                    unique_symbols = set()
                    consecutive_failures += 1
                    await exponential_backoff(consecutive_failures, base_delay=10)
                    continue
                
                # Process and deduplicate symbols
                new_symbols = set()
                new_map = {}
                seen_symbols = set()
                
                for row in fetched_symbols:
                    symbol_str = row.get("symbol")
                    if not symbol_str or symbol_str in seen_symbols:
                        if symbol_str:
                            print(f"Duplicate symbol found, skipping: {symbol_str}")
                        continue
                    
                    seen_symbols.add(symbol_str)
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
                
                # Create prioritized unique symbol list for subscriptions
                unique_symbols = set()
                
                # Add priority symbols first
                for priority in PRIORITY_SYMBOLS:
                    if priority in symbols:
                        unique_symbols.add(priority)
                
                # Add remaining symbols up to reasonable limit
                for symbol in symbols:
                    if symbol not in unique_symbols:
                        unique_symbols.add(symbol)
                
                consecutive_failures = 0
                print(f"SUCCESS: Loaded {len(symbols)} total symbols, {len(unique_symbols)} unique")
                print(f"Unique symbols: {sorted(list(unique_symbols))}")
                
                await asyncio.sleep(60)  # Check every minute
                
        except asyncio.CancelledError:
            print("fetch_symbols_loop cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"Error in fetch_symbols_loop: {str(e)}")
            await log_error_with_deduplication(
                error_type="symbol_fetch",
                severity="error",
                message=f"fetch_symbols_loop error: {str(e)}",
                function_name="fetch_symbols_loop",
                stack_trace=traceback.format_exc(),
                active_symbols_count=len(symbols)
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        finally:
            if session and not session.closed:
                await session.close()

async def receive_twelvedata_price(data):
    """Handle TwelveData price events"""
    global last_price_time

    symbol = data.get("symbol")
    price = data.get("price")
    timestamp = data.get("timestamp")

    print(f"TwelveData: {symbol}: ${price}")

    # Fix timestamp
    try:
        ws_time = datetime.fromtimestamp(timestamp, timezone.utc)
        current_time = datetime.now(timezone.utc)
        time_diff = abs((ws_time - current_time).total_seconds())
        
        if time_diff > 3600:
            ts = current_time.isoformat()
        else:
            ts = ws_time.isoformat()
    except (ValueError, OSError):
        ts = datetime.now(timezone.utc).isoformat()
        
    status = "pulled" if price is not None else "failed"
    if price is None:
        price = 0

    last_price_time = datetime.now(timezone.utc)
    successful_subscriptions.add(symbol)

    matched = symbol_map.get(symbol)
    if not matched:
        print(f"No match in symbol_map for '{symbol}'")
        return

    price_data = {
        "symbol": symbol,
        "standardized_symbol": matched.get("standardized_symbol", symbol),
        "price": price,
        "updated_at": ts,
        "asset_name": matched.get("asset_name", symbol),
        "market_type": matched.get("market_type", "unknown"),
        "status": status,
        "search_symbol": matched.get("standardized_symbol", symbol),
        "exchange": "TwelveData"
    }
    
    asyncio.create_task(insert_single_price_immediate(price_data))

async def receive_binance_price(data):
    """Handle Binance price events"""
    global last_price_time
    
    # Handle stream format: {"stream":"ethusdt@ticker","data":{...}}
    if "data" in data:
        ticker_data = data["data"]
        stream = data.get("stream", "")
        binance_symbol = stream.split("@")[0].upper()  # ethusdt -> ETHUSDT
    else:
        # Direct ticker format
        ticker_data = data
        binance_symbol = data.get("s")  # ETHUSDT
    
    price = ticker_data.get("c")  # Current price
    
    # Convert to database format
    db_symbol = BINANCE_SYMBOL_MAP.get(binance_symbol)
    if not db_symbol:
        print(f"Unknown Binance symbol: {binance_symbol}")
        return
    
    print(f"Binance: {binance_symbol} -> {db_symbol}: ${price}")
    
    # Skip if we don't have this symbol in our game assets
    matched = symbol_map.get(db_symbol)
    if not matched:
        return  # Symbol not in current games
    
    if price is None:
        price = 0
    else:
        last_price_time = datetime.now(timezone.utc)
    
    price_data = {
        "symbol": db_symbol,
        "standardized_symbol": matched.get("standardized_symbol", db_symbol.split("/")[0]),
        "price": float(price) if price else 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "asset_name": matched.get("asset_name", db_symbol),
        "market_type": matched.get("market_type", "crypto"),
        "status": "pulled",
        "search_symbol": matched.get("standardized_symbol", db_symbol.split("/")[0]),
        "exchange": "Binance"
    }
    
    asyncio.create_task(insert_single_price_immediate(price_data))

async def insert_single_price_immediate(price_data):
    """Insert single price immediately"""
    try:
        symbol = price_data["symbol"]
        price = price_data["price"]
        exchange = price_data.get("exchange", "Unknown")
        
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
        
        start_time = datetime.now()
        result = supabase.table("live_prices").upsert(
            row,
            on_conflict="symbol"
        ).execute()
        end_time = datetime.now()
        
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        if result.data:
            print(f"INSERTED ({exchange}) {symbol} in {duration_ms:.0f}ms @ ${price}")
        else:
            print(f"Insert failed for {symbol} from {exchange}")
            
    except Exception as e:
        await log_error_with_deduplication(
            error_type="database",
            severity="error",
            message=f"Insert failed for {price_data.get('symbol', 'unknown')}: {str(e)}",
            function_name="insert_single_price_immediate",
            stack_trace=traceback.format_exc()
        )

async def maintain_twelvedata_connection():
    """TwelveData WebSocket connection with 8-symbol limit"""
    global shutdown_requested
    consecutive_failures = 0
    heartbeat_task = None
    timeout_task = None
    previous_symbols = set()

    while not shutdown_requested:
        session = None
        try:
            if not unique_symbols:
                print("No symbols to subscribe to TwelveData")
                await asyncio.sleep(5)
                continue

            # Select up to 8 symbols for TwelveData (respecting plan limits)
            # Exclude crypto symbols that Binance handles
            binance_handled = set(BINANCE_SYMBOL_MAP.values())
            twelvedata_candidates = [s for s in unique_symbols if s not in binance_handled]
            
            # Prioritize and limit to 8
            subscription_symbols = []
            
            # Add priority symbols first
            for priority in PRIORITY_SYMBOLS:
                if priority in twelvedata_candidates and len(subscription_symbols) < 8:
                    subscription_symbols.append(priority)
            
            # Fill remaining slots
            for symbol in twelvedata_candidates:
                if symbol not in subscription_symbols and len(subscription_symbols) < 8:
                    subscription_symbols.append(symbol)

            if not subscription_symbols:
                print("No TwelveData symbols after filtering")
                await asyncio.sleep(10)
                continue

            resubscribe = set(subscription_symbols) != previous_symbols
            previous_symbols = set(subscription_symbols)

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )

            print(f"Connecting to TwelveData WebSocket (failures: {consecutive_failures})")
            async with session.ws_connect(TD_WEBSOCKET_URL, heartbeat=30) as ws:
                print("TwelveData WebSocket connected successfully!")
                consecutive_failures = 0
                
                if resubscribe:
                    print(f"TwelveData subscribing to {len(subscription_symbols)} symbols: {subscription_symbols}")
                    
                    subscribe_payload = json.dumps({
                        "action": "subscribe",
                        "params": {
                            "symbols": ",".join(subscription_symbols)
                        }
                    })
                    
                    await ws.send_str(subscribe_payload)
                    print("TwelveData subscription sent")

                # Start helper tasks
                async def send_heartbeat():
                    while not shutdown_requested:
                        try:
                            await asyncio.sleep(10)
                            await ws.send_str(json.dumps({"action": "heartbeat"}))
                        except Exception:
                            break

                async def check_timeout():
                    while not shutdown_requested:
                        try:
                            await asyncio.sleep(5)
                            now = datetime.now(timezone.utc)
                            delta = (now - last_price_time).total_seconds()
                            if delta > 30:
                                raise Exception("Price timeout")
                        except Exception:
                            break

                heartbeat_task = asyncio.create_task(send_heartbeat())
                timeout_task = asyncio.create_task(check_timeout())

                async for msg in ws:
                    if shutdown_requested:
                        break
                        
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            
                            if data.get("event") == "price":
                                await receive_twelvedata_price(data)
                            elif data.get("event") == "subscribe-status":
                                success_symbols = [s.get("symbol") for s in data.get("success", [])]
                                failed_symbols = [s.get("symbol") for s in data.get("fails", [])]
                                
                                if success_symbols:
                                    print(f"TwelveData subscription SUCCESS: {success_symbols}")
                                if failed_symbols:
                                    print(f"TwelveData subscription FAILED: {failed_symbols}")
                                    print("These symbols may not be available on your plan")
                            elif data.get("event") == "heartbeat":
                                print(f"TwelveData heartbeat: {data.get('status', 'ok')}")
                            else:
                                print(f"TwelveData other event: {data}")
                        except json.JSONDecodeError as e:
                            await log_error_with_deduplication(
                                error_type="parsing",
                                severity="warning",
                                message=f"TwelveData JSON parse error: {str(e)}",
                                function_name="maintain_twelvedata_connection"
                            )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"TwelveData WebSocket error: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("TwelveData WebSocket closed")
                        break
                        
        except asyncio.CancelledError:
            print("TwelveData connection cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"TwelveData connection error: {str(e)}",
                function_name="maintain_twelvedata_connection",
                stack_trace=traceback.format_exc()
            )
        finally:
            # Cleanup
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
            
            if session and not session.closed:
                await session.close()
            
            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)

async def maintain_binance_connection():
    """Binance WebSocket connection for crypto pairs"""
    global shutdown_requested
    consecutive_failures = 0
    
    while not shutdown_requested:
        session = None
        try:
            # Check if any Binance-handled symbols are in our game assets
            binance_symbols_needed = []
            for binance_sym, db_sym in BINANCE_SYMBOL_MAP.items():
                if db_sym in symbols:
                    binance_symbols_needed.append(binance_sym)
            
            if not binance_symbols_needed:
                print("No Binance crypto symbols needed")
                await asyncio.sleep(5)
                continue

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )
            
            print(f"Connecting to Binance WebSocket (failures: {consecutive_failures})")
            print(f"Binance symbols needed: {binance_symbols_needed}")
            
            async with session.ws_connect(BINANCE_WEBSOCKET_URL) as ws:
                print("Binance WebSocket connected!")
                consecutive_failures = 0
                
                async for msg in ws:
                    if shutdown_requested:
                        break
                        
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            await receive_binance_price(data)
                        except json.JSONDecodeError as e:
                            await log_error_with_deduplication(
                                error_type="parsing",
                                severity="warning",
                                message=f"Binance JSON parse error: {str(e)}",
                                function_name="maintain_binance_connection"
                            )
                        except Exception as e:
                            await log_error_with_deduplication(
                                error_type="websocket",
                                severity="error",
                                message=f"Binance message processing error: {str(e)}",
                                function_name="maintain_binance_connection",
                                stack_trace=traceback.format_exc()
                            )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"Binance WebSocket error: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("Binance WebSocket closed")
                        break
                        
        except asyncio.CancelledError:
            print("Binance connection cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            await log_error_with_deduplication(
                error_type="connection",
                severity="error",
                message=f"Binance connection error: {str(e)}",
                function_name="maintain_binance_connection",
                stack_trace=traceback.format_exc()
            )
        finally:
            if session and not session.closed:
                await session.close()
            
            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)

async def watchdog():
    """Monitor system health and restart tasks"""
    global fetch_task, twelvedata_task, binance_task, shutdown_requested
    print("Starting watchdog...")
    
    while not shutdown_requested:
        try:
            await asyncio.sleep(10)
            
            # Check fetch task
            if fetch_task and fetch_task.done():
                exception = fetch_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="startup",
                        severity="critical",
                        message=f"fetch_symbols_loop died: {str(exception)}",
                        function_name="watchdog"
                    )
                
                print("fetch_symbols_loop died, restarting...")
                fetch_task = asyncio.create_task(fetch_symbols_loop())
            
            # Check TwelveData task
            if twelvedata_task and twelvedata_task.done():
                exception = twelvedata_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="connection",
                        severity="critical",
                        message=f"TwelveData connection died: {str(exception)}",
                        function_name="watchdog"
                    )
                
                print("TwelveData connection died, restarting...")
                twelvedata_task = asyncio.create_task(maintain_twelvedata_connection())
            
            # Check Binance task
            if binance_task and binance_task.done():
                exception = binance_task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="connection",
                        severity="critical",
                        message=f"Binance connection died: {str(exception)}",
                        function_name="watchdog"
                    )
                
                print("Binance connection died, restarting...")
                binance_task = asyncio.create_task(maintain_binance_connection())
            
            # Health check
            now = datetime.now(timezone.utc)
            time_since_price = (now - last_price_time).total_seconds()
            
            if time_since_price > 60:
                await log_error_with_deduplication(
                    error_type="timeout",
                    severity="critical",
                    message=f"No price updates for {int(time_since_price)} seconds",
                    function_name="watchdog",
                    active_symbols_count=len(symbols)
                )
                print(f"Health check failed - no prices for {int(time_since_price)}s")
            else:
                working_symbols = len(successful_subscriptions)
                print(f"System OK: {working_symbols} symbols active, last price {int(time_since_price)}s ago")
            
        except asyncio.CancelledError:
            print("Watchdog cancelled")
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
    """Handle shutdown signals"""
    global shutdown_requested
    print(f"\nReceived signal {signum}, shutting down...")
    shutdown_requested = True

async def graceful_shutdown():
    """Cancel all tasks gracefully"""
    global fetch_task, twelvedata_task, binance_task, watchdog_task
    print("Shutting down gracefully...")
    
    tasks_to_cancel = []
    if fetch_task and not fetch_task.done():
        tasks_to_cancel.append(fetch_task)
    if twelvedata_task and not twelvedata_task.done():
        tasks_to_cancel.append(twelvedata_task)
    if binance_task and not binance_task.done():
        tasks_to_cancel.append(binance_task)
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
    print("All tasks cancelled, goodbye!")

async def main():
    """Main function with hybrid WebSocket connections"""
    global fetch_task, twelvedata_task, binance_task, watchdog_task, shutdown_requested
    restart_count = 0
    max_restarts = 999999
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    while restart_count < max_restarts and not shutdown_requested:
        try:
            print("HYBRID WEBSOCKET MODE: TwelveData + Binance")
            print("- TwelveData: Max 8 symbols (stocks, forex)")
            print("- Binance: Crypto pairs (unlimited)")
            print("- Deduplication: Only unique symbols from game assets")
            
            await log_error_with_deduplication(
                error_type="startup",
                severity="info",
                message=f"Hybrid trading bot started (restart #{restart_count})",
                function_name="main",
                active_symbols_count=0
            )
            
            # Start all tasks
            fetch_task = asyncio.create_task(fetch_symbols_loop())
            twelvedata_task = asyncio.create_task(maintain_twelvedata_connection())
            binance_task = asyncio.create_task(maintain_binance_connection())
            watchdog_task = asyncio.create_task(watchdog())
            
            print("Tasks started:")
            print("  - fetch_symbols_loop: Gets unique symbols from game_assets_view")
            print("  - maintain_twelvedata_connection: Up to 8 non-crypto symbols")
            print("  - maintain_binance_connection: All crypto symbols")
            print("  - watchdog: Task monitoring and restart")
            
            # Wait for any task to complete
            done, pending = await asyncio.wait(
                [fetch_task, twelvedata_task, binance_task, watchdog_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_requested:
                break
            
            restart_count += 1
            for task in done:
                exception = task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="startup",
                        severity="fatal",
                        message=f"Core task crashed: {str(exception)}",
                        function_name="main"
                    )
            
            print(f"Core task died, restarting (attempt {restart_count}/{max_restarts})")
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            
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
                print(f"Main crashed, restarting in 10s (attempt {restart_count}/{max_restarts})")
                await asyncio.sleep(10)
            else:
                print("Max restarts reached!")
                raise

    # Graceful shutdown
    if shutdown_requested:
        await graceful_shutdown()

if __name__ == '__main__':
    try:
        print("HYBRID WEBSOCKET TRADING BOT STARTING...")
        print(f"TwelveData: {TD_WEBSOCKET_URL}")
        print(f"Binance: {BINANCE_WEBSOCKET_URL}")
        print("Mode: Immediate insertion with deduplication")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
