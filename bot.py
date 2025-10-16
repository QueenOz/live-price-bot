"""
PRODUCTION TRADING BOT v2.0
- Auto-restart with watchdog
- Heartbeat monitoring every 30s
- Deduplication (10 BTC games = 1 symbol)
- TwelveData: Max 8 unique symbols
- Binance: Unlimited crypto
- Immediate price insertion
- All USD-focused
"""
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

# ========================================
# CONFIGURATION
# ========================================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TD_WEBSOCKET_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
FETCH_SYMBOLS_ENDPOINT = f"{SUPABASE_URL}/functions/v1/fetch-symbols"

# Binance crypto streams (can add more as needed)
BINANCE_CRYPTO_STREAMS = [
    "btcusdt@ticker",
    "ethusdt@ticker",
    "adausdt@ticker",
    "solusdt@ticker",
    "xrpusdt@ticker",
    "dogeusdt@ticker",
    "avaxusdt@ticker",
    "maticusdt@ticker",
    "linkusdt@ticker",
    "dotusdt@ticker",
    "uniusdt@ticker",
    "ltcusdt@ticker",
    "bchusdt@ticker"
]
BINANCE_WEBSOCKET_URL = f"wss://stream.binance.com:443/stream?streams={'/'.join(BINANCE_CRYPTO_STREAMS)}"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ========================================
# GLOBAL STATE
# ========================================
symbols = set()  # All symbols from game_assets_view
symbol_map = {}  # Symbol details for mapping
unique_symbols = []  # Ordered list for prioritization
active_twelvedata_symbols = []  # Currently subscribed to TwelveData (max 8)
active_binance_symbols = set()  # Crypto symbols handled by Binance
last_price_time = datetime.now(timezone.utc)
shutdown_requested = False

# Task references
fetch_task = None
twelvedata_task = None
binance_task = None
watchdog_task = None
heartbeat_task = None

# ========================================
# SYMBOL PRIORITY & MAPPING
# ========================================
# Most important symbols first (for TwelveData 8-limit)
PRIORITY_SYMBOLS = [
    "BTC/USD", "AAPL", "TSLA", "NVDA", "MSFT", "AMZN",
    "GOOGL", "META", "EUR/USD", "GBP/USD", "USD/JPY"
]

# Binance ticker to database symbol mapping
BINANCE_SYMBOL_MAP = {
    "BTCUSDT": "BTC/USD",
    "ETHUSDT": "ETH/USD",
    "ADAUSDT": "ADA/USD",
    "SOLUSDT": "SOL/USD",
    "XRPUSDT": "XRP/USD",
    "DOGEUSDT": "DOGE/USD",
    "AVAXUSDT": "AVAX/USD",
    "MATICUSDT": "MATIC/USD",
    "LINKUSDT": "LINK/USD",
    "DOTUSDT": "DOT/USD",
    "UNIUSDT": "UNI/USD",
    "LTCUSDT": "LTC/USD",
    "BCHUSDT": "BCH/USD"
}

# ========================================
# UTILITY FUNCTIONS
# ========================================
async def exponential_backoff(attempt, base_delay=1, max_delay=60):
    """Exponential backoff with jitter"""
    delay = min(base_delay * (2 ** min(attempt, 6)), max_delay)
    jitter = random.uniform(0.1, 0.3) * delay
    final_delay = delay + jitter
    print(f"⏳ Backing off {final_delay:.1f}s (attempt #{attempt + 1})")
    await asyncio.sleep(final_delay)
    return final_delay

async def log_error(error_type, severity, message, function_name=None,
                   symbol=None, stack_trace=None):
    """Log error to database with deduplication"""
    try:
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        # Check for existing error in last hour
        existing_query = (
            supabase.table("bot_errors")
            .select("*")
            .eq("error_message", message)
            .gte("created_at", one_hour_ago)
        )

        if function_name:
            existing_query = existing_query.eq("function_name", function_name)
        if symbol:
            existing_query = existing_query.eq("symbol", symbol)

        existing = existing_query.execute()

        if existing.data:
            # Update existing error
            error_record = existing.data[0]
            supabase.table("bot_errors").update({
                "occurrence_count": error_record.get("occurrence_count", 0) + 1,
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "active_symbols_count": len(symbols)
            }).eq("id", error_record["id"]).execute()
            print(f"📝 Updated error #{error_record['id']}: {message[:50]}...")
        else:
            # Insert new error
            error_data = {
                "error_type": error_type,
                "severity": severity,
                "error_message": message,
                "function_name": function_name,
                "symbol": symbol,
                "stack_trace": stack_trace,
                "active_symbols_count": len(symbols),
                "occurrence_count": 1,
                "first_occurrence": datetime.now(timezone.utc).isoformat(),
                "last_occurrence": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            result = supabase.table("bot_errors").insert(error_data).execute()
            if result.data:
                print(f"📝 Logged new error: {message[:50]}...")

    except Exception as e:
        print(f"⚠️  Failed to log error: {e}")
        print(f"   Original error: {message}")

async def clear_live_prices():
    """Clear all rows from live_prices table"""
    try:
        existing_data = supabase.table("live_prices").select("symbol").execute()
        row_count = len(existing_data.data) if existing_data.data else 0

        if row_count > 0:
            result = supabase.table("live_prices").delete().neq("symbol", "").execute()
            print(f"🗑️  Cleared {row_count} rows from live_prices")
        else:
            print("ℹ️  live_prices was already empty")

        await log_error(
            error_type="symbol_fetch",
            severity="warning",
            message=f"No symbols from fetch-symbols, cleared {row_count} rows",
            function_name="clear_live_prices"
        )
    except Exception as e:
        await log_error(
            error_type="database",
            severity="error",
            message=f"Failed to clear live_prices: {str(e)}",
            function_name="clear_live_prices",
            stack_trace=traceback.format_exc()
        )

# ========================================
# SYMBOL FETCHING
# ========================================
async def fetch_symbols_loop():
    """Fetch unique symbols from game_assets_view"""
    global symbols, symbol_map, unique_symbols, shutdown_requested
    consecutive_failures = 0

    while not shutdown_requested:
        session = None
        try:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=20)
            )

            print(f"\n🔍 Fetching symbols from game_assets_view...")
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
                    print(f"❌ fetch-symbols error: HTTP {response.status}")
                    await log_error(
                        "api", "error",
                        f"fetch-symbols failed: {response.status}",
                        "fetch_symbols_loop"
                    )
                    await exponential_backoff(consecutive_failures, base_delay=5)
                    continue

                data = await response.json()
                fetched_symbols = data.get("symbols", [])

                if not fetched_symbols:
                    print("⚠️  No symbols returned - clearing state")
                    await clear_live_prices()
                    symbols = set()
                    symbol_map = {}
                    unique_symbols = []
                    consecutive_failures += 1
                    await exponential_backoff(consecutive_failures, base_delay=10)
                    continue

                # 🔑 Update state with deduplicated symbols
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
                        "market_type": row.get("market_type", "unknown"),
                        "exchange": row.get("exchange"),
                        "currency": row.get("currency", "USD"),
                        "search_symbol": row.get("search_symbol", symbol_str)
                    }

                symbols = new_symbols
                symbol_map = new_map

                # 🎯 Prioritize symbols for TwelveData (max 8)
                prioritized = []

                # Add priority symbols first
                for priority_sym in PRIORITY_SYMBOLS:
                    if priority_sym in symbols:
                        prioritized.append(priority_sym)

                # Add remaining symbols
                for sym in sorted(symbols):
                    if sym not in prioritized:
                        prioritized.append(sym)

                unique_symbols = prioritized

                consecutive_failures = 0
                print(f"✅ Loaded {len(symbols)} unique symbols")
                print(f"📋 Top 10: {', '.join(unique_symbols[:10])}")

                await asyncio.sleep(60)  # Check every minute

        except asyncio.CancelledError:
            print("🛑 fetch_symbols_loop cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"❌ fetch_symbols_loop error: {str(e)}")
            await log_error(
                "symbol_fetch", "error", str(e),
                "fetch_symbols_loop",
                stack_trace=traceback.format_exc()
            )
            await exponential_backoff(consecutive_failures, base_delay=5)
        finally:
            if session and not session.closed:
                await session.close()

# ========================================
# PRICE INSERTION
# ========================================
async def insert_single_price(price_data):
    """Insert single price immediately to live_prices"""
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

        start = datetime.now()
        result = supabase.table("live_prices").upsert(
            row, on_conflict="symbol"
        ).execute()
        duration = (datetime.now() - start).total_seconds() * 1000

        if result.data:
            print(f"💾 {symbol.ljust(12)} ${str(price).ljust(10)} ({exchange.ljust(11)}) [{duration:.0f}ms]")
        else:
            print(f"⚠️  Insert failed: {symbol}")

    except Exception as e:
        await log_error(
            "database", "error",
            f"Insert failed for {price_data.get('symbol')}: {str(e)}",
            "insert_single_price"
        )

# ========================================
# TWELVEDATA WEBSOCKET
# ========================================
async def receive_twelvedata_price(data):
    """Handle TwelveData price events"""
    global last_price_time

    symbol = data.get("symbol")
    price = data.get("price")
    timestamp = data.get("timestamp")

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

    matched = symbol_map.get(symbol)
    if not matched:
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

    asyncio.create_task(insert_single_price(price_data))

async def maintain_twelvedata_connection():
    """TwelveData WebSocket - Max 8 unique symbols"""
    global shutdown_requested, active_twelvedata_symbols
    consecutive_failures = 0
    previous_symbols = []
    heartbeat_task_local = None

    while not shutdown_requested:
        session = None
        ws = None

        try:
            if not unique_symbols:
                print("⏸️  No symbols for TwelveData, waiting...")
                await asyncio.sleep(5)
                continue

            # 🎯 Select up to 8 non-crypto symbols for TwelveData
            binance_handled = set(BINANCE_SYMBOL_MAP.values())
            twelvedata_candidates = [s for s in unique_symbols if s not in binance_handled]

            subscription_symbols = twelvedata_candidates[:8]  # Max 8

            if not subscription_symbols:
                print("⏸️  No TwelveData symbols (all handled by Binance)")
                await asyncio.sleep(10)
                continue

            resubscribe = subscription_symbols != previous_symbols
            previous_symbols = subscription_symbols.copy()
            active_twelvedata_symbols = subscription_symbols.copy()

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )

            print(f"\n🔌 Connecting to TwelveData (failures: {consecutive_failures})")

            async with session.ws_connect(TD_WEBSOCKET_URL, heartbeat=30) as ws:
                print("✅ TwelveData connected!")
                consecutive_failures = 0

                if resubscribe:
                    print(f"📡 Subscribing to {len(subscription_symbols)} symbols:")
                    print(f"   {', '.join(subscription_symbols)}")

                    subscribe_payload = json.dumps({
                        "action": "subscribe",
                        "params": {"symbols": ",".join(subscription_symbols)}
                    })

                    await ws.send_str(subscribe_payload)

                # Heartbeat task
                async def send_heartbeat():
                    while not shutdown_requested:
                        try:
                            await asyncio.sleep(10)
                            await ws.send_str(json.dumps({"action": "heartbeat"}))
                        except Exception:
                            break

                heartbeat_task_local = asyncio.create_task(send_heartbeat())

                async for msg in ws:
                    if shutdown_requested:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)

                            if data.get("event") == "price":
                                await receive_twelvedata_price(data)
                            elif data.get("event") == "subscribe-status":
                                success = data.get("success", [])
                                fails = data.get("fails", [])
                                if success:
                                    success_symbols = [s.get("symbol") for s in success]
                                    print(f"✅ Subscribed: {success_symbols}")
                                if fails:
                                    fail_symbols = [s.get("symbol") for s in fails]
                                    print(f"❌ Failed: {fail_symbols}")
                            elif data.get("event") == "heartbeat":
                                print(f"💓 TwelveData: {data.get('status', 'ok')}")

                        except json.JSONDecodeError:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"❌ TwelveData WebSocket {msg.type}")
                        break

        except asyncio.CancelledError:
            print("🛑 TwelveData connection cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"❌ TwelveData error: {str(e)}")
            await log_error(
                "connection", "error", str(e),
                "maintain_twelvedata_connection",
                stack_trace=traceback.format_exc()
            )
        finally:
            if heartbeat_task_local and not heartbeat_task_local.done():
                heartbeat_task_local.cancel()
                try:
                    await heartbeat_task_local
                except asyncio.CancelledError:
                    pass

            if session and not session.closed:
                await session.close()

            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)

# ========================================
# BINANCE WEBSOCKET
# ========================================
async def receive_binance_price(data):
    """Handle Binance price events"""
    global last_price_time

    if "data" in data:
        ticker_data = data["data"]
        stream = data.get("stream", "")
        binance_symbol = stream.split("@")[0].upper()
    else:
        ticker_data = data
        binance_symbol = data.get("s")

    price = ticker_data.get("c")
    db_symbol = BINANCE_SYMBOL_MAP.get(binance_symbol)

    if not db_symbol:
        return

    matched = symbol_map.get(db_symbol)
    if not matched:
        return

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

    asyncio.create_task(insert_single_price(price_data))

async def maintain_binance_connection():
    """Binance WebSocket for crypto"""
    global shutdown_requested, active_binance_symbols
    consecutive_failures = 0

    while not shutdown_requested:
        session = None
        try:
            # Check which Binance symbols we need
            needed_symbols = []
            for binance_sym, db_sym in BINANCE_SYMBOL_MAP.items():
                if db_sym in symbols:
                    needed_symbols.append(binance_sym)

            if not needed_symbols:
                print("⏸️  No Binance symbols needed, waiting...")
                await asyncio.sleep(5)
                continue

            active_binance_symbols = set(BINANCE_SYMBOL_MAP.values())

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, connect=15)
            )

            print(f"\n🔌 Connecting to Binance (failures: {consecutive_failures})")
            print(f"📡 Tracking: {', '.join(needed_symbols)}")

            async with session.ws_connect(BINANCE_WEBSOCKET_URL) as ws:
                print("✅ Binance connected!")
                consecutive_failures = 0

                async for msg in ws:
                    if shutdown_requested:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            await receive_binance_price(data)
                        except json.JSONDecodeError:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"❌ Binance WebSocket {msg.type}")
                        break

        except asyncio.CancelledError:
            print("🛑 Binance connection cancelled")
            break
        except Exception as e:
            consecutive_failures += 1
            print(f"❌ Binance error: {str(e)}")
            await log_error(
                "connection", "error", str(e),
                "maintain_binance_connection",
                stack_trace=traceback.format_exc()
            )
        finally:
            if session and not session.closed:
                await session.close()

            if consecutive_failures > 0 and not shutdown_requested:
                await exponential_backoff(consecutive_failures, base_delay=3, max_delay=30)
            else:
                await asyncio.sleep(1)

# ========================================
# WATCHDOG & HEARTBEAT
# ========================================
async def watchdog():
    """Monitor and restart dead tasks"""
    global fetch_task, twelvedata_task, binance_task, shutdown_requested
    print("👁️  Watchdog started")

    while not shutdown_requested:
        try:
            await asyncio.sleep(10)

            # Check fetch task
            if fetch_task and fetch_task.done():
                exception = fetch_task.exception()
                if exception:
                    await log_error(
                        "startup", "critical",
                        f"fetch_symbols died: {exception}",
                        "watchdog"
                    )
                print("🔄 Restarting fetch_symbols_loop...")
                fetch_task = asyncio.create_task(fetch_symbols_loop())

            # Check TwelveData task
            if twelvedata_task and twelvedata_task.done():
                exception = twelvedata_task.exception()
                if exception:
                    await log_error(
                        "connection", "critical",
                        f"TwelveData died: {exception}",
                        "watchdog"
                    )
                print("🔄 Restarting TwelveData connection...")
                twelvedata_task = asyncio.create_task(maintain_twelvedata_connection())

            # Check Binance task
            if binance_task and binance_task.done():
                exception = binance_task.exception()
                if exception:
                    await log_error(
                        "connection", "critical",
                        f"Binance died: {exception}",
                        "watchdog"
                    )
                print("🔄 Restarting Binance connection...")
                binance_task = asyncio.create_task(maintain_binance_connection())

            # Health check
            now = datetime.now(timezone.utc)
            time_since_price = (now - last_price_time).total_seconds()

            if time_since_price > 60:
                await log_error(
                    "timeout", "warning",
                    f"No prices for {int(time_since_price)}s",
                    "watchdog"
                )
            else:
                td_count = len(active_twelvedata_symbols)
                bn_count = len(active_binance_symbols & symbols)
                print(f"💚 OK: TD={td_count}, BN={bn_count}, {int(time_since_price)}s ago")

        except asyncio.CancelledError:
            print("🛑 Watchdog cancelled")
            break
        except Exception as e:
            print(f"❌ Watchdog error: {e}")

async def system_heartbeat():
    """Periodic heartbeat log"""
    while not shutdown_requested:
        try:
            await asyncio.sleep(30)
            print(f"\n{'='*60}")
            print(f"💓 HEARTBEAT @ {datetime.now().strftime('%H:%M:%S')}")
            print(f"{'='*60}")
            print(f"   Unique symbols: {len(symbols)}")
            print(f"   TwelveData: {len(active_twelvedata_symbols)}")
            print(f"   Binance: {len(active_binance_symbols & symbols)}")
            print(f"   Last price: {(datetime.now(timezone.utc) - last_price_time).seconds}s ago")
            print(f"{'='*60}\n")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Heartbeat error: {e}")

# ========================================
# SHUTDOWN HANDLING
# ========================================
def handle_shutdown(signum, frame):
    global shutdown_requested
    print(f"\n⚠️  Received signal {signum}, shutting down...")
    shutdown_requested = True

async def graceful_shutdown():
    """Cleanup all tasks"""
    global fetch_task, twelvedata_task, binance_task, watchdog_task, heartbeat_task
    print("🛑 Graceful shutdown initiated...")

    tasks = [fetch_task, twelvedata_task, binance_task, watchdog_task, heartbeat_task]
    tasks_to_cancel = [t for t in tasks if t and not t.done()]

    for task in tasks_to_cancel:
        task.cancel()

    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    print("✅ All tasks cancelled, goodbye!")

# ========================================
# MAIN
# ========================================
async def main():
    """Main loop with auto-restart"""
    global fetch_task, twelvedata_task, binance_task, watchdog_task, heartbeat_task
    global shutdown_requested

    restart_count = 0
    max_restarts = 999999

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    while restart_count < max_restarts and not shutdown_requested:
        try:
            print("\n" + "="*70)
            print("🚀 PRODUCTION TRADING BOT v2.0 - AUTO-RESTART ENABLED")
            print("="*70)
            print("Features:")
            print("  ✅ Auto symbol fetch from game_assets_view")
            print("  ✅ Deduplication (10 BTC games = 1 BTC symbol)")
            print("  ✅ TwelveData: Max 8 unique symbols")
            print("  ✅ Binance: Unlimited crypto")
            print("  ✅ Auto-restart on failure")
            print("  ✅ Heartbeat monitoring")
            print("  ✅ Immediate price insertion")
            print("  ✅ USD-focused pricing")
            print("="*70 + "\n")

            # Start all tasks
            fetch_task = asyncio.create_task(fetch_symbols_loop())
            twelvedata_task = asyncio.create_task(maintain_twelvedata_connection())
            binance_task = asyncio.create_task(maintain_binance_connection())
            watchdog_task = asyncio.create_task(watchdog())
            heartbeat_task = asyncio.create_task(system_heartbeat())

            # Wait for any task to complete (shouldn't happen normally)
            done, pending = await asyncio.wait(
                [fetch_task, twelvedata_task, binance_task, watchdog_task, heartbeat_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            if shutdown_requested:
                break

            restart_count += 1
            print(f"⚠️  Core task died, restarting (attempt {restart_count}/{max_restarts})")

            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            if restart_count < max_restarts:
                await exponential_backoff(restart_count, base_delay=5, max_delay=120)

        except Exception as e:
            restart_count += 1
            print(f"❌ Main crashed: {e}")
            await log_error(
                "startup", "fatal", str(e), "main",
                stack_trace=traceback.format_exc()
            )

            if restart_count < max_restarts:
                await asyncio.sleep(10)
            else:
                print("❌ Max restarts reached!")
                raise

    if shutdown_requested:
        await graceful_shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

