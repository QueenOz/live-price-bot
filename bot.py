import os
import time
import json
import logging
import queue
import threading
import websocket
from supabase import create_client
from dotenv import load_dotenv

# Load .env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

class TDWebSocket:
    def __init__(self, symbols):
        self.symbols = symbols
        self.queue = queue.Queue(maxsize=10000)
        self.url = "wss://ws.twelvedata.com/v1/ws"  # ✅ FIXED URL
        self.logger = logging.getLogger("TDWebSocket")
        logging.basicConfig(level=logging.INFO)

    def start(self):
        def on_message(_, message):
            data = json.loads(message)
            if "symbol" in data and "price" in data:
                self.logger.info(f"📈 Received: {data}")
                self.queue.put(data)

        def on_open(_):
            self.logger.info("✅ WebSocket connection opened")
            self.subscribe(self.symbols)

        def on_close(_, code, msg):
            self.logger.warning(f"⚠️ WebSocket closed: {code}, {msg}")

        def on_error(_, err):
            self.logger.error(f"❌ WebSocket error: {err}")

        self.ws = websocket.WebSocketApp(
            self.url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error
        )
        threading.Thread(target=self.ws.run_forever, daemon=True).start()
        self.process_events()

    def subscribe(self, symbols):
        payload = {
            "action": "subscribe",
            "params": {
                "apikey": TD_API_KEY,
                "channels": [f"price:{s}" for s in symbols]
            }
        }
        self.ws.send(json.dumps(payload))
        self.logger.info(f"🧠 Subscribing to: {payload['params']['channels']}")

    def process_events(self):
        while True:
            try:
                data = self.queue.get()
                self.upsert_price(data)
            except Exception as e:
                self.logger.error(f"Error processing data: {e}")

    def upsert_price(self, data):
        try:
            symbol = data["symbol"]  # e.g., 'EUR/USD'
            supabase.table("live_prices").upsert({
                "symbol": symbol,
                "standardized_symbol": symbol.replace("/", "_"),
                "price": float(data["price"]),
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }).execute()
            self.logger.info(f"✅ Upserted: {symbol} → {data['price']}")
        except Exception as e:
            self.logger.error(f"❌ Upsert failed for {data}: {e}")

def get_symbols_from_supabase():
    try:
        game_ids = supabase.table("games")\
            .select("id")\
            .in_("status", ["upcoming", "active"])\
            .execute().data
        game_ids = [g["id"] for g in game_ids]
        if not game_ids:
            print("🚫 No active or upcoming games.")
            return []

        result = supabase.table("game_assets")\
            .select("asset_name")\
            .in_("game_id", game_ids)\
            .execute()

        symbols = list(set([
            r["asset_name"]
            for r in result.data
            if r.get("asset_name")
        ]))
        return symbols
    except Exception as e:
        print("❌ Error fetching symbols from Supabase:", e)
        return []

if __name__ == "__main__":
    symbols = get_symbols_from_supabase()
    if symbols:
        bot = TDWebSocket(symbols)
        bot.start()
    else:
        print("⚠️ No symbols to subscribe to. Exiting.")
