# bot.py
import os
import time
import json
import logging
import queue
import threading
import websocket
from supabase import create_client
from dotenv import load_dotenv

# Load environment
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
logging.basicConfig(level=logging.INFO)

class TDWebSocket:
    def __init__(self):
        self.ws = None
        self.queue = queue.Queue()
        self.logger = logging.getLogger("TDWebSocket")
        self.symbols = []

    def start(self):
        while True:
            self.symbols = get_symbols_from_supabase()
            if not self.symbols:
                self.logger.warning("🚫 No active/upcoming symbols. Waiting 2 seconds.")
                time.sleep(2)
                continue

            self.logger.info(f"🧠 Subscribing to: {self.symbols}")
            try:
                self.connect_and_run()
            except Exception as e:
                self.logger.error(f"❌ Crash: {e}")
            self.logger.warning("🔁 Reconnecting in 2 seconds...")
            time.sleep(2)

    def connect_and_run(self):
        def on_message(_, message):
            try:
                data = json.loads(message)
                if "symbol" in data and "price" in data:
                    self.logger.info(f"📥 {data}")
                    self.queue.put(data)
            except Exception as e:
                self.logger.error(f"❌ Parse error: {e}")

        def on_open(ws):
            self.logger.info("🟢 Connected to TwelveData")
            payload = {
                "action": "subscribe",
                "params": {
                    "symbols": ",".join(self.symbols)
                }
            }
            ws.send(json.dumps(payload))

        def on_error(_, err):
            self.logger.error(f"❌ WebSocket error: {err}")

        def on_close(_, code, msg):
            self.logger.warning(f"⚠️ WebSocket closed: {code}, {msg}")

        self.ws = websocket.WebSocketApp(
            f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}",
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close
        )

        thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        thread.start()

        # Wait for up to 2 seconds between events
        while thread.is_alive():
            try:
                data = self.queue.get(timeout=2)
                self.upsert_price(data)
            except queue.Empty:
                self.logger.info("⏳ No price updates. Refreshing.")
                self.ws.close()
                break

    def upsert_price(self, data):
        try:
            raw_symbol = data["symbol"]
            std_symbol = raw_symbol.replace("/", "_")
            supabase.table("live_prices").upsert({
                "symbol": raw_symbol,
                "standardized_symbol": std_symbol,
                "price": float(data["price"]),
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }).execute()
            self.logger.info(f"✅ Upserted: {raw_symbol} = {data['price']}")
        except Exception as e:
            self.logger.error(f"❌ Upsert failed for {data}: {e}")


def get_symbols_from_supabase():
    try:
        games = supabase.table("games").select("id").in_("status", ["upcoming", "active"]).execute().data
        game_ids = [g["id"] for g in games]
        if not game_ids:
            return []

        result = supabase.table("game_assets").select("asset_name").in_("game_id", game_ids).execute().data
        return list({r["asset_name"] for r in result if r.get("asset_name")})
    except Exception as e:
        logging.error(f"❌ Symbol fetch error: {e}")
        return []


if __name__ == "__main__":
    TDWebSocket().start()
