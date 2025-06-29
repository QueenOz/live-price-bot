const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;

const symbols = ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "TSLA", "AAPL", "NVDA", "AMZN"];
const standardizedMap = {
  "BTC/USD": "BTC", "ETH/USD": "ETH", "SOL/USD": "SOL", "DOGE/USD": "DOGE",
  "TSLA": "TSLA", "AAPL": "AAPL", "NVDA": "NVDA", "AMZN": "AMZN"
};

symbols.forEach(symbol => {
  const ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?symbol=${symbol}&apikey=${TWELVE_DATA_API_KEY}`);

  ws.on('open', () => console.log(`🟢 Connected to ${symbol}`));

  ws.on('message', async msg => {
    try {
      const data = JSON.parse(msg);
      if (!data.price) return;

      await supabase.from("live_prices").upsert({
        symbol,
        standardized_symbol: standardizedMap[symbol],
        price: data.price,
        updated_at: new Date().toISOString()
      });
    } catch (e) {
      console.error(`❌ Error for ${symbol}`, e);
    }
  });

  ws.on('error', err => console.error(`❗ WebSocket error for ${symbol}:`, err));
});