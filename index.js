const WebSocket = require('ws');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;

// 🟢 WebSocket symbols
const websocketSymbols = ['BTC/USD', 'ETH/USD', 'DOGE/USD', 'SOL/USD'];

// 🔁 REST polling symbols
const restSymbols = ['AAPL', 'TSLA', 'NVDA', 'AMZN'];

// 🔁 Map standardized symbols (same key used in both REST & WebSocket flows)
const standardizedMap = {
  'BTC/USD': 'BTC',
  'ETH/USD': 'ETH',
  'SOL/USD': 'SOL',
  'DOGE/USD': 'DOGE',
  'AAPL': 'AAPL',
  'TSLA': 'TSLA',
  'NVDA': 'NVDA',
  'AMZN': 'AMZN'
};

// 🟢 WebSocket connection logic
websocketSymbols.forEach(symbol => {
  const ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?symbol=${symbol}&apikey=${TWELVE_DATA_API_KEY}`);

  ws.on('open', () => {
    console.log(`🟢 WebSocket connected: ${symbol}`);
  });

  ws.on('message', async (msg) => {
    try {
      const data = JSON.parse(msg);
      if (!data.price) return;

      await supabase.from('live_prices').upsert({
        symbol,
        standardized_symbol: standardizedMap[symbol],
        price: data.price,
        updated_at: new Date().toISOString()
      });

      console.log(`✅ Updated ${symbol} via WebSocket: ${data.price}`);
    } catch (e) {
      console.error(`❌ WebSocket error for ${symbol}`, e);
    }
  });

  ws.on('error', (e) => {
    console.error(`❗ WebSocket failed for ${symbol}`, e.message);
  });
});

// 🔁 REST polling function
const pollRESTPrices = async () => {
  for (const symbol of restSymbols) {
    try {
      const res = await fetch(`https://api.twelvedata.com/price?symbol=${symbol}&apikey=${TWELVE_DATA_API_KEY}`);
      const json = await res.json();

      if (!json.price) {
        console.warn(`⚠️ No price for ${symbol}:`, json);
        continue;
      }

      await supabase.from('live_prices').upsert({
        symbol,
        standardized_symbol: standardizedMap[symbol],
        price: parseFloat(json.price),
        updated_at: new Date().toISOString()
      });

      console.log(`📈 Updated ${symbol} via REST: ${json.price}`);
    } catch (e) {
      console.error(`❌ REST polling error for ${symbol}`, e);
    }
  }
};

// 🔁 Poll REST prices every 2 seconds
setInterval(pollRESTPrices, 2000);
