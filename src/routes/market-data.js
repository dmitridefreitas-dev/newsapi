import express from 'express';
import YahooFinance from 'yahoo-finance2';

const router = express.Router();
const yf = new YahooFinance({ suppressNotices: ['yahooSurvey'] });

// ^GSPC = S&P 500, ^VIX = VIX, ^TNX = 10Y Treasury, ^NDX = Nasdaq 100
const SYMBOLS = ['^GSPC', '^VIX', '^TNX', '^NDX'];

let cache = null;
let cacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

router.get('/', async (req, res) => {
  const now = Date.now();
  if (cache && now - cacheTime < CACHE_TTL) {
    return res.json(cache);
  }

  const quotes = await Promise.all(SYMBOLS.map(s => yf.quote(s)));
  const [spx, vix, t10y, ndx] = quotes;

  const data = [
    {
      label: 'SPX',
      value: spx.regularMarketPrice.toLocaleString('en-US', { maximumFractionDigits: 0 }),
      unit: '',
      positive: spx.regularMarketChangePercent >= 0,
      change: `${spx.regularMarketChangePercent >= 0 ? '+' : ''}${spx.regularMarketChangePercent.toFixed(2)}%`,
    },
    {
      label: 'NDX',
      value: ndx.regularMarketPrice.toLocaleString('en-US', { maximumFractionDigits: 0 }),
      unit: '',
      positive: ndx.regularMarketChangePercent >= 0,
      change: `${ndx.regularMarketChangePercent >= 0 ? '+' : ''}${ndx.regularMarketChangePercent.toFixed(2)}%`,
    },
    {
      label: 'VIX',
      value: vix.regularMarketPrice.toFixed(2),
      unit: '',
      positive: null,
    },
    {
      label: '10Y',
      value: t10y.regularMarketPrice.toFixed(2),
      unit: '%',
      positive: t10y.regularMarketChangePercent >= 0,
    },
  ];

  cache = data;
  cacheTime = now;
  res.json(data);
});

// GET /market-data/quote?symbol=SPY  — single symbol quote + market state
router.get('/quote', async (req, res) => {
  const symbol = (req.query.symbol || 'SPY').toUpperCase();
  const quote = await yf.quote(symbol);
  res.json({
    symbol,
    price:          quote.regularMarketPrice,
    previousClose:  quote.regularMarketPreviousClose,
    change:         quote.regularMarketChange,
    changePercent:  quote.regularMarketChangePercent,
    bid:            quote.bid ?? null,
    ask:            quote.ask ?? null,
    volume:         quote.regularMarketVolume,
    dayHigh:        quote.regularMarketDayHigh,
    dayLow:         quote.regularMarketDayLow,
    fiftyTwoWeekHigh: quote.fiftyTwoWeekHigh,
    fiftyTwoWeekLow:  quote.fiftyTwoWeekLow,
    marketState:    quote.marketState, // 'REGULAR' | 'CLOSED' | 'PRE' | 'POST'
    shortName:      quote.shortName ?? symbol,
  });
});

export default router;
