import express from 'express';
import YahooFinance from 'yahoo-finance2';

const router = express.Router();
const yf = new YahooFinance({ suppressNotices: ['yahooSurvey'] });

// ── Request queues — serialize external calls to avoid burst rate-limits ───────
// Yahoo Finance has no explicit quota, but hammering their endpoints can lead to
// temporary IP bans or crumb failures. 350ms spacing spreads bursts so concurrent
// frontend requests don't all hit Yahoo at once.
const delay = (ms) => new Promise(r => setTimeout(r, ms));

function makeQueue(gapMs) {
  let tail = Promise.resolve();
  return function enqueue(fn) {
    const result = tail.then(() => fn());
    tail = result.then(() => delay(gapMs), () => delay(gapMs));
    return result;
  };
}

// 1 Yahoo call every 350ms — spreads burst, avoids crumb hammering
const yfQueue  = makeQueue(350);

async function yfQuote(symbol) {
  return yfQueue(() => yf.quote(symbol, {}, { validateResult: false }));
}

async function yfOptions(symbol, opts) {
  return yfQueue(() => yf.options(symbol, opts || {}, { validateResult: false }));
}

// Yahoo Finance historical — returns [{date, open, high, low, close, adjClose, volume}]
async function yfHistorical(symbol, { months, interval = '1mo' } = {}) {
  const endDate = new Date();
  const startDate = new Date();
  // Extra buffer for the return calc (we need one row before the first target month)
  const bufferMonths = interval === '1mo' ? 3 : 0;
  startDate.setMonth(startDate.getMonth() - (months ?? 60) - bufferMonths);
  return yfQueue(() => yf.historical(
    symbol,
    {
      period1: startDate.toISOString().slice(0, 10),
      period2: endDate.toISOString().slice(0, 10),
      interval,
    },
    { validateResult: false }
  ));
}

// Yahoo Finance historical — daily, start/end window
async function yfHistoricalDaily(symbol, start, end) {
  return yfQueue(() => yf.historical(
    symbol,
    { period1: start, period2: end, interval: '1d' },
    { validateResult: false }
  ));
}

function parseNum(v) {
  if (v == null || v === 'None' || v === '-' || v === '') return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}

function dateKey(dateStr) {
  return dateStr.slice(0, 7); // "YYYY-MM-DD" -> "YYYY-MM"
}

// Normalize a Yahoo historical row -> {date, adjClose}
function yfRowsToSeries(rows) {
  if (!Array.isArray(rows)) return [];
  return rows
    .map(r => {
      const d = r.date instanceof Date ? r.date.toISOString().slice(0, 10) : String(r.date).slice(0, 10);
      const ac = r.adjClose ?? r.close;
      return ac != null ? { date: d, adjClose: +ac } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.date.localeCompare(b.date));
}

// ── Main header tickers — Yahoo Finance (index symbols not supported by AV) ───
const SYMBOLS = ['^GSPC', '^VIX', '^TNX', '^NDX'];
let cache = null, cacheTime = 0;
const CACHE_TTL = 15 * 60 * 1000; // 15 min

router.get('/', async (req, res) => {
  const now = Date.now();
  if (cache && now - cacheTime < CACHE_TTL) return res.json(cache);

  try {
    const [spx, vix, t10y, ndx] = await Promise.all(SYMBOLS.map(s => yfQuote(s)));

    const data = [
      {
        label: 'SPX',
        value: spx.regularMarketPrice?.toLocaleString('en-US', { maximumFractionDigits: 0 }) ?? '—',
        unit: '',
        positive: (spx.regularMarketChangePercent ?? 0) >= 0,
        change: spx.regularMarketChangePercent != null
          ? `${spx.regularMarketChangePercent >= 0 ? '+' : ''}${spx.regularMarketChangePercent.toFixed(2)}%`
          : '—',
      },
      {
        label: 'NDX',
        value: ndx.regularMarketPrice?.toLocaleString('en-US', { maximumFractionDigits: 0 }) ?? '—',
        unit: '',
        positive: (ndx.regularMarketChangePercent ?? 0) >= 0,
        change: ndx.regularMarketChangePercent != null
          ? `${ndx.regularMarketChangePercent >= 0 ? '+' : ''}${ndx.regularMarketChangePercent.toFixed(2)}%`
          : '—',
      },
      {
        label: 'VIX',
        value: vix.regularMarketPrice?.toFixed(2) ?? '—',
        unit: '',
        positive: null,
      },
      {
        label: '10Y',
        value: t10y.regularMarketPrice?.toFixed(2) ?? '—',
        unit: '%',
        positive: (t10y.regularMarketChangePercent ?? 0) >= 0,
      },
    ];

    cache = data;
    cacheTime = now;
    res.json(data);
  } catch (err) {
    console.error('[market-data /]', err.message);
    if (cache) return res.json(cache);
    res.status(500).json({ error: err.message });
  }
});

// ── Single quote — Yahoo Finance primary ──────────────────────────────────────
const QUOTE_CACHE = new Map();
const QUOTE_TTL = 10 * 60 * 1000;

router.get('/quote', async (req, res) => {
  const symbol = (req.query.symbol || 'SPY').toUpperCase();
  const now = Date.now();
  const cached = QUOTE_CACHE.get(symbol);
  if (cached && now - cached.time < QUOTE_TTL) return res.json(cached.data);

  try {
    const q = await yfQuote(symbol);
    if (!q?.regularMarketPrice) throw new Error(`No quote data for ${symbol}`);

    const data = {
      symbol,
      price:                     q.regularMarketPrice        ?? null,
      previousClose:             q.regularMarketPreviousClose ?? null,
      change:                    q.regularMarketChange       ?? null,
      changePercent:             q.regularMarketChangePercent ?? null,
      volume:                    q.regularMarketVolume       ?? null,
      bid:                       q.bid                       ?? null,
      ask:                       q.ask                       ?? null,
      dayHigh:                   q.regularMarketDayHigh      ?? null,
      dayLow:                    q.regularMarketDayLow       ?? null,
      fiftyTwoWeekHigh:          q.fiftyTwoWeekHigh          ?? null,
      fiftyTwoWeekLow:           q.fiftyTwoWeekLow           ?? null,
      marketState:               q.marketState               ?? null,
      shortName:                 q.shortName                 ?? symbol,
      postMarketPrice:           q.postMarketPrice           ?? null,
      postMarketChange:          q.postMarketChange          ?? null,
      postMarketChangePercent:   q.postMarketChangePercent   ?? null,
      postMarketTime:            q.postMarketTime            ?? null,
      preMarketPrice:            q.preMarketPrice            ?? null,
      preMarketChange:           q.preMarketChange           ?? null,
      preMarketChangePercent:    q.preMarketChangePercent    ?? null,
      preMarketTime:             q.preMarketTime             ?? null,
    };

    QUOTE_CACHE.set(symbol, { data, time: now });
    res.json(data);
  } catch (err) {
    console.error('[market-data /quote]', symbol, err.message);
    const stale = QUOTE_CACHE.get(symbol);
    if (stale) return res.json(stale.data);
    res.status(500).json({ error: err.message });
  }
});

// ── Monthly historical — Yahoo Finance ────────────────────────────────────────
// GET /market-data/monthly?tickers=AAPL,MSFT,SPY&months=36
// Yahoo Finance (yf.historical, 1mo bars) is the sole source — no Alpha Vantage.
const MONTHLY_CACHE = new Map();
const MONTHLY_TTL = 60 * 60 * 1000;

async function monthlySeries(ticker) {
  const rows = await yfHistorical(ticker, { months: 120, interval: '1mo' });
  const series = yfRowsToSeries(rows);
  return { series, source: 'yahoo' };
}

router.get('/monthly', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 10);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });

  const months  = Math.min(parseInt(req.query.months) || 36, 240);
  const cacheKey = `${tickers.join(',')}_${months}`;
  const now = Date.now();
  const cached = MONTHLY_CACHE.get(cacheKey);
  if (cached && now - cached.time < MONTHLY_TTL) return res.json(cached.data);

  const results = {}, errors = {}, sources = {};
  await Promise.allSettled(tickers.map(async (ticker) => {
    try {
      const { series, source } = await monthlySeries(ticker);
      if (series.length < 3) { errors[ticker] = 'Insufficient data'; return; }
      const slice = series.slice(-(months + 1));
      const out = [];
      for (let i = 1; i < slice.length; i++) {
        const prev = slice[i - 1].adjClose;
        if (!prev) continue;
        out.push({
          date:     dateKey(slice[i].date),
          adjClose: +slice[i].adjClose.toFixed(4),
          ret:      +(slice[i].adjClose / prev - 1).toFixed(6),
        });
      }
      if (out.length < 2) { errors[ticker] = 'Insufficient data after slicing'; return; }
      results[ticker] = out;
      sources[ticker] = source;
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors, sources };
  MONTHLY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// ── Yahoo-first monthly alias — explicitly Yahoo-only ─────────────────────────
// GET /market-data/yf-monthly?tickers=SPY,GLD,TLT&months=72
// Used by Strategy, Live Signal, and Backtest Stats pages.
router.get('/yf-monthly', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 10);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });

  const months = Math.min(parseInt(req.query.months) || 60, 240);
  const cacheKey = `yf_${tickers.join(',')}_${months}`;
  const now = Date.now();
  const cached = MONTHLY_CACHE.get(cacheKey);
  if (cached && now - cached.time < MONTHLY_TTL) return res.json(cached.data);

  const results = {}, errors = {};
  await Promise.allSettled(tickers.map(async (ticker) => {
    try {
      const rows = await yfHistorical(ticker, { months, interval: '1mo' });
      const series = yfRowsToSeries(rows);
      if (series.length < 3) { errors[ticker] = 'Insufficient data'; return; }
      const slice = series.slice(-(months + 1));
      const out = [];
      for (let i = 1; i < slice.length; i++) {
        const prev = slice[i - 1].adjClose;
        if (!prev) continue;
        out.push({
          date:     dateKey(slice[i].date),
          adjClose: +slice[i].adjClose.toFixed(4),
          ret:      +(slice[i].adjClose / prev - 1).toFixed(6),
        });
      }
      if (out.length < 2) { errors[ticker] = 'Insufficient data after slicing'; return; }
      results[ticker] = out;
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors };
  MONTHLY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// ── Batch quotes — Yahoo Finance ──────────────────────────────────────────────
// GET /market-data/yf-quotes?tickers=AAPL,MSFT,NVDA
// Used by IC Vault to show live prices alongside memos.
const YFQ_CACHE = new Map();
const YFQ_TTL = 5 * 60 * 1000;

router.get('/yf-quotes', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 50);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });

  const cacheKey = tickers.join(',');
  const now = Date.now();
  const cached = YFQ_CACHE.get(cacheKey);
  if (cached && now - cached.time < YFQ_TTL) return res.json(cached.data);

  const out = {}, errors = {};
  await Promise.allSettled(tickers.map(async (t) => {
    try {
      const q = await yfQuote(t);
      if (!q) { errors[t] = 'No data'; return; }
      out[t] = {
        symbol: t,
        price:         q.regularMarketPrice        ?? null,
        change:        q.regularMarketChange       ?? null,
        changePercent: q.regularMarketChangePercent ?? null,
        previousClose: q.regularMarketPreviousClose ?? null,
        marketState:   q.marketState ?? null,
        shortName:     q.shortName ?? t,
      };
    } catch (err) { errors[t] = err.message; }
  }));

  const data = { data: out, errors };
  YFQ_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// ── Daily historical — Yahoo Finance ──────────────────────────────────────────
// GET /market-data/daily?tickers=AAPL,SPY&start=2023-01-01&end=2024-06-01
const DAILY_CACHE = new Map();
const DAILY_TTL   = 30 * 60 * 1000;

async function dailySeries(ticker, start, end) {
  const rows = await yfHistoricalDaily(ticker, start, end);
  return yfRowsToSeries(rows);
}

router.get('/daily', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 5);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });
  const start = req.query.start;
  if (!start) return res.status(400).json({ error: 'start date required' });
  const end = req.query.end || new Date().toISOString().slice(0, 10);

  const cacheKey = `${tickers.join(',')}_${start}_${end}`;
  const now = Date.now();
  const cached = DAILY_CACHE.get(cacheKey);
  if (cached && now - cached.time < DAILY_TTL) return res.json(cached.data);

  const results = {}, errors = {};
  await Promise.allSettled(tickers.map(async (ticker) => {
    try {
      const series = await dailySeries(ticker, start, end);
      if (series.length < 5) { errors[ticker] = 'Insufficient data'; return; }
      results[ticker] = series.map(r => ({
        date:     r.date,
        adjClose: +r.adjClose.toFixed(4),
      }));
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors };
  DAILY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// ── Fama-French factor proxies — Yahoo Finance ─────────────────────────────────
// GET /market-data/ff-proxy?months=60
// MKT=SPY-BIL, SMB=IWM-SPY, HML=SPYV-SPYG
const FF_TICKERS = ['SPY', 'IWM', 'SPYV', 'SPYG', 'BIL'];
let FF_CACHE = null, FF_CACHE_TIME = 0;
const FF_TTL = 6 * 60 * 60 * 1000;

router.get('/ff-proxy', async (req, res) => {
  const months = Math.min(parseInt(req.query.months) || 60, 240);
  const now = Date.now();
  if (FF_CACHE && now - FF_CACHE_TIME < FF_TTL && FF_CACHE.length >= months)
    return res.json(FF_CACHE.slice(-months));

  try {
    const allData = {};
    await Promise.all(FF_TICKERS.map(async (t) => {
      const { series } = await monthlySeries(t);
      const rets = [];
      for (let i = 1; i < series.length; i++) {
        const prev = series[i - 1].adjClose;
        if (!prev) continue;
        rets.push({
          date: dateKey(series[i].date),
          ret:  series[i].adjClose / prev - 1,
        });
      }
      allData[t] = rets;
    }));

    const spyDates = (allData['SPY'] || []).map(r => r.date);
    const factors = spyDates
      .filter(d => FF_TICKERS.every(t => allData[t]?.some(r => r.date === d)))
      .map(date => {
        const g = (t) => allData[t].find(r => r.date === date)?.ret ?? 0;
        const spy = g('SPY'), iwm = g('IWM'), spyv = g('SPYV'), spyg = g('SPYG'), bil = g('BIL');
        return {
          date,
          rf:  +bil.toFixed(6),
          mkt: +(spy  - bil).toFixed(6),
          smb: +(iwm  - spy).toFixed(6),
          hml: +(spyv - spyg).toFixed(6),
        };
      });

    FF_CACHE = factors;
    FF_CACHE_TIME = now;
    res.json(factors.slice(-months));
  } catch (err) {
    console.error('[market-data /ff-proxy]', err.message);
    if (FF_CACHE) return res.json(FF_CACHE.slice(-months));
    res.status(500).json({ error: err.message });
  }
});

// ── Options chain — Yahoo Finance ──────────────────────────────────────────────
// GET /market-data/options?ticker=QQQ
router.get('/options', async (req, res) => {
  const ticker = (req.query.ticker || 'QQQ').toUpperCase();
  try {
    const base = await yfOptions(ticker);
    const expiryDates = base.expirationDates || [];
    if (!expiryDates.length) return res.status(404).json({ error: 'No options data found for ' + ticker });

    const spotQuote = await yfQuote(ticker);
    const spot = spotQuote.regularMarketPrice;
    if (!spot) return res.status(404).json({ error: 'No spot price for ' + ticker });

    const selected = expiryDates.slice(0, 8);
    const now = new Date();
    const toDate = (d) => d instanceof Date ? d : new Date(d * 1000);

    const chains = await Promise.allSettled(
      selected.map(d => yfOptions(ticker, { date: toDate(d) }))
    );

    const surface = [];
    chains.forEach((result, idx) => {
      if (result.status !== 'fulfilled') return;
      const chain = result.value;
      const expDate = toDate(selected[idx]);
      const dte = Math.max(1, Math.round((expDate - now) / (1000 * 60 * 60 * 24)));
      const expiryStr = expDate.toISOString().slice(0, 10);

      const opts = chain.options?.[0] || {};
      const calls = opts.calls || [];
      const puts  = opts.puts  || [];

      const strikeMap = {};
      puts.forEach(p => {
        const iv = p.impliedVolatility;
        if (iv > 0.01 && iv < 3.0 && p.strike > 0) {
          strikeMap[p.strike] = { strike: p.strike, iv, type: 'put', volume: p.volume ?? 0, oi: p.openInterest ?? 0 };
        }
      });
      calls.forEach(c => {
        const iv = c.impliedVolatility;
        if (iv > 0.01 && iv < 3.0 && c.strike > 0) {
          if (c.strike >= spot || !strikeMap[c.strike]) {
            strikeMap[c.strike] = { strike: c.strike, iv, type: 'call', volume: c.volume ?? 0, oi: c.openInterest ?? 0 };
          }
        }
      });

      const strikes = Object.values(strikeMap).sort((a, b) => a.strike - b.strike);
      if (strikes.length < 3) return;
      surface.push({ expiry: expiryStr, dte, strikes });
    });

    if (!surface.length) return res.status(404).json({ error: 'No usable IV data — all expirations had fewer than 3 strikes with valid IV.' });

    res.json({ ticker, spot, marketState: spotQuote.marketState ?? 'CLOSED', surface });
  } catch (err) {
    console.error('[market-data /options]', ticker, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Put-call parity scanner — Yahoo Finance ────────────────────────────────────
// GET /market-data/options-parity?ticker=SPY&expiry=0
// Returns per-strike parity-violation data for a single expiry.
router.get('/options-parity', async (req, res) => {
  const ticker   = (req.query.ticker || 'SPY').toUpperCase();
  const expiryIdx = Math.max(0, Math.min(parseInt(req.query.expiry) || 0, 12));
  const rRate    = 0.045;

  try {
    const base = await yfOptions(ticker);
    const expiryDates = base.expirationDates || [];
    if (!expiryDates.length) return res.status(404).json({ error: 'No options for ' + ticker });

    const spotQuote = await yfQuote(ticker);
    const spot = spotQuote.regularMarketPrice;
    if (!spot) return res.status(404).json({ error: 'No spot price' });

    const selectable = expiryDates.slice(0, 12);
    const idx = Math.min(expiryIdx, selectable.length - 1);
    const toDate = (d) => d instanceof Date ? d : new Date(d * 1000);
    const expDate = toDate(selectable[idx]);
    const dte = Math.max(1, Math.round((expDate - new Date()) / (1000 * 60 * 60 * 24)));
    const T = dte / 365;

    const chain = await yfOptions(ticker, { date: expDate });
    const opts  = chain.options?.[0] || {};
    const calls = opts.calls || [];
    const puts  = opts.puts  || [];

    const putMap = Object.fromEntries(puts.map(p => [p.strike, p]));
    const pairs = [];
    for (const c of calls) {
      const p = putMap[c.strike];
      if (!p) continue;
      const callBid = c.bid ?? c.lastPrice;
      const callAsk = c.ask ?? c.lastPrice;
      const putBid  = p.bid ?? p.lastPrice;
      const putAsk  = p.ask ?? p.lastPrice;
      if (callBid == null || callAsk == null || putBid == null || putAsk == null) continue;
      const callMid = (callBid + callAsk) / 2;
      const putMid  = (putBid  + putAsk ) / 2;
      if (!(callMid > 0) || !(putMid > 0)) continue;

      const parityLHS = callMid - putMid;
      const parityRHS = spot - c.strike * Math.exp(-rRate * T);
      const violation = parityLHS - parityRHS;
      const callSpread = Math.max(0, callAsk - callBid);
      const putSpread  = Math.max(0, putAsk  - putBid);
      const spread     = (callSpread + putSpread) / 2;

      const callIV = (c.impliedVolatility ?? 0) * 100;
      const putIV  = (p.impliedVolatility ?? 0) * 100;
      const ivSpread = callIV - putIV;

      pairs.push({
        strike: c.strike,
        callBid, callAsk, putBid, putAsk,
        callMid: +callMid.toFixed(4),
        putMid:  +putMid.toFixed(4),
        parityLHS: +parityLHS.toFixed(4),
        parityRHS: +parityRHS.toFixed(4),
        violation: +violation.toFixed(4),
        spread:    +spread.toFixed(4),
        callIV:    +callIV.toFixed(2),
        putIV:     +putIV.toFixed(2),
        ivSpread:  +ivSpread.toFixed(2),
        sigViolation: Math.abs(violation) > Math.max(spread, 0.05),
      });
    }
    pairs.sort((a, b) => a.strike - b.strike);

    const expiryDatesStr = selectable.map(d => toDate(d).toISOString().slice(0, 10));

    res.json({
      ticker, spot,
      expiry: expDate.toISOString().slice(0, 10),
      dte,
      expiryDates: expiryDatesStr,
      pairs,
    });
  } catch (err) {
    console.error('[market-data /options-parity]', ticker, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Fundamentals — Yahoo Finance quoteSummary (no Alpha Vantage dependency) ───
// GET /market-data/fundamentals?ticker=AAPL
const FUND_CACHE = new Map();
const FUND_TTL = 6 * 60 * 60 * 1000;

const YF_FUND_MODULES = [
  'earnings',
  'financialData',
  'defaultKeyStatistics',
  'summaryDetail',
  'price',
];

router.get('/fundamentals', async (req, res) => {
  const ticker = (req.query.ticker || 'AAPL').toUpperCase();
  const now = Date.now();
  const cached = FUND_CACHE.get(ticker);
  if (cached && now - cached.time < FUND_TTL) return res.json(cached.data);

  const getRaw = (v) => {
    if (v == null) return null;
    if (typeof v === 'object' && 'raw' in v) return v.raw;
    return typeof v === 'number' ? v : null;
  };

  try {
    const fiveYearsAgo = new Date();
    fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
    const period1 = fiveYearsAgo.toISOString().slice(0, 10);

    const safeTs = async (module, type) => {
      try {
        return await yfQueue(() => yf.fundamentalsTimeSeries(
          ticker,
          type ? { module, period1, type } : { module, period1 },
          { validateResult: false }
        ));
      } catch (err) {
        console.warn(`[fundamentals ts/${module}/${type || 'default'}] ${ticker}: ${err.message}`);
        return [];
      }
    };

    // Yahoo's default returns only ~5 quarters; `type: 'annual'` returns 4-5
    // years of full-year data which is what we actually want for the DCF.
    // We fetch annual first; if empty, fall back to quarterly and aggregate.
    const [
      summary, priceQuote,
      tsFinancialsA, tsCashFlowA, tsBalanceA,
      tsFinancialsQ, tsCashFlowQ, tsBalanceQ,
    ] = await Promise.all([
      yfQueue(() => yf.quoteSummary(ticker, { modules: YF_FUND_MODULES }, { validateResult: false })),
      yfQuote(ticker),
      safeTs('financials',    'annual'),
      safeTs('cash-flow',     'annual'),
      safeTs('balance-sheet', 'annual'),
      safeTs('financials',    null),
      safeTs('cash-flow',     null),
      safeTs('balance-sheet', null),
    ]);

    const tsFinancials = Array.isArray(tsFinancialsA) && tsFinancialsA.length ? tsFinancialsA : tsFinancialsQ;
    const tsCashFlow   = Array.isArray(tsCashFlowA)   && tsCashFlowA.length   ? tsCashFlowA   : tsCashFlowQ;
    const tsBalance    = Array.isArray(tsBalanceA)    && tsBalanceA.length    ? tsBalanceA    : tsBalanceQ;

    const priceModule   = summary.price                || {};
    const keyStats      = summary.defaultKeyStatistics || {};
    const financialData = summary.financialData        || {};
    const summaryDetail = summary.summaryDetail        || {};
    const earnings      = summary.earnings             || {};

    const currentPrice      = priceQuote?.regularMarketPrice ?? getRaw(priceModule.regularMarketPrice) ?? null;
    const marketCap         = getRaw(priceModule.marketCap)   ?? getRaw(summaryDetail.marketCap)   ?? null;
    const sharesOutstanding = getRaw(keyStats.sharesOutstanding) ?? null;
    const beta              = getRaw(keyStats.beta) ?? getRaw(summaryDetail.beta) ?? null;

    // ── TTM margins (from financialData) — used as fallback estimates ──────
    const ttmGrossMargin      = getRaw(financialData.grossMargins);
    const ttmOperatingMargin  = getRaw(financialData.operatingMargins);
    const ttmEbitdaMargin     = getRaw(financialData.ebitdaMargins);
    const ttmProfitMargin     = getRaw(financialData.profitMargins);
    const ttmGrossProfits     = getRaw(financialData.grossProfits);
    const ttmOperatingCF      = getRaw(financialData.operatingCashflow);
    const ttmFreeCashFlow     = getRaw(financialData.freeCashflow);
    const ttmEbitda           = getRaw(financialData.ebitda);
    const ttmTotalDebt        = getRaw(financialData.totalDebt);
    const ttmTotalCash        = getRaw(financialData.totalCash);

    // ── Base years from earnings.financialsChart.yearly ────────────────────
    const yearlyEarnings = earnings?.financialsChart?.yearly || [];
    if (!yearlyEarnings.length) {
      throw new Error(`No earnings history for ${ticker} — check the ticker.`);
    }

    // ── Helper: pick calendar year from a TS row ──────────────────────────
    const rowYear = (r) => {
      const d = r?.date;
      if (!d) return null;
      if (d instanceof Date) return d.getUTCFullYear();
      const parsed = new Date(d);
      return isNaN(parsed) ? null : parsed.getUTCFullYear();
    };

    // ── Aggregate TS rows → per-calendar-year bucket. Annual rows (12M)
    //    overwrite (count=4, one row per year); quarterly rows (3M) are summed.
    const aggregateByYear = (rows, fields) => {
      const byYear = {};
      const list = (Array.isArray(rows) ? rows : []).filter(
        r => r?.periodType === '3M' || r?.periodType === '12M'
      );
      for (const r of list) {
        const y = rowYear(r);
        if (y == null) continue;
        if (!byYear[y]) byYear[y] = { _count: 0 };
        if (r.periodType === '12M') {
          // Annual row — treat as a complete year
          byYear[y]._count = 4;
          for (const f of fields) {
            const v = getRaw(r[f]);
            if (v != null) byYear[y][f] = v;
          }
        } else {
          byYear[y]._count += 1;
          for (const f of fields) {
            const v = getRaw(r[f]);
            if (v == null) continue;
            byYear[y][f] = (byYear[y][f] ?? 0) + v;
          }
        }
      }
      return byYear;
    };

    const finAnnual = aggregateByYear(tsFinancials, [
      'totalRevenue', 'grossProfit', 'operatingIncome', 'netIncome',
      'EBIT', 'EBITDA', 'reconciledDepreciation',
    ]);
    const cfAnnual  = aggregateByYear(tsCashFlow, [
      'operatingCashFlow', 'capitalExpenditure', 'freeCashFlow',
      'depreciationAndAmortization',
    ]);

    // ── Latest balance sheet row (sorted by date desc) ────────────────────
    const balanceRows = (Array.isArray(tsBalance) ? tsBalance : [])
      .slice()
      .sort((a, b) => {
        const da = a?.date instanceof Date ? a.date.getTime() : new Date(a?.date || 0).getTime();
        const db = b?.date instanceof Date ? b.date.getTime() : new Date(b?.date || 0).getTime();
        return db - da;
      });
    const latestBalance = balanceRows[0] || {};

    // Map each balance-sheet row by calendar year for per-year cash/debt fill
    const balanceByYear = {};
    for (const r of balanceRows) {
      const y = rowYear(r);
      if (y == null) continue;
      // keep the latest (first occurrence in desc-sorted list) per year
      if (!balanceByYear[y]) balanceByYear[y] = r;
    }

    // ── Build `years` array ───────────────────────────────────────────────
    const sortedEarnings = yearlyEarnings
      .slice()
      .sort((a, b) => (a.date ?? 0) - (b.date ?? 0));
    const maxYear = sortedEarnings.reduce(
      (m, y) => Math.max(m, parseInt(y.date) || 0),
      0
    );

    const years = sortedEarnings.map(ey => {
      const year = parseInt(ey.date) || null;
      const revenueBase = getRaw(ey.revenue);
      const netIncomeBase = getRaw(ey.earnings);

      const fin = (year != null && finAnnual[year]) || null;
      const cf  = (year != null && cfAnnual[year]) || null;
      const bs  = (year != null && balanceByYear[year]) || null;

      const hasFinQ = fin && fin._count >= 4;
      const hasCfQ  = cf  && cf._count  >= 4;

      // Revenue: prefer TS aggregated (if full year), else earnings chart
      const revenue = hasFinQ && fin.totalRevenue != null ? fin.totalRevenue : revenueBase;

      // Income-statement line items
      let grossProfit = hasFinQ && fin.grossProfit != null ? fin.grossProfit : null;
      let ebit        = hasFinQ && (fin.EBIT != null || fin.operatingIncome != null)
                          ? (fin.EBIT ?? fin.operatingIncome) : null;
      let ebitda      = hasFinQ && fin.EBITDA != null ? fin.EBITDA : null;
      const netIncome = hasFinQ && fin.netIncome != null ? fin.netIncome : netIncomeBase;

      // Cash-flow line items
      let operatingCF  = hasCfQ && cf.operatingCashFlow != null ? cf.operatingCashFlow : null;
      let capex        = hasCfQ && cf.capitalExpenditure != null ? Math.abs(cf.capitalExpenditure) : null;
      let freeCashFlow = hasCfQ && cf.freeCashFlow != null ? cf.freeCashFlow : null;
      let da           = hasCfQ && cf.depreciationAndAmortization != null
                          ? cf.depreciationAndAmortization
                          : (hasFinQ && fin.reconciledDepreciation != null ? fin.reconciledDepreciation : null);

      // ── Fallback: most recent year — use TTM figures directly ─────────
      const isMostRecent = year === maxYear;
      if (isMostRecent) {
        if (grossProfit == null && ttmGrossProfits != null) grossProfit = ttmGrossProfits;
        if (ebitda == null      && ttmEbitda != null)       ebitda      = ttmEbitda;
        if (operatingCF == null && ttmOperatingCF != null)  operatingCF = ttmOperatingCF;
        if (freeCashFlow == null && ttmFreeCashFlow != null) freeCashFlow = ttmFreeCashFlow;
      }

      // ── Fallback: estimate from TTM margins × revenue ─────────────────
      if (revenue != null) {
        if (grossProfit == null && ttmGrossMargin != null)    grossProfit = ttmGrossMargin   * revenue;
        if (ebit == null        && ttmOperatingMargin != null) ebit       = ttmOperatingMargin * revenue;
        if (ebitda == null      && ttmEbitdaMargin != null)    ebitda     = ttmEbitdaMargin    * revenue;
      }

      // Derive FCF if we have OCF and capex but no explicit FCF
      if (freeCashFlow == null && operatingCF != null && capex != null) {
        freeCashFlow = operatingCF - capex;
      }

      // Per-year balance-sheet snapshot (falls back to latest)
      const bsRow = bs || latestBalance;
      const cash      = getRaw(bsRow.cashAndCashEquivalents)
                        ?? getRaw(bsRow.cashCashEquivalentsAndShortTermInvestments)
                        ?? null;
      const totalDebt = getRaw(bsRow.totalDebt)
                        ?? getRaw(bsRow.longTermDebt)
                        ?? null;

      return {
        year,
        revenue,
        grossProfit,
        ebit,
        ebitda,
        netIncome,
        operatingCF,
        capex,
        da,
        freeCashFlow,
        cash,
        totalDebt,
      };
    }).filter(y => y.year != null && y.revenue != null);

    // ── Tax rate: financialData first, else last-year taxRateForCalcs ────
    let taxRate = null;
    const effTax = getRaw(financialData.effectiveTaxRate);
    if (effTax != null) {
      taxRate = Math.min(Math.max(effTax, 0), 0.60);
    } else {
      const finQRows = (Array.isArray(tsFinancials) ? tsFinancials : [])
        .filter(r => r?.periodType === '3M' || r?.periodType === '12M')
        .slice()
        .sort((a, b) => {
          const da = a?.date instanceof Date ? a.date.getTime() : new Date(a?.date || 0).getTime();
          const db = b?.date instanceof Date ? b.date.getTime() : new Date(b?.date || 0).getTime();
          return db - da;
        });
      for (const r of finQRows) {
        const t = getRaw(r.taxRateForCalcs);
        if (t != null) { taxRate = Math.min(Math.max(t, 0), 0.60); break; }
      }
    }

    // ── Top-level totalDebt / totalCash: prefer latest balance sheet ─────
    const topTotalDebt = getRaw(latestBalance.totalDebt)
                         ?? getRaw(latestBalance.longTermDebt)
                         ?? ttmTotalDebt
                         ?? null;
    const topTotalCash = getRaw(latestBalance.cashAndCashEquivalents)
                         ?? getRaw(latestBalance.cashCashEquivalentsAndShortTermInvestments)
                         ?? ttmTotalCash
                         ?? null;

    const data = {
      ticker,
      shortName: priceModule.shortName ?? priceModule.longName ?? ticker,
      currentPrice,
      marketCap,
      sharesOutstanding,
      beta,
      taxRate,
      totalDebt: topTotalDebt,
      totalCash: topTotalCash,
      years,
    };

    FUND_CACHE.set(ticker, { data, time: now });
    res.json(data);
  } catch (err) {
    console.error('[market-data /fundamentals]', ticker, err.message);
    const stale = FUND_CACHE.get(ticker);
    if (stale) return res.json({ ...stale.data, _stale: true, _error: err.message });
    res.status(500).json({ error: err.message });
  }
});

// ── Morning Note — Yahoo Finance ───────────────────────────────────────────────
let MN_CACHE = null, MN_CACHE_TIME = 0;
const MN_TTL = 15 * 60 * 1000;

const YIELD_TICKERS  = { '3M': '^IRX', '2Y': '^TYX', '5Y': '^FVX', '10Y': '^TNX' };
const FUTURE_TICKERS = { 'ES': 'ES=F', 'NQ': 'NQ=F', 'YM': 'YM=F' };
const MOVER_WATCHLIST = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'JPM', 'GS', 'SPY'];

function buildHeadline({ yields, futures, movers }) {
  const lines = [];
  const esPct = futures.find(f => f.label === 'ES')?.changePercent ?? 0;
  if (Math.abs(esPct) >= 0.1) {
    lines.push(`S&P futures ${esPct >= 0 ? 'pointing higher' : 'pointing lower'} (ES ${esPct >= 0 ? '+' : ''}${esPct.toFixed(2)}%)`);
  } else {
    lines.push('Futures near flat');
  }
  const y10 = yields.find(y => y.label === '10Y')?.value;
  const y3m = yields.find(y => y.label === '3M')?.value;
  if (y10 != null && y3m != null) {
    const spread = (y10 - y3m).toFixed(2);
    lines.push(`10Y-3M spread ${spread >= 0 ? '+' : ''}${spread}bps`);
  }
  const sorted = [...movers].sort((a, b) => Math.abs(b.changePercent) - Math.abs(a.changePercent));
  if (sorted.length) {
    const top = sorted[0];
    lines.push(`${top.symbol} ${top.changePercent >= 0 ? '+' : ''}${top.changePercent?.toFixed(2)}% pre-mkt`);
  }
  return lines.join(' · ');
}

router.get('/morning-note', async (req, res) => {
  const now = Date.now();
  if (MN_CACHE && now - MN_CACHE_TIME < MN_TTL) return res.json(MN_CACHE);

  try {
    const yieldEntries = Object.entries(YIELD_TICKERS);
    const yieldQuotes = await Promise.allSettled(
      yieldEntries.map(([, sym]) => yfQuote(sym))
    );
    const yields = yieldEntries.map(([label], i) => {
      const q = yieldQuotes[i].status === 'fulfilled' ? yieldQuotes[i].value : null;
      return {
        label,
        value:         q?.regularMarketPrice        ?? null,
        change:        q?.regularMarketChange       ?? null,
        changePercent: q?.regularMarketChangePercent ?? null,
      };
    });

    const futureEntries = Object.entries(FUTURE_TICKERS);
    const futureQuotes = await Promise.allSettled(
      futureEntries.map(([, sym]) => yfQuote(sym))
    );
    const futures = futureEntries.map(([label], i) => {
      const q = futureQuotes[i].status === 'fulfilled' ? futureQuotes[i].value : null;
      return {
        label,
        value:         q?.regularMarketPrice        ?? null,
        change:        q?.regularMarketChange       ?? null,
        changePercent: q?.regularMarketChangePercent ?? null,
        marketState:   q?.marketState               ?? 'CLOSED',
      };
    });

    const moverQuotes = await Promise.allSettled(
      MOVER_WATCHLIST.map(sym => yfQuote(sym))
    );
    const movers = MOVER_WATCHLIST
      .map((sym, i) => {
        const q = moverQuotes[i].status === 'fulfilled' ? moverQuotes[i].value : null;
        if (!q) return null;
        const hasPreMarket = q.preMarketPrice != null && q.preMarketChangePercent != null;
        return {
          symbol:        sym,
          price:         hasPreMarket ? q.preMarketPrice         : (q.regularMarketPrice        ?? null),
          changePercent: hasPreMarket ? q.preMarketChangePercent : (q.regularMarketChangePercent ?? 0),
          change:        hasPreMarket ? q.preMarketChange        : (q.regularMarketChange        ?? 0),
          isPreMarket:   hasPreMarket,
          shortName:     q.shortName ?? sym,
        };
      })
      .filter(Boolean)
      .sort((a, b) => Math.abs(b.changePercent) - Math.abs(a.changePercent));

    const headline = buildHeadline({ yields, futures, movers });
    const data = { headline, yields, futures, movers, asOf: new Date().toISOString() };

    MN_CACHE = data;
    MN_CACHE_TIME = now;
    res.json(data);
  } catch (err) {
    console.error('[market-data /morning-note]', err.message);
    if (MN_CACHE) return res.json(MN_CACHE);
    res.status(500).json({ error: err.message });
  }
});

export default router;
