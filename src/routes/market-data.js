import express from 'express';
import YahooFinance from 'yahoo-finance2';

const router = express.Router();
const yf = new YahooFinance({ suppressNotices: ['yahooSurvey'] });

// ── Request queues — serialize external calls to avoid burst rate-limits ───────
// AV free tier: 5 req/min. Yahoo: IP-rate-limits on burst. Both need throttling.
const delay = (ms) => new Promise(r => setTimeout(r, ms));

function makeQueue(gapMs) {
  let tail = Promise.resolve();
  return function enqueue(fn) {
    const result = tail.then(() => fn());
    tail = result.then(() => delay(gapMs), () => delay(gapMs));
    return result;
  };
}

// 1 AV call every 13s → max ~4.6/min, safely under the 5/min cap
const avQueue  = makeQueue(13000);
// 1 Yahoo call every 400ms — spreads burst, avoids crumb hammering
const yfQueue  = makeQueue(400);

// ── Alpha Vantage helpers ──────────────────────────────────────────────────────
const AV_BASE = 'https://www.alphavantage.co/query';

function getAvKey() {
  const k = process.env.ALPHA_VANTAGE_KEY;
  if (!k) throw new Error('ALPHA_VANTAGE_KEY environment variable is not set');
  return k;
}

async function avFetch(params) {
  return avQueue(async () => {
    const url = new URL(AV_BASE);
    url.searchParams.set('apikey', getAvKey());
    for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`Alpha Vantage HTTP ${res.status}`);
    const data = await res.json();
    if (data.Note || data.Information) {
      throw new Error('Alpha Vantage rate limit — try again in a minute');
    }
    if (data['Error Message']) throw new Error(`Alpha Vantage: ${data['Error Message']}`);
    return data;
  });
}

async function yfQuote(symbol) {
  return yfQueue(() => yf.quote(symbol, {}, { validateResult: false }));
}

async function yfOptions(symbol, opts) {
  return yfQueue(() => yf.options(symbol, opts || {}, { validateResult: false }));
}

function parseNum(v) {
  if (v == null || v === 'None' || v === '-' || v === '') return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}

// Fetch full monthly adjusted series; returns sorted [{date:'YYYY-MM-DD', adjClose}]
async function avMonthly(symbol) {
  const data = await avFetch({ function: 'TIME_SERIES_MONTHLY_ADJUSTED', symbol });
  const series = data['Monthly Adjusted Time Series'] || {};
  return Object.entries(series)
    .map(([date, v]) => ({ date, adjClose: parseNum(v['5. adjusted close']) }))
    .filter(r => r.adjClose != null)
    .sort((a, b) => a.date.localeCompare(b.date));
}

// Fetch full daily adjusted series; returns sorted [{date:'YYYY-MM-DD', adjClose}]
async function avDaily(symbol) {
  const data = await avFetch({ function: 'TIME_SERIES_DAILY_ADJUSTED', symbol, outputsize: 'full' });
  const series = data['Time Series (Daily)'] || {};
  return Object.entries(series)
    .map(([date, v]) => ({ date, adjClose: parseNum(v['5. adjusted close']) }))
    .filter(r => r.adjClose != null)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function dateKey(dateStr) {
  return dateStr.slice(0, 7); // "YYYY-MM-DD" -> "YYYY-MM"
}

// ── Main header tickers — Yahoo Finance (index symbols not supported by AV) ───
// ^GSPC = S&P 500, ^VIX = VIX, ^TNX = 10Y Treasury, ^NDX = Nasdaq 100
const SYMBOLS = ['^GSPC', '^VIX', '^TNX', '^NDX'];
let cache = null, cacheTime = 0;
const CACHE_TTL = 15 * 60 * 1000; // 15 min

router.get('/', async (req, res) => {
  const now = Date.now();
  if (cache && now - cacheTime < CACHE_TTL) return res.json(cache);

  try {
    // yfQueue serializes these with 400ms gaps — no burst on cold start
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
    if (cache) return res.json(cache); // serve stale on error
    res.status(500).json({ error: err.message });
  }
});

// ── Single quote — Alpha Vantage GLOBAL_QUOTE ─────────────────────────────────
// (Yahoo Finance is rate-limited by IP on Render's shared infrastructure)
// GET /market-data/quote?symbol=SPY
const QUOTE_CACHE = new Map();
const QUOTE_TTL = 10 * 60 * 1000; // 10 min

router.get('/quote', async (req, res) => {
  const symbol = (req.query.symbol || 'SPY').toUpperCase();
  const now = Date.now();
  const cached = QUOTE_CACHE.get(symbol);
  if (cached && now - cached.time < QUOTE_TTL) return res.json(cached.data);

  try {
    const raw = await avFetch({ function: 'GLOBAL_QUOTE', symbol });
    const q = raw['Global Quote'] || {};
    if (!q['05. price']) throw new Error(`No quote data for ${symbol}`);

    const price         = parseNum(q['05. price']);
    const previousClose = parseNum(q['08. previous close']);
    const change        = parseNum(q['09. change']);
    const changePct     = parseNum((q['10. change percent'] || '').replace('%', ''));
    const volume        = parseNum(q['06. volume']);

    const data = {
      symbol,
      price,
      previousClose,
      change,
      changePercent:           changePct,
      bid:                     null,
      ask:                     null,
      volume,
      dayHigh:                 null,
      dayLow:                  null,
      fiftyTwoWeekHigh:        null,
      fiftyTwoWeekLow:         null,
      marketState:             null,
      shortName:               symbol,
      postMarketPrice:         null,
      postMarketChange:        null,
      postMarketChangePercent: null,
      postMarketTime:          null,
      preMarketPrice:          null,
      preMarketChange:         null,
      preMarketChangePercent:  null,
      preMarketTime:           null,
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

// ── Monthly historical — Alpha Vantage TIME_SERIES_MONTHLY_ADJUSTED ───────────
// GET /market-data/monthly?tickers=AAPL,MSFT,SPY&months=36
const MONTHLY_CACHE = new Map();
const MONTHLY_TTL = 60 * 60 * 1000; // 1 h

router.get('/monthly', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 10);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });

  const months  = Math.min(parseInt(req.query.months) || 36, 120);
  const cacheKey = `${tickers.join(',')}_${months}`;
  const now = Date.now();
  const cached = MONTHLY_CACHE.get(cacheKey);
  if (cached && now - cached.time < MONTHLY_TTL) return res.json(cached.data);

  const results = {}, errors = {};
  await Promise.allSettled(tickers.map(async (ticker) => {
    try {
      const rows = await avMonthly(ticker);
      if (rows.length < 3) { errors[ticker] = 'Insufficient data'; return; }
      // Take last (months + 1) rows so we can produce (months) return observations
      const slice = rows.slice(-(months + 1));
      const out = [];
      for (let i = 1; i < slice.length; i++) {
        out.push({
          date:     dateKey(slice[i].date),
          adjClose: +slice[i].adjClose.toFixed(4),
          ret:      +(slice[i].adjClose / slice[i - 1].adjClose - 1).toFixed(6),
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

// ── Daily historical — Alpha Vantage TIME_SERIES_DAILY_ADJUSTED ───────────────
// GET /market-data/daily?tickers=AAPL,SPY&start=2023-01-01&end=2024-06-01
const DAILY_CACHE = new Map();
const DAILY_TTL   = 30 * 60 * 1000; // 30 min

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
      const rows = await avDaily(ticker);
      const filtered = rows.filter(r => r.date >= start && r.date <= end);
      if (filtered.length < 5) { errors[ticker] = 'Insufficient data'; return; }
      results[ticker] = filtered.map(r => ({
        date:     r.date,
        adjClose: +r.adjClose.toFixed(4),
      }));
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors };
  DAILY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// ── Fama-French factor proxies — Alpha Vantage ────────────────────────────────
// GET /market-data/ff-proxy?months=60
// MKT=SPY-BIL, SMB=IWM-SPY, HML=SPYV-SPYG
const FF_TICKERS = ['SPY', 'IWM', 'SPYV', 'SPYG', 'BIL'];
let FF_CACHE = null, FF_CACHE_TIME = 0;
const FF_TTL = 6 * 60 * 60 * 1000; // 6 h

router.get('/ff-proxy', async (req, res) => {
  const months = Math.min(parseInt(req.query.months) || 60, 120);
  const now = Date.now();
  if (FF_CACHE && now - FF_CACHE_TIME < FF_TTL && FF_CACHE.length >= months)
    return res.json(FF_CACHE.slice(-months));

  try {
    const allData = {};
    await Promise.all(FF_TICKERS.map(async (t) => {
      const rows = await avMonthly(t);
      const rets = [];
      for (let i = 1; i < rows.length; i++) {
        rets.push({
          date: dateKey(rows[i].date),
          ret:  rows[i].adjClose / rows[i - 1].adjClose - 1,
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

// ── Options chain — Yahoo Finance (AV options requires a paid plan) ────────────
// GET /market-data/options?ticker=QQQ
router.get('/options', async (req, res) => {
  const ticker = (req.query.ticker || 'QQQ').toUpperCase();
  try {
    const base = await yfOptions(ticker);
    const expiryDates = base.expirationDates || [];
    if (!expiryDates.length) return res.status(404).json({ error: 'No options data found for ' + ticker });

    const spotQuote = await yfQuote(ticker);
    const spot = spotQuote.regularMarketPrice;

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

    res.json({ ticker, spot, marketState: spotQuote.marketState ?? 'CLOSED', surface });
  } catch (err) {
    console.error('[market-data /options]', ticker, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Fundamentals — Alpha Vantage ──────────────────────────────────────────────
// GET /market-data/fundamentals?ticker=AAPL
const FUND_CACHE = new Map();
const FUND_TTL = 6 * 60 * 60 * 1000; // 6 h

router.get('/fundamentals', async (req, res) => {
  const ticker = (req.query.ticker || 'AAPL').toUpperCase();
  const now = Date.now();
  const cached = FUND_CACHE.get(ticker);
  if (cached && now - cached.time < FUND_TTL) return res.json(cached.data);

  try {
    const [overview, income, cashflow, balance, quoteRaw] = await Promise.all([
      avFetch({ function: 'OVERVIEW',         symbol: ticker }),
      avFetch({ function: 'INCOME_STATEMENT', symbol: ticker }),
      avFetch({ function: 'CASH_FLOW',        symbol: ticker }),
      avFetch({ function: 'BALANCE_SHEET',    symbol: ticker }),
      avFetch({ function: 'GLOBAL_QUOTE',     symbol: ticker }),
    ]);

    const incomeReports  = income.annualReports  || [];
    const cashReports    = cashflow.annualReports || [];
    const balanceReports = balance.annualReports  || [];

    const cashMap    = Object.fromEntries(cashReports.map(r   => [r.fiscalDateEnding, r]));
    const balanceMap = Object.fromEntries(balanceReports.map(r => [r.fiscalDateEnding, r]));

    const years = incomeReports
      .filter(r => r.totalRevenue && r.totalRevenue !== 'None')
      .sort((a, b) => a.fiscalDateEnding.localeCompare(b.fiscalDateEnding))
      .map(inc => {
        const cf = cashMap[inc.fiscalDateEnding]    || {};
        const bs = balanceMap[inc.fiscalDateEnding] || {};

        const operatingCF = parseNum(cf.operatingCashflow);
        const capexRaw    = parseNum(cf.capitalExpenditures);
        const capex       = capexRaw != null ? Math.abs(capexRaw) : null;
        const da          = parseNum(cf.depreciationDepletionAndAmortization)
                         ?? parseNum(inc.depreciationAndAmortization);
        const fcf         = operatingCF != null && capex != null ? operatingCF - capex : null;

        return {
          year:         parseInt(inc.fiscalDateEnding.slice(0, 4)),
          revenue:      parseNum(inc.totalRevenue),
          grossProfit:  parseNum(inc.grossProfit),
          ebit:         parseNum(inc.ebit) ?? parseNum(inc.operatingIncome),
          ebitda:       parseNum(inc.ebitda),
          netIncome:    parseNum(inc.netIncome),
          operatingCF,
          capex,
          da,
          freeCashFlow: fcf,
          cash:      parseNum(bs.cashAndShortTermInvestments)
                  ?? parseNum(bs.cashAndCashEquivalentsAtCarryingValue),
          totalDebt: parseNum(bs.shortLongTermDebtTotal) ?? parseNum(bs.longTermDebt),
        };
      })
      .filter(y => y.revenue != null);

    // Effective tax rate from most recent annual report
    const lastInc      = [...incomeReports].sort((a, b) => b.fiscalDateEnding.localeCompare(a.fiscalDateEnding))[0] || {};
    const incomeBefore = parseNum(lastInc.incomeBeforeTax);
    const incomeTax    = parseNum(lastInc.incomeTaxExpense);
    const taxRate      = incomeBefore && incomeTax && incomeBefore > 0
      ? Math.min(incomeTax / incomeBefore, 0.60)
      : null;

    const gq = quoteRaw['Global Quote'] || {};
    const currentPrice = parseNum(gq['05. price']);

    const latestBs = [...balanceReports].sort((a, b) =>
      b.fiscalDateEnding.localeCompare(a.fiscalDateEnding))[0] || {};

    const data = {
      ticker,
      shortName:         overview.Name || ticker,
      currentPrice,
      marketCap:         parseNum(overview.MarketCapitalization),
      sharesOutstanding: parseNum(overview.SharesOutstanding),
      beta:              parseNum(overview.Beta),
      taxRate,
      totalDebt:  parseNum(latestBs.shortLongTermDebtTotal) ?? parseNum(latestBs.longTermDebt),
      totalCash:  parseNum(latestBs.cashAndShortTermInvestments)
               ?? parseNum(latestBs.cashAndCashEquivalentsAtCarryingValue),
      years,
    };

    FUND_CACHE.set(ticker, { data, time: now });
    res.json(data);
  } catch (err) {
    console.error('[market-data /fundamentals]', ticker, err.message);
    const stale = FUND_CACHE.get(ticker);
    if (stale) return res.json(stale.data);
    res.status(500).json({ error: err.message });
  }
});

// ── Morning Note — Yahoo Finance (needs futures + yield curve, not in AV) ─────
// GET /market-data/morning-note
let MN_CACHE = null, MN_CACHE_TIME = 0;
const MN_TTL = 15 * 60 * 1000; // 15 min

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
    if (MN_CACHE) return res.json(MN_CACHE); // serve stale on error
    res.status(500).json({ error: err.message });
  }
});

export default router;
