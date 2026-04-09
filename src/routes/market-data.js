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

  try {
    const quotes = await Promise.all(
      SYMBOLS.map(s => yf.quote(s, {}, { validateResult: false }))
    );
    const [spx, vix, t10y, ndx] = quotes;

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
    res.status(500).json({ error: err.message });
  }
});

// GET /market-data/quote?symbol=SPY  — single symbol quote + market state
router.get('/quote', async (req, res) => {
  const symbol = (req.query.symbol || 'SPY').toUpperCase();
  try {
    const quote = await yf.quote(symbol, {}, { validateResult: false });
    res.json({
      symbol,
      price:          quote.regularMarketPrice          ?? null,
      previousClose:  quote.regularMarketPreviousClose  ?? null,
      change:         quote.regularMarketChange         ?? null,
      changePercent:  quote.regularMarketChangePercent  ?? null,
      bid:            quote.bid                         ?? null,
      ask:            quote.ask                         ?? null,
      volume:         quote.regularMarketVolume         ?? null,
      dayHigh:        quote.regularMarketDayHigh        ?? null,
      dayLow:         quote.regularMarketDayLow         ?? null,
      fiftyTwoWeekHigh: quote.fiftyTwoWeekHigh          ?? null,
      fiftyTwoWeekLow:  quote.fiftyTwoWeekLow           ?? null,
      marketState:    quote.marketState                 ?? 'CLOSED',
      shortName:      quote.shortName                   ?? symbol,
      postMarketPrice:         quote.postMarketPrice         ?? null,
      postMarketChange:        quote.postMarketChange        ?? null,
      postMarketChangePercent: quote.postMarketChangePercent ?? null,
      postMarketTime:          quote.postMarketTime          ?? null,
      preMarketPrice:          quote.preMarketPrice          ?? null,
      preMarketChange:         quote.preMarketChange         ?? null,
      preMarketChangePercent:  quote.preMarketChangePercent  ?? null,
      preMarketTime:           quote.preMarketTime           ?? null,
    });
  } catch (err) {
    console.error('[market-data /quote]', symbol, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Historical data caches ────────────────────────────────────────────────────────
const MONTHLY_CACHE = new Map();
const DAILY_CACHE   = new Map();
let   FF_CACHE = null, FF_CACHE_TIME = 0;
const MONTHLY_TTL = 60 * 60 * 1000;    // 1 h
const DAILY_TTL   = 30 * 60 * 1000;    // 30 min
const FF_TTL      =  6 * 60 * 60 * 1000; // 6 h

function dateKey(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

// GET /market-data/monthly?tickers=AAPL,MSFT,SPY&months=36
router.get('/monthly', async (req, res) => {
  const tickers = (req.query.tickers || '')
    .split(',').map(t => t.trim().toUpperCase()).filter(Boolean).slice(0, 10);
  if (!tickers.length) return res.status(400).json({ error: 'tickers required' });

  const months  = Math.min(parseInt(req.query.months) || 36, 120);
  const cacheKey = `${tickers.join(',')}_${months}`;
  const now = Date.now();
  const cached = MONTHLY_CACHE.get(cacheKey);
  if (cached && now - cached.time < MONTHLY_TTL) return res.json(cached.data);

  const period2 = new Date();
  const period1 = new Date(); period1.setMonth(period1.getMonth() - months - 3);

  const results = {}, errors = {};
  await Promise.allSettled(tickers.map(async (ticker) => {
    try {
      const rows = await yf.historical(ticker, {
        period1: period1.toISOString().slice(0, 10),
        period2: period2.toISOString().slice(0, 10),
        interval: '1mo',
      });
      const pr = rows.filter(r => r.adjClose != null)
        .sort((a, b) => new Date(a.date) - new Date(b.date));
      if (pr.length < 3) { errors[ticker] = 'Insufficient data'; return; }
      const out = [];
      for (let i = 1; i < pr.length; i++) {
        out.push({
          date: dateKey(new Date(pr[i].date)),
          adjClose: +pr[i].adjClose.toFixed(4),
          ret: +(pr[i].adjClose / pr[i - 1].adjClose - 1).toFixed(6),
        });
      }
      results[ticker] = out;
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors };
  MONTHLY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// GET /market-data/daily?tickers=AAPL,SPY&start=2023-01-01&end=2024-06-01
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
      const rows = await yf.historical(ticker, {
        period1: start, period2: end, interval: '1d',
      });
      const pr = rows.filter(r => r.adjClose != null)
        .sort((a, b) => new Date(a.date) - new Date(b.date));
      if (pr.length < 5) { errors[ticker] = 'Insufficient data'; return; }
      results[ticker] = pr.map(r => ({
        date: new Date(r.date).toISOString().slice(0, 10),
        adjClose: +r.adjClose.toFixed(4),
      }));
    } catch (err) { errors[ticker] = err.message; }
  }));

  const data = { data: results, errors };
  DAILY_CACHE.set(cacheKey, { data, time: now });
  res.json(data);
});

// GET /market-data/ff-proxy?months=60
// Returns FF3 factor proxies: MKT=SPY-BIL, SMB=IWM-SPY, HML=SPYV-SPYG
router.get('/ff-proxy', async (req, res) => {
  const months = Math.min(parseInt(req.query.months) || 60, 120);
  const now = Date.now();
  if (FF_CACHE && now - FF_CACHE_TIME < FF_TTL && FF_CACHE.length >= months)
    return res.json(FF_CACHE.slice(-months));

  const TICKERS = ['SPY', 'IWM', 'SPYV', 'SPYG', 'BIL'];
  const period2 = new Date();
  const period1 = new Date(); period1.setMonth(period1.getMonth() - months - 3);

  const allData = {};
  await Promise.all(TICKERS.map(async (t) => {
    const rows = await yf.historical(t, {
      period1: period1.toISOString().slice(0, 10),
      period2: period2.toISOString().slice(0, 10),
      interval: '1mo',
    });
    const pr = rows.filter(r => r.adjClose != null)
      .sort((a, b) => new Date(a.date) - new Date(b.date));
    const rets = [];
    for (let i = 1; i < pr.length; i++) {
      rets.push({ date: dateKey(new Date(pr[i].date)), ret: pr[i].adjClose / pr[i - 1].adjClose - 1 });
    }
    allData[t] = rets;
  }));

  const spyDates = (allData['SPY'] || []).map(r => r.date);
  const factors = spyDates
    .filter(d => TICKERS.every(t => allData[t]?.some(r => r.date === d)))
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

  FF_CACHE = factors; FF_CACHE_TIME = now;
  res.json(factors.slice(-months));
});

// GET /market-data/options?ticker=QQQ
// Returns vol smile data for up to 8 expirations
router.get('/options', async (req, res) => {
  const ticker = (req.query.ticker || 'QQQ').toUpperCase();
  try {
    // Fetch available expiry dates
    const base = await yf.options(ticker, {}, { validateResult: false });
    const expiryDates = base.expirationDates || [];
    if (!expiryDates.length) return res.status(404).json({ error: 'No options data found for ' + ticker });

    const spotQuote = await yf.quote(ticker, {}, { validateResult: false });
    const spot = spotQuote.regularMarketPrice;

    // Fetch chains for first 8 expirations
    const selected = expiryDates.slice(0, 8);
    const now = new Date();

    const toDate = (d) => d instanceof Date ? d : new Date(d * 1000);

    const chains = await Promise.allSettled(
      selected.map(d => yf.options(ticker, { date: toDate(d) }, { validateResult: false }))
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

      // Use OTM side: puts for K < spot, calls for K >= spot
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

    res.json({ ticker, spot, surface });
  } catch (err) {
    console.error('[market-data /options]', ticker, err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /market-data/fundamentals?ticker=AAPL
// Returns 3-statement historical data + key metrics for DCF
router.get('/fundamentals', async (req, res) => {
  const ticker = (req.query.ticker || 'AAPL').toUpperCase();
  try {
    const summary = await yf.quoteSummary(ticker, {
      modules: [
        'incomeStatementHistory',
        'balanceSheetHistory',
        'cashFlowStatementHistory',
        'financialData',
        'defaultKeyStatistics',
        'price',
      ],
    }, { validateResult: false });

    const incomeStmts  = summary.incomeStatementHistory?.incomeStatementHistory  || [];
    const balanceSheets = summary.balanceSheetHistory?.balanceSheetHistory         || [];
    const cashFlows    = summary.cashFlowStatementHistory?.cashFlowStatementHistory || [];
    const fd  = summary.financialData       || {};
    const ks  = summary.defaultKeyStatistics || {};
    const pr  = summary.price               || {};

    const toYear = (d) => {
      if (!d) return null;
      if (d instanceof Date) return d.getFullYear();
      const ms = d > 1e10 ? d : d * 1000;
      return new Date(ms).getFullYear();
    };

    const years = incomeStmts.map((is, i) => {
      const bs = balanceSheets[i] || {};
      const cf = cashFlows[i]    || {};
      const capex = cf.capitalExpenditures ?? null;
      return {
        year:             toYear(is.endDate),
        revenue:          is.totalRevenue          ?? null,
        grossProfit:      is.grossProfit            ?? null,
        ebit:             is.ebit                   ?? null,
        netIncome:        is.netIncome              ?? null,
        interestExpense:  is.interestExpense        ?? null,
        incomeTaxExpense: is.incomeTaxExpense       ?? null,
        // Cash flow
        operatingCF:      cf.totalCashFromOperatingActivities ?? null,
        capex:            capex !== null ? Math.abs(capex) : null,  // store as positive
        da:               cf.depreciation           ?? null,
        // Balance sheet
        cash:             bs.cash                   ?? null,
        totalDebt:        (bs.longTermDebt ?? 0) + (bs.shortLongTermDebt ?? 0) || null,
        totalCurrentAssets:      bs.totalCurrentAssets      ?? null,
        totalCurrentLiabilities: bs.totalCurrentLiabilities ?? null,
      };
    }).filter(y => y.year !== null).reverse(); // chronological

    res.json({
      ticker,
      shortName:         pr.shortName              ?? ticker,
      currentPrice:      pr.regularMarketPrice     ?? null,
      marketCap:         pr.marketCap              ?? null,
      sharesOutstanding: ks.sharesOutstanding      ?? null,
      beta:              ks.beta                   ?? null,
      taxRate:           fd.effectiveTaxRate       ?? null,
      totalDebt:         fd.totalDebt              ?? null,
      totalCash:         fd.totalCash              ?? null,
      years,
    });
  } catch (err) {
    console.error('[market-data /fundamentals]', ticker, err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
