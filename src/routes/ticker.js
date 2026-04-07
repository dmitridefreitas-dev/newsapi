import express from 'express';
import Parser from 'rss-parser';

const router = express.Router();

const parser = new Parser({
  timeout: 8000,
  headers: { 'User-Agent': 'Mozilla/5.0 (compatible; DDF-Terminal/1.0; +https://findmitridefreitas.com)' },
});

const EDGAR_HEADERS = {
  'User-Agent': 'DDF-Terminal/1.0 contact@findmitridefreitas.com',
  'Accept': 'application/json',
};

const HIGH_KW = [
  'breaking', 'crash', 'crisis', 'collapse', 'plunge', 'surge', 'soar',
  'fed ', 'federal reserve', 'rate cut', 'rate hike', 'recession',
  'bankrupt', 'fraud', 'sec ', 'investigation', 'merger', 'acquisition',
  'earnings', 'layoff', 'tariff', 'sanction', 'default',
];
const MED_KW = [
  'inflation', 'gdp', 'unemployment', 'jobs report', 'market', 'stocks',
  'bonds', 'treasury', 'powell', 'quarterly', 'forecast', 'outlook',
  'ipo', 'hedge fund', 'private equity', 'wall street',
];

const importance = (text) => {
  const t = text.toLowerCase();
  if (HIGH_KW.some((k) => t.includes(k))) return 'HIGH';
  if (MED_KW.some((k) => t.includes(k))) return 'MED';
  return 'LOW';
};

const clean = (str = '') =>
  str
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();

// CIK map cache — large file, 24h TTL
let cikMapCache = null;
let cikMapCacheTime = 0;
const CIK_CACHE_TTL = 24 * 60 * 60 * 1000;

// Per-ticker caches
const filingsCache = new Map();
const FILINGS_CACHE_TTL = 10 * 60 * 1000;

const newsCache = new Map();
const NEWS_CACHE_TTL = 2 * 60 * 1000;

const RELEVANT_FORMS = new Set([
  '10-K', '10-K/A', '10-Q', '10-Q/A',
  '8-K', '8-K/A',
  'DEF 14A', 'DEFA14A', 'DEFC14A',
  'S-1', 'S-1/A', 'S-3', 'S-3/A', 'S-4', 'S-4/A',
  '20-F', '20-F/A',
  'SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A',
  '424B4', '424B3',
]);

async function getCIKInfo(ticker) {
  const now = Date.now();
  if (!cikMapCache || now - cikMapCacheTime > CIK_CACHE_TTL) {
    const res = await fetch('https://www.sec.gov/files/company_tickers.json', {
      headers: EDGAR_HEADERS,
    });
    if (!res.ok) throw new Error('Failed to fetch CIK map from SEC');
    cikMapCache = await res.json();
    cikMapCacheTime = now;
  }
  const upper = ticker.toUpperCase();
  const entry = Object.values(cikMapCache).find((c) => c.ticker === upper);
  if (!entry) return null;
  return {
    cik: String(entry.cik_str),
    paddedCik: String(entry.cik_str).padStart(10, '0'),
    name: entry.title,
    ticker: entry.ticker,
  };
}

const BLOOMBERG_FEEDS = [
  'https://feeds.bloomberg.com/markets/news.rss',
  'https://feeds.bloomberg.com/technology/news.rss',
  'https://feeds.bloomberg.com/business/news.rss',
];

// GET /ticker/news?ticker=AAPL
router.get('/news', async (req, res) => {
  const ticker = (req.query.ticker || '').toUpperCase().trim();
  if (!ticker) return res.status(400).json({ error: 'ticker is required' });

  const now = Date.now();
  const cached = newsCache.get(ticker);
  if (cached && now - cached.time < NEWS_CACHE_TTL) return res.json(cached.data);

  // Get company name for Bloomberg filtering (uses cached CIK map — fast after first call)
  let companyKeyword = ticker.toLowerCase();
  try {
    const info = await getCIKInfo(ticker);
    if (info?.name) {
      // "Apple Inc." → "apple", "NVIDIA Corporation" → "nvidia"
      companyKeyword = info.name.split(/[\s,.(]/)[0].toLowerCase();
    }
  } catch { /* non-fatal */ }

  const [yahooResult, ...bloombergResults] = await Promise.allSettled([
    parser.parseURL(
      `https://feeds.finance.yahoo.com/rss/2.0/headline?s=${encodeURIComponent(ticker)}&region=US&lang=en-US`
    ),
    ...BLOOMBERG_FEEDS.map((url) => parser.parseURL(url)),
  ]);

  const seen = new Set();
  const items = [];

  const pushItem = (rawItem, source) => {
    const title = clean(rawItem.title || '');
    if (!title || seen.has(title)) return;
    seen.add(title);
    items.push({
      id: rawItem.guid || rawItem.link || title,
      title,
      summary: clean(rawItem.contentSnippet || rawItem.summary || rawItem.content || '').slice(0, 300),
      link: rawItem.link || '',
      source,
      publishedAt: rawItem.isoDate || rawItem.pubDate || new Date().toISOString(),
      importance: importance((rawItem.title || '') + ' ' + (rawItem.contentSnippet || '')),
    });
  };

  // Yahoo Finance — all items are ticker-specific
  if (yahooResult.status === 'fulfilled') {
    yahooResult.value.items.slice(0, 30).forEach((i) => pushItem(i, 'YAHOO FIN'));
  }

  // Bloomberg — filter to items mentioning the ticker symbol or company name
  bloombergResults.forEach((result) => {
    if (result.status !== 'fulfilled') return;
    result.value.items.slice(0, 30).forEach((item) => {
      const text = (item.title + ' ' + (item.contentSnippet || '')).toLowerCase();
      if (text.includes(ticker.toLowerCase()) || text.includes(companyKeyword)) {
        pushItem(item, 'BLOOMBERG');
      }
    });
  });

  items.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));

  newsCache.set(ticker, { data: items, time: now });
  res.json(items);
});

// GET /ticker/filings?ticker=AAPL
router.get('/filings', async (req, res) => {
  const ticker = (req.query.ticker || '').toUpperCase().trim();
  if (!ticker) return res.status(400).json({ error: 'ticker is required' });

  const now = Date.now();
  const cached = filingsCache.get(ticker);
  if (cached && now - cached.time < FILINGS_CACHE_TTL) return res.json(cached.data);

  try {
    const info = await getCIKInfo(ticker);
    if (!info) {
      return res.status(404).json({ error: `Ticker "${ticker}" not found in EDGAR` });
    }

    const subUrl = `https://data.sec.gov/submissions/CIK${info.paddedCik}.json`;
    const subRes = await fetch(subUrl, { headers: EDGAR_HEADERS });
    if (!subRes.ok) throw new Error('EDGAR submissions API returned an error');
    const sub = await subRes.json();

    const recent = sub.filings?.recent || {};
    const accNums    = recent.accessionNumber      || [];
    const forms      = recent.form                 || [];
    const filDates   = recent.filingDate           || [];
    const repDates   = recent.reportDate           || [];
    const primDocs   = recent.primaryDocument      || [];
    const primDescs  = recent.primaryDocDescription || [];
    const sizes      = recent.size                 || [];
    const itemsList  = recent.items                || [];

    const filings = accNums
      .map((acc, i) => {
        const accNoDash = acc.replace(/-/g, '');
        const doc = primDocs[i] || '';
        return {
          accessionNumber: acc,
          form: forms[i] || '',
          filingDate: filDates[i] || '',
          reportDate: repDates[i] || '',
          description: primDescs[i] || forms[i] || '',
          items: itemsList[i] || '',
          size: sizes[i] || 0,
          // Prefer direct document link; fall back to index
          fileUrl: doc
            ? `https://www.sec.gov/Archives/edgar/data/${info.cik}/${accNoDash}/${doc}`
            : `https://www.sec.gov/Archives/edgar/data/${info.cik}/${accNoDash}/`,
          indexUrl: `https://www.sec.gov/Archives/edgar/data/${info.cik}/${accNoDash}/`,
        };
      })
      .filter((f) => RELEVANT_FORMS.has(f.form))
      .slice(0, 300);

    const result = {
      ticker: info.ticker,
      name: sub.name || info.name,
      cik: info.cik,
      sic: sub.sic || '',
      sicDescription: sub.sicDescription || '',
      exchanges: sub.exchanges || [],
      filings,
    };

    filingsCache.set(ticker, { data: result, time: now });
    res.json(result);
  } catch (err) {
    res.status(502).json({ error: 'Failed to fetch EDGAR data', detail: err.message });
  }
});

export default router;
