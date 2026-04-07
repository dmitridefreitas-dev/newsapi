import express from 'express';
import Parser from 'rss-parser';

const router = express.Router();

const parser = new Parser({
  timeout: 8000,
  headers: { 'User-Agent': 'Mozilla/5.0 (compatible; DDF-Terminal/1.0; +https://findmitridefreitas.com)' },
  customFields: { item: ['media:thumbnail', 'media:content'] },
});

const FEEDS = [
  { url: 'https://feeds.bloomberg.com/markets/news.rss',                  source: 'BLOOMBERG'   },
  { url: 'https://feeds.bloomberg.com/technology/news.rss',               source: 'BLOOMBERG'   },
  { url: 'https://feeds.reuters.com/reuters/businessNews',                source: 'REUTERS'     },
  { url: 'https://www.cnbc.com/id/10001147/device/rss/rss.html',          source: 'CNBC'        },
  { url: 'https://feeds.marketwatch.com/marketwatch/topstories',          source: 'MARKETWATCH' },
  { url: 'https://finance.yahoo.com/news/rssindex',                       source: 'YAHOO FIN'   },
  { url: 'https://www.investing.com/rss/news_25.rss',                     source: 'INVESTING'   },
  { url: 'https://www.ft.com/markets?format=rss',                         source: 'FT'          },
  { url: 'https://www.theguardian.com/us/business/rss',                   source: 'GUARDIAN'    },
];

const HIGH = [
  'breaking', 'crash', 'crisis', 'collapse', 'plunge', 'surge', 'soar',
  'fed ', 'federal reserve', 'rate cut', 'rate hike', 'recession',
  'bankrupt', 'fraud', 'sec ', 'investigation', 'merger', 'acquisition',
  'earnings', 'layoff', 'tariff', 'sanction', 'default',
];
const MED = [
  'inflation', 'gdp', 'unemployment', 'jobs report', 'market', 'stocks',
  'bonds', 'treasury', 'powell', 'quarterly', 'forecast', 'outlook',
  'ipo', 'hedge fund', 'private equity', 'wall street',
];

const importance = (text) => {
  const t = text.toLowerCase();
  if (HIGH.some((k) => t.includes(k))) return 'HIGH';
  if (MED.some((k) => t.includes(k))) return 'MED';
  return 'LOW';
};

const clean = (str = '') =>
  str.replace(/<[^>]+>/g, '').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").trim();

let cache = null;
let cacheTime = 0;
const CACHE_TTL = 2 * 60 * 1000; // 2 minutes for freshness

router.get('/', async (req, res) => {
  const now = Date.now();
  if (cache && now - cacheTime < CACHE_TTL) {
    return res.json(cache);
  }

  const results = await Promise.allSettled(
    FEEDS.map(async (feed) => {
      const parsed = await parser.parseURL(feed.url);
      return parsed.items.slice(0, 10).map((item) => ({
        id: item.guid || item.link || item.title,
        title: clean(item.title || ''),
        summary: clean(item.contentSnippet || item.summary || item.content || '').slice(0, 300),
        link: item.link || '',
        source: feed.source,
        publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
        importance: importance((item.title || '') + ' ' + (item.contentSnippet || '')),
      }));
    })
  );

  const seen = new Set();
  const items = results
    .filter((r) => r.status === 'fulfilled')
    .flatMap((r) => r.value)
    .filter((item) => {
      if (!item.title || seen.has(item.title)) return false;
      seen.add(item.title);
      return true;
    })
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt))
    .slice(0, 40);

  cache = items;
  cacheTime = now;
  res.json(items);
});

export default router;
