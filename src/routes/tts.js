import 'dotenv/config';
import { Router } from 'express';

const router = Router();

function getIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.length > 0) {
    return forwarded.split(',')[0].trim();
  }
  return req.socket?.remoteAddress || 'unknown';
}

const rateLimitStore = new Map();
function isRateLimited(ip) {
  const now = Date.now();
  const windowMs = 60_000;
  const maxReq = 20;
  const existing = rateLimitStore.get(ip) || [];
  const fresh = existing.filter((t) => now - t < windowMs);
  fresh.push(now);
  rateLimitStore.set(ip, fresh);
  return fresh.length > maxReq;
}

router.post('/', async (req, res) => {
  const ip = getIp(req);
  if (isRateLimited(ip)) return res.status(429).json({ error: 'rate_limited' });

  try {
    const key = process.env.ELEVENLABS_API_KEY;
    if (!key) return res.status(500).json({ error: 'missing_api_key' });

    const { text } = req.body || {};
    if (!text) return res.status(400).json({ error: 'missing_text' });

    const RACHEL_ID = 'EXAVITQu4vr4xnSDxMaL'; // Bella — free-tier default voice
    const elResp = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${RACHEL_ID}/stream`, {
      method: 'POST',
      headers: {
        'xi-api-key': key,
        'Content-Type': 'application/json',
        'Accept': 'audio/mpeg',
      },
      body: JSON.stringify({
        text: String(text).slice(0, 1000),
        model_id: 'eleven_multilingual_v2',
        voice_settings: { stability: 0.4, similarity_boost: 0.8, style: 0.2, use_speaker_boost: true },
      }),
    });

    if (!elResp.ok) {
      const errBody = await elResp.json().catch(() => ({}));
      console.error('[tts] ElevenLabs error:', elResp.status, JSON.stringify(errBody));
      return res.status(502).json({ error: 'upstream_error', detail: errBody });
    }

    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Transfer-Encoding', 'chunked');

    const reader = elResp.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
    console.log(`[tts] ok ip=${ip}`);
  } catch (err) {
    console.error('[tts] error:', err.message);
    return res.status(500).json({ error: 'server_error' });
  }
});

export default router;
