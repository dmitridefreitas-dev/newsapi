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
  const maxReq = 10;
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
    const key = process.env.GROQ_API_KEY;
    if (!key) return res.status(500).json({ error: 'missing_api_key' });

    const { audio, mimeType = 'audio/webm' } = req.body || {};
    if (!audio) return res.status(400).json({ error: 'missing_audio' });

    // Strip data URL prefix if present, decode base64 to buffer
    const base64Data = audio.replace(/^data:[^,]+,/, '');
    const audioBuffer = Buffer.from(base64Data, 'base64');

    if (audioBuffer.length < 500) return res.status(400).json({ error: 'audio_too_short' });
    if (audioBuffer.length > 10_000_000) return res.status(413).json({ error: 'audio_too_large' });

    // Build multipart form for Groq Whisper
    const formData = new FormData();
    const blob = new Blob([audioBuffer], { type: mimeType });
    formData.append('file', blob, 'recording.webm');
    formData.append('model', 'whisper-large-v3-turbo');
    formData.append('response_format', 'json');

    const groqResp = await fetch('https://api.groq.com/openai/v1/audio/transcriptions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}` },
      body: formData,
    });

    if (!groqResp.ok) {
      const errBody = await groqResp.json().catch(() => ({}));
      console.error('[transcribe] Groq error:', groqResp.status, JSON.stringify(errBody));
      return res.status(502).json({ error: 'upstream_error' });
    }

    const data = await groqResp.json();
    const transcript = data?.text?.trim();
    if (!transcript) return res.status(502).json({ error: 'empty_transcript' });

    console.log(`[transcribe] ok ip=${ip} bytes=${audioBuffer.length}`);
    return res.json({ transcript });
  } catch (err) {
    console.error('[transcribe] error:', err.message);
    return res.status(500).json({ error: 'server_error' });
  }
});

export default router;
