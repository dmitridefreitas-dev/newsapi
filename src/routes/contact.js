import 'dotenv/config';
import express from 'express';
import nodemailer from 'nodemailer';

const router = express.Router();

// Gmail app passwords are displayed as "abcd efgh ijkl mnop" — strip any
// whitespace that came along with a copy/paste.
const GMAIL_USER = (process.env.GMAIL_USER || '').trim();
const GMAIL_PASSWORD = (process.env.GMAIL_PASSWORD || '').replace(/\s+/g, '');
const RECIPIENT_EMAIL = (process.env.RECIPIENT_EMAIL || '').trim() || GMAIL_USER;

const TIMEOUTS = {
  connectionTimeout: 10_000,
  greetingTimeout: 10_000,
  socketTimeout: 20_000,
};

// Some hosts block STARTTLS on 587 but allow implicit TLS on 465 (or vice
// versa) — try both before giving up.
const TRANSPORTS = [
  nodemailer.createTransport({
    host: 'smtp.gmail.com', port: 465, secure: true,
    auth: { user: GMAIL_USER, pass: GMAIL_PASSWORD }, ...TIMEOUTS,
  }),
  nodemailer.createTransport({
    host: 'smtp.gmail.com', port: 587, secure: false,
    auth: { user: GMAIL_USER, pass: GMAIL_PASSWORD }, ...TIMEOUTS,
  }),
];

const escapeHtml = (s) =>
  String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

router.post('/send-email', async (req, res) => {
  const { name, email, message } = req.body;

  if (!name || !email || !message) {
    return res.status(400).json({ error: 'missing_fields' });
  }

  if (!GMAIL_USER || !GMAIL_PASSWORD) {
    console.error('[contact] GMAIL_USER / GMAIL_PASSWORD not configured');
    return res.status(503).json({ error: 'email_not_configured' });
  }

  const mailOptions = {
    from: GMAIL_USER,
    to: RECIPIENT_EMAIL,
    replyTo: String(email),
    subject: `New Contact Form Submission from ${String(name).slice(0, 120)}`,
    html: `
      <h2>New Contact Form Submission</h2>
      <p><strong>Name:</strong> ${escapeHtml(name)}</p>
      <p><strong>Email:</strong> ${escapeHtml(email)}</p>
      <p><strong>Message:</strong></p>
      <p>${escapeHtml(message).replace(/\n/g, '<br>')}</p>
    `,
  };

  const failures = [];
  for (const transporter of TRANSPORTS) {
    const port = transporter.options.port;
    try {
      await transporter.sendMail(mailOptions);
      console.log(`[contact] sent via port ${port}`);
      return res.json({ success: true, message: 'Email sent successfully' });
    } catch (err) {
      console.error(`[contact] port ${port} failed:`, err.code || '', err.responseCode || '', err.message);
      failures.push(`${port}:${err.code || err.responseCode || 'ERR'}`);
    }
  }

  // Expose the error codes (never credentials) so the failure mode is
  // diagnosable from the client side: EAUTH = bad app password,
  // ETIMEDOUT/ESOCKET = SMTP egress blocked by the host.
  return res.status(502).json({ error: 'send_failed', detail: failures.join(',') });
});

export default router;
