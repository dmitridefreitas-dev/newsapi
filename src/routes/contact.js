import 'dotenv/config';
import express from 'express';
import nodemailer from 'nodemailer';

const router = express.Router();

const transporter = nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 587,
  secure: false,
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASSWORD,
  },
  // Fail fast instead of hanging the HTTP request when SMTP is unreachable.
  connectionTimeout: 10_000,
  greetingTimeout: 10_000,
  socketTimeout: 20_000,
});

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

  if (!process.env.GMAIL_USER || !process.env.GMAIL_PASSWORD || !process.env.RECIPIENT_EMAIL) {
    console.error('[contact] GMAIL_USER / GMAIL_PASSWORD / RECIPIENT_EMAIL not configured');
    return res.status(503).json({ error: 'email_not_configured' });
  }

  const mailOptions = {
    from: process.env.GMAIL_USER,
    to: process.env.RECIPIENT_EMAIL,
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

  try {
    await transporter.sendMail(mailOptions);
    return res.json({ success: true, message: 'Email sent successfully' });
  } catch (err) {
    console.error('[contact] sendMail failed:', err.code || '', err.message);
    return res.status(502).json({ error: 'send_failed' });
  }
});

export default router;
