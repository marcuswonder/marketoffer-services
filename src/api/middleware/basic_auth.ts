import { Request, Response, NextFunction } from "express";

const COOKIE_NAME = "mo_auth";

export function basicAuth(req: Request, res: Response, next: NextFunction) {
  const user = process.env.ADMIN_USER || "";
  const pass = process.env.ADMIN_PASS || "";
  if (!user || !pass) return res.status(500).send("Auth credentials not configured");
  const allowedPaths = ['/login'];
  if (allowedPaths.includes(req.path)) return next();
  if (req.signedCookies && req.signedCookies[COOKIE_NAME] === 'ok') return next();
  const target = encodeURIComponent(req.originalUrl || "/admin");
  return res.redirect(`/login?return=${target}`);
}

export function handleLogin(req: Request, res: Response) {
  const user = process.env.ADMIN_USER || "";
  const pass = process.env.ADMIN_PASS || "";
  const secret = process.env.COOKIE_SECRET || "";
  if (!user || !pass || !secret) return res.status(500).send("Auth not configured");
  if (req.method === "GET") {
    const err = req.query.error ? '<div class="err">Invalid credentials</div>' : '';
    const ret = typeof req.query.return === 'string' ? req.query.return : '/admin';
    res.type('html').send([
      '<!doctype html>',
      '<html lang="en">',
      '<head>',
      '  <meta charset="utf-8" />',
      '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
      '  <title>Sign In · MarketOffer</title>',
      '  <style>',
      '    :root { --bg:#0f172a; --panel:#111c32; --border:#1f2937; --text:#f3f4f6; --muted:#94a3b8; --accent:#60a5fa; }',
      '    html,body{margin:0;min-height:100vh;background:var(--bg);color:var(--text);font-family:"Inter",ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;display:flex;align-items:center;justify-content:center;}',
      '    .card{width:100%;max-width:380px;background:rgba(17,24,39,0.85);border:1px solid rgba(148,163,184,0.25);border-radius:18px;padding:32px 28px;box-shadow:0 30px 90px rgba(15,23,42,0.55);backdrop-filter:blur(12px);}',
      '    h1{margin:0 0 8px;font-size:1.7rem;text-align:center;}',
      '    p{margin:0 0 24px;text-align:center;color:var(--muted);font-size:0.95rem;}',
      '    label{display:block;font-size:0.85rem;color:var(--muted);margin:12px 0 6px;}',
      '    input{width:100%;padding:10px 12px;border-radius:10px;border:1px solid var(--border);background:#0b1220;color:var(--text);}',
      '    button{width:100%;margin-top:24px;padding:12px;border-radius:10px;border:1px solid var(--border);background:#2563eb;color:#f8fafc;font-weight:600;font-size:1rem;cursor:pointer;}',
      '    button:hover{background:#1d4ed8;}',
      '    .err{margin-top:12px;color:#fca5a5;text-align:center;font-size:0.9rem;}',
      '  </style>',
      '</head>',
      '<body>',
      '  <form class="card" method="post" action="/login">',
      `    <input type="hidden" name="return" value="${ret}" />`,
      '    <h1>Sign In</h1>',
      '    <p>Enter admin credentials to continue.</p>',
      '    <label for="username">Username</label>',
      '    <input id="username" name="username" autocomplete="username" />',
      '    <label for="password">Password</label>',
      '    <input id="password" name="password" type="password" autocomplete="current-password" />',
      `    ${err}`,
      '    <button type="submit">Sign In</button>',
      '  </form>',
      '</body>',
      '</html>'
    ].join('\n'));
    return;
  }
  const { username, password } = req.body || {};
  if (username === user && password === pass) {
    const ret = typeof req.body.return === 'string' ? req.body.return : '/admin';
    res.cookie(COOKIE_NAME, 'ok', {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      signed: true,
      sameSite: 'lax',
      maxAge: 30 * 24 * 60 * 60 * 1000,
    });
    return res.redirect(ret);
  }
  const ret = encodeURIComponent(typeof req.body.return === 'string' ? req.body.return : '/admin');
  res.redirect(`/login?error=1&return=${ret}`);
}

export function handleLogout(_req: Request, res: Response) {
  res.clearCookie(COOKIE_NAME);
  res.redirect('/login');
}
