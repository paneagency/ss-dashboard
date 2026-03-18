// One-time OAuth flow to get a Spotify refresh token with playlist-modify scopes.
// Usage:
//   1. Visit /api/spotify-auth?action=login  → redirects to Spotify authorization page
//   2. After authorizing, Spotify redirects back here with ?code=XXX
//   3. Copy the displayed refresh_token and add it to Vercel as SPOTIFY_REFRESH_TOKEN

export default async function handler(req, res) {
  const { action, code, error } = req.query;
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const redirectUri  = 'https://ss-dashboard-flame.vercel.app/api/spotify-auth';

  // Step 1: redirect to Spotify's authorization page
  if (action === 'login') {
    const params = new URLSearchParams({
      response_type: 'code',
      client_id:     clientId,
      scope:         'playlist-modify-public playlist-modify-private',
      redirect_uri:  redirectUri,
    });
    return res.redirect(`https://accounts.spotify.com/authorize?${params}`);
  }

  // Error returned by Spotify (e.g. user denied)
  if (error) {
    return res.status(400).send(`<h2 style="font-family:monospace;padding:2rem">Error: ${error}</h2>`);
  }

  // Step 2: exchange authorization code for tokens
  if (code) {
    const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    const tokenRes = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        Authorization:  `Basic ${creds}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type:   'authorization_code',
        code,
        redirect_uri: redirectUri,
      }),
    });
    const data = await tokenRes.json();
    if (!tokenRes.ok) return res.status(500).json(data);

    return res.send(`
      <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:700px;margin:0 auto">
        <h2 style="color:#1db954">✅ Autorización exitosa</h2>
        <p>Copiá este <strong>refresh_token</strong> y guardalo en Vercel → Settings → Environment Variables como <code style="background:#222;padding:2px 6px">SPOTIFY_REFRESH_TOKEN</code>:</p>
        <textarea style="width:100%;height:90px;background:#222;color:#1db954;padding:12px;font-size:13px;border:1px solid #1db954;border-radius:6px;resize:none">${data.refresh_token}</textarea>
        <p style="color:#aaa;font-size:12px;margin-top:16px">Ya podés cerrar esta página. Una vez guardado el token en Vercel y redesplegado, la función de eliminar canciones quedará activa.</p>
      </body></html>
    `);
  }

  // Check which account is currently authorized
  if (action === 'whoami') {
    const refreshToken = process.env.SPOTIFY_REFRESH_TOKEN;
    if (!refreshToken) return res.send('<p style="font-family:monospace;padding:2rem;color:red">SPOTIFY_REFRESH_TOKEN no está configurado en Vercel.</p>');
    const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    const tokenRes = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }),
    });
    const tokenData = await tokenRes.json();
    if (!tokenRes.ok) return res.status(500).send(`<p style="font-family:monospace;padding:2rem;color:red">Error al refrescar token: ${tokenData.error_description || tokenData.error}</p>`);
    const meRes = await fetch('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${tokenData.access_token}` } });
    const me = await meRes.json();
    return res.send(`
      <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
        <h2 style="color:#1db954">🎵 Cuenta autorizada</h2>
        <p><strong>Nombre:</strong> ${me.display_name || '—'}</p>
        <p><strong>Email:</strong> ${me.email || '—'}</p>
        <p><strong>ID:</strong> ${me.id || '—'}</p>
        <p><strong>URL:</strong> <a style="color:#1db954" href="${me.external_urls?.spotify}" target="_blank">${me.external_urls?.spotify || '—'}</a></p>
        <hr style="border-color:#333;margin:1rem 0">
        <p style="color:#aaa;font-size:12px">Si esta no es la cuenta correcta, volvé a autorizar con <a style="color:#1db954" href="/api/spotify-auth?action=login">?action=login</a> usando la cuenta correcta.</p>
      </body></html>
    `);
  }

  return res.status(400).send('<p style="font-family:monospace;padding:2rem">Usá <code>?action=login</code> para iniciar el flujo de autorización.</p>');
}
