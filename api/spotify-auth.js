// OAuth flow para obtener tokens Spotify con playlist-modify scopes.
// Los tokens se guardan automáticamente en Upstash KV por Spotify user ID.
// Endpoints:
//   ?action=login   → redirige a Spotify para autorizar
//   ?action=owners  → lista todos los dueños autorizados
//   ?action=whoami  → muestra cuenta del SPOTIFY_REFRESH_TOKEN env var (legacy)

async function kvSet(key, value) {
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return;
  await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['SET', key, value]),
  });
}

async function kvGet(key) {
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['GET', key]),
  });
  const data = await r.json();
  return data.result || null;
}

async function kvScan(pattern) {
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return [];
  const keys = [];
  let cursor = 0;
  do {
    const r = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(['SCAN', cursor, 'MATCH', pattern, 'COUNT', 100]),
    });
    const data = await r.json();
    cursor = parseInt(data.result?.[0] || 0);
    keys.push(...(data.result?.[1] || []));
  } while (cursor !== 0);
  return keys;
}

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
      scope:         'playlist-modify-public playlist-modify-private playlist-read-private playlist-read-collaborative ugc-image-upload user-read-private user-read-email',
      redirect_uri:  redirectUri,
      show_dialog:   'true',
    });
    return res.redirect(`https://accounts.spotify.com/authorize?${params}`);
  }

  // Error returned by Spotify (e.g. user denied) — skip for yt-callback
  if (error && action !== 'yt-callback') {
    return res.status(400).send(`<h2 style="font-family:monospace;padding:2rem">Error: ${error}</h2>`);
  }

  // Step 2: exchange authorization code for tokens + auto-save to KV
  if (code && action !== 'yt-callback') {
    try {
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

      const tokenText = await tokenRes.text();
      console.log('Spotify token response:', tokenText.slice(0, 200));
      let data;
      try { data = JSON.parse(tokenText); } catch (e) {
        return res.status(500).send(`<pre style="padding:2rem">Error parsing Spotify token response:\n${tokenText}</pre>`);
      }
      if (!tokenRes.ok) return res.status(500).send(`<pre style="padding:2rem">Spotify token error: ${JSON.stringify(data)}</pre>`);

      // Fetch user profile to get Spotify user ID
      const meRes = await fetch('https://api.spotify.com/v1/me', {
        headers: { Authorization: `Bearer ${data.access_token}` },
      });
      const meText = await meRes.text();
      let me;
      try { me = JSON.parse(meText); } catch (e) {
        return res.status(500).send(`<pre style="padding:2rem">Error parsing /me response:\n${meText}</pre>`);
      }

      // Auto-save refresh token to Upstash KV keyed by Spotify user ID
      try {
        await kvSet(`spotify:owner:${me.id}`, data.refresh_token);
        console.log(`Saved Spotify token for: ${me.id} (${me.display_name})`);
      } catch (kvErr) {
        console.error('KV save failed (non-fatal):', kvErr.message);
      }

      return res.send(`
        <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
          <h2 style="color:#1db954">✅ Autorización exitosa</h2>
          <p><strong>Cuenta:</strong> ${me.display_name || '—'} (${me.email || '—'})</p>
          <p><strong>ID Spotify:</strong> <code style="background:#222;padding:2px 6px">${me.id}</code></p>
          <p style="color:#aaa;margin-top:1.5rem">El token fue guardado automáticamente. Ya podés cerrar esta página.</p>
          <p style="color:#555;font-size:12px;margin-top:2rem">Ver todos los autorizados: <a style="color:#555" href="/api/spotify-auth?action=owners">/api/spotify-auth?action=owners</a></p>
        </body></html>
      `);
    } catch (e) {
      console.error('Auth callback error:', e.message, e.stack);
      return res.status(500).send(`<pre style="padding:2rem;color:red">Error: ${e.message}</pre>`);
    }
  }

  // List all stored owner tokens
  if (action === 'owners') {
    const keys = await kvScan('spotify:owner:*');
    if (!keys.length) {
      return res.send(`
        <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
          <h2 style="color:#e74c3c">Sin dueños autorizados</h2>
          <p>Ningún dueño de playlist ha completado el flujo de autorización todavía.</p>
          <p>Compartí este link: <a style="color:#1db954" href="/api/spotify-auth?action=login">/api/spotify-auth?action=login</a></p>
        </body></html>
      `);
    }

    const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    const ownerRows = [];

    for (const key of keys) {
      const userId = key.replace('spotify:owner:', '');
      const refreshToken = await kvGet(key);
      if (!refreshToken) continue;
      try {
        const tokenRes = await fetch('https://accounts.spotify.com/api/token', {
          method: 'POST',
          headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }),
        });
        const tokenData = await tokenRes.json();
        if (!tokenRes.ok) {
          ownerRows.push(`<tr><td style="padding:8px">${userId}</td><td style="padding:8px" colspan="2" style="color:#e74c3c">Token inválido</td></tr>`);
          continue;
        }
        const meRes = await fetch('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${tokenData.access_token}` } });
        const me = await meRes.json();
        ownerRows.push(`<tr><td style="padding:8px">${me.display_name || userId}</td><td style="padding:8px">${me.email || '—'}</td><td style="padding:8px;color:#1db954">✅ Activo</td></tr>`);
      } catch (e) {
        ownerRows.push(`<tr><td style="padding:8px">${userId}</td><td style="padding:8px" colspan="2">Error: ${e.message}</td></tr>`);
      }
    }

    return res.send(`
      <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:700px;margin:0 auto">
        <h2 style="color:#1db954">🎵 Dueños autorizados (${keys.length})</h2>
        <table style="width:100%;border-collapse:collapse;margin-top:1rem">
          <thead><tr style="color:#aaa;border-bottom:1px solid #333">
            <th style="text-align:left;padding:8px">Nombre</th>
            <th style="text-align:left;padding:8px">Email</th>
            <th style="text-align:left;padding:8px">Estado</th>
          </tr></thead>
          <tbody style="border-top:1px solid #333">${ownerRows.join('')}</tbody>
        </table>
        <p style="margin-top:2rem;color:#aaa;font-size:12px">Para agregar otro dueño, compartile: <a style="color:#1db954" href="/api/spotify-auth?action=login">/api/spotify-auth?action=login</a></p>
      </body></html>
    `);
  }

  // Legacy: check env var account
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
    if (!tokenRes.ok) return res.status(500).send(`<p style="font-family:monospace;padding:2rem;color:red">Error: ${tokenData.error_description || tokenData.error}</p>`);
    const meRes = await fetch('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${tokenData.access_token}` } });
    const me = await meRes.json();
    return res.send(`
      <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
        <h2 style="color:#1db954">🎵 Cuenta autorizada (env var)</h2>
        <p><strong>Nombre:</strong> ${me.display_name || '—'}</p>
        <p><strong>Email:</strong> ${me.email || '—'}</p>
        <p><strong>ID:</strong> ${me.id || '—'}</p>
        <p><strong>URL:</strong> <a style="color:#1db954" href="${me.external_urls?.spotify}" target="_blank">${me.external_urls?.spotify || '—'}</a></p>
      </body></html>
    `);
  }

  // ── YOUTUBE ANALYTICS OAUTH ─────────────────────────────────

  if (action === 'yt-login') {
    const gClientId = process.env.GOOGLE_CLIENT_ID;
    if (!gClientId) return res.status(500).send('<pre style="padding:2rem;color:red">GOOGLE_CLIENT_ID no configurado en Vercel.</pre>');
    const params = new URLSearchParams({
      response_type: 'code',
      client_id: gClientId,
      scope: 'https://www.googleapis.com/auth/yt-analytics.readonly https://www.googleapis.com/auth/youtube.readonly',
      redirect_uri: 'https://ss-dashboard-flame.vercel.app/api/spotify-auth?action=yt-callback',
      access_type: 'offline',
      prompt: 'consent',
    });
    return res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?${params}`);
  }

  if (action === 'yt-callback') {
    const gClientId = process.env.GOOGLE_CLIENT_ID;
    const gClientSecret = process.env.GOOGLE_CLIENT_SECRET;
    if (!gClientId || !gClientSecret) return res.status(500).send('<pre style="padding:2rem;color:red">GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET no configurados.</pre>');
    if (error) return res.status(400).send(`<h2 style="font-family:monospace;padding:2rem">Error: ${error}</h2>`);
    if (!code) return res.status(400).send('<h2 style="font-family:monospace;padding:2rem">Sin código de autorización</h2>');
    try {
      const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'authorization_code',
          code,
          redirect_uri: 'https://ss-dashboard-flame.vercel.app/api/spotify-auth?action=yt-callback',
          client_id: gClientId,
          client_secret: gClientSecret,
        }),
      });
      const data = await tokenRes.json();
      if (!tokenRes.ok) return res.status(500).send(`<pre style="padding:2rem;color:red">Error Google OAuth: ${JSON.stringify(data)}</pre>`);
      if (data.refresh_token) {
        await kvSet('youtube:refresh_token', data.refresh_token);
        console.log('Saved YouTube Analytics refresh token to KV');
      }
      const meRes = await fetch('https://www.googleapis.com/oauth2/v1/userinfo', {
        headers: { Authorization: `Bearer ${data.access_token}` },
      });
      const me = await meRes.json();
      return res.send(`
        <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
          <h2 style="color:#ff4444">✅ YouTube Analytics conectado</h2>
          <p><strong>Cuenta:</strong> ${me.name || '—'}</p>
          <p><strong>Email:</strong> ${me.email || '—'}</p>
          ${!data.refresh_token ? '<p style="color:#e74c3c">⚠️ Sin refresh token — reintentá desde cero.</p>' : '<p style="color:#1db954">Token guardado en KV correctamente.</p>'}
          <p style="color:#aaa;margin-top:1.5rem">YouTube Analytics listo. Podés cerrar esta página y recargar el dashboard.</p>
        </body></html>
      `);
    } catch (e) {
      return res.status(500).send(`<pre style="padding:2rem;color:red">Error: ${e.message}</pre>`);
    }
  }

  if (action === 'yt-status') {
    const token = await kvGet('youtube:refresh_token');
    return res.json({ connected: !!token });
  }

  return res.status(400).send('<p style="font-family:monospace;padding:2rem">Usá <code>?action=login</code> para iniciar el flujo de autorización.</p>');
}
