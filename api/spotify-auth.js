// OAuth flow para obtener tokens Spotify con playlist-modify scopes.
// Los tokens se guardan automáticamente en Upstash KV por Spotify user ID.
// Endpoints:
//   ?action=login   → redirige a Spotify para autorizar
//   ?action=owners  → lista todos los dueños autorizados
//   ?action=whoami  → muestra cuenta del SPOTIFY_REFRESH_TOKEN env var (legacy)

async function kvSet(key, value) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['SET', key, value]),
  });
}

async function kvGet(key) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['GET', key]),
  });
  const data = await r.json();
  return data.result || null;
}

async function kvKeys(pattern) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return [];
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['KEYS', pattern]),
  });
  const data = await r.json();
  return data.result || [];
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
      scope:         'playlist-modify-public playlist-modify-private playlist-read-private playlist-read-collaborative user-read-private user-read-email',
      redirect_uri:  redirectUri,
    });
    return res.redirect(`https://accounts.spotify.com/authorize?${params}`);
  }

  // Error returned by Spotify (e.g. user denied)
  if (error) {
    return res.status(400).send(`<h2 style="font-family:monospace;padding:2rem">Error: ${error}</h2>`);
  }

  // Step 2: exchange authorization code for tokens + auto-save to KV
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

    // Fetch user profile to get Spotify user ID
    const meRes = await fetch('https://api.spotify.com/v1/me', {
      headers: { Authorization: `Bearer ${data.access_token}` },
    });
    const me = await meRes.json();

    // Auto-save refresh token to Upstash KV keyed by Spotify user ID
    await kvSet(`spotify:owner:${me.id}`, data.refresh_token);
    console.log(`Saved Spotify token for: ${me.id} (${me.display_name})`);

    return res.send(`
      <html><body style="font-family:monospace;padding:2rem;background:#111;color:#eee;max-width:600px;margin:0 auto">
        <h2 style="color:#1db954">✅ Autorización exitosa</h2>
        <p><strong>Cuenta:</strong> ${me.display_name || '—'} (${me.email || '—'})</p>
        <p><strong>ID Spotify:</strong> <code style="background:#222;padding:2px 6px">${me.id}</code></p>
        <p style="color:#aaa;margin-top:1.5rem">El token fue guardado automáticamente. Ya podés cerrar esta página.</p>
        <p style="color:#555;font-size:12px;margin-top:2rem">Ver todos los autorizados: <a style="color:#555" href="/api/spotify-auth?action=owners">/api/spotify-auth?action=owners</a></p>
      </body></html>
    `);
  }

  // List all stored owner tokens
  if (action === 'owners') {
    const keys = await kvKeys('spotify:owner:*');
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

  return res.status(400).send('<p style="font-family:monospace;padding:2rem">Usá <code>?action=login</code> para iniciar el flujo de autorización.</p>');
}
