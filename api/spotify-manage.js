// Manage Spotify playlists (create, update title/description, upload cover).
// Uses OAuth tokens stored in Upstash KV by spotify-auth.js.
//
// POST modes:
//   mode=create  { userId?, name, description?, public? }         → creates playlist, returns playlistId + url
//   mode=update  { userId?, playlistId, name?, description? }     → updates title and/or description
//   mode=cover   { userId?, playlistId, imageUrl OR imageBase64 } → uploads cover image (JPEG, max 256KB)
// GET:
//   ?action=owners → same as spotify-auth owners (lists KV accounts)

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

async function getAccessToken(userId) {
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

  // If no userId, use first available owner
  let resolvedUserId = userId;
  if (!resolvedUserId) {
    const keys = await kvScan('spotify:owner:*');
    if (!keys.length) throw new Error('No hay cuentas Spotify autorizadas. Completá el flujo en /api/spotify-auth?action=login');
    resolvedUserId = keys[0].replace('spotify:owner:', '');
  }

  const refreshToken = await kvGet(`spotify:owner:${resolvedUserId}`);
  if (!refreshToken) throw new Error(`No hay token para el usuario ${resolvedUserId}`);

  const r = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(`Spotify token error: ${data.error_description || data.error}`);
  return { accessToken: data.access_token, userId: resolvedUserId };
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // ── LIST OWNERS ──────────────────────────────────────────────────
  if (req.method === 'GET') {
    try {
      const { accessToken, userId } = await getAccessToken(req.query.userId || null);
      const meRes = await fetch('https://api.spotify.com/v1/me', {
        headers: { Authorization: `Bearer ${accessToken}` },
      });
      const me = await meRes.json();
      return res.json({ ok: true, userId, displayName: me.display_name, email: me.email });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { mode, userId: reqUserId, playlistId, name, description, public: isPublic, imageUrl, imageBase64 } = req.body;

    const { accessToken, userId } = await getAccessToken(reqUserId || null);
    const headers = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

    // ── CREATE PLAYLIST ─────────────────────────────────────────────
    if (!mode || mode === 'create') {
      if (!name) return res.status(400).json({ error: 'name requerido' });
      const r = await fetch(`https://api.spotify.com/v1/users/${userId}/playlists`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          name,
          description: description || '',
          public: isPublic !== false,
        }),
      });
      const data = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: data.error?.message || JSON.stringify(data) });
      return res.json({ ok: true, playlistId: data.id, url: data.external_urls?.spotify, name: data.name });
    }

    // ── UPDATE TITLE / DESCRIPTION ──────────────────────────────────
    if (mode === 'update') {
      if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
      const body = {};
      if (name !== undefined) body.name = name;
      if (description !== undefined) body.description = description;
      if (!Object.keys(body).length) return res.status(400).json({ error: 'Nada para actualizar' });
      const r = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}`, {
        method: 'PUT',
        headers,
        body: JSON.stringify(body),
      });
      if (r.status === 200 || r.status === 204) return res.json({ ok: true });
      const data = await r.json().catch(() => ({}));
      return res.status(r.status).json({ error: data.error?.message || `HTTP ${r.status}` });
    }

    // ── UPLOAD COVER IMAGE ──────────────────────────────────────────
    if (mode === 'cover') {
      if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
      if (!imageUrl && !imageBase64) return res.status(400).json({ error: 'imageUrl o imageBase64 requerido' });

      let base64 = imageBase64;
      if (!base64 && imageUrl) {
        // Fetch image from URL and convert to base64
        const imgRes = await fetch(imageUrl);
        if (!imgRes.ok) return res.status(400).json({ error: `No se pudo obtener la imagen: HTTP ${imgRes.status}` });
        const buffer = await imgRes.arrayBuffer();
        base64 = Buffer.from(buffer).toString('base64');
      }

      // Spotify requires raw base64 (no data: prefix) and JPEG format
      const cleanBase64 = base64.replace(/^data:image\/[a-z]+;base64,/, '');

      const r = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}/images`, {
        method: 'PUT',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'image/jpeg' },
        body: cleanBase64,
      });
      if (r.status === 202) return res.json({ ok: true });
      const text = await r.text();
      let errMsg = `HTTP ${r.status}`;
      try { const d = JSON.parse(text); errMsg = d.error?.message || errMsg; } catch(_) {}
      return res.status(r.status).json({ error: errMsg });
    }

    return res.status(400).json({ error: `mode desconocido: ${mode}` });

  } catch(e) {
    console.error('spotify-manage error:', e.message);
    return res.status(500).json({ error: e.message });
  }
}
