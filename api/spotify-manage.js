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

// Client credentials token (read-only, no OAuth needed) – used for tracks
let _ccToken = null;
let _ccExpiry = 0;
async function getClientCredToken() {
  if (_ccToken && Date.now() < _ccExpiry) return _ccToken;
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const r = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=client_credentials',
  });
  const data = await r.json();
  if (!r.ok) throw new Error(`Spotify CC token error: ${data.error_description || data.error}`);
  _ccToken  = data.access_token;
  _ccExpiry = Date.now() + (data.expires_in - 60) * 1000;
  return _ccToken;
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
  return { accessToken: data.access_token, userId: resolvedUserId, scope: data.scope || '' };
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // ── GET ──────────────────────────────────────────────────────────
  if (req.method === 'GET') {
    const { action, userId: qUserId, playlistId } = req.query;
    try {

      // Check token health — returns userId, displayName, scopes
      if (action === 'check') {
        const { accessToken, userId, scope } = await getAccessToken(qUserId || null);
        const meRes = await fetch('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${accessToken}` } });
        const me = await meRes.json();
        if (!meRes.ok) return res.status(meRes.status).json({ error: me.error?.message || `HTTP ${meRes.status}`, scope });
        const hasModify = scope.includes('playlist-modify-public') || scope.includes('playlist-modify-private');
        return res.json({ ok: true, kvUserId: userId, meId: me.id, displayName: me.display_name, email: me.email, scope, hasModify, idsMatch: userId === me.id });
      }

      // Get playlist detail + tracks — uses client credentials (works for public playlists, no OAuth restrictions)
      if (action === 'tracks' || action === 'detail') {
        if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
        const ccToken = await getClientCredToken();
        const ccHeaders = { Authorization: `Bearer ${ccToken}` };

        // Get playlist metadata (followers, description, image, owner)
        const metaRes = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}?fields=id,name,description,public,followers(total),tracks(total),images,owner,external_urls`, { headers: ccHeaders });
        const metaText = await metaRes.text();
        let meta = {};
        try { meta = JSON.parse(metaText); } catch(_) {}
        if (!metaRes.ok) {
          console.error(`Spotify playlist meta error: status=${metaRes.status} body=${metaText.slice(0,300)}`);
          return res.status(metaRes.status).json({ error: meta.error?.message || `HTTP ${metaRes.status} — la playlist puede ser privada` });
        }

        // Get all tracks
        let tracks = [];
        let url = `https://api.spotify.com/v1/playlists/${playlistId}/tracks?limit=100&fields=next,items(track(id,name,artists,external_urls,album(images)))`;
        while (url) {
          const r = await fetch(url, { headers: ccHeaders });
          const rawText = await r.text();
          let data = {};
          try { data = JSON.parse(rawText); } catch(_) {}
          if (!r.ok) {
            console.error(`Spotify tracks error: status=${r.status} body=${rawText.slice(0,300)}`);
            return res.status(r.status).json({ error: data.error?.message || `HTTP ${r.status}` });
          }
          (data.items || []).forEach(item => {
            if (item.track?.id) {
              tracks.push({
                position: tracks.length + 1,
                id: item.track.id,
                name: item.track.name,
                artist: item.track.artists?.[0]?.name || '',
                image: item.track.album?.images?.[2]?.url || item.track.album?.images?.[0]?.url || null,
                url: item.track.external_urls?.spotify || '',
              });
            }
          });
          url = data.next || null;
        }

        return res.json({
          ok: true,
          playlist: {
            id: meta.id,
            name: meta.name,
            description: meta.description || '',
            public: meta.public,
            followers: meta.followers?.total || 0,
            totalTracks: meta.tracks?.total || 0,
            image: meta.images?.[0]?.url || null,
            url: meta.external_urls?.spotify || '',
            owner: meta.owner?.display_name || meta.owner?.id || '',
          },
          tracks,
        });
      }

      const { accessToken, userId } = await getAccessToken(qUserId || null);

      // List all playlists (handles pagination)
      if (action === 'playlists') {
        let playlists = [];
        let url = 'https://api.spotify.com/v1/me/playlists?limit=50';
        while (url) {
          const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
          const data = await r.json();
          if (!r.ok) return res.status(r.status).json({ error: data.error?.message || 'Error fetching playlists' });
          playlists = playlists.concat(data.items || []);
          url = data.next || null;
        }
        return res.json({ ok: true, userId, playlists: playlists.map(p => ({
          id: p.id,
          name: p.name,
          description: p.description || '',
          public: p.public,
          tracks: p.tracks?.total || 0,
          image: p.images?.[0]?.url || null,
          url: p.external_urls?.spotify,
          ownerId: p.owner?.id,
          ownerName: p.owner?.display_name || p.owner?.id,
        })) });
      }

      // Default: whoami
      const meRes = await fetch('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${accessToken}` } });
      const me = await meRes.json();
      return res.json({ ok: true, userId, displayName: me.display_name, email: me.email });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { mode, userId: reqUserId, playlistId, name, description, public: isPublic, imageUrl, imageBase64 } = req.body;

    const { accessToken, userId, scope } = await getAccessToken(reqUserId || null);
    console.log(`spotify-manage POST mode=${mode} userId=${userId} scopes=${scope}`);
    const headers = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

    // ── CREATE PLAYLIST ─────────────────────────────────────────────
    if (!mode || mode === 'create') {
      if (!name) return res.status(400).json({ error: 'name requerido' });
      // Use /me to get the current user's ID directly (avoids KV mismatch)
      const meCheck = await fetch('https://api.spotify.com/v1/me', { headers });
      const meData = await meCheck.json();
      const createUserId = meData.id || userId;
      console.log(`spotify-manage create: kvUserId=${userId} meUserId=${meData.id}`);
      const r = await fetch(`https://api.spotify.com/v1/users/${createUserId}/playlists`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          name,
          description: description || '',
          public: isPublic !== false,
        }),
      });
      const rawText = await r.text();
      let data = {};
      try { data = JSON.parse(rawText); } catch(_) {}
      if (!r.ok) {
        console.error(`Spotify create error: status=${r.status} userId=${createUserId} scope=${scope} body=${rawText.slice(0,500)}`);
        return res.status(r.status).json({ error: `HTTP ${r.status}: ${rawText.slice(0,300)}` });
      }
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
