// Removes tracks from Spotify playlists using the stored refresh token.
// POST body: { trackIds: string[], playlistIds: string[] }
// Returns:   { success: string[], failed: { playlistId, error }[] }

async function getWriteToken() {
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const refreshToken = process.env.SPOTIFY_REFRESH_TOKEN;

  if (!refreshToken) throw new Error('SPOTIFY_REFRESH_TOKEN no configurado en Vercel');

  const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      Authorization:  `Basic ${creds}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      grant_type:    'refresh_token',
      refresh_token: refreshToken,
    }),
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.error_description || 'Spotify token refresh failed');
  return data.access_token;
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { trackIds, playlistIds } = req.body;
  if (!trackIds?.length || !playlistIds?.length) {
    return res.status(400).json({ error: 'trackIds y playlistIds requeridos' });
  }

  let token;
  try {
    token = await getWriteToken();
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  const tracks  = trackIds.map(id => ({ uri: `spotify:track:${id}` }));
  const results = { success: [], failed: [] };

  for (const playlistId of playlistIds) {
    try {
      const r = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}/tracks`, {
        method: 'DELETE',
        headers: {
          Authorization:  `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ tracks }),
      });

      if (r.ok) {
        results.success.push(playlistId);
      } else {
        const err = await r.json();
        results.failed.push({ playlistId, error: err.error?.message || `HTTP ${r.status}` });
      }
    } catch (e) {
      results.failed.push({ playlistId, error: e.message });
    }
  }

  return res.json(results);
}
