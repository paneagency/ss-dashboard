// Removes tracks from Spotify playlists using the stored refresh token.
// Supports multiple playlist owners via Upstash KV (key: spotify:owner:{userId}).
// Falls back to SPOTIFY_REFRESH_TOKEN env var if no owner-specific token is found.
// POST body: { trackIds: string[], playlistIds: string[] }
// Returns:   { success: string[], failed: { playlistId, error }[] }

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

async function getAccessTokenFromRefresh(refreshToken) {
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
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
  console.log(`Token refresh for owner — scopes: ${data.scope}`);
  return data.access_token;
}

async function getDefaultToken() {
  const refreshToken = process.env.SPOTIFY_REFRESH_TOKEN;
  if (!refreshToken) throw new Error('SPOTIFY_REFRESH_TOKEN no configurado en Vercel');
  return await getAccessTokenFromRefresh(refreshToken);
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { trackIds, playlistIds } = req.body;
  if (!trackIds?.length || !playlistIds?.length) {
    return res.status(400).json({ error: 'trackIds y playlistIds requeridos' });
  }

  // Get default token (used for metadata fetching and as fallback)
  let defaultToken;
  try {
    defaultToken = await getDefaultToken();
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  const tracks     = trackIds.map(id => ({ uri: `spotify:track:${id}` }));
  const results    = { success: [], failed: [] };
  const tokenCache = {}; // ownerId → accessToken (avoid re-fetching per owner)

  for (const playlistId of playlistIds) {
    try {
      // Get playlist metadata to find owner
      const plMeta = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}?fields=collaborative,owner`, {
        headers: { Authorization: `Bearer ${defaultToken}` },
      });

      let token = defaultToken;

      if (plMeta.ok) {
        const meta    = await plMeta.json();
        const ownerId = meta.owner?.id;
        console.log(`Playlist ${playlistId}: collaborative=${meta.collaborative}, owner=${ownerId}`);

        if (ownerId) {
          if (tokenCache[ownerId]) {
            token = tokenCache[ownerId];
          } else {
            // Look up owner-specific token in KV
            const ownerRefresh = await kvGet(`spotify:owner:${ownerId}`);
            if (ownerRefresh) {
              try {
                token = await getAccessTokenFromRefresh(ownerRefresh);
                tokenCache[ownerId] = token;
                console.log(`Using KV token for owner: ${ownerId}`);
              } catch (e) {
                console.warn(`Failed to refresh token for owner ${ownerId}, using default. Error: ${e.message}`);
              }
            } else {
              console.log(`No KV token for owner ${ownerId}, using default`);
            }
          }
        }
      }

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
        let errMsg = `HTTP ${r.status}`;
        try {
          const errBody = await r.json();
          errMsg = errBody.error?.message || errBody.error_description || errBody.error || errMsg;
          console.error(`Spotify DELETE playlist ${playlistId} failed ${r.status}:`, JSON.stringify(errBody));
        } catch (_) { /* response not JSON */ }
        results.failed.push({ playlistId, status: r.status, error: errMsg });
      }
    } catch (e) {
      results.failed.push({ playlistId, error: e.message });
    }
  }

  return res.json(results);
}
