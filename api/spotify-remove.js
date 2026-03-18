// Removes tracks from Spotify playlists via Make (Integromat) webhook.
// Make has extended Spotify API access that bypasses Development Mode restrictions.
// POST body: { trackIds: string[], playlistIds: string[] }
// Returns:   { success: string[], failed: { playlistId, error }[] }

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { trackIds, playlistIds } = req.body;
  if (!trackIds?.length || !playlistIds?.length) {
    return res.status(400).json({ error: 'trackIds y playlistIds requeridos' });
  }

  const webhookUrl = process.env.MAKE_SPOTIFY_WEBHOOK_URL;
  if (!webhookUrl) {
    return res.status(500).json({ error: 'MAKE_SPOTIFY_WEBHOOK_URL no configurado en Vercel' });
  }

  const tracks = trackIds.map(id => ({ uri: `spotify:track:${id}` }));
  const results = { success: [], failed: [] };

  for (const playlistId of playlistIds) {
    try {
      const r = await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playlistId, tracks }),
      });

      const text = await r.text();
      console.log(`Make webhook response for ${playlistId}: ${r.status} — ${text.slice(0, 200)}`);

      if (r.ok) {
        results.success.push(playlistId);
      } else {
        let errMsg = `HTTP ${r.status}`;
        try {
          const errBody = JSON.parse(text);
          errMsg = errBody.error?.message || errBody.message || errBody.error || errMsg;
        } catch (_) { errMsg = text || errMsg; }
        results.failed.push({ playlistId, status: r.status, error: errMsg });
      }
    } catch (e) {
      results.failed.push({ playlistId, error: e.message });
    }
  }

  return res.json(results);
}
