// Removes tracks from Spotify playlists via Make (Integromat) webhook.
// Calls Make once per (track, playlist) pair so Make always receives exactly 1 track.
// POST body: { pairs: [{ trackId: string, playlistId: string }] }
// Returns:   { success: string[], failed: { trackId, playlistId, error }[] }

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { pairs } = req.body;
  if (!pairs?.length) {
    return res.status(400).json({ error: 'pairs requerido' });
  }

  const webhookUrl = process.env.MAKE_SPOTIFY_WEBHOOK_URL;
  if (!webhookUrl) {
    return res.status(500).json({ error: 'MAKE_SPOTIFY_WEBHOOK_URL no configurado en Vercel' });
  }

  const results = { success: [], failed: [] };

  for (const { trackId, playlistId } of pairs) {
    try {
      const r = await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playlistId,
          tracks: [{ uri: `spotify:track:${trackId}` }],
        }),
      });

      const text = await r.text();
      console.log(`Make webhook ${playlistId}/${trackId}: ${r.status} — ${text.slice(0, 200)}`);

      if (r.ok) {
        results.success.push(`${trackId}:${playlistId}`);
      } else {
        let errMsg = `HTTP ${r.status}`;
        try { const b = JSON.parse(text); errMsg = b.error?.message || b.message || b.error || errMsg; } catch (_) { errMsg = text || errMsg; }
        results.failed.push({ trackId, playlistId, status: r.status, error: errMsg });
      }
    } catch (e) {
      results.failed.push({ trackId, playlistId, error: e.message });
    }
  }

  return res.json(results);
}
