let _spotifyToken = null;
let _tokenExpiry = 0;

async function getAccessToken() {
  if (_spotifyToken && Date.now() < _tokenExpiry) return _spotifyToken;

  const creds = Buffer.from(
    `${process.env.SPOTIFY_CLIENT_ID}:${process.env.SPOTIFY_CLIENT_SECRET}`
  ).toString('base64');

  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      Authorization: `Basic ${creds}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });

  if (!res.ok) throw new Error('Spotify auth failed');
  const data = await res.json();
  _spotifyToken = data.access_token;
  _tokenExpiry = Date.now() + (data.expires_in - 60) * 1000;
  return _spotifyToken;
}

function extractSpotifyId(url, type) {
  // Soporta: open.spotify.com/track/ID, open.spotify.com/intl-xx/track/ID
  const match = url.match(new RegExp(`spotify\\.com(?:/intl-[^/]+)?/${type}/([A-Za-z0-9]+)`));
  return match ? match[1] : null;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { url, mode, trackUrl, playlistUrl } = req.query;

  // Modo: buscar posición de un track en una playlist
  if (mode === 'position') {
    if (!trackUrl || !playlistUrl) return res.status(400).json({ error: 'trackUrl y playlistUrl requeridos' });
    const trackId = extractSpotifyId(trackUrl, 'track');
    const playlistId = extractSpotifyId(playlistUrl, 'playlist');
    if (!trackId) return res.status(400).json({ error: 'URL de track inválida' });
    if (!playlistId) return res.status(400).json({ error: 'URL de playlist inválida' });

    try {
      const token = await getAccessToken();

      // Info básica de la playlist
      const plRes = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!plRes.ok) return res.status(404).json({ error: 'Playlist no encontrada o es privada' });
      const plData = await plRes.json();
      const total = plData.tracks?.total ?? 0;
      const playlistName = plData.name;

      // Paginar tracks hasta encontrar el track o llegar al final (máx 1000)
      let position = null;
      let offset = 0;
      const limit = 100;
      while (offset < total && offset < 1000) {
        const r = await fetch(
          `https://api.spotify.com/v1/playlists/${playlistId}/tracks?fields=items(track(id))&limit=${limit}&offset=${offset}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        if (!r.ok) break;
        const data = await r.json();
        const items = data.items || [];
        for (let i = 0; i < items.length; i++) {
          if (items[i]?.track?.id === trackId) {
            position = offset + i + 1;
            break;
          }
        }
        if (position !== null) break;
        offset += limit;
      }

      return res.json({ ok: true, position, total, playlistName });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (!url) return res.status(400).json({ error: 'url requerida' });

  try {
    const token = await getAccessToken();

    // Intentar como track primero, luego artist
    const trackId = extractSpotifyId(url, 'track');
    const artistId = extractSpotifyId(url, 'artist');

    if (trackId) {
      const r = await fetch(`https://api.spotify.com/v1/tracks/${trackId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!r.ok) return res.status(404).json({ error: 'Track no encontrado' });
      const track = await r.json();

      // Fetch artista y audio features en paralelo
      let artistImage = null, artistGenres = [], artistFollowers = null, artistPopularity = null;
      const mainArtistId = track.artists[0]?.id;

      if (mainArtistId) {
        const ar = await fetch(`https://api.spotify.com/v1/artists/${mainArtistId}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (ar.ok) {
          const d = await ar.json();
          artistImage = d.images[0]?.url || null;
          artistGenres = d.genres || [];
          artistFollowers = d.followers?.total ?? null;
          artistPopularity = d.popularity ?? null;
        }
      }

      return res.json({
        type: 'track',
        artist: track.artists[0]?.name || '',
        artistSpotifyId: mainArtistId || null,
        allArtists: track.artists.map(a => a.name).join(', '),
        track: track.name,
        album: track.album.name,
        albumImage: track.album.images[0]?.url || null,
        image: artistImage,
        genres: artistGenres,
        releaseDate: track.album.release_date || null,
      });
    }

    if (artistId) {
      const r = await fetch(`https://api.spotify.com/v1/artists/${artistId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!r.ok) return res.status(404).json({ error: 'Artista no encontrado' });
      const artist = await r.json();
      return res.json({
        type: 'artist',
        artist: artist.name,
        track: null,
        image: artist.images[0]?.url || null,
      });
    }

    return res.status(400).json({ error: 'URL de Spotify no reconocida (debe ser /track/ o /artist/)' });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
