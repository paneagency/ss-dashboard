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

  const { url } = req.query;
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

      // Obtener foto de perfil del artista (no la portada del álbum)
      let artistImage = null;
      let artistGenres = [];
      const mainArtistId = track.artists[0]?.id;
      if (mainArtistId) {
        const ra = await fetch(`https://api.spotify.com/v1/artists/${mainArtistId}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (ra.ok) {
          const artistData = await ra.json();
          artistImage = artistData.images[0]?.url || null;
          artistGenres = artistData.genres || [];
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
