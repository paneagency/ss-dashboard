const { google } = require('googleapis');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const SONGSTATS_HOST = 'songstats.p.rapidapi.com';
const RATE_LIMIT     = 20;

// ── Upstash KV ──────────────────────────────────────────────────
async function kvCmd(cmd) {
  const url   = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  const d = await r.json();
  return d.result ?? null;
}

const kvGet    = key => kvCmd(['GET', key]);
const kvSet    = (key, val, ttl) => kvCmd(['SET', key, JSON.stringify(val), 'EX', ttl]);
const kvGetNum = async key => parseInt((await kvGet(key)) || '0');

async function kvGetJson(key) {
  const v = await kvGet(key);
  if (!v) return null;
  try { return JSON.parse(v); } catch { return null; }
}

async function kvIncr(key) {
  const n = await kvCmd(['INCR', key]);
  if (n === 1) await kvCmd(['EXPIRE', key, 86400]);
  return n;
}

// ── Rate limit ──────────────────────────────────────────────────
function todayStr() { return new Date().toISOString().split('T')[0]; }
function rateKey()  { return `songstats:calls:${todayStr()}`; }

async function getRateStatus() {
  const key  = rateKey();
  const used = await kvGetNum(key);
  return { key, used, limit: RATE_LIMIT, remaining: Math.max(0, RATE_LIMIT - used) };
}

// ── Google Sheets ───────────────────────────────────────────────
function getAuth() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

// SongstatsArtistas columns:
// FECHA | SPOTIFY_ID | ARTISTA | BIO | PAIS | GENEROS | INSTAGRAM | TWITTER | YOUTUBE | TIKTOK | MONTHLY_LISTENERS | FOLLOWERS | PLAYLIST_COUNT | PLAYLIST_REACH | STREAMS_TOTAL
async function saveArtistSnapshot(artistId, artistName, artistData) {
  const sheets = google.sheets({ version: 'v4', auth: getAuth() });
  const fecha  = todayStr();
  const info   = artistData.info  || {};
  const stats  = artistData.stats || {};
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: 'SongstatsArtistas!A:A',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[
      fecha,
      artistId,
      info.name || artistName || '',
      info.bio        || '',
      info.country    || '',
      (info.genres || []).join(', '),
      info.links?.instagram || '',
      info.links?.twitter   || '',
      info.links?.youtube   || '',
      info.links?.tiktok    || '',
      stats.monthlyListeners ?? '',
      stats.followers        ?? '',
      stats.playlistCount    ?? '',
      stats.playlistReach    ?? '',
      stats.streamsTotal     ?? '',
    ]] },
  });
}

// SongstatsTracks columns:
// FECHA | SPOTIFY_ID | TRACK | ARTISTA | STREAMS_TOTAL | STREAMS_DAILY | STREAMS_MONTHLY | PLAYLIST_COUNT | PLAYLIST_REACH
async function saveTrackSnapshot(trackId, trackName, artistName, trackData) {
  const sheets = google.sheets({ version: 'v4', auth: getAuth() });
  const fecha  = todayStr();
  const info   = trackData.info  || {};
  const stats  = trackData.stats || {};
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: 'SongstatsTracks!A:A',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[
      fecha,
      trackId,
      info.name || trackName || '',
      artistName || '',
      stats.streamsTotal   ?? '',
      stats.streamsDaily   ?? '',
      stats.streamsMonthly ?? '',
      stats.playlistCount  ?? '',
      stats.playlistReach  ?? '',
    ]] },
  });
}

// ── Songstats fetch ──────────────────────────────────────────────
async function ss(path) {
  const apiKey = process.env.SONGSTATS_API_KEY;
  if (!apiKey) throw new Error('SONGSTATS_API_KEY no configurada');
  const r = await fetch(`https://${SONGSTATS_HOST}${path}`, {
    headers: { 'x-rapidapi-host': SONGSTATS_HOST, 'x-rapidapi-key': apiKey },
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`Songstats ${r.status}: ${text.slice(0, 200)}`);
  try { return JSON.parse(text); } catch { throw new Error(`Parse error: ${text.slice(0,100)}`); }
}

// Build URL helpers — ssId optional for all
function qs(spId, ssId, extra = '') {
  return `spotify_artist_id=${spId}${ssId ? '&songstats_artist_id=' + ssId : ''}${extra}`;
}
function qst(spId, ssId, extra = '') {
  return `spotify_track_id=${spId}${ssId ? '&songstats_track_id=' + ssId : ''}${extra}`;
}

// ── Safe parallel fetch: returns null on error ───────────────────
async function safeSS(path) {
  try { return await ss(path); }
  catch(e) { return { _error: e.message }; }
}

const delay = ms => new Promise(r => setTimeout(r, ms));

// ── Fetch all artist data ────────────────────────────────────────
// Returns { info, stats, callsUsed, _rawInfo, _rawStats }
async function fetchArtistFull(spotifyArtistId) {
  // Sequential to avoid per-second rate limit on BASIC plan
  const info = await safeSS(`/artists/info?${qs(spotifyArtistId, null)}`);
  await delay(1200);
  const stats = await safeSS(`/artists/stats?source=spotify&${qs(spotifyArtistId, null)}`);
  const ssId = info?.artist_info?.songstats_artist_id || null;
  return { info, stats, audience: null, topTracks: [], topPlaylists: [], callsUsed: 2, ssId, _rawInfo: info, _rawStats: stats };
}

// ── Fetch track data ─────────────────────────────────────────────
async function fetchTrackFull(spotifyTrackId) {
  const info = await safeSS(`/tracks/info?${qst(spotifyTrackId, null)}`);
  await delay(1200);
  const stats = await safeSS(`/tracks/stats?source=spotify&${qst(spotifyTrackId, null)}`);
  return { info, stats, callsUsed: 2, _rawInfo: info, _rawStats: stats };
}

// ── Normalise helpers ────────────────────────────────────────────
function normaliseArtistInfo(raw) {
  // API returns data under artist_info key; links is an array of {source, url}
  const i = raw?.artist_info || raw?.info || raw?.artist || raw || {};
  const linksArr = Array.isArray(i.links) ? i.links : [];
  const findLink = src => linksArr.find(l => l.source === src)?.url || null;
  return {
    name:     i.name || null,
    bio:      i.bio || i.biography || null,
    country:  i.country || null,
    genres:   i.genres || [],
    image:    i.avatar || i.image || i.image_url || null,
    ssId:     i.songstats_artist_id || null,
    links: {
      spotify:   findLink('spotify'),
      instagram: findLink('instagram'),
      twitter:   findLink('twitter') || findLink('x'),
      facebook:  findLink('facebook'),
      youtube:   findLink('youtube'),
      tiktok:    findLink('tiktok'),
      wikipedia: findLink('wikipedia'),
    },
  };
}

function normaliseArtistStats(raw) {
  const sp = (raw?.stats || []).find(s => s.source === 'spotify') || {};
  const d  = sp.data || sp || {};
  return {
    monthlyListeners: d.monthly_listeners_current ?? d.monthly_listeners   ?? null,
    followers:        d.followers_total            ?? d.follower_count      ?? d.followers ?? null,
    playlistCount:    d.playlists_current          ?? d.playlist_count      ?? d.playlists_count ?? null,
    playlistReach:    d.playlist_reach_current     ?? d.playlist_reach      ?? null,
    streamsTotal:     d.streams_total              ?? d.total_streams       ?? null,
  };
}

function normaliseAudience(raw) {
  const sp = (raw?.audience || []).find(s => s.source === 'spotify') || raw?.audience?.[0] || {};
  const d  = sp.data || {};
  return {
    topCountries: (d.top_cities || d.top_countries || []).slice(0, 8).map(c => ({
      code:  c.country_code || c.code || '',
      name:  c.country_name || c.name || c.city_name || '',
      value: c.listeners    || c.value || 0,
    })),
  };
}

function normaliseTopTracks(raw) {
  const list = raw?.tracks || raw?.top_tracks || raw?.catalog || [];
  return list.slice(0, 10).map(t => ({
    id:       t.spotify_track_id || t.track_id || null,
    name:     t.name || t.track_name || '—',
    streams:  t.total_streams   || t.streams_total || null,
    playlists:t.playlist_count  || t.playlists     || null,
    image:    t.image           || t.image_url     || null,
  }));
}

function normaliseTopPlaylists(raw) {
  const list = raw?.playlists || raw?.top_playlists || [];
  return list.slice(0, 10).map(p => ({
    name:      p.name          || p.playlist_name || '—',
    curator:   p.curator_name  || p.curator       || p.owner || '',
    followers: p.followers     || p.follower_count || null,
    image:     p.image         || p.image_url     || null,
    type:      p.playlist_type || p.type          || '',
    url:       p.spotify_url   || p.url           || null,
  }));
}

// statsRaw is passed as fallback: track_info is also returned in /tracks/stats response
function normaliseTrackInfo(infoRaw, statsRaw) {
  const t  = infoRaw?.track_info  || infoRaw?.track  || infoRaw?.info  || infoRaw  || {};
  const fb = statsRaw?.track_info || {};
  return {
    name:        t.title || t.name || t.track_name || fb.title || fb.name || null,
    releaseDate: t.release_date || fb.release_date || null,
    isrc:        t.isrc || null,
    image:       t.avatar || t.image || t.image_url || fb.avatar || null,
    ssId:        t.songstats_track_id || fb.songstats_track_id || null,
  };
}

function normaliseTrackStats(raw) {
  const sp = (raw?.stats || []).find(s => s.source === 'spotify') || {};
  const d  = sp.data || sp || {};
  return {
    streamsTotal:   d.streams_total              ?? d.total_streams       ?? null,
    streamsMonthly: d.streams_monthly            ?? null,
    streamsDaily:   d.streams_daily              ?? null,
    playlistCount:  d.playlists_current          ?? d.playlist_count      ?? d.playlists_count ?? null,
    playlistReach:  d.playlist_reach_current     ?? d.playlist_reach      ?? null,
    popularity:     d.popularity_current         ?? d.popularity          ?? null,
  };
}

// ── Handler ──────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET ?action=credits
  if (req.method === 'GET') {
    if (req.query.action === 'credits') {
      const rl = await getRateStatus();
      return res.json({ ok: true, used: rl.used, limit: rl.limit, remaining: rl.remaining });
    }
    return res.status(400).json({ error: 'action requerida' });
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const {
    mode,             // 'artist' | 'track' | 'both'
    spotifyArtistId,
    spotifyTrackId,
    artistName,
    trackName,
    save  = false,
    debug = false,    // bypass cache, return raw API response
  } = req.body || {};

  if (!mode) return res.status(400).json({ error: 'mode requerido' });

  const today      = todayStr();
  const rl         = await getRateStatus();
  const artistKey  = spotifyArtistId ? `songstats:v2:artist:${spotifyArtistId}:${today}` : null;
  const trackKey   = spotifyTrackId  ? `songstats:v2:track:${spotifyTrackId}:${today}`   : null;

  let artistData = null, trackData = null;
  let artistCached = false, trackCached = false;

  // Check cache (bypassed when debug=true)
  if (!debug && artistKey && (mode === 'artist' || mode === 'both')) {
    const c = await kvGetJson(artistKey);
    if (c) { artistData = c; artistCached = true; }
  }
  if (!debug && trackKey && (mode === 'track' || mode === 'both')) {
    const c = await kvGetJson(trackKey);
    if (c) { trackData = c; trackCached = true; }
  }

  const needArtist = (mode === 'artist' || mode === 'both') && !artistCached && !!spotifyArtistId;
  const needTrack  = (mode === 'track'  || mode === 'both') && !trackCached  && !!spotifyTrackId;

  // Estimate calls needed (artist = 2: info+stats, track = 2: info+stats)
  const callsNeeded = (needArtist ? 2 : 0) + (needTrack ? 2 : 0);
  if (callsNeeded > 0 && rl.used + callsNeeded > rl.limit) {
    return res.status(429).json({
      ok: false,
      error: `Límite diario alcanzado (${rl.used}/${rl.limit} llamadas usadas)`,
      artist: artistData, artistCached,
      track:  trackData,  trackCached,
      credits: { used: rl.used, limit: rl.limit, remaining: 0 },
    });
  }

  let usedCalls = 0;

  // Fetch artist
  if (needArtist) {
    try {
      const raw = await fetchArtistFull(spotifyArtistId);
      artistData = {
        info:         normaliseArtistInfo(raw.info),
        stats:        normaliseArtistStats(raw.stats),
        audience:     normaliseAudience(raw.audience),
        topTracks:    normaliseTopTracks(raw.topTracks),
        topPlaylists: normaliseTopPlaylists(raw.topPlaylists),
        _errors: {
          info:      raw.info?._error         || null,
          stats:     raw.stats?._error        || null,
          audience:  raw.audience?._error     || null,
          topTracks: raw.topTracks?._error    || null,
          topPl:     raw.topPlaylists?._error || null,
        },
        _rawInfo:  raw._rawInfo,
        _rawStats: raw._rawStats,
      };
      await kvSet(artistKey, artistData, 86400);
      // Increment once per batch (Songstats seems to count calls server-side, we track locally)
      for (let i = 0; i < raw.callsUsed; i++) { await kvIncr(rl.key); }
      usedCalls += raw.callsUsed;
    } catch(e) {
      artistData = { error: e.message };
    }
  }

  // Fetch track (add delay if artist was just fetched to avoid per-second rate limit)
  if (needTrack) {
    if (usedCalls > 0) await delay(1500);
    try {
      const raw = await fetchTrackFull(spotifyTrackId);
      trackData = {
        info:  normaliseTrackInfo(raw.info, raw.stats),  // stats as fallback for track_info
        stats: normaliseTrackStats(raw.stats),
        _errors: {
          info:  raw.info?._error  || null,
          stats: raw.stats?._error || null,
        },
        _rawInfo:  raw._rawInfo,
        _rawStats: raw._rawStats,
      };
      await kvSet(trackKey, trackData, 86400);
      for (let i = 0; i < raw.callsUsed; i++) { await kvIncr(rl.key); }
      usedCalls += raw.callsUsed;
    } catch(e) {
      trackData = { error: e.message };
    }
  }

  // ── Persist to Sheets (fire & forget) ───────────────────────────
  if (save) {
    (async () => {
      try {
        if (artistData && !artistData.error && needArtist) {
          await saveArtistSnapshot(spotifyArtistId, artistName, artistData);
        }
        if (trackData && !trackData.error && needTrack) {
          await saveTrackSnapshot(spotifyTrackId, trackName, artistName, trackData);
        }
      } catch(e) {
        console.error('Songstats sheet save error:', e.message);
      }
    })();
  }

  const finalUsed = rl.used + usedCalls;
  return res.json({
    ok: true,
    artist: artistData, artistCached,
    track:  trackData,  trackCached,
    credits: { used: finalUsed, limit: RATE_LIMIT, remaining: Math.max(0, RATE_LIMIT - finalUsed) },
    _debug: { artistRawInfo: artistData?._rawInfo, artistRawStats: artistData?._rawStats, trackRawInfo: trackData?._rawInfo, trackRawStats: trackData?._rawStats },
  });
};
