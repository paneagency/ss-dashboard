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
  if (n === 1) await kvCmd(['EXPIRE', key, 86400]); // TTL on first call
  return n;
}

// ── Rate limit ──────────────────────────────────────────────────
function todayKey() {
  return `songstats:calls:${new Date().toISOString().split('T')[0]}`;
}

async function getRateStatus() {
  const key  = todayKey();
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

async function appendRow(sheets, sheetName, row) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:A`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [row] },
  });
}

// ── Songstats API ────────────────────────────────────────────────
async function fetchSongstats(path) {
  const apiKey = process.env.SONGSTATS_API_KEY;
  if (!apiKey) throw new Error('SONGSTATS_API_KEY no configurada');
  const r = await fetch(`https://${SONGSTATS_HOST}${path}`, {
    headers: {
      'x-rapidapi-host': SONGSTATS_HOST,
      'x-rapidapi-key':  apiKey,
    },
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`Songstats ${r.status}: ${text.slice(0, 200)}`);
  try { return JSON.parse(text); } catch { throw new Error(`Songstats parse error: ${text.slice(0,100)}`); }
}

// ── Normalise raw Songstats response into a clean object ─────────
function normaliseArtist(raw) {
  // The API wraps data in stats[] array per source
  const sp = raw?.stats?.find?.(s => s.source === 'spotify') || {};
  const data = sp.data || sp || {};
  return {
    monthlyListeners:  data.monthly_listeners   ?? data.listeners ?? null,
    playlistCount:     data.playlist_count       ?? data.playlists_count ?? null,
    playlistReach:     data.playlist_reach       ?? null,
    streamsTotal:      data.total_streams        ?? data.streams_total   ?? null,
    streamsMonthly:    data.streams_monthly      ?? null,
    followers:         data.follower_count       ?? data.followers       ?? null,
    popularity:        data.popularity           ?? null,
    _raw: raw,
  };
}

function normaliseTrack(raw) {
  const sp = raw?.stats?.find?.(s => s.source === 'spotify') || {};
  const data = sp.data || sp || {};
  return {
    streamsTotal:   data.total_streams    ?? data.streams_total  ?? null,
    streamsDaily:   data.streams_daily    ?? null,
    streamsMonthly: data.streams_monthly  ?? null,
    playlistCount:  data.playlist_count   ?? data.playlists_count ?? null,
    playlistReach:  data.playlist_reach   ?? null,
    popularity:     data.popularity       ?? null,
    _raw: raw,
  };
}

// ── Save snapshots ───────────────────────────────────────────────
async function saveArtistSnapshot(sheets, artistId, artistName, norm) {
  const fecha = new Date().toISOString().split('T')[0];
  await appendRow(sheets, 'SongstatsArtistas', [
    fecha, artistId, artistName || '',
    norm.monthlyListeners ?? '', norm.playlistCount ?? '',
    norm.playlistReach ?? '', norm.streamsTotal ?? '',
    norm.streamsMonthly ?? '', norm.followers ?? '',
    JSON.stringify(norm._raw).slice(0, 3000),
  ]);
}

async function saveTrackSnapshot(sheets, trackId, trackName, artistName, norm) {
  const fecha = new Date().toISOString().split('T')[0];
  await appendRow(sheets, 'SongstatsTracks', [
    fecha, trackId, trackName || '', artistName || '',
    norm.streamsTotal ?? '', norm.streamsDaily ?? '',
    norm.streamsMonthly ?? '', norm.playlistCount ?? '',
    norm.playlistReach ?? '',
    JSON.stringify(norm._raw).slice(0, 3000),
  ]);
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
    mode,               // 'artist' | 'track' | 'both'
    spotifyArtistId,
    spotifyTrackId,
    artistName,
    trackName,
    save = false,       // if true, persist snapshot to Sheets
  } = req.body || {};

  if (!mode) return res.status(400).json({ error: 'mode requerido (artist|track|both)' });

  const today = new Date().toISOString().split('T')[0];

  // ── Check daily cache ──────────────────────────────────────────
  const artistCacheKey = spotifyArtistId ? `songstats:cache:artist:${spotifyArtistId}:${today}` : null;
  const trackCacheKey  = spotifyTrackId  ? `songstats:cache:track:${spotifyTrackId}:${today}`   : null;

  let artistNorm   = null, trackNorm   = null;
  let artistCached = false, trackCached = false;

  if (artistCacheKey && (mode === 'artist' || mode === 'both')) {
    const c = await kvGetJson(artistCacheKey);
    if (c) { artistNorm = c; artistCached = true; }
  }
  if (trackCacheKey && (mode === 'track' || mode === 'both')) {
    const c = await kvGetJson(trackCacheKey);
    if (c) { trackNorm = c; trackCached = true; }
  }

  const needArtist = (mode === 'artist' || mode === 'both') && !artistCached && !!spotifyArtistId;
  const needTrack  = (mode === 'track'  || mode === 'both') && !trackCached  && !!spotifyTrackId;
  const callsNeeded = (needArtist ? 1 : 0) + (needTrack ? 1 : 0);

  // ── Rate limit check ───────────────────────────────────────────
  const rl = await getRateStatus();
  if (callsNeeded > 0 && rl.used + callsNeeded > rl.limit) {
    return res.status(429).json({
      ok: false,
      error: `Límite diario alcanzado (${rl.used}/${rl.limit} llamadas usadas hoy)`,
      artist: artistNorm, artistCached,
      track:  trackNorm,  trackCached,
      credits: { used: rl.used, limit: rl.limit, remaining: 0 },
    });
  }

  // ── Fetch from Songstats API (parallel) ────────────────────────
  let usedCalls = 0;
  const [artistRaw, trackRaw] = await Promise.all([
    needArtist ? fetchSongstats(`/artists/stats?spotify_artist_id=${spotifyArtistId}&source=spotify`).catch(e => ({ _err: e.message })) : null,
    needTrack  ? fetchSongstats(`/tracks/stats?spotify_track_id=${spotifyTrackId}&source=spotify`).catch(e => ({ _err: e.message })) : null,
  ]);

  if (artistRaw !== null) {
    if (!artistRaw._err) {
      artistNorm = normaliseArtist(artistRaw);
      await kvSet(artistCacheKey, artistNorm, 86400);
      await kvIncr(rl.key);
      usedCalls++;
    } else {
      artistNorm = { error: artistRaw._err };
    }
  }
  if (trackRaw !== null) {
    if (!trackRaw._err) {
      trackNorm = normaliseTrack(trackRaw);
      await kvSet(trackCacheKey, trackNorm, 86400);
      await kvIncr(rl.key);
      usedCalls++;
    } else {
      trackNorm = { error: trackRaw._err };
    }
  }

  // ── Persist snapshot to Sheets (fire & forget) ────────────────
  if (save) {
    (async () => {
      try {
        const sheets = google.sheets({ version: 'v4', auth: getAuth() });
        if (artistNorm && !artistNorm.error && needArtist) {
          await saveArtistSnapshot(sheets, spotifyArtistId, artistName, artistNorm);
        }
        if (trackNorm && !trackNorm.error && needTrack) {
          await saveTrackSnapshot(sheets, spotifyTrackId, trackName, artistName, trackNorm);
        }
      } catch (e) {
        console.error('Songstats sheet save error:', e.message);
      }
    })();
  }

  const finalUsed = rl.used + usedCalls;
  return res.json({
    ok: true,
    artist: artistNorm, artistCached,
    track:  trackNorm,  trackCached,
    credits: { used: finalUsed, limit: RATE_LIMIT, remaining: Math.max(0, RATE_LIMIT - finalUsed) },
  });
}
