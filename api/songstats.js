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

// SongstatsArtistas columns (0-indexed):
// 0=FECHA | 1=SPOTIFY_ID | 2=ARTISTA | 3=BIO | 4=PAIS | 5=GENEROS |
// 6=INSTAGRAM | 7=TWITTER | 8=YOUTUBE | 9=TIKTOK |
// 10=MONTHLY_LISTENERS | 11=FOLLOWERS | 12=PLAYLIST_COUNT | 13=PLAYLIST_REACH | 14=STREAMS_TOTAL
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

// ── Read artist history from sheet ──────────────────────────────
async function getArtistHistory(spotifyArtistId) {
  const sheets = google.sheets({ version: 'v4', auth: getAuth() });
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'SongstatsArtistas!A:O',
  });
  const rows = resp.data.values || [];
  // Skip header row, filter by SPOTIFY_ID (col 1)
  return rows.slice(1)
    .filter(r => r[1] === spotifyArtistId)
    .map(r => ({
      fecha:            r[0]  || '',
      spotifyId:        r[1]  || '',
      artista:          r[2]  || '',
      bio:              r[3]  || '',
      pais:             r[4]  || '',
      generos:          r[5]  || '',
      instagram:        r[6]  || '',
      twitter:          r[7]  || '',
      youtube:          r[8]  || '',
      tiktok:           r[9]  || '',
      monthlyListeners: r[10] ? parseInt(r[10]) : null,
      followers:        r[11] ? parseInt(r[11]) : null,
      playlistCount:    r[12] ? parseInt(r[12]) : null,
      playlistReach:    r[13] ? parseInt(r[13]) : null,
      streamsTotal:     r[14] ? parseInt(r[14]) : null,
    }));
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

function qs(spId) {
  return `spotify_artist_id=${spId}`;
}

async function safeSS(path) {
  try { return await ss(path); }
  catch(e) { return { _error: e.message }; }
}

const delay = ms => new Promise(r => setTimeout(r, ms));

// ── Fetch artist data (2 calls: info + stats) ────────────────────
async function fetchArtistFull(spotifyArtistId) {
  const info = await safeSS(`/artists/info?${qs(spotifyArtistId)}`);
  await delay(1200);
  const stats = await safeSS(`/artists/stats?source=spotify&${qs(spotifyArtistId)}`);
  return { info, stats, callsUsed: 2 };
}

// ── Normalise helpers ────────────────────────────────────────────
function normaliseArtistInfo(raw) {
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

// ── Handler ──────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  if (req.method === 'GET') {
    const { action, spotifyArtistId } = req.query;

    if (action === 'credits') {
      const rl = await getRateStatus();
      return res.json({ ok: true, used: rl.used, limit: rl.limit, remaining: rl.remaining });
    }

    if (action === 'history' && spotifyArtistId) {
      try {
        const rows = await getArtistHistory(spotifyArtistId);
        return res.json({ ok: true, rows });
      } catch(e) {
        return res.status(500).json({ ok: false, error: e.message });
      }
    }

    return res.status(400).json({ error: 'action requerida' });
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const {
    spotifyArtistId,
    artistName,
    save  = false,
  } = req.body || {};

  if (!spotifyArtistId) return res.status(400).json({ error: 'spotifyArtistId requerido' });

  const today     = todayStr();
  const rl        = await getRateStatus();
  const artistKey = `songstats:v2:artist:${spotifyArtistId}:${today}`;

  // Check cache
  const cached = await kvGetJson(artistKey);
  if (cached) {
    if (save) {
      // Already cached today — no need to save again
    }
    return res.json({ ok: true, artist: cached, artistCached: true, credits: { used: rl.used, limit: RATE_LIMIT, remaining: rl.remaining } });
  }

  // Rate limit check
  if (rl.used + 2 > rl.limit) {
    return res.status(429).json({
      ok: false,
      error: `Límite diario alcanzado (${rl.used}/${rl.limit} llamadas usadas)`,
      credits: { used: rl.used, limit: RATE_LIMIT, remaining: 0 },
    });
  }

  // Fetch
  let artistData;
  try {
    const raw = await fetchArtistFull(spotifyArtistId);
    artistData = {
      info:  normaliseArtistInfo(raw.info),
      stats: normaliseArtistStats(raw.stats),
      _errors: {
        info:  raw.info?._error  || null,
        stats: raw.stats?._error || null,
      },
    };
    await kvSet(artistKey, artistData, 86400);
    for (let i = 0; i < raw.callsUsed; i++) { await kvIncr(rl.key); }
  } catch(e) {
    return res.status(500).json({ ok: false, error: e.message });
  }

  // Persist to Sheets (fire & forget, delayed to avoid Sheets quota conflicts)
  if (save) {
    (async () => {
      try {
        await delay(8000); // wait for other campaign writes to finish
        await saveArtistSnapshot(spotifyArtistId, artistName, artistData);
      } catch(e) { console.error('Songstats sheet save error:', e.message); }
    })();
  }

  const finalUsed = rl.used + 2;
  return res.json({
    ok: true,
    artist: artistData,
    artistCached: false,
    credits: { used: finalUsed, limit: RATE_LIMIT, remaining: Math.max(0, RATE_LIMIT - finalUsed) },
  });
};
