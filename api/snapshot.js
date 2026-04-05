// api/snapshot.js
// Daily snapshot of all Spotify playlists → appends rows to HistorialPlaylists sheet.
// Called once per day by a Make scheduled scenario (1 Make operation total).
// Optional security: set SNAPSHOT_SECRET env var; pass as ?secret=xxx or body.secret

const { google } = require('googleapis');
const nodemailer = require('nodemailer');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const CAMPANAS_SHEET = 'CampañasCalendario';
const ACTIVE_STATES  = ['activa','pendiente_pago','prueba','regalo','pendiente_inicio'];

const SP_RANGES = [
  { key: 'TOP5_20',   from: 6,  to: 20  },
  { key: 'TOP20_40',  from: 21, to: 40  },
  { key: 'TOP40_60',  from: 41, to: 60  },
  { key: 'TOP60_70',  from: 61, to: 70  },
  { key: 'TOP70_100', from: 71, to: 100 },
];
const SP_DEFAULT_CAPS = { TOP5_20: 3, TOP20_40: 5, TOP40_60: 10, TOP60_70: 10, TOP70_100: 30 };

// ── Google Sheets ────────────────────────────────────────────
function getSheets() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ── Spotify Client Credentials ───────────────────────────────
let _ccToken = null, _ccExpiry = 0;
async function getCCToken() {
  if (_ccToken && Date.now() < _ccExpiry) return _ccToken;
  const creds = Buffer.from(`${process.env.SPOTIFY_CLIENT_ID}:${process.env.SPOTIFY_CLIENT_SECRET}`).toString('base64');
  const r = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=client_credentials',
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`Spotify CC token: ${d.error_description || d.error}`);
  _ccToken = d.access_token;
  _ccExpiry = Date.now() + (d.expires_in - 60) * 1000;
  return _ccToken;
}

// ── Spotify OAuth (for listing our playlists) ────────────────
async function kvCmd(cmd) {
  const url = process.env.KV_REST_API_URL;
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

async function kvGet(key) { return kvCmd(['GET', key]); }
async function kvSet(key, val, ttl) { return kvCmd(['SET', key, JSON.stringify(val), 'EX', ttl]); }
async function kvDel(key) { return kvCmd(['DEL', key]); }
async function kvGetJson(key) {
  const v = await kvGet(key);
  if (!v) return null;
  try { return JSON.parse(v); } catch { return null; }
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
    const d = await r.json();
    cursor = parseInt(d.result?.[0] || 0);
    keys.push(...(d.result?.[1] || []));
  } while (cursor !== 0);
  return keys;
}

async function getOAuthToken() {
  const clientId     = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const keys = await kvScan('spotify:owner:*');
  if (!keys.length) throw new Error('No hay cuentas Spotify autorizadas');
  const userId = keys[0].replace('spotify:owner:', '');
  const refreshToken = await kvGet(`spotify:owner:${userId}`);
  const r = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }),
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`OAuth token: ${d.error_description || d.error}`);
  return d.access_token;
}

// ── Fetch all playlists via OAuth ────────────────────────────
async function fetchAllPlaylists(accessToken) {
  const playlists = [];
  let url = 'https://api.spotify.com/v1/me/playlists?limit=50';
  while (url) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
    const d = await r.json();
    if (!r.ok) throw new Error(`Spotify playlists: ${d.error?.message || r.status}`);
    playlists.push(...(d.items || []));
    url = d.next || null;
  }
  return playlists;
}

// ── Fetch public playlists of any Spotify user (CC token) ────
async function fetchUserPublicPlaylists(userId, ccToken) {
  const playlists = [];
  let url = `https://api.spotify.com/v1/users/${encodeURIComponent(userId)}/playlists?limit=50`;
  while (url) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${ccToken}` } });
    if (!r.ok) break;
    const d = await r.json();
    playlists.push(...(d.items || []));
    url = d.next || null;
  }
  return playlists;
}

// ── Get sheet ID by name ──────────────────────────────────────
async function getSheetId(sheets, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties' });
  const sheet = (meta.data.sheets || []).find(s => s.properties.title === sheetName);
  return sheet?.properties?.sheetId ?? null;
}

// ── Fetch playlist detail + tracks via OAuth ─────────────────
async function fetchPlaylistDetail(playlistId, accessToken) {
  const headers = { Authorization: `Bearer ${accessToken}` };

  // Metadata (followers, name) — tracks field may be null in some cases
  const metaRes = await fetch(`https://api.spotify.com/v1/playlists/${playlistId}`, { headers });
  if (!metaRes.ok) return null;
  const meta = await metaRes.json();

  // Dedicated tracks endpoint — always works regardless of tracks field in meta
  const tracks = [];
  let tracksUrl = `https://api.spotify.com/v1/playlists/${playlistId}/tracks?limit=100&fields=items(track(id,artists,name)),next,total`;
  let totalTracks = 0;
  while (tracksUrl && tracks.length < 100) {
    const r = await fetch(tracksUrl, { headers });
    if (!r.ok) break;
    const d = await r.json();
    if (!totalTracks) totalTracks = d.total || 0;
    (d.items || []).forEach(item => {
      if (item.track?.id && tracks.length < 100) tracks.push({
        position: tracks.length + 1,
        id: item.track.id,
        artist: item.track.artists?.[0]?.name || '',
      });
    });
    tracksUrl = d.next || null;
  }

  return {
    id: meta.id,
    name: meta.name,
    followers: meta.followers?.total || 0,
    totalTracks,
    tracks,
  };
}

// ── Email alert helper ────────────────────────────────────────
async function sendCronAlert(subject, body) {
  const user = process.env.GMAIL_USER;
  const pass = process.env.GMAIL_PASS;
  if (!user || !pass) return;
  try {
    const transporter = nodemailer.createTransport({ service: 'gmail', auth: { user, pass } });
    await transporter.sendMail({
      from: `SS Dashboard <${user}>`,
      to: 'paneagency@gmail.com',
      subject,
      text: body,
    });
  } catch(e) {
    console.error('[alert email]', e.message);
  }
}

// ── Refresh YouTube Analytics KPIs (called from cron) ────────
async function refreshYtAnalytics(sheets) {
  const rt = await kvGet('youtube:refresh_token');
  if (!rt) { console.log('[YT cron] No YouTube refresh token, skipping'); return { count: 0 }; }

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token', refresh_token: rt,
      client_id: process.env.GOOGLE_CLIENT_ID, client_secret: process.env.GOOGLE_CLIENT_SECRET,
    }),
  });
  const td = await tokenRes.json();
  if (!tokenRes.ok) throw new Error(`YT token: ${td.error}`);
  const accessToken = td.access_token;

  const endDate = new Date().toISOString().slice(0, 10);
  const startDate = new Date(Date.now() - 28 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const analyticsUrl = new URL('https://youtubeanalytics.googleapis.com/v2/reports');
  analyticsUrl.searchParams.set('ids', 'channel==MINE');
  analyticsUrl.searchParams.set('startDate', startDate);
  analyticsUrl.searchParams.set('endDate', endDate);
  analyticsUrl.searchParams.set('dimensions', 'playlist');
  analyticsUrl.searchParams.set('metrics', 'playlistStarts,viewsPerPlaylistStart,averageTimeInPlaylist');
  analyticsUrl.searchParams.set('sort', '-playlistStarts');
  analyticsUrl.searchParams.set('maxResults', '50');

  const analyticsRes = await fetch(analyticsUrl.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
  const analyticsData = await analyticsRes.json();
  if (!analyticsRes.ok) throw new Error(`YT Analytics: ${analyticsData.error?.message || analyticsRes.status}`);

  // Get playlist names from sheet
  const plSheetResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID, range: 'YouTubePlaylists!A:B',
  }).catch(() => ({ data: { values: [] } }));
  const plNameMap = {};
  (plSheetResp.data.values || []).slice(1).forEach(r => {
    if (r[0]) plNameMap[r[0].trim()] = r[1]?.trim() || r[0].trim();
  });

  // Fetch names for playlists not in the sheet via YouTube Data API
  const rows = analyticsData.rows || [];
  const unknownIds = rows.map(r => r[0]).filter(id => !plNameMap[id]);
  if (unknownIds.length && process.env.YOUTUBE_API_KEY) {
    try {
      for (let i = 0; i < unknownIds.length; i += 50) {
        const batch = unknownIds.slice(i, i + 50).join(',');
        const url = new URL('https://www.googleapis.com/youtube/v3/playlists');
        url.searchParams.set('part', 'snippet');
        url.searchParams.set('id', batch);
        url.searchParams.set('key', process.env.YOUTUBE_API_KEY);
        const r = await fetch(url.toString());
        const d = await r.json();
        (d.items || []).forEach(item => { plNameMap[item.id] = item.snippet?.title || item.id; });
      }
    } catch(_) {}
  }
  const playlists = rows.map(r => ({
    playlistId: r[0],
    nombre: plNameMap[r[0]] || r[0],
    starts: r[1],
    viewsPerStart: Math.round(r[2] * 10) / 10,
    avgMinutes: Math.round(r[3] * 10) / 10,
    estimatedViews: Math.round(r[1] * r[2]),
  })).sort((a, b) => b.estimatedViews - a.estimatedViews);

  const totalViews = playlists.reduce((s, p) => s + p.estimatedViews, 0);
  const payload = { ok: true, totalViews, playlists, startDate, endDate, updatedAt: new Date().toISOString() };
  await kvSet('ytAnalytics:all', payload, 26 * 3600);
  console.log(`[YT cron] Analytics refreshed: ${playlists.length} playlists, ${totalViews.toLocaleString()} est. views`);
  return { count: playlists.length };
}

// ── Main handler ─────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Optional secret check
  const secret = req.query.secret || req.body?.secret;
  if (process.env.SNAPSHOT_SECRET && secret !== process.env.SNAPSHOT_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  // ── GET action=history: leer HistorialPlaylists para gráficos ──
  if (req.method === 'GET' && req.query.action === 'history') {
    const HISTORY_CACHE_KEY = 'snapshot:history:v1';
    const HISTORY_TTL = 4 * 3600; // 4 horas
    try {
      // Serve from KV cache if available
      const cached = await kvGetJson(HISTORY_CACHE_KEY);
      if (cached) {
        res.setHeader('X-Cache', 'HIT');
        return res.json({ ok: true, data: cached });
      }
      const sheets = getSheets();
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'HistorialPlaylists!A:S',
      });
      const rows = (resp.data.values || []).slice(1);
      const data = rows.map(r => ({
        date: r[0] || '',
        id: r[1] || '',
        name: r[2] || '',
        followers: parseInt(r[3]) || 0,
        totalTracks: parseInt(r[4]) || 0,
        image: r[16] || '',
        grupo: r[18] || '',
      })).filter(r => r.date && r.id);
      // Cache for next requests (fire-and-forget)
      kvSet(HISTORY_CACHE_KEY, data, HISTORY_TTL).catch(() => {});
      res.setHeader('X-Cache', 'MISS');
      return res.json({ ok: true, data });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET action=deltas: delta followers últimas 24hs por playlist ──
  if (req.method === 'GET' && req.query.action === 'deltas') {
    try {
      const sheets = getSheets();
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'HistorialPlaylists!A:D',
      });
      const rows = (resp.data.values || []).slice(1).filter(r => r[0] && r[1]);
      // Group by playlist ID, keep last 2 entries (sorted by date)
      const byId = {};
      for (const r of rows) {
        const id = r[1];
        if (!byId[id]) byId[id] = [];
        byId[id].push({ date: r[0], followers: parseInt(r[3]) || 0 });
      }
      const deltas = {};
      for (const [id, entries] of Object.entries(byId)) {
        if (entries.length < 2) { deltas[id] = null; continue; }
        entries.sort((a, b) => a.date.localeCompare(b.date));
        // Deduplicate by date keeping last entry per date
        const deduped = [];
        for (const e of entries) {
          if (deduped.length && deduped[deduped.length - 1].date === e.date) {
            deduped[deduped.length - 1] = e;
          } else {
            deduped.push(e);
          }
        }
        if (deduped.length < 2) { deltas[id] = null; continue; }
        const last = deduped[deduped.length - 1];
        const prev = deduped[deduped.length - 2];
        deltas[id] = last.followers - prev.followers;
      }
      return res.json({ ok: true, deltas });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET action=cron: snapshot automático diario (Vercel cron) ──
  if (req.method === 'GET' && req.query.action === 'cron') {
    try {
      const sheets = getSheets();
      const ccToken = await getCCToken();

      // 1. Leer ProveedoresSpotify: A=GRUPO, B=PLAYLIST_ID (CC token funciona para playlists públicas)
      const provResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'ProveedoresSpotify!A:B',
      }).catch(() => ({ data: { values: [] } }));
      const providerPlaylists = (provResp.data.values || []).slice(1)
        .filter(r => r[0] && r[1])
        .map(r => ({ grupo: r[0].trim(), playlistId: r[1].trim() }));

      // 2. Mapa playlistId → grupo para playlists de proveedores
      const playlistGrupoMap = {};
      providerPlaylists.forEach(p => { if (!playlistGrupoMap[p.playlistId]) playlistGrupoMap[p.playlistId] = p.grupo; });

      // 3. Traer playlists de la cuenta OAuth vinculada
      const oauthToken = await getOAuthToken();
      const rawPlaylists = await fetchAllPlaylists(oauthToken);

      // 4. Unir IDs únicos (playlists de proveedores + cuenta propia)
      const allIds = [...new Set([...rawPlaylists.map(p => p.id), ...Object.keys(playlistGrupoMap)])];

      const today = new Date().toISOString().slice(0, 10);

      // 5. Skip playlists already processed today (resumable across multiple cron runs)
      const doneKey = `snapshot:done:${today}`;
      const doneRaw = await kvGetJson(doneKey);
      const doneSet = new Set(Array.isArray(doneRaw) ? doneRaw : []);
      const pendingIds = allIds.filter(id => !doneSet.has(id));
      console.log(`[cron] ${pendingIds.length} pendientes de ${allIds.length} totales (${doneSet.size} ya procesadas hoy)`);

      // 6. Fetch en batches paralelos de 5 con 1s entre batches → ~1500 playlists en 300s
      const delay = ms => new Promise(r => setTimeout(r, ms));
      const fetchOne = async (plId) => {
        try {
          const r = await fetch(
            `https://api.spotify.com/v1/playlists/${plId}?fields=id,name,description,followers,tracks(total),images`,
            { headers: { Authorization: `Bearer ${ccToken}` } }
          );
          if (r.status === 429) { console.warn(`[cron] Rate limit en ${plId}, skipping`); return null; }
          if (!r.ok) return null;
          const full = await r.json();
          return { id: full.id, name: full.name, description: full.description || '', image: full.images?.[0]?.url || '', followers: full.followers?.total || 0, totalTracks: full.tracks?.total || 0, grupo: playlistGrupoMap[plId] || '' };
        } catch(_) { return null; }
      };

      const BATCH_SIZE = 5;
      const BATCH_DELAY = 1000; // ms entre batches → ~5 req/s en burst, promedio seguro
      const playlists = [];
      const processedThisRun = [];
      for (let i = 0; i < pendingIds.length; i += BATCH_SIZE) {
        const batch = pendingIds.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(batch.map(fetchOne));
        results.forEach((pl, j) => {
          if (pl) { playlists.push(pl); processedThisRun.push(batch[j]); }
        });
        if (i + BATCH_SIZE < pendingIds.length) await delay(BATCH_DELAY);
      }

      // Persistir IDs procesados en KV para que la segunda corrida los saltee
      const allDone = [...doneSet, ...processedThisRun];
      kvSet(doneKey, allDone, 26 * 3600).catch(() => {});

      const rows = playlists.map(pl => [
        today, pl.id, pl.name, pl.followers, pl.totalTracks,
        '', '', '', '', '', '', '', '', '', '', '', pl.image, pl.description, pl.grupo,
      ]);
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'HistorialPlaylists!A:S',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rows },
      });
      console.log(`Cron snapshot ${today}: ${rows.length} playlists (${Object.keys(playlistGrupoMap).length} de proveedores)`);
      // Invalidate history cache so next request reads fresh data
      kvDel('snapshot:history:v1').catch(() => {});

      // Refresh YouTube Analytics KPIs
      let ytPlaylists = 0;
      try {
        const ytResult = await refreshYtAnalytics(sheets);
        ytPlaylists = ytResult.count;
      } catch(e) {
        console.error('[YT cron]', e.message);
        sendCronAlert(
          `⚠️ SS Dashboard — Error en cron YouTube (${today})`,
          `El cron diario falló al actualizar YouTube Analytics.\n\nError: ${e.message}\n\nLos KPIs de YouTube pueden estar desactualizados.`
        ).catch(() => {});
      }

      return res.json({ ok: true, date: today, playlists: rows.length, byProvider: Object.keys(playlistGrupoMap).length, ytPlaylists });
    } catch(e) {
      console.error('cron snapshot error:', e.message);
      sendCronAlert(
        `🚨 SS Dashboard — Error en cron Spotify (${new Date().toISOString().slice(0,10)})`,
        `El cron diario de snapshots falló y NO se guardaron los datos del día.\n\nError: ${e.message}\n\nRevisá los logs en Vercel: https://vercel.com/paneagency/ss-dashboard/logs`
      ).catch(() => {});
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET action=preview: metadata de una playlist por ID (CC token, sin snapshot) ──
  if (req.method === 'GET' && req.query.action === 'preview') {
    const { playlistId } = req.query;
    if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
    try {
      // Serve from KV cache (24h TTL) to avoid hitting Spotify rate limits
      const cacheKey = `spPreview:${playlistId}`;
      const cached = await kvGetJson(cacheKey);
      if (cached) return res.json({ ok: true, playlist: cached, fromCache: true });

      const ccToken = await getCCToken();
      const r = await fetch(
        `https://api.spotify.com/v1/playlists/${playlistId}?fields=id,name,description,followers,tracks(total),images,owner`,
        { headers: { Authorization: `Bearer ${ccToken}` } }
      );
      const text = await r.text();
      if (!r.ok) {
        let spotifyError = null;
        try { spotifyError = JSON.parse(text); } catch(_) {}
        const retryAfter = r.headers.get('Retry-After');
        const errorMsg = r.status === 429
          ? `Rate limit de Spotify${retryAfter ? ` — esperá ${retryAfter}s` : ' — intentá en unos segundos'}`
          : (spotifyError?.error?.message || text.slice(0, 100));
        return res.json({ ok: false, playlistId, status: r.status, error: errorMsg });
      }
      const body = JSON.parse(text);
      const playlist = {
        id: body.id,
        name: body.name,
        image: body.images?.[0]?.url || '',
        followers: body.followers?.total || 0,
        totalTracks: body.tracks?.total || 0,
        owner: body.owner?.display_name || body.owner?.id || '',
        description: body.description || '',
      };
      kvSet(cacheKey, playlist, 24 * 3600).catch(() => {});
      return res.json({ ok: true, playlist });
    } catch(e) {
      return res.status(500).json({ ok: false, playlistId: req.query.playlistId || null, error: e.message });
    }
  }

  // ── CRUD ProveedoresSpotify ───────────────────────────────────
  if (req.query.action === 'providers') {
    const sheets = getSheets();

    // GET: listar todos
    if (req.method === 'GET') {
      try {
        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: 'ProveedoresSpotify!A:F',
        }).catch(() => ({ data: { values: [] } }));
        const providers = (resp.data.values || []).slice(1)
          .map((r, i) => ({ row: i + 2, grupo: r[0]?.trim() || '', userId: r[1]?.trim() || '', name: r[2]?.trim() || '', image: r[3]?.trim() || '', followers: parseInt(r[4]) || undefined, owner: r[5]?.trim() || '' }))
          .filter(p => p.grupo && p.userId);
        return res.json({ ok: true, providers });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    // POST: agregar entradas — acepta array {items:[{grupo,userId,name,image,followers,owner}]} o single {grupo,userId,...}
    if (req.method === 'POST') {
      const body = req.body || {};
      // Normalizar a array
      const items = body.items
        ? body.items
        : (body.grupo && body.userId ? [body] : null);
      if (!items?.length) return res.status(400).json({ error: 'items[] o grupo+userId requeridos' });
      try {
        // Batch write a ProveedoresSpotify
        const rows = items.map(({ grupo, userId, name, image, followers, owner }) =>
          [grupo.trim(), userId.trim(), name || '', image || '', followers || '', owner || '']
        );
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: 'ProveedoresSpotify!A:F',
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values: rows },
        });
        // Agregar grupos nuevos a hoja Proveedores (una sola lectura + una escritura)
        const provResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: 'Proveedores!A:A',
        }).catch(() => ({ data: { values: [] } }));
        const existingProviders = new Set((provResp.data.values || []).slice(1).map(r => r[0]?.trim().toLowerCase()).filter(Boolean));
        const newGroups = [...new Set(items.map(i => i.grupo.trim()))].filter(g => !existingProviders.has(g.toLowerCase()));
        if (newGroups.length) {
          await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: 'Proveedores!A:A',
            valueInputOption: 'RAW',
            insertDataOption: 'INSERT_ROWS',
            resource: { values: newGroups.map(g => [g]) },
          });
        }
        return res.json({ ok: true, saved: rows.length });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    // DELETE: eliminar fila por número de fila
    if (req.method === 'DELETE') {
      const { row } = req.body || {};
      if (!row) return res.status(400).json({ error: 'row requerido' });
      try {
        const sheetId = await getSheetId(sheets, 'ProveedoresSpotify');
        if (sheetId === null) return res.status(404).json({ error: 'Hoja ProveedoresSpotify no encontrada' });
        await sheets.spreadsheets.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
          resource: {
            requests: [{
              deleteDimension: {
                range: { sheetId, dimension: 'ROWS', startIndex: row - 1, endIndex: row },
              },
            }],
          },
        });
        return res.json({ ok: true });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    return res.status(405).json({ error: 'Método no soportado' });
  }

  // ── DEBUG: ver respuesta cruda de Spotify para una playlist ──
  if (req.query.action === 'debug' && req.query.playlistId) {
    try {
      const oauthToken = await getOAuthToken();
      const r = await fetch(`https://api.spotify.com/v1/playlists/${req.query.playlistId}`, {
        headers: { Authorization: `Bearer ${oauthToken}` },
      });
      const raw = await r.json();
      // Also test dedicated tracks endpoint
      const tr = await fetch(`https://api.spotify.com/v1/playlists/${req.query.playlistId}/tracks?limit=3`, {
        headers: { Authorization: `Bearer ${oauthToken}` },
      });
      const tracksData = await tr.json();
      return res.json({
        meta_status: r.status,
        followersTotal: raw.followers?.total,
        meta_hasTracks: !!raw.tracks,
        tracks_status: tr.status,
        tracks_total: tracksData.total,
        tracks_itemsCount: tracksData.items?.length,
        firstTrack: tracksData.items?.[0]?.track?.name || null,
        tracks_error: tracksData.error || null,
      });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  try {
    const sheets = getSheets();
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

    const clientPlaylists = req.body?.playlists;
    if (!clientPlaylists || !Array.isArray(clientPlaylists)) {
      return res.status(400).json({ error: 'playlists[] requerido en el body' });
    }

    const hasTrackData = clientPlaylists.some(pl => pl.tracks?.length > 0);

    let campTrackMap = {};
    let plCaps = {};

    if (hasTrackData) {
      // Solo leer campañas y caps si hay datos de tracks
      const campResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:V`,
      });
      (campResp.data.values || []).slice(1)
        .filter(r => ACTIVE_STATES.includes(r[7]))
        .forEach(r => {
          const pauta = r[16] || '';
          let m; const re = /open\.spotify\.com(?:\/intl-[^/]+)?\/track\/([A-Za-z0-9]+)/g;
          while ((m = re.exec(pauta)) !== null) campTrackMap[m[1]] = r[0] || '';
        });

      const capsResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'CapacidadPlaylists!A:G',
      }).catch(() => ({ data: { values: [] } }));
      (capsResp.data.values || []).slice(1).forEach(r => {
        if (!r[0]?.trim()) return;
        plCaps[r[0].trim()] = {
          TOP5_20:   parseInt(r[2]) || SP_DEFAULT_CAPS.TOP5_20,
          TOP20_40:  parseInt(r[3]) || SP_DEFAULT_CAPS.TOP20_40,
          TOP40_60:  parseInt(r[4]) || SP_DEFAULT_CAPS.TOP40_60,
          TOP60_70:  parseInt(r[5]) || SP_DEFAULT_CAPS.TOP60_70,
          TOP70_100: parseInt(r[6]) || SP_DEFAULT_CAPS.TOP70_100,
        };
      });
    }

    // Build snapshot rows
    // Columns: A=Fecha B=ID C=Nombre D=Seguidores E=TotalTracks F=EnCampaña G-K=VendidosPorRango L-P=Caps Q=Imagen R=Descripción
    const rows = [];
    for (const pl of clientPlaylists) {
      const caps = plCaps[pl.id] || SP_DEFAULT_CAPS;
      const tracks = pl.tracks || [];
      const inCampaign = hasTrackData ? tracks.filter(t => campTrackMap[t.id]).length : '';
      const row = [
        today,
        pl.id,
        pl.name,
        pl.followers || 0,
        pl.totalTracks || 0,
        inCampaign,
      ];
      SP_RANGES.forEach(range => {
        row.push(hasTrackData ? tracks.filter(t => t.position >= range.from && t.position <= range.to && campTrackMap[t.id]).length : '');
      });
      SP_RANGES.forEach(range => {
        row.push(hasTrackData ? caps[range.key] : '');
      });
      // Q: Imagen, R: Descripción
      row.push(pl.image || '');
      row.push(pl.description || '');
      rows.push(row);
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'HistorialPlaylists!A:R',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      resource: { values: rows },
    });

    console.log(`Snapshot ${today}: ${rows.length} playlists guardadas`);
    return res.json({ ok: true, date: today, playlists: rows.length });

  } catch(e) {
    console.error('snapshot error:', e.message);
    return res.status(500).json({ error: e.message });
  }
};
