const { google } = require('googleapis');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const SHEET_GID      = 162664118;
const SHEET_RANGE    = 'A:L';
const YT_API_BASE    = 'https://www.googleapis.com/youtube/v3';

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

async function ytGet(endpoint, params) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) throw new Error('YOUTUBE_API_KEY no configurado en Vercel');
  const url = new URL(`${YT_API_BASE}/${endpoint}`);
  url.searchParams.set('key', apiKey);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, String(v));
  const r = await fetch(url.toString());
  const data = await r.json();
  if (!r.ok) throw new Error(data.error?.message || `YouTube API error ${r.status}`);
  return data;
}

function extractVideoId(url) {
  const m = (url || '').match(/(?:v=|youtu\.be\/|embed\/|shorts\/)([A-Za-z0-9_-]{11})/);
  return m ? m[1] : null;
}

function getSheets() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  const auth  = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

function normalize(s) { return (s || '').toString().trim().toLowerCase(); }

function parseNum(val) {
  if (val === null || val === undefined || val === '') return 0;
  let s = String(val).trim().replace(/\s/g,'').replace(/[$€£¥%]/g,'');
  const hasDot = s.includes('.');
  const hasComma = s.includes(',');
  if (hasDot && hasComma) {
    const lastComma = s.lastIndexOf(',');
    const lastDot   = s.lastIndexOf('.');
    if (lastComma > lastDot) { s = s.replace(/\./g,'').replace(',','.'); }
    else { s = s.replace(/,/g,''); }
  } else if (hasComma) {
    const parts = s.split(',');
    s = (parts.length === 2 && parts[1].length <= 2) ? s.replace(',','.') : s.replace(/,/g,'');
  }
  return parseFloat(s) || 0;
}

function normFecha(s) {
  const str = (s || '').toString().trim();
  let m;
  m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  m = str.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  m = str.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return `${m[1]}-${m[2].padStart(2,'0')}-${m[3].padStart(2,'0')}`;
  return str;
}

async function getSheetId(sheets, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties' });
  const sheet = (meta.data.sheets || []).find(s => s.properties.title === sheetName);
  return sheet?.properties?.sheetId ?? null;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { mode } = req.query;

  try {
    const sheets = getSheets();

    // ── YOUTUBE ────────────────────────────────────────────────
    if (mode && mode.startsWith('yt-')) {
      const ytMode = mode.slice(3); // strip 'yt-' prefix

      if (req.method === 'GET' && ytMode === 'playlists') {
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubePlaylists!A:H' }).catch(() => ({ data: { values: [] } }));
        const playlists = (resp.data.values || []).slice(1).map((r, i) => ({ row: i+2, playlistId: r[0]?.trim()||'', nombre: r[1]?.trim()||'', precioK: parseFloat(r[2])||0, gastoArs: parseFloat(r[3])||0, gastoUsd: parseFloat(r[4])||0, diarioUsd: parseFloat(r[5])||0, imagen: r[6]?.trim()||'', vistas: parseInt(r[7])||0 })).filter(p => p.playlistId);
        return res.json({ ok: true, playlists });
      }

      if (req.method === 'GET' && ytMode === 'tracks') {
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubeTracks!A:K' }).catch(() => ({ data: { values: [] } }));
        const all = (resp.data.values || []).slice(1).map((r, i) => ({ row: i+2, playlistId: r[0]?.trim()||'', artista: r[1]?.trim()||'', cancion: r[2]?.trim()||'', posicion: parseInt(r[3])||0, estimadoK: parseFloat(r[4])||0, valor: parseFloat(r[5])||0, cobrado: parseFloat(r[6])||0, gasto: parseFloat(r[7])||0, ganancia: parseFloat(r[8])||0, videoId: r[9]?.trim()||'', fechaInicio: r[10]?.trim()||'' })).filter(t => t.playlistId);
        const { playlistId } = req.query;
        return res.json({ ok: true, tracks: playlistId ? all.filter(t => t.playlistId === playlistId) : all });
      }

      if (req.method === 'GET' && ytMode === 'ads') {
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubeAds!A:I' }).catch(() => ({ data: { values: [] } }));
        const ads = (resp.data.values || []).slice(1).map((r, i) => ({ row: i+2, artista: r[0]?.trim()||'', videoUrl: r[1]?.trim()||'', objetivoK: parseFloat(r[2])||0, precio: parseFloat(r[3])||0, fechaInicio: r[4]?.trim()||'', fechaFin: r[5]?.trim()||'', estado: r[6]?.trim()||'activa', vendedor: r[7]?.trim()||'', vistasActuales: parseInt(r[8])||0 })).filter(a => a.artista);
        return res.json({ ok: true, ads });
      }

      if (req.method === 'GET' && ytMode === 'videoInfo') {
        const { videoId } = req.query;
        if (!videoId) return res.status(400).json({ error: 'videoId requerido' });
        if (!process.env.YOUTUBE_API_KEY) return res.json({ ok: false, error: 'YOUTUBE_API_KEY no configurado' });
        const data = await ytGet('videos', { part: 'snippet,statistics', id: videoId });
        const item = data.items?.[0];
        if (!item) return res.json({ ok: false, error: 'Video no encontrado' });
        return res.json({ ok: true, video: { id: item.id, title: item.snippet?.title||'', thumbnail: item.snippet?.thumbnails?.medium?.url||item.snippet?.thumbnails?.default?.url||'', channelTitle: item.snippet?.channelTitle||'', views: parseInt(item.statistics?.viewCount)||0, publishedAt: item.snippet?.publishedAt||'' } });
      }

      if (req.method === 'GET' && ytMode === 'playlistInfo') {
        const { playlistId } = req.query;
        if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
        if (!process.env.YOUTUBE_API_KEY) return res.json({ ok: false, error: 'YOUTUBE_API_KEY no configurado' });
        const data = await ytGet('playlists', { part: 'snippet,contentDetails', id: playlistId });
        const item = data.items?.[0];
        if (!item) return res.json({ ok: false, error: 'Playlist no encontrada' });
        return res.json({ ok: true, playlist: { id: item.id, title: item.snippet?.title||'', thumbnail: item.snippet?.thumbnails?.medium?.url||item.snippet?.thumbnails?.standard?.url||item.snippet?.thumbnails?.default?.url||'', channelTitle: item.snippet?.channelTitle||'', itemCount: item.contentDetails?.itemCount||0 } });
      }

      if (req.method === 'POST' && ytMode === 'playlist') {
        let { playlistId, nombre, precioK, gastoArs, gastoUsd, diarioUsd, imagen } = req.body || {};
        if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
        playlistId = playlistId.trim();
        // Auto-fetch thumbnail if not provided and API key is available
        if (!imagen && process.env.YOUTUBE_API_KEY) {
          try {
            const d = await ytGet('playlists', { part: 'snippet', id: playlistId });
            const t = d.items?.[0]?.snippet?.thumbnails;
            imagen = t?.standard?.url || t?.high?.url || t?.medium?.url || t?.default?.url || '';
            if (!nombre) nombre = d.items?.[0]?.snippet?.title || '';
          } catch(_) {}
        }
        await sheets.spreadsheets.values.append({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubePlaylists!A:G', valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', resource: { values: [[playlistId, nombre||'', precioK||'', gastoArs||'', gastoUsd||'', diarioUsd||'', imagen||'']] } });
        return res.json({ ok: true, imagen: imagen||'' });
      }

      if (req.method === 'POST' && ytMode === 'track') {
        const { playlistId, artista, cancion, posicion, estimadoK, cobrado, gasto, videoId, fechaInicio } = req.body || {};
        if (!playlistId || !artista) return res.status(400).json({ error: 'playlistId y artista requeridos' });
        const c = parseFloat(cobrado)||0, g = parseFloat(gasto)||0;
        await sheets.spreadsheets.values.append({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubeTracks!A:K', valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', resource: { values: [[playlistId, artista, cancion||'', posicion||'', estimadoK||'', '', c, g, c-g, videoId||'', fechaInicio||new Date().toISOString().slice(0,10)]] } });
        return res.json({ ok: true });
      }

      if (req.method === 'POST' && ytMode === 'ad') {
        const { artista, videoUrl, objetivoK, precio, fechaInicio, fechaFin, estado, vendedor } = req.body || {};
        if (!artista || !videoUrl) return res.status(400).json({ error: 'artista y videoUrl requeridos' });
        await sheets.spreadsheets.values.append({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubeAds!A:I', valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', resource: { values: [[artista, videoUrl, objetivoK||'', precio||'', fechaInicio||'', fechaFin||'', estado||'activa', vendedor||'', 0]] } });
        return res.json({ ok: true });
      }

      if (req.method === 'PUT' && ytMode === 'track') {
        const { row, ...fields } = req.body || {};
        if (!row) return res.status(400).json({ error: 'row requerido' });
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `YouTubeTracks!A${row}:K${row}` });
        const c = (resp.data.values||[[]])[0]||[];
        const cn = parseFloat(fields.cobrado??c[6])||0, gn = parseFloat(fields.gasto??c[7])||0;
        await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: `YouTubeTracks!A${row}:K${row}`, valueInputOption: 'RAW', resource: { values: [[fields.playlistId??c[0]??'', fields.artista??c[1]??'', fields.cancion??c[2]??'', fields.posicion??c[3]??'', fields.estimadoK??c[4]??'', fields.valor??c[5]??'', cn, gn, cn-gn, fields.videoId??c[9]??'', fields.fechaInicio??c[10]??'']] } });
        return res.json({ ok: true });
      }

      if (req.method === 'PUT' && ytMode === 'ad') {
        const { row, ...fields } = req.body || {};
        if (!row) return res.status(400).json({ error: 'row requerido' });
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `YouTubeAds!A${row}:I${row}` });
        const c = (resp.data.values||[[]])[0]||[];
        await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: `YouTubeAds!A${row}:I${row}`, valueInputOption: 'RAW', resource: { values: [[fields.artista??c[0]??'', fields.videoUrl??c[1]??'', fields.objetivoK??c[2]??'', fields.precio??c[3]??'', fields.fechaInicio??c[4]??'', fields.fechaFin??c[5]??'', fields.estado??c[6]??'activa', fields.vendedor??c[7]??'', fields.vistasActuales??c[8]??0]] } });
        return res.json({ ok: true });
      }

      if (req.method === 'DELETE' && (ytMode === 'track' || ytMode === 'playlist' || ytMode === 'ad')) {
        const { row } = req.body || {};
        if (!row) return res.status(400).json({ error: 'row requerido' });
        const sheetMap = { playlist: 'YouTubePlaylists', track: 'YouTubeTracks', ad: 'YouTubeAds' };
        const sheetId = await getSheetId(sheets, sheetMap[ytMode]);
        if (sheetId === null) return res.status(404).json({ error: `Hoja ${sheetMap[ytMode]} no encontrada` });
        await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, resource: { requests: [{ deleteDimension: { range: { sheetId, dimension: 'ROWS', startIndex: row-1, endIndex: row } } }] } });
        return res.json({ ok: true });
      }

      if (req.method === 'POST' && ytMode === 'refreshPlViews') {
        if (!process.env.YOUTUBE_API_KEY) return res.status(400).json({ error: 'YOUTUBE_API_KEY no configurado' });
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubePlaylists!A:H' }).catch(() => ({ data: { values: [] } }));
        const pls = (resp.data.values || []).slice(1).map((r, i) => ({ row: i+2, playlistId: r[0]?.trim()||'' })).filter(p => p.playlistId);
        let updated = 0;
        for (const pl of pls) {
          try {
            // 1. Fetch all video IDs from playlist (paginated, max 50/page)
            const videoIds = [];
            let pageToken = '';
            do {
              const params = { part: 'contentDetails', playlistId: pl.playlistId, maxResults: '50' };
              if (pageToken) params.pageToken = pageToken;
              const d = await ytGet('playlistItems', params);
              (d.items || []).forEach(item => { const id = item.contentDetails?.videoId; if (id) videoIds.push(id); });
              pageToken = d.nextPageToken || '';
            } while (pageToken);
            if (!videoIds.length) continue;
            // 2. Fetch view counts in batches of 50
            let totalViews = 0;
            for (let i = 0; i < videoIds.length; i += 50) {
              const batch = videoIds.slice(i, i + 50);
              const vd = await ytGet('videos', { part: 'statistics', id: batch.join(',') });
              (vd.items || []).forEach(item => { totalViews += parseInt(item.statistics?.viewCount || 0); });
              if (i + 50 < videoIds.length) await new Promise(r => setTimeout(r, 150));
            }
            // 3. Save to column H
            await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: `YouTubePlaylists!H${pl.row}`, valueInputOption: 'RAW', resource: { values: [[totalViews]] } });
            updated++;
            await new Promise(r => setTimeout(r, 200));
          } catch(_) {}
        }
        return res.json({ ok: true, updated });
      }

      if (req.method === 'POST' && ytMode === 'refreshAdViews') {
        if (!process.env.YOUTUBE_API_KEY) return res.status(400).json({ error: 'YOUTUBE_API_KEY no configurado' });
        const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'YouTubeAds!A:I' }).catch(() => ({ data: { values: [] } }));
        const active = (resp.data.values||[]).slice(1).map((r,i) => ({ row: i+2, videoUrl: r[1]?.trim()||'', estado: r[6]?.trim()||'' })).filter(a => a.estado==='activa' && a.videoUrl);
        let updated = 0;
        for (const item of active) {
          const videoId = extractVideoId(item.videoUrl);
          if (!videoId) continue;
          try {
            const data = await ytGet('videos', { part: 'statistics', id: videoId });
            const views = parseInt(data.items?.[0]?.statistics?.viewCount)||0;
            await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: `YouTubeAds!I${item.row}`, valueInputOption: 'RAW', resource: { values: [[views]] } });
            updated++;
            await new Promise(r => setTimeout(r, 200));
          } catch(_) {}
        }
        return res.json({ ok: true, updated });
      }

      if (req.method === 'GET' && ytMode === 'analyticsStatus') {
        const token = await kvGet('youtube:refresh_token');
        return res.json({ connected: !!token });
      }

      if (req.method === 'GET' && ytMode === 'analytics') {
        const refreshToken = await kvGet('youtube:refresh_token');
        if (!refreshToken) return res.json({ ok: false, error: 'not_connected' });
        const gClientId = process.env.GOOGLE_CLIENT_ID;
        const gClientSecret = process.env.GOOGLE_CLIENT_SECRET;
        const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            grant_type: 'refresh_token',
            refresh_token: refreshToken,
            client_id: gClientId,
            client_secret: gClientSecret,
          }),
        });
        const tokenData = await tokenRes.json();
        if (!tokenRes.ok) return res.json({ ok: false, error: tokenData.error_description || tokenData.error });
        const accessToken = tokenData.access_token;

        // Find the right channel ID — tries mine=true, then managedByMe=true (Brand Accounts)
        let channelId = 'MINE';
        const channelRes = await fetch('https://www.googleapis.com/youtube/v3/channels?part=id,snippet&mine=true', {
          headers: { Authorization: `Bearer ${accessToken}` },
        });
        const channelData = await channelRes.json();
        const allChannels = channelData.items || [];
        // Also fetch managed channels (Brand Accounts)
        const managedRes = await fetch('https://www.googleapis.com/youtube/v3/channels?part=id,snippet,statistics&managedByMe=true&maxResults=50', {
          headers: { Authorization: `Bearer ${accessToken}` },
        }).then(r => r.json()).catch(() => ({ items: [] }));
        const managed = managedRes.items || [];
        // Pick channel with most subscribers (likely Modo List), else fall back to MINE
        const all = [...allChannels, ...managed];
        if (all.length > 0) {
          const best = all.sort((a, b) => parseInt(b.statistics?.subscriberCount || 0) - parseInt(a.statistics?.subscriberCount || 0))[0];
          channelId = best.id || 'MINE';
        }

        const endDate = new Date().toISOString().slice(0, 10);
        const startDate = new Date(Date.now() - 28 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

        // Load our playlist names from sheet for display
        const plSheetResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID, range: 'YouTubePlaylists!A:B',
        }).catch(() => ({ data: { values: [] } }));
        const plNameMap = {};
        (plSheetResp.data.values || []).slice(1).forEach(r => {
          if (r[0]) plNameMap[r[0].trim()] = r[1]?.trim() || r[0].trim();
        });

        // Query: per-playlist analytics
        const analyticsUrl = new URL('https://youtubeanalytics.googleapis.com/v2/reports');
        analyticsUrl.searchParams.set('ids', `channel==${channelId}`);
        analyticsUrl.searchParams.set('startDate', startDate);
        analyticsUrl.searchParams.set('endDate', endDate);
        analyticsUrl.searchParams.set('dimensions', 'playlist');
        analyticsUrl.searchParams.set('metrics', 'playlistStarts,viewsPerPlaylistStart,averageTimeInPlaylist');
        analyticsUrl.searchParams.set('sort', '-playlistStarts');
        analyticsUrl.searchParams.set('maxResults', '50');
        const analyticsRes = await fetch(analyticsUrl.toString(), {
          headers: { Authorization: `Bearer ${accessToken}` },
        });
        const analyticsData = await analyticsRes.json();
        if (!analyticsRes.ok) {
          const errMsg = analyticsData.error?.message || JSON.stringify(analyticsData.error) || 'Analytics API error';
          return res.json({ ok: false, error: errMsg, channelId });
        }

        // Build playlist name map: first from our sheet, then fetch missing from YouTube API
        const rows = analyticsData.rows || [];
        const unknownIds = rows.map(r => r[0]).filter(id => !plNameMap[id]);
        if (unknownIds.length && process.env.YOUTUBE_API_KEY) {
          try {
            // Fetch in batches of 50
            for (let i = 0; i < unknownIds.length; i += 50) {
              const batch = unknownIds.slice(i, i + 50).join(',');
              const plData = await ytGet('playlists', { part: 'snippet', id: batch });
              (plData.items || []).forEach(item => {
                plNameMap[item.id] = item.snippet?.title || item.id;
              });
            }
          } catch(_) {}
        }

        // estimatedViews = starts × viewsPerStart (counts ALL videos incl. 3rd party)
        const playlists = rows.map(r => ({
          playlistId: r[0],
          nombre: plNameMap[r[0]] || r[0],
          starts: r[1],
          viewsPerStart: Math.round(r[2] * 10) / 10,
          avgMinutes: Math.round(r[3] * 10) / 10,
          estimatedViews: Math.round(r[1] * r[2]),
        })).sort((a, b) => b.estimatedViews - a.estimatedViews);

        const totalViews = playlists.reduce((s, p) => s + p.estimatedViews, 0);

        return res.json({ ok: true, totalViews, playlists, startDate, endDate, channelId });
      }

      if (req.method === 'GET' && ytMode === 'playlistSongs') {
        const { playlistId } = req.query;
        if (!playlistId) return res.json({ ok: false, error: 'missing playlistId' });
        if (!process.env.YOUTUBE_API_KEY) return res.json({ ok: false, error: 'YOUTUBE_API_KEY missing' });

        // Data API only — YouTube Analytics API does not support per-video breakdown
        // for playlists with 3rd-party content (channel doesn't own those videos)
        const [plInfoData, plPage1] = await Promise.all([
          ytGet('playlists', { part: 'snippet,contentDetails', id: playlistId }).catch(() => ({ items: [] })),
          ytGet('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }).catch(() => ({ items: [] })),
        ]);

        // Paginate up to 200 items
        const allPlItems = [...(plPage1.items || [])];
        let nextToken = plPage1.nextPageToken;
        while (nextToken && allPlItems.length < 200) {
          try {
            const page = await ytGet('playlistItems', { part: 'snippet', playlistId, maxResults: 50, pageToken: nextToken });
            allPlItems.push(...(page.items || []));
            nextToken = page.nextPageToken;
          } catch(_) { break; }
        }

        const plItem = plInfoData.items?.[0];
        const plThumb = plItem?.snippet?.thumbnails?.medium?.url || plItem?.snippet?.thumbnails?.default?.url || '';
        const plTotalItems = parseInt(plItem?.contentDetails?.itemCount) || allPlItems.length;

        const songs = allPlItems.map((item, idx) => {
          const sn = item.snippet || {};
          return {
            videoId: sn.resourceId?.videoId || '',
            title: sn.title || '',
            thumb: sn.thumbnails?.medium?.url || sn.thumbnails?.default?.url || '',
            position: idx + 1,
            channelTitle: sn.videoOwnerChannelTitle || '',
          };
        }).filter(s => s.videoId && s.title !== 'Private video' && s.title !== 'Deleted video');

        return res.json({ ok: true, songs, plThumb, plTotalItems });
      }

      return res.status(400).json({ error: `YouTube mode '${ytMode}' no reconocido` });
    }

    // ── CAPACIDAD DE PLAYLISTS ─────────────────────────────────
    if (req.method === 'GET' && req.query.mode === 'playlist-caps') {
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'CapacidadPlaylists!A:G',
      }).catch(() => ({ data: { values: [] } }));
      const rows = (resp.data.values || []).slice(1); // skip header
      const caps = {};
      rows.forEach(r => {
        if (!r[0]?.trim()) return;
        caps[r[0].trim()] = {
          nombre: r[1]?.trim() || '',
          TOP5_20:   parseInt(r[2]) || 5,
          TOP20_40:  parseInt(r[3]) || 8,
          TOP40_60:  parseInt(r[4]) || 10,
          TOP60_70:  parseInt(r[5]) || 10,
          TOP70_100: parseInt(r[6]) || 20,
        };
      });
      return res.json({ caps });
    }

    if (req.method === 'POST' && req.body.mode === 'playlist-caps') {
      const { playlistId, nombre, TOP5_20, TOP20_40, TOP40_60, TOP60_70, TOP70_100 } = req.body;
      if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
      // Read current rows to find if playlist already exists
      const existing = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'CapacidadPlaylists!A:A',
      }).catch(() => ({ data: { values: [] } }));
      const rows = existing.data.values || [];
      let rowIndex = rows.findIndex((r, i) => i > 0 && r[0]?.trim() === playlistId);
      const newRow = [playlistId, nombre || '', TOP5_20 ?? 5, TOP20_40 ?? 8, TOP40_60 ?? 10, TOP60_70 ?? 10, TOP70_100 ?? 20];
      if (rowIndex > 0) {
        // Update existing row (rowIndex is 0-based array index, sheet row = rowIndex+1)
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `CapacidadPlaylists!A${rowIndex + 1}:G${rowIndex + 1}`,
          valueInputOption: 'RAW',
          resource: { values: [newRow] },
        });
      } else {
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: 'CapacidadPlaylists!A:G',
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values: [newRow] },
        });
      }
      return res.json({ ok: true });
    }

    // ── LISTA DE VENDEDORES ────────────────────────────────────
    if (req.method === 'GET') {
      const [vendResp, provResp] = await Promise.all([
        sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Vendedores!A:I' }),
        sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Proveedores!A:A' }).catch(() => ({ data: { values: [] } })),
      ]);
      const vendRows = vendResp.data.values || [];
      const vendStart = /^(nombre|vendedor|name|vendor)/i.test(vendRows[0]?.[0] || '') ? 1 : 0;
      const vendors = vendRows.slice(vendStart)
        .filter(r => r[0]?.trim())
        .map(r => ({ name: r[0].trim(), commission: parseFloat(r[1]) || 0, email: r[2]?.trim() || '', direccion: r[3]?.trim() || '', taxId: r[4]?.trim() || '', notas: r[5]?.trim() || '', nombreFiscal: r[6]?.trim() || '', autoFactura: r[7]?.trim() === '1', facturarA: r[8]?.trim() === '1' }));
      const providers = (provResp.data.values || []).slice(1)
        .map(r => r[0]?.trim()).filter(Boolean);
      return res.json({ vendors, providers });
    }

    // ── AGREGAR VENTA ─────────────────────────────────────────
    if (req.method === 'POST') {
      const { values, campaignId } = req.body;
      if (!values || !Array.isArray(values)) return res.status(400).json({ error: 'values requerido' });

      const colResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'A:A',
      });
      const nextRow = (colResp.data.values || []).length + 1;

      const rowToWrite = [...values];
      rowToWrite[6] = `=(E${nextRow} - F${nextRow}) * (1 - D${nextRow} / 100)`;
      rowToWrite[7] = `=BUSCARV(B${nextRow}, Vendedores!A:B, 2, FALSO)`;
      rowToWrite[8] = `=G${nextRow} * (1 - H${nextRow} / 100)`;
      if (campaignId) rowToWrite[11] = campaignId;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `A${nextRow}:L${nextRow}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [rowToWrite] },
      });

      return res.json({ ok: true, row: nextRow });
    }

    // ── ACTUALIZAR VENDEDOR ────────────────────────────────────
    if (req.method === 'PUT' && req.body.mode === 'vendedor') {
      const { nombre, commission, email, direccion, taxId, notas, nombreFiscal, autoFactura, facturarA } = req.body;
      if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
      const vendResp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Vendedores!A:A' });
      const vendRows = vendResp.data.values || [];
      const startIdx = /^(nombre|vendedor|name|vendor)/i.test(vendRows[0]?.[0] || '') ? 1 : 0;
      const rowIdx = vendRows.slice(startIdx).findIndex(r => (r[0] || '').trim().toLowerCase() === nombre.trim().toLowerCase());
      if (rowIdx === -1) return res.status(404).json({ error: `Vendedor "${nombre}" no encontrado` });
      const rowNum = startIdx + rowIdx + 1;
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `Vendedores!A${rowNum}:I${rowNum}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[nombre, commission ?? '', email || '', direccion || '', taxId || '', notas || '', nombreFiscal || '', autoFactura ? '1' : '', facturarA ? '1' : '']] },
      });
      return res.json({ ok: true });
    }

    // ── CREAR NUEVO VENDEDOR ────────────────────────────────────
    if (req.method === 'POST' && req.body.mode === 'vendedor') {
      const { nombre, commission, email, direccion, taxId, notas, nombreFiscal, autoFactura, facturarA } = req.body;
      if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Vendedores!A:I',
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[nombre, commission ?? '', email || '', direccion || '', taxId || '', notas || '', nombreFiscal || '', autoFactura ? '1' : '', facturarA ? '1' : '']] },
      });
      return res.json({ ok: true });
    }

    // ── VINCULAR VENTA A CAMPAÑA (actualizar col L) ───────────
    if (req.method === 'PUT') {
      const { saleRow, campaignId } = req.body;
      if (!saleRow || !campaignId) return res.status(400).json({ error: 'saleRow y campaignId requeridos' });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `L${saleRow}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[campaignId]] },
      });
      return res.json({ ok: true });
    }

    // ── BORRAR VENTA ──────────────────────────────────────────
    if (req.method === 'DELETE') {
      const { artista, precio, fechaNorm } = req.body;
      if (!artista) return res.status(400).json({ error: 'artista requerido' });

      const dataResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: SHEET_RANGE,
      });
      const rows = dataResp.data.values || [];

      const targetArtista = normalize(artista);
      const targetPrecio  = parseFloat(precio) || 0;

      let matchIndex = -1;
      let bestScore  = -1;

      for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        if (normalize(row[0]) !== targetArtista) continue;
        if (Math.abs((parseNum(row[4])) - targetPrecio) > 0.5) continue;
        const fechaScore = normFecha(row[9]) === fechaNorm ? 2 : 1;
        if (fechaScore > bestScore) { bestScore = fechaScore; matchIndex = i; }
      }

      if (matchIndex === -1) return res.status(404).json({ error: `No se encontró la fila de "${artista}" en el Sheet.` });

      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          requests: [{
            deleteDimension: {
              range: { sheetId: SHEET_GID, dimension: 'ROWS', startIndex: matchIndex, endIndex: matchIndex + 1 }
            }
          }]
        }
      });

      return res.json({ ok: true });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
