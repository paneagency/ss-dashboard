// api/youtube.js
// YouTube integration: Playlists (Modo List) + Ads
// Uses YouTube Data API v3 (YOUTUBE_API_KEY env var) for public video/playlist data
// + Google Sheets (GOOGLE_SERVICE_ACCOUNT) for internal tracking
//
// Sheets required (create manually in the spreadsheet):
//   YouTubePlaylists: A=PLAYLIST_ID, B=NOMBRE, C=PRECIO_K, D=GASTO_ARS, E=GASTO_USD, F=DIARIO_USD
//   YouTubeTracks:   A=PLAYLIST_ID, B=ARTISTA, C=CANCION, D=POSICION, E=ESTIMADO_K,
//                    F=VALOR, G=COBRADO, H=GASTO, I=GANANCIA, J=VIDEO_ID, K=FECHA_INICIO
//   YouTubeAds:      A=ARTISTA, B=VIDEO_URL, C=OBJETIVO_K, D=PRECIO, E=FECHA_INICIO,
//                    F=FECHA_FIN, G=ESTADO, H=VENDEDOR, I=VISTAS_ACTUALES

const { google } = require('googleapis');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const YT_API_BASE = 'https://www.googleapis.com/youtube/v3';

function getSheets() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
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

async function getSheetId(sheets, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties' });
  const sheet = (meta.data.sheets || []).find(s => s.properties.title === sheetName);
  return sheet?.properties?.sheetId ?? null;
}

function extractVideoId(url) {
  const m = (url || '').match(/(?:v=|youtu\.be\/|embed\/|shorts\/)([A-Za-z0-9_-]{11})/);
  return m ? m[1] : null;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { mode } = req.query;

  try {
    const sheets = getSheets();

    // ── GET playlists ───────────────────────────────────────────
    if (req.method === 'GET' && mode === 'playlists') {
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubePlaylists!A:F',
      }).catch(() => ({ data: { values: [] } }));
      const playlists = (resp.data.values || []).slice(1)
        .map((r, i) => ({
          row: i + 2,
          playlistId: r[0]?.trim() || '',
          nombre: r[1]?.trim() || '',
          precioK: parseFloat(r[2]) || 0,
          gastoArs: parseFloat(r[3]) || 0,
          gastoUsd: parseFloat(r[4]) || 0,
          diarioUsd: parseFloat(r[5]) || 0,
        }))
        .filter(p => p.playlistId);
      return res.json({ ok: true, playlists });
    }

    // ── GET tracks ──────────────────────────────────────────────
    if (req.method === 'GET' && mode === 'tracks') {
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubeTracks!A:K',
      }).catch(() => ({ data: { values: [] } }));
      const all = (resp.data.values || []).slice(1)
        .map((r, i) => ({
          row: i + 2,
          playlistId: r[0]?.trim() || '',
          artista: r[1]?.trim() || '',
          cancion: r[2]?.trim() || '',
          posicion: parseInt(r[3]) || 0,
          estimadoK: parseFloat(r[4]) || 0,
          valor: parseFloat(r[5]) || 0,
          cobrado: parseFloat(r[6]) || 0,
          gasto: parseFloat(r[7]) || 0,
          ganancia: parseFloat(r[8]) || 0,
          videoId: r[9]?.trim() || '',
          fechaInicio: r[10]?.trim() || '',
        }))
        .filter(t => t.playlistId);
      const { playlistId } = req.query;
      return res.json({ ok: true, tracks: playlistId ? all.filter(t => t.playlistId === playlistId) : all });
    }

    // ── GET ads ─────────────────────────────────────────────────
    if (req.method === 'GET' && mode === 'ads') {
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubeAds!A:I',
      }).catch(() => ({ data: { values: [] } }));
      const ads = (resp.data.values || []).slice(1)
        .map((r, i) => ({
          row: i + 2,
          artista: r[0]?.trim() || '',
          videoUrl: r[1]?.trim() || '',
          objetivoK: parseFloat(r[2]) || 0,
          precio: parseFloat(r[3]) || 0,
          fechaInicio: r[4]?.trim() || '',
          fechaFin: r[5]?.trim() || '',
          estado: r[6]?.trim() || 'activa',
          vendedor: r[7]?.trim() || '',
          vistasActuales: parseInt(r[8]) || 0,
        }))
        .filter(a => a.artista);
      return res.json({ ok: true, ads });
    }

    // ── GET videoInfo ───────────────────────────────────────────
    if (req.method === 'GET' && mode === 'videoInfo') {
      const { videoId } = req.query;
      if (!videoId) return res.status(400).json({ error: 'videoId requerido' });
      if (!process.env.YOUTUBE_API_KEY) return res.json({ ok: false, error: 'YOUTUBE_API_KEY no configurado' });
      const data = await ytGet('videos', { part: 'snippet,statistics', id: videoId });
      const item = data.items?.[0];
      if (!item) return res.json({ ok: false, error: 'Video no encontrado' });
      return res.json({
        ok: true,
        video: {
          id: item.id,
          title: item.snippet?.title || '',
          thumbnail: item.snippet?.thumbnails?.medium?.url || item.snippet?.thumbnails?.default?.url || '',
          channelTitle: item.snippet?.channelTitle || '',
          views: parseInt(item.statistics?.viewCount) || 0,
          publishedAt: item.snippet?.publishedAt || '',
        },
      });
    }

    // ── GET playlistInfo ────────────────────────────────────────
    if (req.method === 'GET' && mode === 'playlistInfo') {
      const { playlistId } = req.query;
      if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
      if (!process.env.YOUTUBE_API_KEY) return res.json({ ok: false, error: 'YOUTUBE_API_KEY no configurado' });
      const data = await ytGet('playlists', { part: 'snippet,contentDetails', id: playlistId });
      const item = data.items?.[0];
      if (!item) return res.json({ ok: false, error: 'Playlist no encontrada' });
      return res.json({
        ok: true,
        playlist: {
          id: item.id,
          title: item.snippet?.title || '',
          thumbnail: item.snippet?.thumbnails?.medium?.url || item.snippet?.thumbnails?.standard?.url || item.snippet?.thumbnails?.default?.url || '',
          channelTitle: item.snippet?.channelTitle || '',
          itemCount: item.contentDetails?.itemCount || 0,
        },
      });
    }

    // ── POST playlist ───────────────────────────────────────────
    if (req.method === 'POST' && mode === 'playlist') {
      const { playlistId, nombre, precioK, gastoArs, gastoUsd, diarioUsd } = req.body || {};
      if (!playlistId) return res.status(400).json({ error: 'playlistId requerido' });
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubePlaylists!A:F',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: [[playlistId.trim(), nombre || '', precioK || '', gastoArs || '', gastoUsd || '', diarioUsd || '']] },
      });
      return res.json({ ok: true });
    }

    // ── POST track ──────────────────────────────────────────────
    if (req.method === 'POST' && mode === 'track') {
      const { playlistId, artista, cancion, posicion, estimadoK, valor, cobrado, gasto, videoId, fechaInicio } = req.body || {};
      if (!playlistId || !artista) return res.status(400).json({ error: 'playlistId y artista requeridos' });
      const cobradoN = parseFloat(cobrado) || 0;
      const gastoN = parseFloat(gasto) || 0;
      const today = new Date().toISOString().slice(0, 10);
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubeTracks!A:K',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: [[
          playlistId, artista, cancion || '', posicion || '', estimadoK || '',
          valor || '', cobradoN, gastoN, cobradoN - gastoN,
          videoId || '', fechaInicio || today,
        ]] },
      });
      return res.json({ ok: true });
    }

    // ── POST ad ─────────────────────────────────────────────────
    if (req.method === 'POST' && mode === 'ad') {
      const { artista, videoUrl, objetivoK, precio, fechaInicio, fechaFin, estado, vendedor } = req.body || {};
      if (!artista || !videoUrl) return res.status(400).json({ error: 'artista y videoUrl requeridos' });
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubeAds!A:I',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: [[artista, videoUrl, objetivoK || '', precio || '', fechaInicio || '', fechaFin || '', estado || 'activa', vendedor || '', 0]] },
      });
      return res.json({ ok: true });
    }

    // ── PUT track ───────────────────────────────────────────────
    if (req.method === 'PUT' && mode === 'track') {
      const { row, ...fields } = req.body || {};
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `YouTubeTracks!A${row}:K${row}`,
      });
      const c = (resp.data.values || [[]])[0] || [];
      const cobradoN = parseFloat(fields.cobrado !== undefined ? fields.cobrado : c[6]) || 0;
      const gastoN = parseFloat(fields.gasto !== undefined ? fields.gasto : c[7]) || 0;
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `YouTubeTracks!A${row}:K${row}`,
        valueInputOption: 'RAW',
        resource: { values: [[
          fields.playlistId ?? c[0] ?? '',
          fields.artista ?? c[1] ?? '',
          fields.cancion ?? c[2] ?? '',
          fields.posicion ?? c[3] ?? '',
          fields.estimadoK ?? c[4] ?? '',
          fields.valor ?? c[5] ?? '',
          cobradoN,
          gastoN,
          cobradoN - gastoN,
          fields.videoId ?? c[9] ?? '',
          fields.fechaInicio ?? c[10] ?? '',
        ]] },
      });
      return res.json({ ok: true });
    }

    // ── PUT ad ──────────────────────────────────────────────────
    if (req.method === 'PUT' && mode === 'ad') {
      const { row, ...fields } = req.body || {};
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `YouTubeAds!A${row}:I${row}`,
      });
      const c = (resp.data.values || [[]])[0] || [];
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `YouTubeAds!A${row}:I${row}`,
        valueInputOption: 'RAW',
        resource: { values: [[
          fields.artista ?? c[0] ?? '',
          fields.videoUrl ?? c[1] ?? '',
          fields.objetivoK ?? c[2] ?? '',
          fields.precio ?? c[3] ?? '',
          fields.fechaInicio ?? c[4] ?? '',
          fields.fechaFin ?? c[5] ?? '',
          fields.estado ?? c[6] ?? 'activa',
          fields.vendedor ?? c[7] ?? '',
          fields.vistasActuales ?? c[8] ?? 0,
        ]] },
      });
      return res.json({ ok: true });
    }

    // ── DELETE ──────────────────────────────────────────────────
    if (req.method === 'DELETE') {
      const { row } = req.body || {};
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const sheetMap = { playlist: 'YouTubePlaylists', track: 'YouTubeTracks', ad: 'YouTubeAds' };
      const sheetName = sheetMap[mode];
      if (!sheetName) return res.status(400).json({ error: 'mode inválido para DELETE' });
      const sheetId = await getSheetId(sheets, sheetName);
      if (sheetId === null) return res.status(404).json({ error: `Hoja ${sheetName} no encontrada` });
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        resource: {
          requests: [{ deleteDimension: { range: { sheetId, dimension: 'ROWS', startIndex: row - 1, endIndex: row } } }],
        },
      });
      return res.json({ ok: true });
    }

    // ── POST refreshAdViews ─────────────────────────────────────
    if (req.method === 'POST' && mode === 'refreshAdViews') {
      if (!process.env.YOUTUBE_API_KEY) return res.status(400).json({ error: 'YOUTUBE_API_KEY no configurado' });
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'YouTubeAds!A:I',
      }).catch(() => ({ data: { values: [] } }));
      const rows = (resp.data.values || []).slice(1);
      const active = rows.map((r, i) => ({ row: i + 2, videoUrl: r[1]?.trim() || '', estado: r[6]?.trim() || '' }))
        .filter(a => a.estado === 'activa' && a.videoUrl);

      let updated = 0;
      for (const item of active) {
        const videoId = extractVideoId(item.videoUrl);
        if (!videoId) continue;
        try {
          const data = await ytGet('videos', { part: 'statistics', id: videoId });
          const views = parseInt(data.items?.[0]?.statistics?.viewCount) || 0;
          await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `YouTubeAds!I${item.row}`,
            valueInputOption: 'RAW',
            resource: { values: [[views]] },
          });
          updated++;
          await new Promise(r => setTimeout(r, 200));
        } catch (_) {}
      }
      return res.json({ ok: true, updated });
    }

    return res.status(400).json({ error: 'mode o método no soportado' });

  } catch (e) {
    console.error('youtube api error:', e.message);
    return res.status(500).json({ error: e.message });
  }
};
