// api/snapshot.js
// Daily snapshot of all Spotify playlists → appends rows to HistorialPlaylists sheet.
// Called once per day by a Make scheduled scenario (1 Make operation total).
// Optional security: set SNAPSHOT_SECRET env var; pass as ?secret=xxx or body.secret

const { google } = require('googleapis');

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
async function kvGet(key) {
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(['GET', key]),
  });
  const d = await r.json();
  return d.result || null;
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
    try {
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
      return res.json({ ok: true, data });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET action=cron: snapshot automático diario (Vercel cron) ──
  if (req.method === 'GET' && req.query.action === 'cron') {
    try {
      const sheets = getSheets();
      const ccToken = await getCCToken();

      // 1. Leer ProveedoresSpotify para mapear playlist → grupo
      const provResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'ProveedoresSpotify!A:B',
      }).catch(() => ({ data: { values: [] } }));
      const providers = (provResp.data.values || []).slice(1)
        .filter(r => r[0] && r[1])
        .map(r => ({ grupo: r[0].trim(), userId: r[1].trim() }));

      // 2. Construir mapa playlistId → grupo desde perfiles de proveedores
      const playlistGrupoMap = {};
      for (const prov of providers) {
        try {
          const provPls = await fetchUserPublicPlaylists(prov.userId, ccToken);
          for (const pl of provPls) {
            if (pl?.id && !playlistGrupoMap[pl.id]) playlistGrupoMap[pl.id] = prov.grupo;
          }
        } catch(_) {}
      }

      // 3. Traer playlists de la cuenta OAuth vinculada
      const oauthToken = await getOAuthToken();
      const rawPlaylists = await fetchAllPlaylists(oauthToken);

      // 4. Unir IDs únicos (proveedores + cuenta propia)
      const allIds = new Set([...rawPlaylists.map(p => p.id), ...Object.keys(playlistGrupoMap)]);

      // 5. Fetch metadata completa para cada playlist
      const playlists = [];
      for (const plId of allIds) {
        try {
          const r = await fetch(
            `https://api.spotify.com/v1/playlists/${plId}?fields=id,name,description,followers,tracks(total),images`,
            { headers: { Authorization: `Bearer ${ccToken}` } }
          );
          if (!r.ok) continue;
          const full = await r.json();
          playlists.push({
            id: full.id,
            name: full.name,
            description: full.description || '',
            image: full.images?.[0]?.url || '',
            followers: full.followers?.total || 0,
            totalTracks: full.tracks?.total || 0,
            grupo: playlistGrupoMap[plId] || '',
          });
        } catch(_) {}
      }

      const today = new Date().toISOString().slice(0, 10);
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
      return res.json({ ok: true, date: today, playlists: rows.length, byProvider: Object.keys(playlistGrupoMap).length });
    } catch(e) {
      console.error('cron snapshot error:', e.message);
      return res.status(500).json({ error: e.message });
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
          range: 'ProveedoresSpotify!A:B',
        }).catch(() => ({ data: { values: [] } }));
        const providers = (resp.data.values || []).slice(1)
          .map((r, i) => ({ row: i + 2, grupo: r[0]?.trim() || '', userId: r[1]?.trim() || '' }))
          .filter(p => p.grupo && p.userId);
        return res.json({ ok: true, providers });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    // POST: agregar entrada
    if (req.method === 'POST') {
      const { grupo, userId } = req.body || {};
      if (!grupo || !userId) return res.status(400).json({ error: 'grupo y userId requeridos' });
      try {
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: 'ProveedoresSpotify!A:B',
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values: [[grupo.trim(), userId.trim()]] },
        });
        return res.json({ ok: true });
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
