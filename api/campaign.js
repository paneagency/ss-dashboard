const { google } = require('googleapis');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const CLIENTES_SHEET = 'Clientes';
const CAMPANAS_SHEET = 'CampañasCalendario';

// ── Calendarios ────────────────────────────────────────────────
const MASTER_CAL  = 'paneagency@gmail.com';
const VENDOR_CALS = {
  'Pane Agency':         'e2645026fe0ceccee8a8942f709b4f8a7c6e7646950c36b82d548b6295ae55ff@group.calendar.google.com',
  'Santi - Pane Agency': '80255759be0dfe38e43ac89857f08c16e55dbcf7f65a1420ce968758997e2655@group.calendar.google.com',
  'Moscu':               'abf54a8eec6e02c1e66334afc7d5a8070db80a00a55b54eaef4f3e5f04f676c8@group.calendar.google.com',
};

// ── Auth ───────────────────────────────────────────────────────
function getAuth() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/calendar',
    ],
  });
}

// ── Helpers: Sheets ────────────────────────────────────────────
async function ensureSheets(sheets) {
  const meta     = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = meta.data.sheets.map(s => s.properties.title);
  const toCreate = [];
  if (!existing.includes(CLIENTES_SHEET)) toCreate.push(CLIENTES_SHEET);
  if (!existing.includes(CAMPANAS_SHEET))  toCreate.push(CAMPANAS_SHEET);
  if (!toCreate.length) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: toCreate.map(title => ({ addSheet: { properties: { title } } })) },
  });

  const headerData = [];
  if (toCreate.includes(CLIENTES_SHEET))
    headerData.push({ range: `${CLIENTES_SHEET}!A1:G1`,
      values: [['ID','NOMBRE','GÉNERO','TELÉFONO','SPOTIFY','INSTAGRAM','FECHA_PRIMERA_COMPRA']] });
  if (toCreate.includes(CAMPANAS_SHEET))
    headerData.push({ range: `${CAMPANAS_SHEET}!A1:H1`,
      values: [['ARTISTA','VENDEDOR','FECHA_INICIO','FECHA_VENCIMIENTO','DURACION_DIAS','EVENT_ID_MASTER','EVENT_ID_VENDOR','ESTADO']] });

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { valueInputOption: 'RAW', data: headerData },
  });
}

async function getOrCreateClient(sheets, artista, genero, fechaVenta) {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:B`,
  });
  const rows  = resp.data.values || [];
  const found = rows.slice(1).find(r =>
    (r[1] || '').toLowerCase().trim() === artista.toLowerCase().trim()
  );
  if (found) return found[0];

  const newId = `C${String(rows.length).padStart(3, '0')}`; // rows.length includes header
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:G`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[newId, artista, genero || '', '', '', '', fechaVenta]] },
  });
  return newId;
}

// ── Helpers: Calendar ──────────────────────────────────────────
function fmtDate(dateStr) {
  const d = new Date(dateStr + 'T12:00:00');
  return d.toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo) {
  return {
    summary:     `🎵 ${artista} · Vence ${fmtDate(fechaVencimiento)}`,
    description: `Vendedor: ${vendedor}\nDuración: ${duracion} días\nPrecio: $${precio}\nMétodo: ${metodo}`,
    start: { date: fechaVencimiento },
    end:   { date: fechaVencimiento },
    colorId: '2',
  };
}

async function createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio, metodo) {
  const event       = buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo);
  const vendorCalId = VENDOR_CALS[vendedor];
  const [masterRes, vendorRes] = await Promise.all([
    cal.events.insert({ calendarId: MASTER_CAL,    requestBody: event }),
    vendorCalId
      ? cal.events.insert({ calendarId: vendorCalId, requestBody: event })
      : Promise.resolve(null),
  ]);
  return {
    masterEventId: masterRes.data.id,
    vendorEventId: vendorRes?.data?.id || '',
  };
}

async function deleteCalEvents(cal, vendedor, masterEventId, vendorEventId) {
  const vendorCalId = VENDOR_CALS[vendedor];
  await Promise.all([
    masterEventId
      ? cal.events.delete({ calendarId: MASTER_CAL, eventId: masterEventId }).catch(() => {})
      : null,
    (vendorEventId && vendorCalId)
      ? cal.events.delete({ calendarId: vendorCalId, eventId: vendorEventId }).catch(() => {})
      : null,
  ].filter(Boolean));
}

function addDays(dateStr, days) {
  const d = new Date(dateStr + 'T12:00:00');
  d.setDate(d.getDate() + parseInt(days));
  return d.toISOString().split('T')[0];
}

// ── Handler ────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const auth   = getAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const cal    = google.calendar({ version: 'v3', auth });
    await ensureSheets(sheets);

    // ── GET: listar campañas o clientes ───────────────────────
    if (req.method === 'GET') {
      const { mode, vendedor } = req.query;

      if (mode === 'clients') {
        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CLIENTES_SHEET}!A:G`,
        });
        const rows = (resp.data.values || []).slice(1);
        return res.json({
          clients: rows.map(r => ({
            id: r[0], nombre: r[1], genero: r[2],
            telefono: r[3], spotify: r[4], instagram: r[5],
            fechaPrimeraCompra: r[6],
          })),
        });
      }

      // Default: campañas activas
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:H`,
      });
      let campanias = (resp.data.values || []).slice(1)
        .map((r, i) => ({
          row: i + 2,
          artista: r[0], vendedor: r[1],
          fechaInicio: r[2], fechaVencimiento: r[3],
          duracion: parseInt(r[4]) || 30,
          masterEventId: r[5] || '', vendorEventId: r[6] || '',
          estado: r[7] || '',
        }))
        .filter(c => c.estado === 'activa' && c.artista);

      if (vendedor && vendedor !== 'all')
        campanias = campanias.filter(c => c.vendedor === vendedor);

      return res.json({ campanias });
    }

    // ── POST: nueva campaña ───────────────────────────────────
    if (req.method === 'POST') {
      const { artista, genero, vendedor, fechaInicio, duracion, precio, metodo } = req.body;
      if (!artista || !vendedor || !fechaInicio || !duracion)
        return res.status(400).json({ error: 'artista, vendedor, fechaInicio y duracion son requeridos' });

      const fechaVencimiento = addDays(fechaInicio, duracion);
      const clientId = await getOrCreateClient(sheets, artista, genero, fechaInicio);

      let masterEventId = '', vendorEventId = '';
      try {
        const ids = await createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio || 0, metodo || '');
        masterEventId = ids.masterEventId;
        vendorEventId = ids.vendorEventId;
      } catch(calErr) {
        console.error('Calendar error (non-fatal):', calErr.message);
      }

      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:H`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[artista, vendedor, fechaInicio, fechaVencimiento, duracion, masterEventId, vendorEventId, 'activa']] },
      });

      return res.json({ ok: true, clientId, fechaVencimiento });
    }

    // ── PUT: renovar campaña ──────────────────────────────────
    if (req.method === 'PUT') {
      const { row, artista, vendedor, duracion, masterEventId, vendorEventId, precio, metodo } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      // Leer vencimiento actual para usarlo como base de la renovación
      const campResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!D${row}`,
      });
      const baseDate           = campResp.data.values?.[0]?.[0] || new Date().toISOString().split('T')[0];
      const nuevaFechaVenc     = addDays(baseDate, duracion);

      await deleteCalEvents(cal, vendedor, masterEventId, vendorEventId);

      let newMasterId = '', newVendorId = '';
      try {
        const ids = await createCalEvents(cal, artista, vendedor, nuevaFechaVenc, duracion, precio || 0, metodo || '');
        newMasterId  = ids.masterEventId;
        newVendorId  = ids.vendorEventId;
      } catch(calErr) {
        console.error('Calendar error (non-fatal):', calErr.message);
      }

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!D${row}:H${row}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[nuevaFechaVenc, duracion, newMasterId, newVendorId, 'activa']] },
      });

      return res.json({ ok: true, nuevaFechaVenc });
    }

    // ── DELETE: no renueva ────────────────────────────────────
    if (req.method === 'DELETE') {
      const { row, masterEventId, vendorEventId, vendedor } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      await deleteCalEvents(cal, vendedor, masterEventId, vendorEventId);

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!H${row}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [['finalizada']] },
      });

      return res.json({ ok: true });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch(e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
