const { google } = require('googleapis');

const SPREADSHEET_ID    = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const CLIENTES_SHEET    = 'Clientes';
const CAMPANAS_SHEET    = 'CampañasCalendario';
const CLIENTES_DB_ID    = '1-RgACwkV412FbwWe8J1PvI3gaftwY5w276v1NY3tyxc';
const CLIENTES_DB_SHEET = '2025';

// ── Calendarios ────────────────────────────────────────────────
const MASTER_CAL  = 'paneagency@gmail.com';
const VENDOR_CALS = {
  'Pane Agency':        'e2645026fe0ceccee8a8942f709b4f8a7c6e7646950c36b82d548b6295ae55ff@group.calendar.google.com',
  'Santivgg':           '80255759be0dfe38e43ac89857f08c16e55dbcf7f65a1420ce968758997e2655@group.calendar.google.com',
  'Bizarro':            '5abd76466dc9ee1a9b4e86822cd1ad2b8f659881d4c60671a94936cbcf198876@group.calendar.google.com',
  'Mariana Gagliardi':  'e21d08bee0274f9545842aa69f9d77e311cf5fdb26bc43a59f0ed5ab2066c6e7@group.calendar.google.com',
  'Mariana Mazu':       '4880346ff5999d806cfc670e9452d54e39eb830010f19061a569e220005e6995@group.calendar.google.com',
  'Martin Ciolfi':      '191359f0544e99e6433590ffd3dafc7277716a7578b645444184e300750e3115@group.calendar.google.com',
  'Maxi Jayat':         '63464bc0aba05756866cd406bc52db5933a5f6f4928d98fdac0b13583a8e7904@group.calendar.google.com',
  'Melboss':            '56dae3ce019a26a4028b2bf95a6720d5601b23475c72cfde4e7e849173549666@group.calendar.google.com',
  'Moscu':              'abf54a8eec6e02c1e66334afc7d5a8070db80a00a55b54eaef4f3e5f04f676c8@group.calendar.google.com',
  'Pablo Galleguillos': '6420d18ad20d054f99be6c4e71e086d5e02d624bdba3291944d497b01a93db49@group.calendar.google.com',
  'Tobias Ehlen':       '1feef31ac54a013871bb35c6612cebb0af0c6d08ea0c17b1fd90dd4897191390@group.calendar.google.com',
  'Tomas Magna':        '10de3039123c75d84404f5fae5526ffc19d582b4ba798cabf1333e8fa6e3f84c@group.calendar.google.com',
};

// ── Comisiones ─────────────────────────────────────────────────
const METHOD_COMMISSIONS = {
  'PayPal': 11, 'Wise': 5.2, 'Mercado Pago': 11,
  'Transferencia Ars': 1, 'Sin Comision': 0,
};

function calcFinancials(precio, gasto, metodo, vendedor) {
  const p = parseFloat(precio) || 0;
  const g = parseFloat(gasto)  || 0;
  const methodPct = METHOD_COMMISSIONS[metodo] ?? 0;
  const users = JSON.parse(process.env.USERS_CONFIG || '{"users":[]}').users;
  const vendorCommission = users.find(u => u.vendorName === vendedor)?.commission ?? 0;
  const neto  = (p - g) * (1 - methodPct / 100);
  const final = neto * (1 - vendorCommission);
  const margen = vendorCommission * 100;
  return {
    neto:   +neto.toFixed(2),
    final:  +final.toFixed(2),
    margen: +margen.toFixed(2),
  };
}

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
    headerData.push({ range: `${CLIENTES_SHEET}!A1:I1`,
      values: [['ID','NOMBRE','GÉNERO','PAIS','TELÉFONO','EMAIL','SPOTIFY','INSTAGRAM','FECHA_PRIMERA_COMPRA']] });
  if (toCreate.includes(CAMPANAS_SHEET))
    headerData.push({ range: `${CAMPANAS_SHEET}!A1:O1`,
      values: [['ARTISTA','VENDEDOR','FECHA_INICIO','FECHA_VENCIMIENTO','DURACION_DIAS','EVENT_ID_MASTER','EVENT_ID_VENDOR','ESTADO','METODO','PRECIO','GASTO','NETO','MARGEN_PCT','FINAL','GENERO']] });

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { valueInputOption: 'RAW', data: headerData },
  });
}

async function lookupClientDB(sheets, artista) {
  try {
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: CLIENTES_DB_ID,
      range: `${CLIENTES_DB_SHEET}!A:F`,
    });
    const rows = (resp.data.values || []).slice(1);
    const match = rows.find(r =>
      (r[0] || '').toLowerCase().trim() === artista.toLowerCase().trim()
    );
    if (!match) return null;
    return {
      pais:     match[3] || '',
      telefono: match[4] || '',
      email:    match[5] || '',
    };
  } catch (e) {
    console.warn('lookupClientDB error:', e.message);
    return null;
  }
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

  const db = await lookupClientDB(sheets, artista);
  const newId = `C${String(rows.length).padStart(3, '0')}`;
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:I`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[
      newId, artista,
      genero || '',
      db?.pais     || '',
      db?.telefono || '',
      db?.email    || '',
      '',
      '',
      fechaVenta,
    ]] },
  });
  return newId;
}

// ── Helpers: Calendar ──────────────────────────────────────────

function buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo, pauta, link, gastosRows) {
  const parts = [];
  if (pauta) parts.push(pauta);
  if (link)  parts.push(link);
  if (!parts.length) parts.push(`Vendedor: ${vendedor}\nDuración: ${duracion} días\nPrecio: $${precio}\nMétodo: ${metodo}`);
  if (gastosRows?.length) {
    parts.push('-');
    gastosRows.forEach(r => parts.push(`(${r.amount})${r.provider ? ' ' + r.provider : ''}`));
  }
  return {
    summary:     `(${vendedor}) - ${artista}`,
    description: parts.join('\n'),
    start: { date: fechaVencimiento },
    end:   { date: fechaVencimiento },
    colorId: '2',
  };
}

async function createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio, metodo, pauta, link, gastosRows) {
  const event       = buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo, pauta, link, gastosRows);
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
  const results = { masterEventId, vendorEventId, vendorCalId, errors: [] };
  await Promise.all([
    masterEventId
      ? cal.events.delete({ calendarId: MASTER_CAL, eventId: masterEventId })
          .then(() => { results.masterDeleted = true; })
          .catch(e => { results.errors.push('master: ' + e.message); })
      : null,
    (vendorEventId && vendorCalId)
      ? cal.events.delete({ calendarId: vendorCalId, eventId: vendorEventId })
          .then(() => { results.vendorDeleted = true; })
          .catch(e => { results.errors.push('vendor: ' + e.message); })
      : null,
  ].filter(Boolean));
  return results;
}

function addDays(dateStr, days) {
  const d = new Date(dateStr + 'T12:00:00');
  const n = parseInt(days);
  // 7 y 14 días: suma días exactos. Todo lo demás (mensual): mismo día N meses después.
  if (n <= 21) {
    d.setDate(d.getDate() + n);
  } else {
    d.setMonth(d.getMonth() + Math.round(n / 30));
  }
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
          range: `${CLIENTES_SHEET}!A:I`,
        });
        const rows = (resp.data.values || []).slice(1);
        return res.json({
          clients: rows.map(r => ({
            id: r[0], nombre: r[1], genero: r[2],
            pais: r[3], telefono: r[4], email: r[5],
            spotify: r[6], instagram: r[7],
            fechaPrimeraCompra: r[8],
          })),
        });
      }

      // Default: campañas activas
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:Q`,
      });
      let campanias = (resp.data.values || []).slice(1)
        .map((r, i) => ({
          row: i + 2,
          artista: r[0], vendedor: r[1],
          fechaInicio: r[2], fechaVencimiento: r[3],
          duracion: parseInt(r[4]) || 30,
          masterEventId: r[5] || '', vendorEventId: r[6] || '',
          estado: r[7] || '',
          metodo: r[8] || '', precio: r[9] || '', gasto: r[10] || '',
          genero: r[14] || '',
          detalleGastos: r[15] || '',
          pauta: r[16] || '',
        }))
        .filter(c => c.estado === 'activa' && c.artista);

      if (vendedor && vendedor !== 'all')
        campanias = campanias.filter(c => c.vendedor === vendedor);

      return res.json({ campanias });
    }

    // ── POST: nueva campaña ───────────────────────────────────
    if (req.method === 'POST') {
      const { artista, genero, vendedor, fechaInicio, duracion, fechaVencimiento: fechaVencBody, precio, metodo, gasto, pauta, link, gastosRows } = req.body;
      const { neto, final, margen } = calcFinancials(precio, gasto, metodo, vendedor);
      if (!artista || !vendedor || !fechaInicio || !duracion)
        return res.status(400).json({ error: 'artista, vendedor, fechaInicio y duracion son requeridos' });

      const fechaVencimiento = fechaVencBody || addDays(fechaInicio, duracion);
      const clientId = await getOrCreateClient(sheets, artista, genero, fechaInicio);

      let masterEventId = '', vendorEventId = '';
      try {
        const ids = await createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio || 0, metodo || '', pauta || '', link || '', gastosRows || []);
        masterEventId = ids.masterEventId;
        vendorEventId = ids.vendorEventId;
      } catch(calErr) {
        console.error('Calendar error (non-fatal):', calErr.message);
      }

      const detalleGastos = (gastosRows || [])
        .filter(r => r.amount > 0)
        .map(r => `(${r.amount})${r.provider ? ' ' + r.provider : ''}`)
        .join('\n');

      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:Q`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[artista, vendedor, fechaInicio, fechaVencimiento, duracion, masterEventId, vendorEventId, 'activa', metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero || '', detalleGastos, pauta || '']] },
      });

      return res.json({ ok: true, clientId, fechaVencimiento });
    }

    // ── PUT: renovar campaña ──────────────────────────────────
    if (req.method === 'PUT') {
      const { row, artista, vendedor, duracion, fechaVencimiento: fechaVencBody, masterEventId, vendorEventId, precio, gasto, metodo, pauta, gastosRows, genero: generoBody, editOnly } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      // Leer fila completa actual para obtener fechaInicio y vencimiento base
      const campResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A${row}:Q${row}`,
      });
      const oldRow      = campResp.data.values?.[0] || [];
      const fechaInicio = oldRow[2] || new Date().toISOString().split('T')[0];
      const baseDate    = oldRow[3] || new Date().toISOString().split('T')[0];
      const genero      = generoBody || oldRow[14] || '';

      const nuevaFechaVenc = editOnly
        ? (fechaVencBody || baseDate)
        : addDays(baseDate, duracion);

      await deleteCalEvents(cal, vendedor, masterEventId, vendorEventId);

      let newMasterId = '', newVendorId = '';
      try {
        const ids = await createCalEvents(cal, artista, vendedor, nuevaFechaVenc, duracion, precio || 0, metodo || '', pauta || '', gastosRows || []);
        newMasterId = ids.masterEventId;
        newVendorId = ids.vendorEventId;
      } catch(calErr) {
        console.error('Calendar error (non-fatal):', calErr.message);
      }

      const detalleGastos = (gastosRows || [])
        .filter(r => r.amount > 0)
        .map(r => `(${r.amount})${r.provider ? ' ' + r.provider : ''}`)
        .join('\n');

      const { neto, final, margen } = calcFinancials(precio, gasto, metodo, vendedor);

      if (editOnly) {
        // Actualizar fila existente en place, sin crear nueva venta
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A${row}:Q${row}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[artista, vendedor, fechaInicio, nuevaFechaVenc, duracion, newMasterId, newVendorId, 'activa', metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero, detalleGastos, pauta || '']] },
        });
      } else {
        // 1. Marcar fila vieja como "renovada"
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!H${row}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [['renovada']] },
        });

        // 2. Agregar nueva fila activa con los datos actualizados
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:Q`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[artista, vendedor, fechaInicio, nuevaFechaVenc, duracion, newMasterId, newVendorId, 'activa', metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero, detalleGastos, pauta || '']] },
        });
      }

      return res.json({ ok: true, nuevaFechaVenc });
    }

    // ── DELETE: no renueva / borrar campaña por artista ──────
    if (req.method === 'DELETE') {
      let { row, masterEventId, vendorEventId, vendedor, artista } = req.body;

      // Si no viene row, buscar por artista+vendedor
      if (!row && artista) {
        const campResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:H`,
        });
        const campRows = (campResp.data.values || []).slice(1);
        // Buscar la ÚLTIMA campaña activa para ese artista (más reciente)
        let match = -1;
        for (let i = campRows.length - 1; i >= 0; i--) {
          const r = campRows[i];
          if ((r[0] || '').toLowerCase().trim() === artista.toLowerCase().trim() &&
              (!vendedor || (r[1] || '').toLowerCase().trim() === vendedor.toLowerCase().trim()) &&
              (r[7] || '') === 'activa') {
            match = i;
            break;
          }
        }
        if (match === -1) return res.json({ ok: true, skipped: true }); // No hay campaña activa, no es error
        row           = match + 2; // +1 header, +1 base-1
        masterEventId = campRows[match][5] || '';
        vendorEventId = campRows[match][6] || '';
        vendedor      = campRows[match][1] || vendedor;
      }

      if (!row) return res.status(400).json({ error: 'row o artista requerido' });

      const calResult = await deleteCalEvents(cal, vendedor, masterEventId, vendorEventId);

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!H${row}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [['finalizada']] },
      });

      return res.json({ ok: true, debug: calResult });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch(e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
