const { google } = require('googleapis');

const SPREADSHEET_ID    = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const CLIENTES_SHEET    = 'Clientes';
const CAMPANAS_SHEET    = 'CampañasCalendario';
const CLIENTES_DB_ID    = '1-RgACwkV412FbwWe8J1PvI3gaftwY5w276v1NY3tyxc';
const CLIENTES_DB_SHEET = '2025';
const REPRESENTANTES_SHEET = 'Representantes';

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
    headerData.push({ range: `${CLIENTES_SHEET}!A1:O1`,
      values: [['ID','ARTISTA','GÉNERO','PAIS','TELÉFONO','EMAIL','SPOTIFY','FECHA_PRIMERA_COMPRA','REPRESENTANTE','NOMBRE','APODO','VENDEDOR','METODO_PAGO','ESTADO','IMAGEN']] });
  if (toCreate.includes(CAMPANAS_SHEET))
    headerData.push({ range: `${CAMPANAS_SHEET}!A1:V1`,
      values: [['ARTISTA','VENDEDOR','FECHA_INICIO','FECHA_VENCIMIENTO','DURACION_DIAS','EVENT_ID_MASTER','EVENT_ID_VENDOR','ESTADO','METODO','PRECIO','GASTO','NETO','MARGEN_PCT','FINAL','GENERO','DETALLE_GASTOS','PAUTA','REPRESENTANTE','NOTAS','CAMPAIGN_ID','TIMESTAMP','EDITADO_POR']] });

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

// Columnas de la hoja Clientes (post-INSTAGRAM):
// A=ID, B=ARTISTA, C=GÉNERO, D=PAIS, E=TELÉFONO, F=EMAIL, G=SPOTIFY,
// H=FECHA_PRIMERA_COMPRA, I=REPRESENTANTE, J=NOMBRE, K=APODO,
// L=VENDEDOR, M=METODO_PAGO, N=ESTADO
const CLIENT_COLS = { spotify:'G', representante:'I', nombre:'J', apodo:'K', vendedor:'L', metodoPago:'M', estado:'N', imagen:'O', tipo:'P', direccion:'Q', taxId:'R', autoFactura:'S' };

async function updateClientFields(sheets, artista, fields) {
  const entries = Object.entries(fields).filter(([k, v]) => CLIENT_COLS[k] && v !== undefined && v !== null && v !== '');
  if (!entries.length) return;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:B`,
  });
  const rows = resp.data.values || [];
  const idx = rows.slice(1).findIndex(r =>
    (r[1] || '').toLowerCase().trim() === artista.toLowerCase().trim()
  );
  if (idx === -1) return;
  const rowNum = idx + 2;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      valueInputOption: 'USER_ENTERED',
      data: entries.map(([field, value]) => ({
        range: `${CLIENTES_SHEET}!${CLIENT_COLS[field]}${rowNum}`,
        values: [[value]],
      })),
    },
  });
}

function parseDateStr(s) {
  const str = (s || '').trim();
  let m;
  m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1]);
  m = str.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return new Date(+m[1], +m[2]-1, +m[3]);
  m = str.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1]);
  return null;
}

async function findPrimeraCompra(sheets, artista) {
  try {
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: '2024!A:J',
    });
    const rows = (resp.data.values || []).slice(1);
    const norm = artista.toLowerCase().trim();
    let earliest = null;
    rows.forEach(r => {
      if ((r[0] || '').toLowerCase().trim() !== norm) return;
      const d = parseDateStr(r[9] || '');
      if (d && (!earliest || d < earliest)) earliest = d;
    });
    if (!earliest) return null;
    const y = earliest.getFullYear();
    const m = String(earliest.getMonth() + 1).padStart(2, '0');
    const d = String(earliest.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  } catch (e) {
    console.warn('findPrimeraCompra error:', e.message);
    return null;
  }
}

async function getOrCreateClient(sheets, artista, genero, fechaVenta, representante, vendedor, metodo) {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:B`,
  });
  const rows  = resp.data.values || [];
  const found = rows.slice(1).findIndex(r =>
    (r[1] || '').toLowerCase().trim() === artista.toLowerCase().trim()
  );
  if (found !== -1) {
    const clientId = rows[found + 1][0];
    // Actualizar campos relevantes del cliente existente
    await updateClientFields(sheets, artista, {
      representante, vendedor, metodoPago: metodo, estado: 'Activa',
    });
    return clientId;
  }

  const [db, primeraCompra] = await Promise.all([
    lookupClientDB(sheets, artista),
    findPrimeraCompra(sheets, artista),
  ]);
  const newId = `C${String(rows.length).padStart(3, '0')}`;
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CLIENTES_SHEET}!A:R`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[
      newId, artista,
      genero || '',
      db?.pais     || '',
      db?.telefono || '',
      db?.email    || '',
      '',                           // SPOTIFY
      primeraCompra || fechaVenta,  // FECHA_PRIMERA_COMPRA
      representante || '',  // REPRESENTANTE
      '',            // NOMBRE (contacto)
      '',            // APODO
      vendedor || '',       // VENDEDOR
      metodo   || '',       // METODO_PAGO
      'Activa',             // ESTADO
      '',            // IMAGEN
      '',            // TIPO
      '',            // DIRECCION
      '',            // TAX_ID
    ]] },
  });
  return newId;
}

// ── Helpers: Calendar ──────────────────────────────────────────

function buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo, pauta, link, gastosRows, representante, colorId, grupal, regalo) {
  const parts = [];
  if (pauta) parts.push(pauta);
  // Solo agregar el link si no está ya incluido en la pauta
  if (link && !(pauta || '').includes(link)) parts.push(link);
  if (!parts.length) parts.push(`Vendedor: ${vendedor}\nDuración: ${duracion} días\nPrecio: $${precio}\nMétodo: ${metodo}`);
  if (gastosRows?.length) {
    parts.push('-');
    gastosRows.forEach(r => parts.push(`(${r.amount})${r.provider ? ' ' + r.provider : ''}`));
  }
  const repTag = representante ? `[${representante}]` : '';
  const displayArtist = artista;
  const giftPrefix = regalo ? '🎁 ' : '';
  return {
    summary:     `${giftPrefix}(${vendedor})${repTag} - ${displayArtist}`,
    description: parts.join('\n'),
    start: { date: fechaVencimiento },
    end:   { date: fechaVencimiento },
    colorId: colorId || '2',
  };
}

async function createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio, metodo, pauta, link, gastosRows, representante, colorId, grupal, regalo) {
  const event       = buildEvent(artista, fechaVencimiento, vendedor, duracion, precio, metodo, pauta, link, gastosRows, representante, colorId, grupal, regalo);
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

      if (mode === 'migrate-campaign-rows') {
        // Leer ventas (A:L) y campañas (A:T para incluir campaignId en col T)
        const [ventasResp, campResp] = await Promise.all([
          sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'A:L' }),
          sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${CAMPANAS_SHEET}!A:T` }),
        ]);
        const ventasRows = (ventasResp.data.values || []).slice(1);
        const campRows   = (campResp.data.values  || []).slice(1);

        const cutoff = Date.now() - 15 * 24 * 60 * 60 * 1000;
        const parseDate = s => { if (!s) return null; const p = String(s).trim(); let m; m = p.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (m) return new Date(+m[3],+m[2]-1,+m[1]); m = p.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/); if (m) return new Date(+m[1],+m[2]-1,+m[3]); return null; };

        const updates = [];
        for (let i = 0; i < ventasRows.length; i++) {
          const v = ventasRows[i];
          if (v[11]) continue; // ya tiene CAMPAIGN_ID
          const fecha = parseDate(v[9]);
          if (!fecha || fecha.getTime() < cutoff) continue;

          const artista  = (v[0] || '').toLowerCase().trim();
          const vendedor = (v[1] || '').toLowerCase().trim();
          const precio   = parseFloat(v[4]) || 0;
          const metodo   = (v[2] || '').toLowerCase().trim();

          // Buscar mejor campaña: artista+vendedor+precio+metodo, luego sin metodo
          let best = null;
          for (let j = 0; j < campRows.length; j++) {
            const c = campRows[j];
            if ((c[0]||'').toLowerCase().trim() !== artista) continue;
            if ((c[1]||'').toLowerCase().trim() !== vendedor) continue;
            if (Math.abs(parseFloat(c[9]) - precio) > 0.5) continue;
            const metodoCamp = (c[8]||'').toLowerCase().trim();
            const campId = c[19] || '';
            if (!best || metodoCamp === metodo) best = { campId, metodoMatch: metodoCamp === metodo };
            if (best.metodoMatch) break;
          }
          if (best && best.campId) updates.push({ sheetRow: i + 2, campaignId: best.campId });
        }

        // Escribir en batch
        if (updates.length) {
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            requestBody: {
              valueInputOption: 'USER_ENTERED',
              data: updates.map(u => ({ range: `L${u.sheetRow}`, values: [[u.campaignId]] })),
            },
          });
        }

        return res.json({ ok: true, updated: updates.length, updates });
      }

      if (mode === 'clients') {
        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CLIENTES_SHEET}!A:S`,
        });
        const rows = (resp.data.values || []).slice(1);
        return res.json({
          clients: rows.map((r, i) => ({
            row: i + 2,
            id: r[0], artista: r[1], genero: r[2],
            pais: r[3], telefono: r[4], email: r[5],
            spotify: r[6],
            fechaPrimeraCompra: r[7] || '',
            representante: r[8] || '',
            nombre: r[9] || '',
            apodo: r[10] || '',
            vendedor: r[11] || '',
            metodoPago: r[12] || '',
            estado: r[13] || '',
            imagen: r[14] || '',
            tipo: r[15] || '',
            direccion: r[16] || '',
            taxId: r[17] || '',
            autoFactura: r[18] === '1',
          })),
        });
      }

      if (mode === 'representantes') {
        try {
          const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${REPRESENTANTES_SHEET}!A:G` });
          const rows = (resp.data.values || []).slice(1);
          return res.json({ representantes: rows.map((r, i) => ({ row: i + 2, nombre: r[0] || '', email: r[1] || '', direccion: r[2] || '', taxId: r[3] || '', notas: r[4] || '', nombreFiscal: r[5] || '', autoFactura: r[6] === '1' })).filter(r => r.nombre) });
        } catch(e) { return res.json({ representantes: [] }); }
      }

      // Historial: finalizadas, eliminadas, editadas, renovadas
      if (mode === 'historial') {
        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:V`,
        });
        const HIST_STATES = ['finalizada', 'finalizada_regalo', 'finalizada_sin_cobrar', 'eliminada', 'editada', 'renovada', 'extendida', 'cobrada'];
        let historial = (resp.data.values || []).slice(1)
          .map((r, i) => ({
            row: i + 2,
            artista: r[0], vendedor: r[1],
            fechaInicio: r[2], fechaVencimiento: r[3],
            duracion: parseInt(r[4]) || 30,
            masterEventId: r[5] || '', vendorEventId: r[6] || '',
            estado: r[7] || '',
            metodo: r[8] || '', precio: r[9] || '', gasto: r[10] || '',
            neto: r[11] || '', final: r[13] || '',
            genero: r[14] || '',
            detalleGastos: r[15] || '',
            pauta: r[16] || '',
            representante: r[17] || '',
            notas: r[18] || '',
            campaignId: r[19] || '',
            timestamp: r[20] || '',
            editadoPor: r[21] || '',
          }))
          .filter(c => HIST_STATES.includes(c.estado) && c.artista);
        if (vendedor && vendedor !== 'all')
          historial = historial.filter(c => c.vendedor === vendedor);
        return res.json({ historial });
      }

      // Pendientes de cobro: extendidas + finalizadas sin cobrar para el mismo cliente
      if (mode === 'pendientes_cobro') {
        const masterEventId = req.query.masterEventId;
        const artista       = req.query.artista || '';
        const vendedor      = req.query.vendedor || '';
        const representante = req.query.representante || '';

        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:V`,
        });
        const rows = (resp.data.values || []).slice(1).map((r, i) => ({
          row: i + 2,
          artista: r[0] || '', vendedor: r[1] || '',
          fechaInicio: r[2] || '', fechaVencimiento: r[3] || '',
          duracion: parseInt(r[4]) || 30,
          masterEventId: r[5] || '', vendorEventId: r[6] || '',
          estado: r[7] || '',
          metodo: r[8] || '', precio: r[9] || '', gasto: r[10] || '',
          detalleGastos: r[15] || '',
          pauta: r[16] || '',
          representante: r[17] || '',
          campaignId: r[19] || '',
        }));

        // extendidas: mismo masterEventId
        const extendidas = masterEventId
          ? rows.filter(c => c.masterEventId === masterEventId && c.estado === 'extendida' && c.artista)
          : [];

        // finalizada_sin_cobrar + pendiente_pago (distinto masterEventId): mismo artista O representante, mismo vendedor
        const sinCobrar = rows.filter(c => {
          if (!['finalizada_sin_cobrar', 'pendiente_pago'].includes(c.estado)) return false;
          if (c.vendedor !== vendedor) return false;
          if (c.masterEventId && c.masterEventId === masterEventId) return false; // ya está en extendidas o es la campaña actual
          const mismoArtista = artista && c.artista.toLowerCase().trim() === artista.toLowerCase().trim();
          const mismoRep = representante && c.representante && c.representante.toLowerCase().trim() === representante.toLowerCase().trim();
          return mismoArtista || mismoRep;
        });

        return res.json({ extendidas, sinCobrar });
      }

      // Períodos de deuda para copiar: dedup por rango de fechas, una entrada por ciclo lógico.
      // Combina byMaster + byArtVend para recuperar cadenas rotas por ediciones históricas.
      if (mode === 'periodos_deuda') {
        const masterEventId = req.query.masterEventId || '';
        const artista       = req.query.artista || '';
        const vendedor      = req.query.vendedor || '';

        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:J`,
        });
        const rows = (resp.data.values || []).slice(1).map((r, i) => ({
          row: i + 2,
          artista: r[0] || '', vendedor: r[1] || '',
          fechaInicio: r[2] || '', fechaVencimiento: r[3] || '',
          masterEventId: r[5] || '',
          estado: r[7] || '',
          precio: r[9] || '',
        }));

        // byMaster: extendidas con el masterEventId actual
        const byMaster = masterEventId
          ? rows.filter(r => r.masterEventId === masterEventId && r.estado === 'extendida' && r.artista)
          : [];

        // byArtVend: extendidas del mismo artista+vendedor (cualquier masterEventId)
        // Útil para recuperar ciclos con masterEventId desactualizado.
        // Para campañas grupales, buscamos por TODOS los artistas del grupo (mismo masterEventId activo).
        // Usamos artista del query (primer hermano) como semilla.
        const byArtVend = artista && vendedor
          ? rows.filter(r => r.artista === artista && r.vendedor === vendedor && r.estado === 'extendida')
          : [];

        // Unión sin duplicados por row
        const seenRows = new Set(byMaster.map(r => r.row));
        const allExt = [...byMaster];
        for (const r of byArtVend) {
          if (!seenRows.has(r.row)) { seenRows.add(r.row); allExt.push(r); }
        }

        // Deduplicar por rango de fechas: hermanas grupales comparten mismo rango.
        // Tomar UNA entrada representativa por rango (precio de esa fila, no sumar).
        const periodMap = new Map();
        for (const r of allExt) {
          const key = `${r.fechaInicio}|${r.fechaVencimiento}`;
          if (!periodMap.has(key)) periodMap.set(key, r);
        }
        const periodos = [...periodMap.values()]
          .sort((a, b) => (a.fechaInicio || '').localeCompare(b.fechaInicio || ''));

        return res.json({ periodos });
      }

      // Extendidas legacy (kept for backward compatibility)
      if (mode === 'extendidas') {
        const masterEventId = req.query.masterEventId;
        if (!masterEventId) return res.json({ extendidas: [] });
        const resp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:V`,
        });
        const extendidas = (resp.data.values || []).slice(1)
          .map((r, i) => ({
            row: i + 2,
            artista: r[0], vendedor: r[1],
            fechaInicio: r[2], fechaVencimiento: r[3],
            duracion: parseInt(r[4]) || 30,
            masterEventId: r[5] || '',
            estado: r[7] || '',
            metodo: r[8] || '', precio: r[9] || '', gasto: r[10] || '',
            detalleGastos: r[15] || '',
            pauta: r[16] || '',
            campaignId: r[19] || '',
          }))
          .filter(c => c.masterEventId === masterEventId && c.estado === 'extendida' && c.artista);
        return res.json({ extendidas });
      }

      // Default: campañas activas
      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:V`,
      });
      const allRows = (resp.data.values || []).slice(1).map((r, i) => ({
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
        representante: r[17] || '',
        notas: r[18] || '',
        campaignId: r[19] || '',
        timestamp: r[20] || '',
        editadoPor: r[21] || '',
      }));

      // Índice de extendidas por masterEventId y por artista+vendedor (fallback)
      const extendidasByMaster = {};
      const extendidasByArtVend = {};
      for (const r of allRows) {
        if (r.estado === 'extendida' && r.artista) {
          if (r.masterEventId) {
            if (!extendidasByMaster[r.masterEventId]) extendidasByMaster[r.masterEventId] = [];
            extendidasByMaster[r.masterEventId].push(r);
          }
          const avKey = `${r.artista}||${r.vendedor}`;
          if (!extendidasByArtVend[avKey]) extendidasByArtVend[avKey] = [];
          extendidasByArtVend[avKey].push(r);
        }
      }

      let campanias = allRows
        .filter(c => ['activa', 'pendiente_pago', 'prueba', 'regalo', 'pendiente_inicio'].includes(c.estado) && c.artista)
        .map(c => {
          // Para pendiente_pago: adjuntar datos de períodos extendidos acumulados
          if (c.estado === 'pendiente_pago') {
            // Misma lógica que pendientes_cobro + cobrar modal:
            // cada fila extendida = 1 ítem (sin deduplicar por fechas).
            // byMaster es la fuente única; fallback byArtVend solo si byMaster vacío.
            const byMaster = (c.masterEventId ? extendidasByMaster[c.masterEventId] : null) || [];
            const extRows  = byMaster.length > 0
              ? byMaster
              : ((c.artista && c.vendedor ? extendidasByArtVend[`${c.artista}||${c.vendedor}`] : null) || []);
            if (extRows.length > 0) {
              const sumPrecio = extRows.reduce((s, e) => s + (parseFloat(e.precio) || 0), parseFloat(c.precio) || 0);
              const sumGasto  = extRows.reduce((s, e) => s + (parseFloat(e.gasto)  || 0), parseFloat(c.gasto)  || 0);
              c._acumulado = { periodos: extRows.length + 1, totalPrecio: +sumPrecio.toFixed(2), totalGasto: +sumGasto.toFixed(2) };
            }
          }
          return c;
        });

      if (vendedor && vendedor !== 'all')
        campanias = campanias.filter(c => c.vendedor === vendedor);

      return res.json({ campanias });
    }

    // ── POST: nuevo cliente ───────────────────────────────────
    if (req.method === 'POST' && req.body.mode === 'client') {
      const { artista, genero, pais, telefono, email, spotify, representante, nombre, apodo, vendedor: vend, metodoPago, estado, imagen } = req.body;
      if (!artista) return res.status(400).json({ error: 'artista requerido' });

      // ID correlativo: leer IDs existentes y generar el siguiente C00X
      const idResp = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${CLIENTES_SHEET}!A:A` });
      const existingIds = (idResp.data.values || []).slice(1).map(r => r[0] || '');
      const maxNum = existingIds.reduce((max, id) => {
        const m = (id || '').match(/^C(\d+)$/i);
        return m ? Math.max(max, parseInt(m[1])) : max;
      }, 0);
      const newId = `C${String(maxNum + 1).padStart(3, '0')}`;

      // Buscar primera compra real en hoja 2024; si no existe, usar fecha de hoy
      const today = new Date();
      const todayStr = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
      const primeraCompra = (await findPrimeraCompra(sheets, artista)) || todayStr;

      // Prefijo ' en teléfono para evitar que Sheets lo interprete como fórmula/número
      const safeTel = telefono ? (telefono.startsWith('+') ? `'${telefono}` : telefono) : '';

      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CLIENTES_SHEET}!A:R`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[newId, artista, genero || '', pais || '', safeTel, email || '', spotify || '', primeraCompra, representante || '', nombre || '', apodo || '', vend || '', metodoPago || '', estado || 'Activa', imagen || '', req.body.tipo || '', req.body.direccion || '', req.body.taxId || '']] },
      });
      return res.json({ ok: true });
    }

    if (req.method === 'POST' && req.body.mode === 'representante') {
      const { nombre, email, direccion, taxId, notas, nombreFiscal, autoFactura } = req.body;
      if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${REPRESENTANTES_SHEET}!A:G`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[nombre, email || '', direccion || '', taxId || '', notas || '', nombreFiscal || '', autoFactura ? '1' : '']] },
      });
      return res.json({ ok: true });
    }

    // ── POST: nueva campaña ───────────────────────────────────
    if (req.method === 'POST') {
      const { artista, genero, vendedor, fechaInicio, duracion, fechaVencimiento: fechaVencBody, precio, metodo, gasto, pauta, link, gastosRows, representante, spotifyArtistId, spotifyImageUrl, sinPago, skipCalendar, grupal, notas, regalo } = req.body;
      const { neto, final, margen } = calcFinancials(precio, gasto, metodo, vendedor);
      if (!artista || !vendedor || !fechaInicio || !duracion)
        return res.status(400).json({ error: 'artista, vendedor, fechaInicio y duracion son requeridos' });

      const fechaVencimiento = fechaVencBody || addDays(fechaInicio, duracion);
      const esPrueba = (artista || '').toLowerCase().trim() === 'campedrinos';
      const hoyIso = new Date().toISOString().split('T')[0];
      const esFuturo = fechaInicio > hoyIso;
      const estadoCampana    = regalo ? 'regalo' : esPrueba ? 'prueba' : sinPago ? 'pendiente_pago' : esFuturo ? 'pendiente_inicio' : 'activa';
      // colorId: '5' = banana (amarillo) para regalo, '6' = tangerine (naranja) para pendiente_pago, '7' = peacock (azul) para pendiente_inicio, '2' = sage (verde) para activa
      const calColorId = regalo ? '5' : sinPago ? '6' : esFuturo ? '7' : '2';

      const clientId = await getOrCreateClient(sheets, artista, genero, fechaInicio, representante || '', vendedor, metodo || '');

      if (spotifyArtistId) {
        await updateClientFields(sheets, artista, {
          spotify: `https://open.spotify.com/artist/${spotifyArtistId}`,
          ...(spotifyImageUrl ? { imagen: spotifyImageUrl } : {}),
        });
      }

      // skipCalendar: true → reusar eventIds existentes (para artistas adicionales de pauta grupal)
      let masterEventId = req.body.masterEventId || '';
      let vendorEventId = req.body.vendorEventId || '';
      if (!skipCalendar) {
        try {
          const ids = await createCalEvents(cal, artista, vendedor, fechaVencimiento, duracion, precio || 0, metodo || '', pauta || '', link || '', gastosRows || [], representante || '', calColorId, grupal, regalo);
          masterEventId = ids.masterEventId;
          vendorEventId = ids.vendorEventId;
        } catch(calErr) {
          console.error('Calendar error (non-fatal):', calErr.message);
        }
      }

      const detalleGastos = (gastosRows || [])
        .filter(r => r.amount > 0)
        .map(r => `(${r.amount})${r.provider ? ' ' + r.provider : ''}`)
        .join('\n');

      const campaignId = 'CP_' + Date.now();
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:U`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[artista, vendedor, fechaInicio, fechaVencimiento, duracion, masterEventId, vendorEventId, estadoCampana, metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero || '', detalleGastos, pauta || '', representante || '', notas || '', campaignId, new Date().toISOString()]] },
      });

      return res.json({ ok: true, clientId, fechaVencimiento, masterEventId, vendorEventId, campaignId });
    }

    if (req.method === 'PUT' && req.body.mode === 'representante') {
      const { row, nombre, email, direccion, taxId, notas, nombreFiscal, autoFactura } = req.body;
      if (!row || !nombre) return res.status(400).json({ error: 'row y nombre requeridos' });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${REPRESENTANTES_SHEET}!A${row}:G${row}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[nombre, email || '', direccion || '', taxId || '', notas || '', nombreFiscal || '', autoFactura ? '1' : '']] },
      });
      return res.json({ ok: true });
    }

    // ── PUT: editar cliente ───────────────────────────────────
    if (req.method === 'PUT' && req.body.mode === 'client') {
      const { row, artista, genero, pais, telefono, email, spotify, representante, nombre, apodo, vendedor: vend, metodoPago, estado, imagen, tipo, direccion, taxId, autoFactura } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const existing = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CLIENTES_SHEET}!A${row}:S${row}`,
      });
      const existingRow = existing.data.values?.[0] || [];
      const id = existingRow[0] || Date.now().toString();
      const fechaPrimeraCompra = existingRow[7] || ''; // Preservar siempre la fecha original
      const safeTel = telefono ? (telefono.startsWith('+') ? `'${telefono}` : telefono) : '';
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CLIENTES_SHEET}!A${row}:S${row}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[id, artista, genero || '', pais || '', safeTel, email || '', spotify || '', fechaPrimeraCompra, representante || '', nombre || '', apodo || '', vend || '', metodoPago || '', estado || '', imagen ?? existingRow[14] ?? '', tipo || existingRow[15] || '', direccion ?? existingRow[16] ?? '', taxId ?? existingRow[17] ?? '', autoFactura ? '1' : (existingRow[18] ?? '')]] },
      });
      return res.json({ ok: true });
    }

    // ── PUT: extender campaña sin pago ───────────────────────
    if (req.method === 'PUT' && req.body.esExtension) {
      const { row, precio, gasto, metodo, pauta, gastosRows, duracion: duracionBody, fechaVencimiento: fechaVencBody, notas: notasBody, editadoPor } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      // Leer fila actual completa
      const campResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A${row}:V${row}`,
      });
      const oldRow = campResp.data.values?.[0] || [];
      const artista       = oldRow[0] || '';
      const vendedor      = oldRow[1] || '';
      const masterEventId = oldRow[5] || '';
      const vendorEventId = oldRow[6] || '';
      const genero        = oldRow[14] || '';
      const representante = oldRow[17] || '';
      const notas         = notasBody !== undefined ? notasBody : (oldRow[18] || '');
      const duracion      = duracionBody || parseInt(oldRow[4]) || 30;

      // Fecha vencimiento del período actual (base para el nuevo)
      const baseVenc = oldRow[3] || new Date().toISOString().split('T')[0];
      const nuevaFechaVenc = fechaVencBody || addDays(baseVenc, duracion);
      // Fecha inicio del nuevo período = fecha vencimiento del anterior
      const nuevaFechaInicio = baseVenc;

      const ts = new Date().toISOString();
      const campaignId = 'CP_' + Date.now();

      const detalleGastos = (gastosRows || [])
        .filter(r => r.amount > 0)
        .map(r => `(${r.amount})${r.provider ? ' ' + r.provider : ''}`)
        .join('\n');

      const { neto, final, margen } = calcFinancials(precio, gasto, metodo || oldRow[8], vendedor);

      // Buscar filas hermanas (misma campaña grupal) si hay masterEventId
      let siblingRows = [{ rowNum: row, rowData: oldRow }]; // default: solo la fila clickeada
      if (masterEventId) {
        const allRowsResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:V`,
        });
        const allRows = allRowsResp.data.values || [];
        const SIBLING_STATES = new Set(['activa', 'pendiente_pago', 'pendiente_inicio', 'regalo', 'prueba']);
        // header is row index 0 = sheet row 1; data starts at index 1 = sheet row 2
        const candidates = [];
        for (let i = 1; i < allRows.length; i++) {
          const r = allRows[i];
          const rMaster = r[5] || '';
          const rEstado = r[7] || '';
          if (rMaster === masterEventId && SIBLING_STATES.has(rEstado)) {
            candidates.push({ rowNum: i + 1, rowData: r }); // sheet row = array index + 1
          }
        }
        if (candidates.length > 1) siblingRows = candidates;
      }

      // 1. Marcar TODAS las filas hermanas como 'extendida'
      const extendData = [];
      for (const sib of siblingRows) {
        extendData.push({ range: `${CAMPANAS_SHEET}!H${sib.rowNum}`, values: [['extendida']] });
        extendData.push({ range: `${CAMPANAS_SHEET}!U${sib.rowNum}`, values: [[ts]] });
        extendData.push({ range: `${CAMPANAS_SHEET}!V${sib.rowNum}`, values: [[editadoPor || '']] });
      }
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: 'USER_ENTERED', data: extendData },
      });

      // 2. Actualizar fecha de vencimiento en calendario (reusar masterEventId)
      try {
        const patchBody = { end: { date: nuevaFechaVenc } };
        await Promise.allSettled([
          masterEventId ? cal.events.patch({ calendarId: MASTER_CAL, eventId: masterEventId, requestBody: patchBody }) : null,
          (vendorEventId && VENDOR_CALS[vendedor]) ? cal.events.patch({ calendarId: VENDOR_CALS[vendedor], eventId: vendorEventId, requestBody: patchBody }) : null,
        ].filter(Boolean));
      } catch(calErr) {
        console.error('Calendar patch error (non-fatal):', calErr.message);
      }

      // 3. Agregar nueva fila pendiente_pago por cada hermana (misma masterEventId)
      const newRowValues = siblingRows.map(sib => {
        const r = sib.rowData;
        const sibArtista = r[0] || artista;
        const sibVendedor = r[1] || vendedor;
        const sibVendorEventId = r[6] || vendorEventId;
        const sibGenero = r[14] || genero;
        const sibRep = r[17] || representante;
        const sibNotas = notasBody !== undefined ? notasBody : (r[18] || '');
        const sibMetodo = metodo || r[8] || '';
        const { neto: sibNeto, final: sibFinal, margen: sibMargen } = calcFinancials(precio, gasto, sibMetodo, sibVendedor);
        return [sibArtista, sibVendedor, nuevaFechaInicio, nuevaFechaVenc, duracion, masterEventId, sibVendorEventId, 'pendiente_pago', sibMetodo, precio || '', gasto || '', sibNeto || '', sibMargen || '', sibFinal || '', sibGenero, detalleGastos, pauta || '', sibRep, sibNotas, campaignId, ts, editadoPor || ''];
      });

      const appendResp = await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A:V`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: newRowValues },
      });
      const updatedRange = appendResp.data?.updates?.updatedRange || '';
      const rowMatch = updatedRange.match(/(\d+)$/);
      const newCampaignRow = rowMatch ? parseInt(rowMatch[1]) : null;

      return res.json({ ok: true, newCampaignRow, nuevaFechaVenc, campaignId });
    }

    // ── PUT: cobrar campaña pendiente ─────────────────────────
    if (req.method === 'PUT' && req.body.mode === 'cobrar') {
      const { row, masterEventId, vendorEventId, vendedor, extraRows } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      const ts = new Date().toISOString();
      const updateData = [
        { range: `${CAMPANAS_SHEET}!H${row}`, values: [['activa']] },
        { range: `${CAMPANAS_SHEET}!U${row}`, values: [[ts]] },
      ];

      // Marcar extendidas seleccionadas como 'cobrada'
      if (extraRows && extraRows.length) {
        extraRows.forEach(r => {
          updateData.push({ range: `${CAMPANAS_SHEET}!H${r}`, values: [['cobrada']] });
          updateData.push({ range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] });
        });
      }

      // Marcar finalizada_sin_cobrar seleccionadas como 'cobrada'
      if (req.body.sinCobrarRows && req.body.sinCobrarRows.length) {
        req.body.sinCobrarRows.forEach(r => {
          updateData.push({ range: `${CAMPANAS_SHEET}!H${r}`, values: [['cobrada']] });
          updateData.push({ range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] });
        });
      }
      // Marcar pendiente_pago de otras campañas como 'activa'
      if (req.body.otrosPendientesRows && req.body.otrosPendientesRows.length) {
        req.body.otrosPendientesRows.forEach(r => {
          updateData.push({ range: `${CAMPANAS_SHEET}!H${r}`, values: [['activa']] });
          updateData.push({ range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] });
        });
      }

      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: 'USER_ENTERED', data: updateData },
      });

      // Actualizar color del evento de calendario a verde (colorId '2')
      if (masterEventId || vendorEventId) {
        const vendorCalId = VENDOR_CALS[vendedor];
        const patchColor = { colorId: '2' };
        await Promise.allSettled([
          masterEventId
            ? cal.events.patch({ calendarId: MASTER_CAL, eventId: masterEventId, requestBody: patchColor })
            : null,
          (vendorEventId && vendorCalId)
            ? cal.events.patch({ calendarId: vendorCalId, eventId: vendorEventId, requestBody: patchColor })
            : null,
        ].filter(Boolean));
      }

      return res.json({ ok: true });
    }

    // ── PUT: cobrar extendidas solamente (sin tocar período activo) ──
    if (req.method === 'PUT' && req.body.mode === 'cobrar_extendidas_only') {
      const { rows, sinCobrarRows } = req.body;
      const ts = new Date().toISOString();
      const updateData = (rows || []).flatMap(r => [
        { range: `${CAMPANAS_SHEET}!H${r}`, values: [['cobrada']] },
        { range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] },
      ]);
      if (sinCobrarRows?.length) {
        sinCobrarRows.forEach(r => {
          updateData.push({ range: `${CAMPANAS_SHEET}!H${r}`, values: [['cobrada']] });
          updateData.push({ range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] });
        });
      }
      if (req.body.otrosPendientesRows?.length) {
        req.body.otrosPendientesRows.forEach(r => {
          updateData.push({ range: `${CAMPANAS_SHEET}!H${r}`, values: [['activa']] });
          updateData.push({ range: `${CAMPANAS_SHEET}!U${r}`, values: [[ts]] });
        });
      }
      if (!updateData.length) return res.json({ ok: true });
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: 'USER_ENTERED', data: updateData },
      });
      return res.json({ ok: true });
    }

    // ── PUT: editar/renovar campaña ───────────────────────────
    if (req.method === 'PUT') {
      const { row, artista, vendedor, duracion, fechaVencimiento: fechaVencBody, masterEventId, vendorEventId, precio, gasto, metodo, pauta, gastosRows, genero: generoBody, representante: representanteBody, esEdicion, notas: notasBody, editadoPor, esActivar } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });

      // ── Activar campaña pendiente_inicio → activa ──────────
      if (esActivar) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!H${row}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [['activa']] },
        });
        try {
          const patchColor = { colorId: '2' }; // sage = verde
          await Promise.allSettled([
            masterEventId ? cal.events.patch({ calendarId: MASTER_CAL, eventId: masterEventId, requestBody: patchColor }) : null,
            (vendorEventId && VENDOR_CALS[vendedor]) ? cal.events.patch({ calendarId: VENDOR_CALS[vendedor], eventId: vendorEventId, requestBody: patchColor }) : null,
          ].filter(Boolean));
        } catch(calErr) { console.error('Calendar patch error (non-fatal):', calErr.message); }
        return res.json({ ok: true });
      }

      // Leer fila completa actual para obtener fechaInicio, vencimiento base y campaignId
      const campResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${CAMPANAS_SHEET}!A${row}:T${row}`,
      });
      const oldRow        = campResp.data.values?.[0] || [];
      const fechaInicio   = oldRow[2] || new Date().toISOString().split('T')[0];
      const baseDate      = oldRow[3] || new Date().toISOString().split('T')[0];
      const oldEstado     = oldRow[7] || 'activa'; // preservar estado original (pendiente_pago, regalo, etc.)
      const genero        = generoBody || oldRow[14] || '';
      const representante = representanteBody !== undefined ? representanteBody : (oldRow[17] || '');
      const notas         = notasBody !== undefined ? notasBody : (oldRow[18] || '');
      const oldCampaignId = oldRow[19] || '';

      // Sincronizar campos en hoja Clientes
      await updateClientFields(sheets, artista, {
        representante, vendedor, metodoPago: metodo, estado: 'Activa',
      });

      // Fecha vencimiento: usar la enviada si viene (edición manual), sino calcular desde base
      const nuevaFechaVenc = fechaVencBody || addDays(baseDate, duracion);

      // Detectar si es campaña grupal: buscar otros rows activos con el mismo masterEventId
      let siblingRows = []; // [{rowNum}]
      if (masterEventId) {
        const allResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!F:H`, // F=EVENT_ID_MASTER, H=ESTADO
        });
        const allVals = (allResp.data.values || []).slice(1); // skip header
        siblingRows = allVals
          .map((r, i) => ({ rowNum: i + 2, masterId: r[0] || '', estado: r[2] || '' }))
          .filter(r => r.masterId === masterEventId && ['activa', 'pendiente_pago', 'pendiente_inicio', 'regalo', 'prueba'].includes(r.estado) && r.rowNum !== parseInt(row));
      }
      const esGrupal = siblingRows.length > 0;

      await deleteCalEvents(cal, vendedor, masterEventId, vendorEventId);

      let newMasterId = '', newVendorId = '';
      try {
        const ids = await createCalEvents(cal, artista, vendedor, nuevaFechaVenc, duracion, precio || 0, metodo || '', pauta || '', '', gastosRows || [], representante, null, esGrupal);
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

      // Campañas grupales: siempre sobreescribir (nunca historial) para mantener consistencia del grupo
      // Campañas individuales: respetar período de gracia de 3 días
      const fechaInicioDate = parseDateStr(fechaInicio) || new Date();
      const diasDesdeCreacion = Math.floor((Date.now() - fechaInicioDate.getTime()) / 86400000);
      const enPeriodoGracia = esEdicion && (esGrupal || diasDesdeCreacion <= 3);
      // campaignId: en edición se conserva el mismo; en renovación se genera uno nuevo
      const campaignId = esEdicion ? (oldCampaignId || 'CP_' + Date.now()) : 'CP_' + Date.now();
      let newCampaignRow = row; // para enPeriodoGracia el row no cambia

      if (enPeriodoGracia) {
        // Sobreescribir fila existente sin dejar historial
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A${row}:V${row}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[artista, vendedor, fechaInicio, nuevaFechaVenc, duracion, newMasterId, newVendorId, oldEstado, metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero, detalleGastos, pauta || '', representante, notas, campaignId, new Date().toISOString(), editadoPor || '']] },
        });
        // Actualizar masterEventId/vendorEventId y artista en todos los siblings del grupo
        if (siblingRows.length && newMasterId) {
          await Promise.all(siblingRows.map(sib =>
            sheets.spreadsheets.values.batchUpdate({
              spreadsheetId: SPREADSHEET_ID,
              requestBody: {
                valueInputOption: 'USER_ENTERED',
                data: [
                  { range: `${CAMPANAS_SHEET}!A${sib.rowNum}`, values: [[artista]] },
                  { range: `${CAMPANAS_SHEET}!F${sib.rowNum}:G${sib.rowNum}`, values: [[newMasterId, newVendorId]] },
                ],
              },
            })
          ));
        }
        // Actualizar masterEventId en filas extendidas (historial) del mismo evento para que _acumulado las siga encontrando
        if (masterEventId && newMasterId && masterEventId !== newMasterId) {
          const extAllResp = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${CAMPANAS_SHEET}!F:H`,
          });
          const extRows = (extAllResp.data.values || []).slice(1);
          const extUpdates = [];
          extRows.forEach((r, i) => {
            if ((r[0] || '') === masterEventId && (r[2] || '') === 'extendida') {
              extUpdates.push({ range: `${CAMPANAS_SHEET}!F${i + 2}`, values: [[newMasterId]] });
            }
          });
          if (extUpdates.length) {
            await sheets.spreadsheets.values.batchUpdate({
              spreadsheetId: SPREADSHEET_ID,
              requestBody: { valueInputOption: 'USER_ENTERED', data: extUpdates },
            });
          }
        }
      } else {
        // Marcar fila vieja como historial ('editada' o 'renovada') y agregar nueva fila activa
        const estadoHistorial = esEdicion ? 'editada' : 'renovada';
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!H${row}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[estadoHistorial]] },
        });
        const appendResp = await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:V`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[artista, vendedor, fechaInicio, nuevaFechaVenc, duracion, newMasterId, newVendorId, 'activa', metodo || '', precio || '', gasto || '', neto || '', margen || '', final || '', genero, detalleGastos, pauta || '', representante, notas, campaignId, new Date().toISOString(), editadoPor || '']] },
        });
        // Extraer row number de la respuesta del append
        const updatedRange = appendResp.data?.updates?.updatedRange || '';
        const rowMatch = updatedRange.match(/(\d+)$/);
        if (rowMatch) newCampaignRow = parseInt(rowMatch[1]);

        // Actualizar masterEventId en filas extendidas del mismo evento (igual que enPeriodoGracia)
        if (masterEventId && newMasterId && masterEventId !== newMasterId) {
          const extAllResp = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${CAMPANAS_SHEET}!F:H`,
          });
          const extRows = (extAllResp.data.values || []).slice(1);
          const extUpdates = [];
          extRows.forEach((r, i) => {
            if ((r[0] || '') === masterEventId && (r[2] || '') === 'extendida') {
              extUpdates.push({ range: `${CAMPANAS_SHEET}!F${i + 2}`, values: [[newMasterId]] });
            }
          });
          if (extUpdates.length) {
            await sheets.spreadsheets.values.batchUpdate({
              spreadsheetId: SPREADSHEET_ID,
              requestBody: { valueInputOption: 'USER_ENTERED', data: extUpdates },
            });
          }
        }
      }

      // Actualizar METODO, COMISION_PCT y CAMPAIGN_ID en la hoja de ventas principal
      try {
        const ventasResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: 'A:L',
        });
        const ventasRows = ventasResp.data.values || [];
        const targetArtista = (artista || '').toLowerCase().trim();
        const targetPrecio  = parseFloat(precio) || 0;
        const nuevaComision = METHOD_COMMISSIONS[metodo] ?? 0;
        // Buscar primero por CAMPAIGN_ID (col L), fallback artista+precio
        let matchIdx = -1;
        for (let i = 1; i < ventasRows.length; i++) {
          if (ventasRows[i][11] === oldCampaignId && oldCampaignId) { matchIdx = i; break; }
        }
        if (matchIdx === -1) {
          for (let i = 1; i < ventasRows.length; i++) {
            const r = ventasRows[i];
            if ((r[0] || '').toLowerCase().trim() !== targetArtista) continue;
            if (Math.abs(parseFloat(r[4]) - targetPrecio) > 0.5) continue;
            matchIdx = i; break;
          }
        }
        if (matchIdx !== -1) {
          const sheetRow = matchIdx + 1;
          const oldArtista = (ventasRows[matchIdx][0] || '').trim();
          const updateData = [
            ...(artista && artista !== oldArtista ? [{ range: `A${sheetRow}`, values: [[artista]] }] : []),
            { range: `C${sheetRow}:F${sheetRow}`, values: [[metodo || '', nuevaComision, precio || '', gasto || '']] },
            { range: `G${sheetRow}:I${sheetRow}`, values: [[`=(E${sheetRow} - F${sheetRow}) * (1 - D${sheetRow} / 100)`, `=BUSCARV(B${sheetRow}, Vendedores!A:B, 2, FALSO)`, `=G${sheetRow} * (1 - H${sheetRow} / 100)`]] },
            { range: `L${sheetRow}`, values: [[campaignId]] },
          ];
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            requestBody: { valueInputOption: 'USER_ENTERED', data: updateData },
          });
        }
      } catch (e) {
        console.error('Error actualizando hoja ventas:', e.message);
      }

      return res.json({ ok: true, nuevaFechaVenc, newCampaignRow, campaignId });
    }

    if (req.method === 'DELETE' && req.body?.mode === 'representante') {
      const { row } = req.body;
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      const repSheet = meta.data.sheets.find(s => s.properties.title === REPRESENTANTES_SHEET);
      if (!repSheet) return res.status(404).json({ error: 'Hoja Representantes no encontrada' });
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId: repSheet.properties.sheetId, dimension: 'ROWS', startIndex: row - 1, endIndex: row } } }] },
      });
      return res.json({ ok: true });
    }

    // ── DELETE: borrar cliente ────────────────────────────────
    if (req.method === 'DELETE' && req.body?.mode === 'client') {
      const row = parseInt(req.body.row);
      if (!row) return res.status(400).json({ error: 'row requerido' });
      const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      const sheetObj = meta.data.sheets.find(s => s.properties.title === CLIENTES_SHEET);
      if (!sheetObj) return res.status(404).json({ error: 'Hoja Clientes no encontrada' });
      const sheetId = sheetObj.properties.sheetId;
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId, dimension: 'ROWS', startIndex: row - 1, endIndex: row } } }] },
      });
      return res.json({ ok: true });
    }

    // ── DELETE: no renueva / borrar campaña por artista ──────
    if (req.method === 'DELETE') {
      let { row, masterEventId, vendorEventId, vendedor, artista, fechaInicio, estado: estadoFinal, campaignId } = req.body;
      estadoFinal = estadoFinal || 'finalizada';

      // Si viene campaignId, buscar la fila en col T
      if (!row && campaignId) {
        const campIdResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:T`,
        });
        const campIdRows = (campIdResp.data.values || []).slice(1);
        const matchIdx = campIdRows.findIndex(r => r[19] === campaignId);
        if (matchIdx !== -1) {
          row = matchIdx + 2;
          masterEventId = masterEventId || campIdRows[matchIdx][5] || '';
          vendorEventId = vendorEventId || campIdRows[matchIdx][6] || '';
          vendedor      = vendedor      || campIdRows[matchIdx][1] || '';
          artista       = artista       || campIdRows[matchIdx][0] || '';
        }
      }

      // Pauta grupal (artista = "Varios"): borrar todas las campañas que comparten masterEventId
      if (!row && artista === 'Varios' && vendedor && fechaInicio) {
        const campResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:H`,
        });
        const campRows = (campResp.data.values || []).slice(1);
        // Encontrar campaña activa del mismo vendedor + fechaInicio para obtener el masterEventId compartido
        const ACTIVE_STATES = ['activa', 'pendiente_pago', 'prueba', 'regalo', 'pendiente_inicio'];
        const anchor = campRows.find(r =>
          (r[1] || '').toLowerCase().trim() === vendedor.toLowerCase().trim() &&
          (r[2] || '').trim() === fechaInicio &&
          ACTIVE_STATES.includes(r[7] || '')
        );
        if (!anchor) return res.json({ ok: true, skipped: true });
        const sharedMasterId = anchor[5] || '';
        // Marcar como finalizada TODAS las campañas con ese masterEventId
        const toFinalize = campRows
          .map((r, i) => ({ r, i }))
          .filter(({ r }) => r[5] === sharedMasterId && ACTIVE_STATES.includes(r[7] || ''));
        if (!toFinalize.length) return res.json({ ok: true, skipped: true });
        // Borrar evento de calendario (solo una vez)
        await deleteCalEvents(cal, vendedor, sharedMasterId, anchor[6] || '');
        // Marcar todas las filas con el estado correspondiente
        const ts = new Date().toISOString();
        await Promise.all(toFinalize.map(({ i }) =>
          sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            requestBody: {
              valueInputOption: 'USER_ENTERED',
              data: [
                { range: `${CAMPANAS_SHEET}!H${i + 2}`, values: [[estadoFinal]] },
                { range: `${CAMPANAS_SHEET}!U${i + 2}`, values: [[ts]] },
              ],
            },
          })
        ));
        return res.json({ ok: true });
      }

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
              ['activa', 'prueba', 'pendiente_pago', 'regalo'].includes(r[7] || '')) {
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

      const ts = new Date().toISOString();
      const updateData = [
        { range: `${CAMPANAS_SHEET}!H${row}`, values: [[estadoFinal]] },
        { range: `${CAMPANAS_SHEET}!U${row}`, values: [[ts]] },
      ];

      // Si hay masterEventId, finalizar también todas las demás filas que lo compartan (campañas grupales)
      if (masterEventId) {
        const allResp = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:H`,
        });
        const allRows = (allResp.data.values || []).slice(1);
        allRows.forEach((r, i) => {
          const rowNum = i + 2;
          if (rowNum !== row && (r[5] || '') === masterEventId &&
              ['activa', 'pendiente_pago', 'prueba', 'regalo'].includes(r[7] || '')) {
            updateData.push({ range: `${CAMPANAS_SHEET}!H${rowNum}`, values: [[estadoFinal]] });
            updateData.push({ range: `${CAMPANAS_SHEET}!U${rowNum}`, values: [[ts]] });
          }
        });
      }

      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: 'USER_ENTERED', data: updateData },
      });

      // Actualizar ESTADO en Clientes si no quedan campañas activas para este artista
      if (artista) {
        const remaining = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: `${CAMPANAS_SHEET}!A:H`,
        });
        const stillActive = (remaining.data.values || []).slice(1).some(r =>
          (r[0] || '').toLowerCase().trim() === artista.toLowerCase().trim() &&
          (r[7] || '') === 'activa'
        );
        if (!stillActive) {
          await updateClientFields(sheets, artista, { estado: 'Sin pauta' });
        }
      }

      return res.json({ ok: true, debug: calResult });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch(e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
