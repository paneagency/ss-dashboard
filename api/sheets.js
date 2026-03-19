const { google } = require('googleapis');
const crypto     = require('crypto');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const SHEET_GID      = 162664118;
const SHEET_RANGE    = 'A:M'; // M = BILLDU_ID

const BILLDU_BASE    = 'https://api.billdu.com/v3';
const BILLDU_METHODS = ['paypal', 'wise'];

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

// ── Billdu helpers ─────────────────────────────────────────────────────────

function sortObjKeys(obj) {
  if (Array.isArray(obj)) return obj.map(sortObjKeys);
  if (obj && typeof obj === 'object') {
    return Object.keys(obj).sort().reduce((acc, k) => {
      acc[k] = sortObjKeys(obj[k]);
      return acc;
    }, {});
  }
  return obj;
}

function billduSign(data, apiSecret) {
  const sorted  = sortObjKeys(data);
  const json    = JSON.stringify(sorted);
  return crypto.createHmac('sha512', apiSecret).update(json).digest('base64');
}

async function billduRequest(method, path, body, apiKey, apiSecret) {
  const timestamp = Math.floor(Date.now() / 1000);
  const signData  = body ? { ...body, apiKey, timestamp } : { apiKey, timestamp };
  const signature = encodeURIComponent(billduSign(signData, apiSecret));
  const url = `${BILLDU_BASE}${path}?apiKey=${apiKey}&timestamp=${timestamp}&signature=${signature}`;
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body && method !== 'GET') opts.body = JSON.stringify(body);
  const r    = await fetch(url, opts);
  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch (_) { data = text; }
  return { ok: r.ok, status: r.status, data };
}

async function findBillduClient(name, apiKey, apiSecret) {
  const result = await billduRequest('GET', '/clients', null, apiKey, apiSecret);
  if (!result.ok) return null;
  const list = Array.isArray(result.data) ? result.data : (result.data?.data || []);
  const needle = name.toLowerCase();
  const found = list.find(c => {
    const n = (c.name || c.company || c.fullName || '').toLowerCase();
    return n === needle || n.includes(needle) || needle.includes(n);
  });
  return found ? found.id : null;
}

async function createBillduClient(name, apiKey, apiSecret) {
  const result = await billduRequest('POST', '/clients', { name }, apiKey, apiSecret);
  if (result.ok && result.data?.id) return result.data.id;
  throw new Error(`No se pudo crear cliente Billdu "${name}": ${JSON.stringify(result.data)}`);
}

async function getBillduClientId(vendedor, artista, apiKey, apiSecret) {
  // Buscar por vendedor primero
  if (vendedor) {
    const id = await findBillduClient(vendedor, apiKey, apiSecret);
    if (id) return id;
  }
  // Buscar por artista
  const idArtista = await findBillduClient(artista, apiKey, apiSecret);
  if (idArtista) return idArtista;
  // Crear con nombre del artista
  return await createBillduClient(artista, apiKey, apiSecret);
}

async function createBillduInvoice(artista, vendedor, precio, fecha, apiKey, apiSecret) {
  const clientId = await getBillduClientId(vendedor, artista, apiKey, apiSecret);
  const body = {
    type:     'invoice',
    client:   clientId,
    currency: 'USD',
    date:     normFecha(fecha),
    items: [{
      label:       'Servicio de promoción musical',
      price:       parseFloat(precio) || 0,
      tax:         0,
      count:       1,
      unit:        'pcs',
      stockNumber: '',
    }],
  };
  const result = await billduRequest('POST', '/documents', body, apiKey, apiSecret);
  if (!result.ok) throw new Error(`Billdu error ${result.status}: ${JSON.stringify(result.data)}`);
  const invoiceId = result.data?.id || result.data?.custom_id;
  if (!invoiceId) throw new Error(`Billdu no devolvió ID: ${JSON.stringify(result.data)}`);
  return String(invoiceId);
}

async function deleteBillduInvoice(invoiceId, apiKey, apiSecret) {
  const result = await billduRequest('DELETE', `/documents/${invoiceId}`, null, apiKey, apiSecret);
  if (!result.ok && result.status !== 404) {
    console.warn(`Billdu DELETE ${invoiceId} failed ${result.status}:`, JSON.stringify(result.data));
  }
  return result.ok || result.status === 404;
}

// ── Handler ────────────────────────────────────────────────────────────────

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const sheets = getSheets();

    // ── LISTA DE VENDEDORES ────────────────────────────────────
    if (req.method === 'GET') {
      const [vendResp, provResp] = await Promise.all([
        sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Vendedores!A:B' }),
        sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Proveedores!A:A' }).catch(() => ({ data: { values: [] } })),
      ]);
      const vendRows = vendResp.data.values || [];
      const vendStart = /^(nombre|vendedor|name|vendor)/i.test(vendRows[0]?.[0] || '') ? 1 : 0;
      const vendors = vendRows.slice(vendStart)
        .filter(r => r[0]?.trim())
        .map(r => ({ name: r[0].trim(), commission: parseFloat(r[1]) || 0 }));
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

      // ── Billdu: crear factura si método es PayPal o Wise ──
      const metodo   = (values[2] || '').toLowerCase();
      const apiKey   = process.env.BILLDU_API_KEY;
      const apiSecret= process.env.BILLDU_API_SECRET;
      if (BILLDU_METHODS.includes(metodo) && apiKey && apiSecret) {
        try {
          const billduId = await createBillduInvoice(
            values[0],  // artista
            values[1],  // vendedor
            values[4],  // precio bruto
            values[9],  // fecha
            apiKey, apiSecret
          );
          await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `M${nextRow}`,
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[billduId]] },
          });
          console.log(`Billdu invoice created: ${billduId} for ${values[0]} (${values[2]})`);
          return res.json({ ok: true, row: nextRow, billduId });
        } catch (e) {
          console.error('Billdu invoice creation failed (non-fatal):', e.message);
          // No es fatal — la venta ya fue guardada
        }
      }

      return res.json({ ok: true, row: nextRow });
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

      // ── Billdu: eliminar factura si existe ────────────────
      const billduId = (rows[matchIndex][12] || '').toString().trim();
      const apiKey    = process.env.BILLDU_API_KEY;
      const apiSecret = process.env.BILLDU_API_SECRET;
      if (billduId && apiKey && apiSecret) {
        try {
          await deleteBillduInvoice(billduId, apiKey, apiSecret);
          console.log(`Billdu invoice deleted: ${billduId} for ${artista}`);
        } catch (e) {
          console.error('Billdu invoice deletion failed (non-fatal):', e.message);
        }
      }

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
