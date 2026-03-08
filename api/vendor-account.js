const { google } = require('googleapis');

const GID_RETIROS  = '544530893';
const GID_RECIBIDO = '985424718';

function getSheets() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  const auth  = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

function parseNum(val) {
  if (!val) return 0;
  const s = String(val).trim().replace(/[$\s,]/g, '').replace(',', '.');
  return parseFloat(s) || 0;
}

async function fetchCSV(sheetId, gid) {
  const url = `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv&gid=${gid}`;
  const resp = await fetch(url, { redirect: 'follow' });
  if (!resp.ok) throw new Error(`Error leyendo sheet gid=${gid}: ${resp.status}`);
  return resp.text();
}

function parseCSVRows(text) {
  const lines = text.trim().split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  return lines.slice(1).map(line => {
    const cols = [];
    let cur = '', inQ = false;
    for (const ch of line) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cols.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
    cols.push(cur.trim());
    return { detalle: cols[0] || '', monto: parseNum(cols[1]), fecha: (cols[2] || '').replace(/"/g, '').trim() };
  }).filter(r => r.detalle && r.monto !== 0);
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const sheetId = req.query?.sheetId || req.body?.sheetId;
    if (!sheetId) return res.status(400).json({ error: 'sheetId requerido' });

    // ── LEER movimientos ─────────────────────────────────────
    if (req.method === 'GET') {
      const [retirosText, recibidoText] = await Promise.all([
        fetchCSV(sheetId, GID_RETIROS),
        fetchCSV(sheetId, GID_RECIBIDO),
      ]);

      const retiros  = parseCSVRows(retirosText);
      const recibido = parseCSVRows(recibidoText);

      const totalRetiros  = retiros.reduce((s, r) => s + r.monto, 0);
      const totalRecibido = recibido.reduce((s, r) => s + r.monto, 0);

      return res.json({ retiros, recibido, totalRetiros, totalRecibido });
    }

    // ── AGREGAR movimiento ────────────────────────────────────
    if (req.method === 'POST') {
      const { type, detalle, monto, fecha } = req.body;
      if (!type || !detalle || !monto || !fecha)
        return res.status(400).json({ error: 'Faltan campos: type, detalle, monto, fecha' });
      if (!['retiros', 'recibido'].includes(type))
        return res.status(400).json({ error: 'type debe ser "retiros" o "recibido"' });

      const sheetName = type === 'retiros' ? 'Retiros' : 'Recibido';
      const sheets    = getSheets();

      const colResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:A`,
      });
      const nextRow = (colResp.data.values || []).length + 1;

      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${sheetName}!A${nextRow}:C${nextRow}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[detalle, monto, fecha]] },
      });

      return res.json({ ok: true, row: nextRow });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
