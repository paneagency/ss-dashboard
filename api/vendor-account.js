const { google } = require('googleapis');

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
  let s = String(val).trim().replace(/[$\s]/g, '');
  if (s.includes('.') && s.includes(',')) {
    const lastDot = s.lastIndexOf('.');
    const lastComma = s.lastIndexOf(',');
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(',', '.');
    else s = s.replace(/,/g, '');
  } else if (s.includes(',')) {
    const parts = s.split(',');
    s = (parts.length === 2 && parts[1].length <= 2) ? s.replace(',', '.') : s.replace(/,/g, '');
  }
  return parseFloat(s) || 0;
}

// values[0]=row1(header), values[1]=row2(header), values[2]=row3(first data)
// so sheet row = sliceIndex + 3
function parseSheetRows(values) {
  if (!values || values.length < 3) return [];
  return values.slice(2)
    .map((row, i) => ({
      row:     i + 3,
      detalle: (row[0] || '').trim(),
      monto:   parseNum(row[1]),
      fecha:   (row[2] || '').trim(),
    }))
    .filter(r => r.detalle && r.monto !== 0);
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const sheetId = req.query?.sheetId || req.body?.sheetId;
    if (!sheetId) return res.status(400).json({ error: 'sheetId requerido' });

    // ── LEER movimientos ─────────────────────────────────────
    if (req.method === 'GET') {
      const sheets = getSheets();

      const [retirosResp, recibidoResp, saldoResp] = await Promise.all([
        sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range: 'Retiros!A:C' }),
        sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range: 'Recibido!A:C' }),
        sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range: 'J2' }),
      ]);

      const retiros  = parseSheetRows(retirosResp.data.values);
      const recibido = parseSheetRows(recibidoResp.data.values);

      const totalRetiros  = retiros.reduce((s, r) => s + r.monto, 0);
      const totalRecibido = recibido.reduce((s, r) => s + r.monto, 0);

      const saldoRaw = saldoResp.data.values?.[0]?.[0] ?? '0';
      const saldo = parseNum(saldoRaw);

      return res.json({ retiros, recibido, totalRetiros, totalRecibido, saldo });
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

    // ── BORRAR movimiento ─────────────────────────────────────
    if (req.method === 'DELETE') {
      const sheetName = req.query?.sheetName || req.body?.sheetName;
      const row       = parseInt(req.query?.row || req.body?.row);
      if (!sheetName || !row)
        return res.status(400).json({ error: 'Faltan campos: sheetName, row' });
      if (!['Retiros', 'Recibido'].includes(sheetName))
        return res.status(400).json({ error: 'sheetName debe ser "Retiros" o "Recibido"' });

      const sheets = getSheets();

      // Obtener el ID numérico de la pestaña por nombre
      const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
      const sheet = meta.data.sheets.find(s => s.properties.title === sheetName);
      if (!sheet) return res.status(404).json({ error: `Pestaña "${sheetName}" no encontrada` });
      const numericSheetId = sheet.properties.sheetId;

      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: sheetId,
        requestBody: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId:    numericSheetId,
                dimension:  'ROWS',
                startIndex: row - 1,  // 0-based
                endIndex:   row,
              },
            },
          }],
        },
      });

      return res.json({ ok: true });
    }

    return res.status(405).json({ error: 'Método no permitido' });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
