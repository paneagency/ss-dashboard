const { google } = require('googleapis');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const SHEET_GID      = 162664118;
const SHEET_RANGE    = 'A:K';

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
      const vendors = (vendResp.data.values || []).slice(1)
        .filter(r => r[0]?.trim())
        .map(r => ({ name: r[0].trim(), commission: parseFloat(r[1]) || 0 }));
      const providers = (provResp.data.values || []).slice(1)
        .map(r => r[0]?.trim()).filter(Boolean);
      return res.json({ vendors, providers });
    }

    // ── AGREGAR VENTA ─────────────────────────────────────────
    if (req.method === 'POST') {
      const { values } = req.body;
      if (!values || !Array.isArray(values)) return res.status(400).json({ error: 'values requerido' });

      const colResp = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'A:A',
      });
      const nextRow = (colResp.data.values || []).length + 1;

      // Fórmulas en columnas G, H e I
      const rowToWrite = [...values];
      rowToWrite[6] = `=(E${nextRow} - F${nextRow}) * (1 - D${nextRow} / 100)`;
      rowToWrite[7] = `=BUSCARV(B${nextRow}, Vendedores!A:B, 2, FALSO)`;
      rowToWrite[8] = `=G${nextRow} * (1 - H${nextRow} / 100)`;

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `A${nextRow}:K${nextRow}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [rowToWrite] },
      });

      return res.json({ ok: true, row: nextRow });
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
