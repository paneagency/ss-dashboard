const { google } = require('googleapis');
const { Storage } = require('@google-cloud/storage');
const PDFDocument = require('pdfkit');
const nodemailer = require('nodemailer');

const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const FACTURAS_SHEET = 'Facturas';
const CONFIG_SHEET = 'Config';

const AGENCY = {
  name: 'Pane Agency LLC',
  address1: '30 N. Gould St. Ste R',
  address2: 'Sheridan WY 82801',
  country: 'United States',
  taxReg: '36-4983944',
  email: 'paneagency@gmail.com',
};

function getAuth() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive',
    ],
  });
}

function getStorage() {
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  return new Storage({ credentials: creds });
}

async function uploadToGCS(pdfBuffer, fileName) {
  const bucketName = process.env.GCS_BUCKET_NAME;
  if (!bucketName) throw new Error('GCS_BUCKET_NAME no configurado en Vercel');
  const storage = getStorage();
  const file = storage.bucket(bucketName).file(fileName);
  await file.save(pdfBuffer, { contentType: 'application/pdf', public: true, resumable: false });
  return `https://storage.googleapis.com/${bucketName}/${encodeURIComponent(fileName)}`;
}

async function ensureSheets(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = meta.data.sheets.map(s => s.properties.title);
  const toCreate = [];
  if (!existing.includes(FACTURAS_SHEET)) toCreate.push(FACTURAS_SHEET);
  if (!existing.includes(CONFIG_SHEET)) toCreate.push(CONFIG_SHEET);
  if (!toCreate.length) return;
  await sheetsClient.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: toCreate.map(title => ({ addSheet: { properties: { title } } })) },
  });
  if (toCreate.includes(FACTURAS_SHEET)) {
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${FACTURAS_SHEET}!A1:K1`,
      valueInputOption: 'RAW',
      requestBody: { values: [['INVOICE_NUM','FECHA','ARTISTA','VENDEDOR','REPRESENTANTE','MONTO','METODO','ESTADO','DRIVE_URL','EMAIL_ENVIADO','PARA']] },
    });
  }
}

async function getNextInvoiceNumber(sheetsClient) {
  try {
    const resp = await sheetsClient.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${FACTURAS_SHEET}!A:A` });
    const rows = resp.data.values || [];
    const year = new Date().getFullYear();
    const yearRows = rows.slice(1).filter(r => (r[0] || '').startsWith(`INV-${year}-`));
    return `INV-${year}-${String(yearRows.length + 1).padStart(3, '0')}`;
  } catch(e) {
    return `INV-${new Date().getFullYear()}-001`;
  }
}

function generateInvoicePDF(data) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({ margin: 50, size: 'A4' });
    doc.on('data', c => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const { invoiceNum, issueDate, artista, clienteDireccion, clientePais, clienteTaxId, monto } = data;
    const ACCENT = '#6366f1';
    const DARK = '#1a1a2e';
    const GRAY = '#6b7280';
    const LGRAY = '#f3f4f6';

    // Header band
    doc.rect(0, 0, 595.28, 110).fill(ACCENT);
    doc.fillColor('#ffffff').fontSize(30).font('Helvetica-Bold').text('INVOICE', 50, 32);
    doc.fontSize(10).font('Helvetica-Bold').text(AGENCY.name, 350, 32, { align: 'right', width: 195 });
    doc.fontSize(9).font('Helvetica')
      .fillColor('rgba(255,255,255,0.85)')
      .text(`Invoice #: ${invoiceNum}`, 350, 50, { align: 'right', width: 195 })
      .text(`Issue Date: ${issueDate}`, 350, 63, { align: 'right', width: 195 })
      .text(`Due Date: ${issueDate}`, 350, 76, { align: 'right', width: 195 })
      .text(`Currency: USD`, 350, 89, { align: 'right', width: 195 });

    // FROM block
    doc.fillColor(GRAY).fontSize(8).font('Helvetica-Bold').text('FROM', 50, 130);
    doc.fillColor(DARK).fontSize(10).font('Helvetica-Bold').text(AGENCY.name, 50, 143);
    doc.fillColor(GRAY).fontSize(9).font('Helvetica')
      .text(AGENCY.address1, 50, 157)
      .text(AGENCY.address2, 50, 169)
      .text(AGENCY.country, 50, 181)
      .text(`Tax Reg. No.: ${AGENCY.taxReg}`, 50, 193)
      .text(AGENCY.email, 50, 205);

    // TO block
    doc.fillColor(GRAY).fontSize(8).font('Helvetica-Bold').text('BILL TO', 300, 130);
    doc.fillColor(DARK).fontSize(10).font('Helvetica-Bold').text(artista, 300, 143);
    let toY = 157;
    doc.fillColor(GRAY).fontSize(9).font('Helvetica');
    if (clienteDireccion) { doc.text(clienteDireccion, 300, toY); toY += 12; }
    if (clientePais) { doc.text(clientePais, 300, toY); toY += 12; }
    if (clienteTaxId) { doc.text(`Tax ID: ${clienteTaxId}`, 300, toY); }

    // Divider
    doc.moveTo(50, 228).lineTo(545.28, 228).strokeColor('#e5e7eb').lineWidth(1).stroke();

    // Table header
    doc.rect(50, 238, 495.28, 24).fill(LGRAY);
    doc.fillColor(DARK).fontSize(9).font('Helvetica-Bold')
      .text('DESCRIPTION', 60, 246)
      .text('QTY', 370, 246, { width: 40, align: 'center' })
      .text('UNIT PRICE', 410, 246, { width: 80, align: 'right' })
      .text('TOTAL', 490, 246, { width: 55, align: 'right' });

    // Table row
    const fmt = v => `$${(parseFloat(v)||0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    doc.fillColor(DARK).fontSize(10).font('Helvetica')
      .text('Music campaign and marketing advisory', 60, 278, { width: 295 })
      .text('1', 370, 278, { width: 40, align: 'center' })
      .text(fmt(monto), 410, 278, { width: 80, align: 'right' })
      .text(fmt(monto), 490, 278, { width: 55, align: 'right' });

    doc.moveTo(50, 306).lineTo(545.28, 306).strokeColor('#e5e7eb').lineWidth(0.5).stroke();

    // Subtotal
    doc.fillColor(GRAY).fontSize(9).font('Helvetica').text('Subtotal', 390, 320);
    doc.fillColor(DARK).fontSize(9).font('Helvetica-Bold').text(fmt(monto), 490, 320, { width: 55, align: 'right' });

    // Total box
    doc.rect(350, 340, 195.28, 34).fill(ACCENT);
    doc.fillColor('#ffffff').fontSize(11).font('Helvetica-Bold')
      .text('TOTAL', 360, 350)
      .text(`${fmt(monto)} USD`, 360, 350, { width: 180, align: 'right' });

    // PAID badge
    doc.rect(50, 340, 90, 34).fill('rgba(16,185,129,0.1)').strokeColor('#10b981').lineWidth(2).stroke();
    doc.fillColor('#10b981').fontSize(16).font('Helvetica-Bold').text('✓ PAID', 55, 350);

    // Footer
    doc.moveTo(50, 760).lineTo(545.28, 760).strokeColor('#e5e7eb').lineWidth(0.5).stroke();
    doc.fillColor(GRAY).fontSize(8).font('Helvetica')
      .text('Thank you for your business.', 50, 770, { align: 'center', width: 495.28 })
      .text(`${AGENCY.name} · ${AGENCY.address1}, ${AGENCY.address2} · ${AGENCY.email}`, 50, 782, { align: 'center', width: 495.28 });

    doc.end();
  });
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const auth = getAuth();
    const sheetsClient = google.sheets({ version: 'v4', auth });

    // ── GENERATE INVOICE ─────────────────────────────────────────
    if (req.method === 'POST' && (!req.body.mode || req.body.mode === 'generate')) {
      const { artista, vendedor, representante, precio, metodo, fecha, clienteDireccion, clientePais, clienteTaxId } = req.body;
      if (!artista || !precio) return res.status(400).json({ error: 'artista y precio requeridos' });

      await ensureSheets(sheetsClient);
      const invoiceNum = await getNextInvoiceNumber(sheetsClient);
      const d = new Date();
      const issueDate = fecha || `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}`;
      const monto = parseFloat(precio) || 0;

      // Generar PDF y subir a GCS
      const pdfBuffer = await generateInvoicePDF({ invoiceNum, issueDate, artista, clienteDireccion: clienteDireccion || '', clientePais: clientePais || '', clienteTaxId: clienteTaxId || '', monto });
      const gcsUrl = await uploadToGCS(pdfBuffer, `${invoiceNum} - ${artista}.pdf`);

      // Log en hoja Facturas (cols A:N para guardar datos completos para reenvío)
      await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${FACTURAS_SHEET}!A:N`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[invoiceNum, issueDate, artista, vendedor || '', representante || '', monto, metodo || '', 'Generada', gcsUrl, '', '', clienteDireccion || '', clientePais || '', clienteTaxId || '']] },
      });

      return res.json({ ok: true, invoiceNum, gcsUrl, artista, vendedor, representante, precio: monto, metodo, issueDate, clienteDireccion: clienteDireccion || '', clientePais: clientePais || '', clienteTaxId: clienteTaxId || '' });
    }

    // ── SEND INVOICE BY EMAIL ─────────────────────────────────────
    if (req.method === 'POST' && req.body.mode === 'send') {
      const { invoiceNum, to, artista, vendedor, representante, precio, issueDate, clienteDireccion, clientePais, clienteTaxId, gcsUrl } = req.body;
      if (!to || !to.length) return res.status(400).json({ error: 'destinatario requerido' });
      const gmailUser = process.env.GMAIL_USER;
      const gmailPass = process.env.GMAIL_PASS;
      if (!gmailUser || !gmailPass) return res.status(500).json({ error: 'GMAIL_USER y GMAIL_PASS no configurados en Vercel' });

      // Regenerar PDF (o usar GCS si está disponible)
      const monto = parseFloat(precio) || 0;
      const pdfBuffer = await generateInvoicePDF({ invoiceNum, issueDate, artista, clienteDireccion: clienteDireccion || '', clientePais: clientePais || '', clienteTaxId: clienteTaxId || '', monto });

      const transporter = nodemailer.createTransport({ service: 'gmail', auth: { user: gmailUser, pass: gmailPass } });
      const gcsLine = gcsUrl ? `<p><a href="${gcsUrl}" style="color:#6366f1">Ver factura online</a></p>` : '';

      await transporter.sendMail({
        from: `Pane Agency <${gmailUser}>`,
        to: Array.isArray(to) ? to.join(', ') : to,
        subject: `Factura ${invoiceNum} - Pane Agency`,
        html: `<div style="font-family:sans-serif;max-width:500px;margin:auto"><h2 style="color:#6366f1">Pane Agency LLC</h2><p>Hola,</p><p>Te enviamos tu factura <strong>${invoiceNum}</strong> adjunta a este email.</p>${gcsLine}<hr><p style="color:#888;font-size:12px">${AGENCY.address1}, ${AGENCY.address2} · ${AGENCY.email}</p></div>`,
        attachments: [{ filename: `${invoiceNum} - ${artista || 'Factura'}.pdf`, content: pdfBuffer, contentType: 'application/pdf' }],
      });

      // Actualizar hoja Facturas
      try {
        const resp = await sheetsClient.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${FACTURAS_SHEET}!A:A` });
        const rows = resp.data.values || [];
        const rowIdx = rows.findIndex(r => r[0] === invoiceNum);
        if (rowIdx !== -1) {
          const nd = new Date();
          const dateStr = `${String(nd.getDate()).padStart(2,'0')}/${String(nd.getMonth()+1).padStart(2,'0')}/${nd.getFullYear()}`;
          await sheetsClient.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            requestBody: { valueInputOption: 'USER_ENTERED', data: [
              { range: `${FACTURAS_SHEET}!H${rowIdx + 1}`, values: [['Enviada']] },
              { range: `${FACTURAS_SHEET}!J${rowIdx + 1}`, values: [[dateStr]] },
              { range: `${FACTURAS_SHEET}!K${rowIdx + 1}`, values: [[(Array.isArray(to) ? to : [to]).join(', ')]] },
            ]},
          });
        }
      } catch(e) {}

      return res.json({ ok: true });
    }

    // ── LIST INVOICES ─────────────────────────────────────────────
    if (req.method === 'GET') {
      try {
        const resp = await sheetsClient.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${FACTURAS_SHEET}!A:N` });
        const rows = (resp.data.values || []).slice(1);
        return res.json({ facturas: rows.map(r => ({ invoiceNum: r[0], fecha: r[1], artista: r[2], vendedor: r[3], representante: r[4], monto: r[5], metodo: r[6], estado: r[7], gcsUrl: r[8], emailEnviado: r[9], para: r[10], clienteDireccion: r[11]||'', clientePais: r[12]||'', clienteTaxId: r[13]||'' })) });
      } catch(e) { return res.json({ facturas: [] }); }
    }

    return res.status(405).json({ error: 'Método no permitido' });
  } catch(e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
};
