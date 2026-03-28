/**
 * Spotify for Artists Scraper
 * ───────────────────────────
 * Lee ReferenciasTracks del spreadsheet, para cada canción de referencia
 * extrae cuántos streams generó cada playlist en los últimos 28 días,
 * y guarda los resultados en StreamsPlaylists.
 *
 * Variables de entorno requeridas:
 *   SPOTIFY_EMAIL             — email de la cuenta Spotify for Artists
 *   SPOTIFY_PASSWORD          — contraseña
 *   GOOGLE_SERVICE_ACCOUNT    — JSON completo de la service account (string)
 *
 * Hoja "ReferenciasTracks" (llenar manualmente):
 *   A=Artista | B=TrackNombre | C=ArtistSpotifyId | D=TrackSpotifyId | E=PlaylistNombre | F=Posicion
 *   (los IDs se sacan de la URL de Spotify, ej: open.spotify.com/track/4cluDES4hQ...)
 *
 * Hoja "StreamsPlaylists" (el script escribe acá):
 *   A=Fecha | B=Artista | C=TrackNombre | D=PlaylistNombre | E=Posicion | F=Streams28d | G=FuenteCompleta
 */

const { chromium } = require('playwright');
const { google }   = require('googleapis');
const path         = require('path');
const fs           = require('fs');

// ── Config ────────────────────────────────────────────────────
const SPREADSHEET_ID = '15N3dznVTgTx2C1CPlSas-NKWeYK4WcaikmEoh_frO0k';
const EMAIL          = process.env.SPOTIFY_EMAIL;
const PASSWORD       = process.env.SPOTIFY_PASSWORD;
const HEADLESS       = process.env.DEBUG !== 'true';
const SCREENSHOT_DIR = path.join(__dirname, 'screenshots');
const SESSION_FILE   = path.join(__dirname, 'session.json');

// ── Google Sheets ─────────────────────────────────────────────
function getSheets() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT;
  if (!raw) throw new Error('GOOGLE_SERVICE_ACCOUNT no configurado');
  const creds = typeof raw === 'string' ? JSON.parse(raw) : raw;
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function readReferenciasTracks(sheets) {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'ReferenciasTracks!A:F',
  });
  const rows = (resp.data.values || []).slice(1); // skip header
  return rows
    .map(r => ({
      artista:   (r[0] || '').trim(),
      trackName: (r[1] || '').trim(),
      artistId:  (r[2] || '').trim(),
      trackId:   (r[3] || '').trim(),
      playlist:  (r[4] || '').trim(),
      posicion:  parseInt(r[5]) || 0,
    }))
    .filter(r => r.artistId && r.trackId && r.artista);
}

async function appendStreamResults(sheets, results) {
  if (!results.length) return;
  const today = new Date().toISOString().slice(0, 10);
  const rows = results.map(r => [
    today,
    r.artista,
    r.trackName,
    r.playlist,
    r.posicion,
    r.streams28d ?? '',
    r.fuenteCompleta ?? '',
  ]);
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: 'StreamsPlaylists!A:G',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    resource: { values: rows },
  });
  console.log(`✅ Guardadas ${rows.length} filas en StreamsPlaylists`);
}

// ── Helpers ───────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function screenshot(page, name) {
  if (!fs.existsSync(SCREENSHOT_DIR)) fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });
  const file = path.join(SCREENSHOT_DIR, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  console.log(`  📸 Screenshot: ${name}.png`);
}

function parseStreams(text) {
  if (!text) return null;
  // "1.234" → 1234 | "15,678" → 15678 | "847" → 847
  const clean = text.replace(/[^\d.,]/g, '').replace(',', '').replace('.', '');
  const n = parseInt(clean);
  return isNaN(n) ? null : n;
}

// ── Login ─────────────────────────────────────────────────────
async function login(page) {
  console.log('🔑 Iniciando login...');

  await page.goto(
    'https://accounts.spotify.com/en/login?continue=https%3A%2F%2Fartists.spotify.com%2F',
    { waitUntil: 'domcontentloaded', timeout: 30000 }
  );
  await sleep(3000);

  // 1. Ingresar email
  const emailInput = page.locator(
    'input[data-testid="login-username"], input[name="username"], #login-username, input[autocomplete="username"]'
  ).first();
  await emailInput.waitFor({ state: 'visible', timeout: 15000 });
  await emailInput.fill(EMAIL);
  await sleep(800);
  await screenshot(page, '01-email-filled');

  // 2. Click en Continuar / Next
  const continueBtn = page.locator(
    'button[data-testid="login-button"], button[id="login-button"], button[type="submit"]'
  ).first();
  await continueBtn.click();
  await sleep(4000);
  await screenshot(page, '02-after-email');

  // 3. Si Spotify muestra "Enviamos un link" → clickear "Ingresar con contraseña"
  //    Probamos múltiples variantes del texto (ES/EN/PT)
  const passwordLinkSelectors = [
    'a:has-text("password")',
    'button:has-text("password")',
    'a:has-text("contraseña")',
    'button:has-text("contraseña")',
    'a:has-text("senha")',
    '[data-testid*="password"]',
    'a[href*="password"]',
    'span:has-text("password")',
  ];
  for (const sel of passwordLinkSelectors) {
    const el = page.locator(sel).first();
    const visible = await el.isVisible({ timeout: 1500 }).catch(() => false);
    if (visible) {
      console.log(`  → Clickeando "Log in with password" (${sel})...`);
      await el.click();
      await sleep(3000);
      await screenshot(page, '03-after-password-link');
      break;
    }
  }

  // 4. Ingresar contraseña — esperar que el campo aparezca
  const passwordInput = page.locator(
    'input[data-testid="login-password"], input[name="password"], #login-password, input[type="password"]'
  ).first();

  const pwVisible = await passwordInput.isVisible({ timeout: 8000 }).catch(() => false);
  if (!pwVisible) {
    await screenshot(page, '04-no-password-input');
    // Puede que ya estemos logueados o en una pantalla inesperada
    const url = page.url();
    console.log(`  URL actual: ${url}`);
    if (url.includes('artists.spotify.com')) {
      console.log('✅ Ya en artists.spotify.com (sin necesitar contraseña)');
      return;
    }
    throw new Error('No apareció el campo de contraseña — revisar screenshot 04-no-password-input.png');
  }

  await passwordInput.fill(PASSWORD);
  await sleep(800);
  await screenshot(page, '04-password-filled');

  // 5. Submit — intentar click y también Enter como fallback
  const loginBtn = page.locator(
    'button[data-testid="login-button"], button[id="login-button"], button[type="submit"]'
  ).first();
  const btnVisible = await loginBtn.isVisible({ timeout: 3000 }).catch(() => false);
  if (btnVisible) {
    await loginBtn.click();
  } else {
    await passwordInput.press('Enter');
  }
  await screenshot(page, '05-after-submit');

  // 6. Esperar redirect REAL a artists.spotify.com
  // (no alcanza con regex — la URL de accounts.spotify.com contiene "artists.spotify.com" en el query param)
  try {
    await page.waitForURL(
      url => { try { return new URL(url).hostname === 'artists.spotify.com'; } catch { return false; } },
      { timeout: 35000 }
    );
    console.log('✅ Login exitoso →', page.url());
  } catch(e) {
    await screenshot(page, '06-login-error');
    console.log('  URL actual al fallar:', page.url());
    throw new Error('Login falló — revisar screenshots en artifacts de GitHub Actions');
  }
}

// ── Scrape track sources ──────────────────────────────────────
async function scrapeTrackSources(page, ref) {
  console.log(`\n🎵 ${ref.artista} — "${ref.trackName}"`);
  console.log(`   Playlist objetivo: "${ref.playlist}" (pos. ${ref.posicion})`);

  // Navegar a la página de canciones del artista
  const songsUrl = `https://artists.spotify.com/c/artist/${ref.artistId}/music/songs`;
  await page.goto(songsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await sleep(3000);
  await screenshot(page, `03-songs-${ref.artistId}`);

  // Intentar ir directo al track
  // Spotify for Artists puede tener URL directa al track
  const trackUrl = `https://artists.spotify.com/c/artist/${ref.artistId}/music/songs/${ref.trackId}`;
  await page.goto(trackUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await sleep(3000);

  // Si no navegó al track, buscar la canción en la lista y clickear
  const currentUrl = page.url();
  if (!currentUrl.includes(ref.trackId)) {
    console.log('  → URL directa no funcionó, buscando en lista...');
    await page.goto(songsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await sleep(3000);

    // Buscar la fila de la canción por nombre
    const trackRow = page.locator(`tr:has-text("${ref.trackName}"), li:has-text("${ref.trackName}")`).first();
    const found = await trackRow.isVisible({ timeout: 8000 }).catch(() => false);
    if (found) {
      await trackRow.click();
      await sleep(3000);
    } else {
      console.log(`  ⚠️  No se encontró "${ref.trackName}" en la lista`);
      await screenshot(page, `04-track-notfound-${ref.trackId}`);
      return null;
    }
  }

  await screenshot(page, `04-track-${ref.trackId}`);

  // Buscar sección de Sources / Fuentes
  // Intentar selector del período 28 días
  await setTimePeriod28d(page);
  await sleep(2000);
  await screenshot(page, `05-sources-${ref.trackId}`);

  // Extraer la tabla de fuentes
  return await extractSourceData(page, ref);
}

async function setTimePeriod28d(page) {
  // Buscar un selector de período de tiempo y poner 28 días
  const selectors = [
    'button:has-text("28")',
    '[data-testid*="timerange"]:has-text("28")',
    'select option[value*="28"]',
    'li:has-text("28 days")',
    'li:has-text("28 días")',
    'button:has-text("Last 28")',
    'button:has-text("Últimos 28")',
  ];
  for (const sel of selectors) {
    const el = page.locator(sel).first();
    const visible = await el.isVisible({ timeout: 1500 }).catch(() => false);
    if (visible) {
      await el.click();
      console.log('  → Período 28d seleccionado');
      return;
    }
  }
  console.log('  ℹ️  No se encontró selector de período (usando default)');
}

async function extractSourceData(page, ref) {
  // Buscar tabla/lista de fuentes de streams
  // Spotify for Artists muestra una sección con el desglose por fuente
  await page.waitForTimeout(2000);

  // Intentar encontrar la sección de fuentes / sources
  const sourcesHeading = page.locator([
    'h2:has-text("Sources")',
    'h2:has-text("Fuentes")',
    '[data-testid*="sources"]',
    'section:has-text("Playlists")',
    'div:has-text("Spotify Playlists")',
  ].join(', ')).first();

  const headingVisible = await sourcesHeading.isVisible({ timeout: 5000 }).catch(() => false);
  if (!headingVisible) {
    console.log('  ⚠️  No se encontró sección de fuentes');
    await screenshot(page, `06-nosources-${ref.trackId}`);
    // Intentar extraer de toda la página de todas formas
  }

  // Estrategia 1: buscar la playlist por nombre en la página completa
  // y extraer el número de streams cercano
  const pageContent = await page.content();
  const streams28d = await findPlaylistStreamsInPage(page, ref.playlist);

  if (streams28d !== null) {
    console.log(`  ✅ "${ref.playlist}": ${streams28d.toLocaleString()} streams (28d)`);
    return {
      ...ref,
      streams28d,
      fuenteCompleta: await getAllSourcesText(page),
    };
  }

  // Estrategia 2: volcar todas las fuentes y hacer matching flexible
  const allSources = await extractAllSources(page);
  console.log(`  📋 Fuentes encontradas: ${allSources.map(s => s.name).join(', ') || 'ninguna'}`);

  const match = allSources.find(s =>
    s.name.toLowerCase().includes(ref.playlist.toLowerCase()) ||
    ref.playlist.toLowerCase().includes(s.name.toLowerCase().split(' ').slice(0, 3).join(' '))
  );

  if (match) {
    console.log(`  ✅ Match: "${match.name}" → ${match.streams?.toLocaleString()} streams`);
    return {
      ...ref,
      streams28d: match.streams,
      fuenteCompleta: allSources.map(s => `${s.name}: ${s.streams}`).join(' | '),
    };
  }

  console.log(`  ⚠️  Playlist "${ref.playlist}" no encontrada en fuentes`);
  await screenshot(page, `06-nomatch-${ref.trackId}`);
  return {
    ...ref,
    streams28d: null,
    fuenteCompleta: allSources.map(s => `${s.name}: ${s.streams}`).join(' | ') || 'sin datos',
  };
}

async function findPlaylistStreamsInPage(page, playlistName) {
  try {
    // Buscar el texto de la playlist en la página
    const el = page.locator(`*:has-text("${playlistName}")`).last();
    const visible = await el.isVisible({ timeout: 3000 }).catch(() => false);
    if (!visible) return null;

    // Buscar el número cercano a ese elemento
    const parent = el.locator('..');
    const text = await parent.textContent({ timeout: 2000 }).catch(() => '');
    const nums = text.match(/[\d.,]+/g);
    if (nums?.length) return parseStreams(nums[nums.length - 1]);
    return null;
  } catch { return null; }
}

async function extractAllSources(page) {
  const sources = [];
  try {
    // Buscar todas las filas de la tabla de fuentes
    // Los selectores varían por versión de Spotify for Artists
    const rows = await page.locator([
      'table tr',
      '[data-testid*="row"]',
      'li[class*="source"]',
      'div[class*="source"]',
    ].join(', ')).all();

    for (const row of rows.slice(0, 30)) { // máximo 30 fuentes
      const text = await row.textContent().catch(() => '');
      if (!text?.trim()) continue;
      // Intentar extraer nombre y número
      const parts = text.trim().split(/\s{2,}|\t/);
      if (parts.length >= 2) {
        const name = parts[0].trim();
        const numStr = parts.find(p => /^[\d.,]+$/.test(p.trim()));
        const streams = numStr ? parseStreams(numStr) : null;
        if (name && streams !== null) sources.push({ name, streams });
      }
    }
  } catch(e) {
    console.log('  ℹ️  Error extrayendo fuentes:', e.message);
  }
  return sources;
}

async function getAllSourcesText(page) {
  try {
    const sources = await extractAllSources(page);
    return sources.map(s => `${s.name}: ${s.streams}`).join(' | ');
  } catch { return ''; }
}

// ── Main ──────────────────────────────────────────────────────
async function main() {
  if (!EMAIL || !PASSWORD) {
    console.error('❌ SPOTIFY_EMAIL y SPOTIFY_PASSWORD son requeridos');
    process.exit(1);
  }

  console.log('📊 Spotify for Artists Scraper');
  console.log('================================');

  // Leer configuración desde Sheets
  const sheets = getSheets();
  const referencias = await readReferenciasTracks(sheets);

  if (!referencias.length) {
    console.log('⚠️  No hay filas en ReferenciasTracks (o falta el header)');
    console.log('   Formato: Artista | TrackNombre | ArtistSpotifyId | TrackSpotifyId | PlaylistNombre | Posicion');
    process.exit(0);
  }

  console.log(`📋 ${referencias.length} tracks de referencia cargados`);

  // Lanzar browser
  const browser = await chromium.launch({
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });

  // Cargar sesión guardada si existe (evita login y email de seguridad)
  const sessionExists = fs.existsSync(SESSION_FILE);
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    viewport: { width: 1280, height: 800 },
    locale: 'es-AR',
    ...(sessionExists ? { storageState: SESSION_FILE } : {}),
  });

  const page = await context.newPage();
  const results = [];

  try {
    // Verificar si la sesión guardada sigue válida
    let loggedIn = false;
    if (sessionExists) {
      console.log('🔄 Intentando sesión guardada...');
      await page.goto('https://artists.spotify.com/', { waitUntil: 'domcontentloaded', timeout: 20000 });
      await sleep(3000);
      loggedIn = new URL(page.url()).hostname === 'artists.spotify.com';
      if (loggedIn) {
        console.log('✅ Sesión reutilizada — sin nuevo login');
      } else {
        console.log('  Sesión expirada, haciendo login nuevo...');
      }
    }

    if (!loggedIn) {
      await login(page);
      // Guardar sesión para el próximo run
      await context.storageState({ path: SESSION_FILE });
      console.log('💾 Sesión guardada para el próximo run');
    }

    // Esperar a que el dashboard cargue completamente
    await sleep(3000);
    await screenshot(page, '00-dashboard');

    // Agrupar referencias por artista para evitar relogins
    const byArtist = {};
    for (const ref of referencias) {
      if (!byArtist[ref.artistId]) byArtist[ref.artistId] = [];
      byArtist[ref.artistId].push(ref);
    }

    for (const [artistId, refs] of Object.entries(byArtist)) {
      for (const ref of refs) {
        try {
          const result = await scrapeTrackSources(page, ref);
          if (result) results.push(result);
        } catch(e) {
          console.log(`  ❌ Error en "${ref.trackName}": ${e.message}`);
          await screenshot(page, `error-${ref.trackId}`);
        }
        await sleep(2000); // pausa entre tracks
      }
    }

    // Guardar resultados
    if (results.length) {
      await appendStreamResults(sheets, results);
    } else {
      console.log('\n⚠️  No se obtuvieron resultados. Revisar screenshots en scraper/screenshots/');
    }

    // Resumen
    console.log('\n📊 Resumen:');
    for (const r of results) {
      const streams = r.streams28d !== null ? r.streams28d.toLocaleString() + ' streams' : 'sin datos';
      console.log(`  ${r.artista} — "${r.playlist}" pos.${r.posicion}: ${streams}`);
    }

  } finally {
    await browser.close();
  }
}

main().catch(e => {
  console.error('❌ Error fatal:', e.message);
  process.exit(1);
});
