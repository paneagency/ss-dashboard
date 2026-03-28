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

async function appendStreamResults(sheets, rows) {
  if (!rows.length) return;
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: 'StreamsPlaylists!A:H',
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
  await sleep(6000); // más tiempo para que cargue la pantalla de magic link
  await screenshot(page, '02-after-email');

  // Diagnóstico: loguear todos los botones/links visibles
  const visibleButtons = await page.evaluate(() => {
    const els = [...document.querySelectorAll('button, a, [role="button"]')];
    return els.map(e => e.textContent?.trim()).filter(t => t && t.length < 100).slice(0, 20);
  });
  console.log('  Elementos clickeables visibles:', visibleButtons.join(' | '));

  // 3. Si Spotify muestra pantalla de código → clickear "Ingresar con contraseña"
  //    Probamos todos los textos posibles con click REAL de Playwright
  const pwLinkTexts = [
    'contraseña', 'password', 'senha', 'mot de passe',
    'ingresar con', 'log in with', 'sign in with', 'usar contraseña',
  ];
  let clickedPwLink = false;
  for (const txt of pwLinkTexts) {
    const el = page.locator(`button:has-text("${txt}"), a:has-text("${txt}"), [role="button"]:has-text("${txt}")`).first();
    const visible = await el.isVisible({ timeout: 1000 }).catch(() => false);
    if (visible) {
      const fullText = await el.textContent().catch(() => txt);
      console.log(`  → Clickeando botón contraseña: "${fullText?.trim()}"`);
      await el.click();
      clickedPwLink = true;
      await sleep(4000);
      await screenshot(page, '03-after-password-link');
      break;
    }
  }
  if (!clickedPwLink) {
    console.log('  → No se encontró link de contraseña (puede que el campo ya esté visible)');
  }

  // 4. Ingresar contraseña — esperar que el campo aparezca
  const passwordInput = page.locator(
    'input[data-testid="login-password"], input[name="password"], #login-password, input[type="password"]'
  ).first();

  const pwVisible = await passwordInput.isVisible({ timeout: 10000 }).catch(() => false);
  if (!pwVisible) {
    await screenshot(page, '04-no-password-input');
    const url = page.url();
    console.log(`  URL actual: ${url}`);
    // Log adicional de diagnóstico
    const pageText = await page.evaluate(() => document.body.innerText?.slice(0, 500));
    console.log('  Texto en pantalla:', pageText?.replace(/\n+/g, ' '));
    if (new URL(url).hostname === 'artists.spotify.com') {
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

// ── Parser de texto de Spotify for Artists (tab "Playlists") ──
// Formato real del innerText:
//   "1\t\nCUMBIA ARGENTINA 2026\n—\t36,881\t20 may 2022\n"
//   "18\t\nMixes\n\nSpotify\t301\t—\n"
function parsePlaylistsText(rawText) {
  const playlists = [];
  const lines = rawText.split('\n').map(l => l.trim());

  for (let i = 0; i < lines.length; i++) {
    // Detectar línea de posición: número solo (con o sin tab al final)
    const posMatch = lines[i].match(/^(\d+)\t?$/);
    if (!posMatch) continue;
    const position = parseInt(posMatch[1]);

    // Siguiente línea no vacía y no especial = nombre de playlist
    let name = '';
    for (let j = i + 1; j < Math.min(i + 5, lines.length); j++) {
      const l = lines[j];
      if (l && l !== '—' && !/^Spotify$/.test(l) && !/^\d+\t?$/.test(l)) {
        name = l.replace(/\t.*$/, '').trim();
        break;
      }
    }
    if (!name) continue;

    // Buscar streams y fecha: línea con patrón "—\t{número}\t{fecha}" o "Spotify\t{número}\t..."
    let streams = null;
    let fecha = '';
    for (let k = i + 1; k < Math.min(i + 7, lines.length); k++) {
      const m = lines[k].match(/(?:—|Spotify)\t([\d,.]+)\t?(.*)?/);
      if (m) {
        streams = parseInt(m[1].replace(/[,.]/g, ''));
        fecha = (m[2] || '').replace('—', '').trim();
        break;
      }
      // Fallback: línea que es solo un número con comas (ej: "36,881")
      const numOnly = lines[k].match(/^([\d,.]+)$/);
      if (numOnly && parseInt(numOnly[1].replace(/[,.]/g, '')) > 0) {
        streams = parseInt(numOnly[1].replace(/[,.]/g, ''));
        break;
      }
    }

    if (streams !== null && streams > 0) {
      playlists.push({ position, name, streams, fecha });
    }
  }

  return playlists;
}

// ── Navegar directo a la vista de Playlists del track ─────────
// URL formato: artists.spotify.com/c/es-419/artist/{artistId}/song/{trackId}/playlists
async function navigateToTrackPlaylists(page, ref) {
  const playlistsUrl = `https://artists.spotify.com/c/es-419/artist/${ref.artistId}/song/${ref.trackId}/playlists`;
  console.log(`  → Navegando a: ${playlistsUrl}`);
  await page.goto(playlistsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await sleep(5000);
  await screenshot(page, `04-playlists-${ref.trackId}`);

  const currentUrl = page.url();
  console.log(`  URL actual: ${currentUrl}`);

  // Si redirigió a login, la sesión expiró
  if (new URL(currentUrl).hostname !== 'artists.spotify.com' || currentUrl.includes('/login')) {
    console.log('  ❌ Redirigido a login — sesión expirada');
    return false;
  }

  // Esperar a que cargue el contenido de playlists
  await sleep(3000);
  await screenshot(page, `05-playlists-loaded-${ref.trackId}`);
  return true;
}

// ── Scrape track sources ──────────────────────────────────────
// Devuelve array de filas [fecha, artista, trackName, playlistName, posicion, streams, fecha_added]
// Una fila por cada playlist encontrada en la página
async function scrapeTrackSources(page, ref, today) {
  console.log(`\n🎵 ${ref.artista} — "${ref.trackName}"`);

  const currentHost = (() => { try { return new URL(page.url()).hostname; } catch { return ''; } })();
  if (currentHost !== 'artists.spotify.com') {
    console.log('  ❌ No en artists.spotify.com — sesión expirada');
    return [];
  }

  const ok = await navigateToTrackPlaylists(page, ref);
  if (!ok) return [];

  // Extraer todo el texto de la página
  const rawText = await page.evaluate(() => {
    const el = document.querySelector('main') || document.querySelector('[role="main"]') || document.body;
    return el?.innerText || document.body.innerText;
  });

  const allPlaylists = parsePlaylistsText(rawText);
  console.log(`  📋 ${allPlaylists.length} playlists encontradas`);

  if (!allPlaylists.length) {
    await screenshot(page, `nodata-${ref.trackId}`);
    console.log('  Primeras líneas (diagnóstico):');
    rawText.split('\n').slice(0, 30).forEach(l => l.trim() && console.log('    |' + l));
    return [];
  }

  // Una fila por playlist: Fecha | Artista | Track | Playlist | Posición | Streams | FechaAgregada
  const rows = allPlaylists.map(p => [
    today,
    ref.artista,
    ref.trackName,
    p.name,
    p.position,
    p.streams,
    p.fecha || '',
  ]);

  console.log(`  → ${rows.length} filas a guardar`);
  rows.slice(0, 5).forEach(r => console.log(`     ${r[4]}. ${r[3]} — ${r[5].toLocaleString()} streams`));
  if (rows.length > 5) console.log(`     ... y ${rows.length - 5} más`);

  return rows;
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

  try {
    // Verificar si la sesión guardada sigue válida navegando a una ruta protegida
    let loggedIn = false;
    if (sessionExists) {
      console.log('🔄 Intentando sesión guardada...');
      await page.goto('https://artists.spotify.com/c/es-419/home', { waitUntil: 'domcontentloaded', timeout: 20000 });
      await sleep(4000);
      const currentUrl = page.url();
      console.log(`  URL post-sesión: ${currentUrl}`);
      loggedIn = new URL(currentUrl).hostname === 'artists.spotify.com' &&
                 !currentUrl.includes('/login') &&
                 !currentUrl.includes('accounts.spotify.com');
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

    const today = new Date().toISOString().slice(0, 10);
    const allRows = [];

    for (const ref of referencias) {
      try {
        const rows = await scrapeTrackSources(page, ref, today);
        for (const row of rows) allRows.push(row);
      } catch(e) {
        console.log(`  ❌ Error en "${ref.trackName}": ${e.message}`);
        await screenshot(page, `error-${ref.trackId}`);
      }
      await sleep(2000);
    }

    // Guardar todas las filas
    if (allRows.length) {
      await appendStreamResults(sheets, allRows);
    } else {
      console.log('\n⚠️  No se obtuvieron resultados.');
    }

    console.log(`\n✅ Total: ${allRows.length} filas guardadas en StreamsPlaylists`);

  } finally {
    await browser.close();
  }
}

main().catch(e => {
  console.error('❌ Error fatal:', e.message);
  process.exit(1);
});
