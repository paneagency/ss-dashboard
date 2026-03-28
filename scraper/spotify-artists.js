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

    // Buscar streams: línea con patrón "—\t{número}" o "Spotify\t{número}"
    let streams = null;
    for (let k = i + 1; k < Math.min(i + 7, lines.length); k++) {
      // Formato con tabs: "—\t36,881\t..." o "Spotify\t301\t—"
      const m = lines[k].match(/(?:—|Spotify)\t([\d,.]+)/);
      if (m) {
        streams = parseInt(m[1].replace(/[,.]/g, ''));
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
      playlists.push({ position, name, streams });
    }
  }

  return playlists;
}

// ── Navegar al detalle del track y abrir tab Playlists ────────
async function navigateToTrackPlaylists(page, ref) {
  // Paso 1: ir a la lista de canciones del artista
  const songsUrl = `https://artists.spotify.com/c/artist/${ref.artistId}/music/songs`;
  await page.goto(songsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await sleep(4000);
  await screenshot(page, `03-songslist-${ref.trackId}`);
  console.log(`  URL songs list: ${page.url()}`);

  // Paso 2: clickear la canción por nombre en la lista
  // Spotify for Artists muestra las canciones en filas clickeables
  const trackRow = page.locator([
    `a:has-text("${ref.trackName}")`,
    `button:has-text("${ref.trackName}")`,
    `tr:has-text("${ref.trackName}") td:first-child`,
    `[data-testid*="track"]:has-text("${ref.trackName}")`,
    `li:has-text("${ref.trackName}")`,
  ].join(', ')).first();

  const rowFound = await trackRow.isVisible({ timeout: 8000 }).catch(() => false);
  if (rowFound) {
    console.log(`  → Clickeando "${ref.trackName}" en la lista`);
    await trackRow.click();
    await sleep(4000);
    await screenshot(page, `03b-trackclick-${ref.trackId}`);
    console.log(`  URL post-click: ${page.url()}`);
  } else {
    // Fallback: intentar URL directa igual
    console.log(`  → Fila no encontrada en lista, intentando URL directa`);
    const trackUrl = `https://artists.spotify.com/c/artist/${ref.artistId}/music/songs/${ref.trackId}`;
    await page.goto(trackUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await sleep(4000);
    console.log(`  URL directa: ${page.url()}`);
  }

  await screenshot(page, `04-trackpage-${ref.trackId}`);

  // Diagnóstico: loguear todos los tabs/botones visibles para debugging
  const allTabs = await page.evaluate(() => {
    const tabs = [...document.querySelectorAll('[role="tab"], [data-testid*="tab"], nav a, nav button')];
    return tabs.map(t => t.textContent?.trim()).filter(Boolean).slice(0, 10);
  });
  console.log(`  Tabs/nav disponibles: ${allTabs.join(' | ') || 'ninguno'}`);

  // Paso 3: clickear tab "Playlists"
  // Spotify for Artists puede usar distintas estructuras según la versión
  const playlistsTab = page.locator([
    '[role="tab"]:has-text("Playlists")',
    'button:has-text("Playlists")',
    'a:has-text("Playlists")',
    'li:has-text("Playlists")',
    'span:has-text("Playlists")',
    '[data-testid*="playlist"]:not([data-testid*="item"])',
  ].join(', ')).first();

  const tabFound = await playlistsTab.isVisible({ timeout: 10000 }).catch(() => false);
  if (!tabFound) {
    console.log('  ⚠️  Tab "Playlists" no encontrado — revisá screenshot 04-trackpage');
    return false;
  }

  await playlistsTab.click();
  console.log('  → Tab Playlists clickeado');
  await sleep(5000);
  await screenshot(page, `05-playlists-loaded-${ref.trackId}`);
  return true;
}

// ── Scrape track sources ──────────────────────────────────────
async function scrapeTrackSources(page, ref) {
  console.log(`\n🎵 ${ref.artista} — "${ref.trackName}"`);

  if (!page.url().includes('artists.spotify.com')) {
    return { ...ref, streams28d: null, fuenteCompleta: 'sesión expirada' };
  }

  // Navegar al track y abrir tab Playlists
  const tabOpened = await navigateToTrackPlaylists(page, ref);
  if (!tabOpened) {
    return { ...ref, streams28d: null, fuenteCompleta: 'tab Playlists no encontrado' };
  }

  // Extraer innerText del área principal
  const rawText = await page.evaluate(() => {
    const candidates = [
      document.querySelector('main'),
      document.querySelector('[role="main"]'),
      document.body,
    ];
    for (const el of candidates) {
      if (el?.innerText?.length > 200) return el.innerText;
    }
    return document.body.innerText;
  });

  // Parsear todas las playlists
  const allPlaylists = parsePlaylistsText(rawText);
  console.log(`  📋 ${allPlaylists.length} playlists encontradas`);

  if (!allPlaylists.length) {
    await screenshot(page, `06-nodata-${ref.trackId}`);
    console.log('  Primeras líneas del texto (diagnóstico):');
    rawText.split('\n').slice(0, 25).forEach(l => l.trim() && console.log('    |' + l));
  }

  // Buscar la playlist objetivo
  const target = ref.playlist.toLowerCase();
  const match = allPlaylists.find(p =>
    p.name.toLowerCase() === target ||
    p.name.toLowerCase().includes(target) ||
    target.includes(p.name.toLowerCase().split(' ').slice(0, 4).join(' '))
  );

  if (match) {
    console.log(`  ✅ "${match.name}" pos.${match.position}: ${match.streams.toLocaleString()} streams`);
  } else {
    const top5 = allPlaylists.slice(0, 5).map(p => `${p.position}. ${p.name}`).join(' | ');
    console.log(`  ℹ️  "${ref.playlist}" no encontrada. Top 5: ${top5 || 'ninguna'}`);
  }

  return {
    ...ref,
    streams28d: match?.streams ?? null,
    fuenteCompleta: allPlaylists.slice(0, 30).map(p => `${p.position}|${p.name}|${p.streams}`).join('; '),
  };
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
