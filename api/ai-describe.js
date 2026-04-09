export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { name, mode, artist, track } = req.body || {};

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'ANTHROPIC_API_KEY no configurada' });

  try {
    let prompt, maxTokens;

    if (mode === 'genre') {
      if (!artist?.trim()) return res.status(400).json({ error: 'artist requerido' });
      prompt = `Sos un experto en música latinoamericana y campañas de Spotify. Analizá este track:
Artista: "${artist.trim()}"${track ? `\nCanción: "${track.trim()}"` : ''}

Respondé en este formato exacto (sin texto extra):
GENEROS: [géneros separados por coma, minúsculas, máximo 5]
RECOMENDACION: [1 oración sobre en qué tipo de playlists o campaña encajaría esta canción]`;
      maxTokens = 150;
    } else if (mode === 'recommend') {
      const context = req.body.context || '';
      if (!context.trim()) return res.status(400).json({ error: 'context requerido' });
      prompt = `Sos experto en curación de playlists de Spotify para el mercado latinoamericano. Con estos datos del track:

${context.trim()}

Respondé en este formato exacto (una sola línea, sin explicaciones adicionales):
Géneros: [2-3 géneros específicos] · Playlists: [3-4 tipos de playlist reales de Spotify, separados por coma]`;
      maxTokens = 100;
    } else {
      if (!name?.trim()) return res.status(400).json({ error: 'name requerido' });
      prompt = `Sos un experto en SEO musical y posicionamiento en plataformas de streaming. Creá una descripción para una playlist de Spotify llamada "${name.trim()}", orientada a búsquedas en 2026. Usá palabras clave de alto volumen relacionadas con el género, mood y ocasión que sugiere el nombre. Máximo 200 caracteres, sin emojis, en español neutro. Solo dame el resultado final, sin explicaciones ni comillas.`;
      maxTokens = 120;
    }

    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: (mode === 'genre' || mode === 'recommend') ? 'claude-sonnet-4-6' : 'claude-haiku-4-5-20251001',
        max_tokens: maxTokens,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message || `API error ${r.status}`);

    const text = d.content?.[0]?.text?.trim() || '';

    if (mode === 'recommend') {
      return res.json({ ok: true, recommendation: text });
    }

    if (mode === 'genre') {
      let genres = [], recommendation = '';
      const genresMatch = text.match(/GENEROS:\s*(.+)/i);
      const recMatch = text.match(/RECOMENDACION:\s*(.+)/i);
      if (genresMatch) {
        genres = genresMatch[1].split(',').map(g => g.trim().toLowerCase()).filter(g => g.length > 0 && g.length < 40);
      } else {
        // fallback: todo el texto como géneros
        genres = text.split(',').map(g => g.trim().toLowerCase()).filter(g => g.length > 0 && g.length < 40 && !g.includes('?'));
      }
      if (recMatch) recommendation = recMatch[1].trim();
      return res.json({ ok: true, genres, recommendation, fromAI: true });
    }

    return res.json({ ok: true, description: text });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
