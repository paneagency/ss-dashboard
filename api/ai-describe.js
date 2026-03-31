export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { name, mode, artist, track } = req.body || {};

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'ANTHROPIC_API_KEY no configurada' });

  try {
    let prompt, maxTokens;

    if (mode === 'genre') {
      if (!artist?.trim()) return res.status(400).json({ error: 'artist requerido' });
      prompt = `Clasificá el género musical del artista "${artist.trim()}"${track ? ` (canción: "${track.trim()}")` : ''}. Respondé ÚNICAMENTE con géneros separados por coma, en minúsculas, sin ninguna explicación ni pregunta. Si hay ambigüedad, hacé tu mejor estimación. Máximo 5 géneros cortos. Ejemplo de respuesta válida: cumbia, pop latino, tropical`;
      maxTokens = 60;
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
        model: mode === 'genre' ? 'claude-sonnet-4-6' : 'claude-haiku-4-5-20251001',
        max_tokens: maxTokens,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message || `API error ${r.status}`);

    const text = d.content?.[0]?.text?.trim() || '';

    if (mode === 'genre') {
      const genres = text.split(',').map(g => g.trim().toLowerCase()).filter(g => g.length > 0 && g.length < 40 && !g.includes('?') && !g.includes('necesit'));
      return res.json({ ok: true, genres, fromAI: true });
    }

    return res.json({ ok: true, description: text });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
