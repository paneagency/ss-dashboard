export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { name } = req.body || {};
  if (!name?.trim()) return res.status(400).json({ error: 'name requerido' });

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'ANTHROPIC_API_KEY no configurada' });

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 120,
        messages: [{
          role: 'user',
          content: `Sos un experto en SEO musical y posicionamiento en plataformas de streaming. Creá una descripción para una playlist de Spotify llamada "${name.trim()}", orientada a búsquedas en 2026. Usá palabras clave de alto volumen relacionadas con el género, mood y ocasión que sugiere el nombre. Máximo 200 caracteres, sin emojis, en español neutro. Solo dame el resultado final, sin explicaciones ni comillas.`,
        }],
      }),
    });

    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message || `API error ${r.status}`);

    const description = d.content?.[0]?.text?.trim() || '';
    return res.json({ ok: true, description });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
