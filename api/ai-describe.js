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
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 120,
        messages: [{
          role: 'user',
          content: `Escribí una descripción para una playlist de Spotify llamada "${name.trim()}".

Requisitos:
- Máximo 180 caracteres
- En español
- Orientada a SEO: incluí palabras clave naturales relacionadas al género/mood del nombre
- Tono atractivo, que invite a escuchar
- Sin comillas, sin hashtags, sin emojis
- Solo el texto de la descripción, nada más`,
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
