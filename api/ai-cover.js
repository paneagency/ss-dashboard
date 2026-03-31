export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { name } = req.body || {};
  if (!name?.trim()) return res.status(400).json({ error: 'name requerido' });

  const apiKey = process.env.GOOGLE_AI_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'GOOGLE_AI_API_KEY no configurada' });

  try {
    const prompt = `Spotify playlist cover art for a music playlist called "${name.trim()}". Vibrant colors, artistic, abstract, no text, no letters, no words, square format, music themed, high quality digital illustration.`;

    const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/imagen-4.0-fast-generate-001:predict?key=${apiKey}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        instances: [{ prompt }],
        parameters: { sampleCount: 1, aspectRatio: '1:1', personGeneration: 'dont_allow' },
      }),
    });

    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message || `API error ${r.status}`);

    const b64 = d.predictions?.[0]?.bytesBase64Encoded;
    const mimeType = d.predictions?.[0]?.mimeType || 'image/png';
    if (!b64) throw new Error('No image generated');

    return res.json({ ok: true, imageData: `data:${mimeType};base64,${b64}` });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
