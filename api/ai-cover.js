export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { name } = req.body || {};
  if (!name?.trim()) return res.status(400).json({ error: 'name requerido' });

  const apiKey = process.env.GOOGLE_AI_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'GOOGLE_AI_API_KEY no configurada' });

  try {
    const prompt = `Spotify playlist cover art for a music playlist called "${name.trim()}". Vibrant colors, artistic, abstract, no text, no letters, no words, square format, music themed, high quality digital illustration.`;

    const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent?key=${apiKey}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { responseModalities: ['IMAGE', 'TEXT'] },
      }),
    });

    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message || `API error ${r.status}`);

    const parts = d.candidates?.[0]?.content?.parts || [];
    const imgPart = parts.find(p => p.inlineData?.data);
    if (!imgPart) throw new Error('No image generated');

    const { mimeType, data } = imgPart.inlineData;
    return res.json({ ok: true, imageData: `data:${mimeType};base64,${data}` });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
