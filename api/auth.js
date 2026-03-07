module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  try {
    const { username, password } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'Faltan credenciales' });

    const config = JSON.parse(process.env.USERS_CONFIG || '{"users":[]}');
    const user = config.users.find(u => u.username === username && u.password === password);
    if (!user) return res.status(401).json({ error: 'Usuario o contraseña incorrectos' });

    const { password: _, ...safeUser } = user;
    return res.json(safeUser);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
};
