/* ═══════════════════════════════════════════════════════════════
   Huddle Server 4.5
   Express + WebSocket + REST API + Signal + Search
   ═══════════════════════════════════════════════════════════════ */

'use strict';

const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const cors       = require('cors');
const bcrypt     = require('bcryptjs');
const jwt        = require('jsonwebtoken');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');

const PORT       = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'huddle-secret-2024';
const DB_FILE    = path.join(__dirname, 'db.json');

const app = express();
app.use(cors());
app.use(express.json({ limit: '25mb' }));
app.use(express.static(__dirname));

/* ─────────────── БАЗА (JSON-файл) ─────────────── */
let db = {
  users: [],
  messages: {},
  communities: [],
  posts: []
};

if (fs.existsSync(DB_FILE)) {
  try {
    const raw = fs.readFileSync(DB_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    db = Object.assign(db, parsed);
  } catch (e) {
    console.error('⚠ db.json повреждён, создаю новый');
  }
}

let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(function () {
    try {
      fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
    } catch (e) {
      console.error('saveDB error', e);
    }
  }, 200);
}

/* ═══════════════════════ REST API ═══════════════════════ */

/* Регистрация */
app.post('/api/register', async (req, res) => {
  try {
    const { username, email, password } = req.body || {};
    if (!username || !email || !password || password.length < 6)
      return res.status(400).json({ error: 'Проверьте данные' });

    if (db.users.find(u => u.username.toLowerCase() === username.toLowerCase()))
      return res.status(409).json({ error: 'Имя уже занято' });

    const hash = await bcrypt.hash(password, 10);
    const user = {
      id: 'u_' + Date.now(),
      username: username,
      email: email,
      password: hash,
      avatar: null,
      bio: '',
      online: false,
      lastSeen: new Date().toISOString(),
      created_at: new Date().toISOString()
    };
    db.users.push(user);
    saveDB();

    const token = jwt.sign({ id: user.id, username }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ ok: true, user: safeUser(user), token });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Ошибка сервера' });
  }
});

/* Логин */
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    const user = db.users.find(u =>
      u.username.toLowerCase() === String(username || '').toLowerCase()
    );
    if (!user) return res.status(404).json({ error: 'Пользователь не найден' });

    const ok = await bcrypt.compare(password, user.password);
    if (!ok) return res.status(401).json({ error: 'Неверный пароль' });

    const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ ok: true, user: safeUser(user), token });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Ошибка сервера' });
  }
});

/* Поиск пользователей (REST) */
app.get('/api/users/search', (req, res) => {
  const q = (req.query.q || '').toLowerCase().trim();
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 100);

  let list;
  if (!q) {
    list = db.users.slice(0, limit);
  } else {
    list = db.users
      .filter(u => u.username.toLowerCase().includes(q))
      .slice(0, limit);
  }

  res.json({
    users: list.map(u => ({
      id: u.id,
      username: u.username,
      online: clientsMap.has(u.username),
      lastSeen: u.lastSeen || null
    }))
  });
});

/* Все пользователи */
app.get('/api/users', (req, res) => {
  res.json({
    users: db.users.map(u => ({
      id: u.id,
      username: u.username,
      online: clientsMap.has(u.username)
    }))
  });
});

/* Онлайн */
app.get('/api/users/online', (req, res) => {
  const online = db.users.filter(u => clientsMap.has(u.username));
  res.json({ users: online.map(safeUser) });
});

/* Сообщения чата */
app.get('/api/messages/:chatId', (req, res) => {
  res.json({ messages: db.messages[req.params.chatId] || [] });
});

app.post('/api/messages/:chatId', (req, res) => {
  const chatId = req.params.chatId;
  if (!db.messages[chatId]) db.messages[chatId] = [];

  const msg = {
    id: 'm_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
    from: req.body.from,
    to: req.body.to || '',
    content: req.body.content || '',
    type: req.body.type || 'text',
    file_data: req.body.file_data || null,
    file_name: req.body.file_name || null,
    file_size: req.body.file_size || 0,
    media_mime: req.body.media_mime || '',
    duration: req.body.duration || 0,
    created_at: new Date().toISOString(),
    read: false
  };
  db.messages[chatId].push(msg);
  if (db.messages[chatId].length > 5000)
    db.messages[chatId] = db.messages[chatId].slice(-5000);

  saveDB();
  broadcast({ type: 'new-message', chatId, message: msg });

  if (msg.to) {
    sendTo(msg.to, { type: 'chat-message', chatId: chatId, message: msg });
  }

  res.json({ ok: true, message: msg });
});

/* Посты */
app.get('/api/posts', (req, res) => res.json({ posts: db.posts }));
app.post('/api/posts', (req, res) => {
  const p = req.body || {};
  p.id = p.id || 'p_' + Date.now();
  p.created_at = new Date().toISOString();
  db.posts.unshift(p);
  if (db.posts.length > 1000) db.posts = db.posts.slice(0, 1000);
  saveDB();
  broadcast({ type: 'new-post', post: p });
  res.json({ ok: true, post: p });
});

/* Сообщества */
app.get('/api/communities', (req, res) => res.json({ communities: db.communities }));
app.post('/api/communities', (req, res) => {
  const c = req.body || {};
  c.id = c.id || 'c_' + Date.now();
  c.created_at = new Date().toISOString();
  db.communities.push(c);
  saveDB();
  broadcast({ type: 'new-community', community: c });
  res.json({ ok: true, community: c });
});

/* Обновление профиля */
app.put('/api/me', (req, res) => {
  try {
    const auth = req.headers['authorization'] || '';
    const m = auth.match(/^Bearer\s+(.+)$/i);
    if (!m) return res.status(401).json({ error: 'Unauthorized' });
    const decoded = jwt.verify(m[1], JWT_SECRET);
    const user = db.users.find(u => u.username === decoded.username);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    if (typeof req.body.bio === 'string') user.bio = req.body.bio.slice(0, 500);
    if (typeof req.body.avatar === 'string') user.avatar = req.body.avatar;
    if (typeof req.body.phone === 'string') user.phone = req.body.phone.slice(0, 30);
    saveDB();
    broadcast({ type: 'profile-update', username: user.username, avatar: user.avatar, bio: user.bio });
    res.json({ ok: true, user: safeUser(user) });
  } catch (e) {
    res.status(401).json({ error: 'Unauthorized' });
  }
});

/* Healthcheck */
app.get('/health', (req, res) => res.json({ ok: true, service: 'huddle-signal', ts: Date.now() }));

function safeUser(u) {
  if (!u) return null;
  return {
    id: u.id,
    username: u.username,
    email: u.email || '',
    phone: u.phone || '',
    avatar: u.avatar || null,
    bio: u.bio || '',
    online: clientsMap.has(u.username),
    lastSeen: u.lastSeen || null,
    created_at: u.created_at || null
  };
}

/* ═══════════════════════ WEBSOCKET ═══════════════════════ */

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

/* username → WebSocket */
const clientsMap = new Map();

function broadcast(obj, exceptUsername) {
  const s = JSON.stringify(obj);
  clientsMap.forEach((ws, uname) => {
    if (uname === exceptUsername) return;
    if (ws.readyState === 1) {
      try { ws.send(s); } catch (e) {}
    }
  });
}

function sendTo(username, obj) {
  const ws = clientsMap.get(username);
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); return true; } catch (e) {}
  }
  return false;
}

wss.on('connection', (ws) => {
  let me = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || !msg.type) return;

    /* ─── Регистрация клиента ─── */
    if (msg.type === 'register') {
      const username = String(msg.username || '').trim();
      if (!username) return;

      me = username;

      /* создаём, если такого нет */
      if (!db.users.find(u => u.username === username)) {
        db.users.push({
          id: 'u_' + Date.now(),
          username: username,
          email: '',
          password: null,
          avatar: null,
          bio: '',
          online: true,
          lastSeen: new Date().toISOString(),
          created_at: new Date().toISOString()
        });
        saveDB();
      }

      const u = db.users.find(u => u.username === username);
      u.online = true;
      u.lastSeen = new Date().toISOString();

      clientsMap.set(me, ws);

      ws.send(JSON.stringify({
        type: 'registered',
        username: me,
        user: safeUser(u)
      }));

      broadcast({ type: 'user-online', username: me }, me);

      console.log('+ Online:', me, '(всего: ' + clientsMap.size + ')');
      return;
    }

    /* ─── WebRTC + typing + presence ─── */
    if (['offer', 'answer', 'candidate', 'call-invite', 'hangup', 'typing', 'presence', 'call-invite-group']
        .includes(msg.type)) {
      if (msg.to) sendTo(msg.to, Object.assign({}, msg, { from: msg.from || me }));
      return;
    }

    /* ═══ 🔎 ПОИСК ПОЛЬЗОВАТЕЛЕЙ ═══ */
    if (msg.type === 'search-users') {
      const q = String(msg.q || '').trim().toLowerCase();
      const reqId = msg.reqId || ('srch_' + Date.now());
      const from = msg.from || me;
      const limit = Math.min(parseInt(msg.limit || '50', 10), 100);

      let users;

      if (!q) {
        /* Пустой запрос = все пользователи */
        users = db.users.slice(0, limit).map(u => ({
          username: u.username,
          name: u.username,
          avatar: u.avatar || null,
          online: clientsMap.has(u.username),
          lastSeen: u.lastSeen || null
        }));
      } else {
        users = db.users
          .filter(u => u.username.toLowerCase().includes(q))
          .slice(0, limit)
          .map(u => ({
            username: u.username,
            name: u.username,
            avatar: u.avatar || null,
            online: clientsMap.has(u.username),
            lastSeen: u.lastSeen || null
          }));
      }

      ws.send(JSON.stringify({
        type: 'search-users-results',
        reqId: reqId,
        q: msg.q,
        users: users
      }));

      console.log(`[search-users] "${q || '*'}" -> ${users.length} (from ${from || '?'})`);
      return;
    }

    /* ═══ 🔎 ПОИСК ПО СООБЩЕНИЯМ ═══ */
    if (msg.type === 'search-messages') {
      const q = String(msg.q || '').trim().toLowerCase();
      const reqId = msg.reqId || ('csearch_' + Date.now());
      const from = msg.from || me;

      if (!q) {
        ws.send(JSON.stringify({
          type: 'search-messages-results',
          reqId: reqId,
          messages: []
        }));
        return;
      }

      let pool = [];

      if (msg.chatId && db.messages[msg.chatId]) {
        pool = db.messages[msg.chatId];
      } else if (msg.chatUser && from) {
        /* личный чат */
        const directKey = [from, msg.chatUser].sort().join('|');
        if (db.messages[directKey]) pool = pool.concat(db.messages[directKey]);

        /* групповые чаты */
        Object.keys(db.messages).forEach(k => {
          if (k.startsWith('group:') && k.indexOf(from) >= 0 && k.indexOf(msg.chatUser) >= 0) {
            pool = pool.concat(db.messages[k]);
          }
        });
      } else if (from) {
        /* по всем чатам пользователя */
        Object.keys(db.messages).forEach(k => {
          if (k.indexOf(from) >= 0) pool = pool.concat(db.messages[k]);
        });
      }

      const messages = pool
        .filter(m => ((m.content || '') + ' ' + (m.type || '')).toLowerCase().includes(q))
        .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0))
        .slice(-200);

      ws.send(JSON.stringify({
        type: 'search-messages-results',
        reqId: reqId,
        q: msg.q,
        messages: messages
      }));

      console.log(`[search-messages] "${q}" -> ${messages.length} (from ${from || '?'})`);
      return;
    }

    /* ─── chat-message через WS ─── */
    if (msg.type === 'chat-message') {
      const from = msg.from || me;
      const to = msg.to;
      if (!from || !to) return;

      let chatId = msg.chatId;
      if (!chatId) {
        if (to.startsWith('group:')) chatId = to;
        else chatId = [from, to].sort().join('|');
      }

      if (!db.messages[chatId]) db.messages[chatId] = [];

      const record = {
        id: msg.id || ('m_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6)),
        from: from,
        fromName: msg.fromName || from,
        to: to,
        content: msg.content || '',
        type: msg.media_type || msg.type || 'text',
        media_key: msg.media_key || null,
        file_data: msg.file_data || null,
        file_name: msg.file_name || null,
        file_size: msg.file_size || 0,
        media_mime: msg.media_mime || '',
        duration: msg.duration || 0,
        created_at: msg.created_at || new Date().toISOString(),
        read: false
      };

      db.messages[chatId].push(record);
      if (db.messages[chatId].length > 5000)
        db.messages[chatId] = db.messages[chatId].slice(-5000);

      saveDB();

      /* Групповой чат → всем участникам */
      if (to.startsWith('group:')) {
        const gid = to.slice(6);
        const g = db.communities.find(c => String(c.id) === String(gid)) ||
                  db.groups && db.groups[gid];
        if (g && Array.isArray(g.members)) {
          g.members.forEach(m => sendTo(m, { type: 'chat-message', chatId: chatId, message: record }));
        } else {
          broadcast({ type: 'chat-message', chatId: chatId, message: record }, from);
        }
      } else {
        sendTo(to, { type: 'chat-message', chatId: chatId, message: record });
      }

      /* Подтверждение отправителю */
      ws.send(JSON.stringify({
        type: 'chat-message-sent',
        id: record.id,
        chatId: chatId,
        ts: Date.now()
      }));

      return;
    }

    /* ─── ping / pong ─── */
    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', t: msg.t }));
      return;
    }
    if (msg.type === 'pong') return;

    /* ─── ping-all ─── */
    if (msg.type === 'ping-all') {
      broadcast({ type: 'ping-all', from: me, t: msg.t || Date.now() }, me);
      return;
    }

    /* ─── group-invite ─── */
    if (msg.type === 'group-invite') {
      if (msg.to) sendTo(msg.to, msg);
      return;
    }

    /* ─── story-broadcast ─── */
    if (msg.type === 'story-broadcast') {
      if (msg.story) broadcast({ type: 'story-broadcast', story: msg.story }, me);
      return;
    }

    /* ─── profile-update ─── */
    if (msg.type === 'profile-update') {
      const u = db.users.find(u => u.username === me);
      if (u) {
        if (typeof msg.avatar === 'string') u.avatar = msg.avatar;
        if (typeof msg.bio === 'string') u.bio = msg.bio;
        saveDB();
        broadcast({ type: 'profile-update', username: me, avatar: u.avatar, bio: u.bio }, me);
      }
      return;
    }

    /* ─── sync ─── */
    if (msg.type === 'sync-push') {
      if (msg.to) sendTo(msg.to, msg);
      else broadcast(msg, me);
      return;
    }
    if (msg.type === 'sync-request') {
      if (msg.from) sendTo(msg.from, { type: 'sync-push', to: me, from: me, data: {} });
      return;
    }
  });

  ws.on('close', () => {
    if (me) {
      clientsMap.delete(me);
      const u = db.users.find(u => u.username === me);
      if (u) {
        u.online = false;
        u.lastSeen = new Date().toISOString();
        saveDB();
      }
      broadcast({ type: 'user-offline', username: me }, me);
      console.log('- Offline:', me, '(всего: ' + clientsMap.size + ')');
    }
  });

  ws.on('error', () => {});
});

/* Heartbeat — отключает зависшие WS */
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

/* ═══════════════════════ СТАРТ ═══════════════════════ */

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('  ╔═══════════════════════════════════════════════╗');
  console.log('  ║         📡 Huddle Server 4.5                  ║');
  console.log('  ╠═══════════════════════════════════════════════╣');
  console.log('  ║  HTTP  : http://localhost:' + PORT + '        ');
  console.log('  ║  WS    : ws://localhost:' + PORT + '/ws       ');
  console.log('  ║  Users : ' + db.users.length + '  /  Msgs: ' + Object.keys(db.messages).length + ' чатов');
  console.log('  ║  Data  : ' + DB_FILE);
  console.log('  ╚═══════════════════════════════════════════════╝');

  const nets = os.networkInterfaces();
  Object.keys(nets).forEach(k => {
    nets[k].forEach(net => {
      if (net.family === 'IPv4' && !net.internal) {
        console.log('   LAN:   http://' + net.address + ':' + PORT);
      }
    });
  });
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\n⏹  Shutting down…');
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
  } catch (e) {}
  process.exit(0);
});

process.on('uncaughtException', (e) => {
  console.error('⚠ uncaughtException:', e && e.message);
});

process.on('unhandledRejection', (e) => {
  console.error('⚠ unhandledRejection:', e && e.message);
});