/* ============================================================
   Huddle Server v2.2 — Non-blocking edition
   ------------------------------------------------------------
   ⚡ Все FS-операции асинхронные (fs.promises)
   ⚡ Стрим записи звонков напрямую на диск (RAM = 0)
   ⚡ Очередь сохранения JSON (debounce 8 сек)
   ⚡ Кэш директорий (readdir → TTL 5 сек)
   ⚡ Base64 decode с yield (setImmediate)
   ⚡ Atomic writes (.tmp + rename)
   ⚡ Graceful shutdown с ожиданием записей
   ============================================================ */
require('dotenv').config();
'use strict';

const fs       = require('fs');
const fsp      = fs.promises;
const path     = require('path');
const http     = require('http');
const crypto   = require('crypto');
const express  = require('express');
const cors     = require('cors');
const multer   = require('multer');
const bcrypt   = require('bcryptjs');
const jwt      = require('jsonwebtoken');
const { WebSocketServer } = require('ws');
const { v4: uuid } = require('uuid');

/* ============================================================
   1. КОНФИГ
   ============================================================ */
const HTTP_PORT     = Number(process.env.PORT) || 8080;
const WS_PORT       = Number(process.env.WS_PORT) || 8787;
const JWT_SECRET    = process.env.JWT_SECRET || 'huddle-dev-secret-change-me';
const JWT_TTL       = process.env.JWT_EXPIRES_IN || '7d';
const ADMIN_KEY     = process.env.ADMIN_KEY || 'huddle-admin-secret-change-me';
const DATA_DIR      = path.resolve(process.env.DATA_DIR || './data');
const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB) || 100;
const MAX_BASE64_MB = 25;
const SAVE_DEBOUNCE = 8000;   // ms — задержка автосохранения JSON
const READDIR_TTL   = 5000;   // ms — кэш листинга папок
const REC_MAX_BYTES = 500 * 1024 * 1024;   // лимит записи звонка

const DIRS = {
  root:       DATA_DIR,
  recordings: path.join(DATA_DIR, 'recordings'),
  uploads:    path.join(DATA_DIR, 'uploads'),
  photos:     path.join(DATA_DIR, 'uploads', 'photo'),
  videos:     path.join(DATA_DIR, 'uploads', 'video'),
  voices:     path.join(DATA_DIR, 'uploads', 'voice'),
  files:      path.join(DATA_DIR, 'uploads', 'files'),
  avatars:    path.join(DATA_DIR, 'uploads', 'avatars'),
  stories:    path.join(DATA_DIR, 'uploads', 'stories')
};

const FILES = {
  users:        path.join(DIRS.root, 'users.json'),
  messages:     path.join(DIRS.root, 'messages.json'),
  polls:        path.join(DIRS.root, 'polls.json'),
  stories:      path.join(DIRS.root, 'stories.json'),
  posts:        path.join(DIRS.root, 'posts.json'),
  groups:       path.join(DIRS.root, 'groups.json'),
  contacts:     path.join(DIRS.root, 'contacts.json'),
  blocked:      path.join(DIRS.root, 'blocked.json'),
  logs:         path.join(DIRS.root, 'logs.json'),
  fingerprints: path.join(DIRS.root, 'fingerprints.json')
};

/* ============================================================
   2. ЛОГГЕР
   ============================================================ */
const ts = () => new Date().toISOString();
const log = {
  info:  (...a) => console.log (`[${ts()}] [INFO ]`, ...a),
  warn:  (...a) => console.warn (`[${ts()}] [WARN ]`, ...a),
  error: (...a) => console.error(`[${ts()}] [ERROR]`, ...a)
};

/* ============================================================
   3. АСИНХРОННАЯ ИНИЦИАЛИЗАЦИЯ ПАПОК
   ============================================================ */
async function initDirs() {
  await Promise.all(Object.values(DIRS).map(d =>
    fsp.mkdir(d, { recursive: true }).catch(() => {})
  ));
  for (const [k, f] of Object.entries(FILES)) {
    try { await fsp.access(f); }
    catch { await fsp.writeFile(f, k === 'fingerprints' ? '{}' : '[]'); }
  }
}

/* ============================================================
   4. JSON-ХРАНИЛИЩЕ (асинхронное)
   ============================================================ */
async function loadJson(f, fb) {
  try { return JSON.parse(await fsp.readFile(f, 'utf8')); }
  catch { return fb; }
}

/**
 * Асинхронное атомарное сохранение:
 * 1. yield event loop для stringify
 * 2. writeFile(.tmp)
 * 3. rename(.tmp → final)
 */
async function saveJsonAsync(file, data) {
  try {
    // Уступаем event loop перед stringify больших массивов
    if (Array.isArray(data) && data.length > 5000) {
      await new Promise(r => setImmediate(r));
    }
    const str = JSON.stringify(data, null, 2);
    const tmp = file + '.tmp';
    await fsp.writeFile(tmp, str);
    await fsp.rename(tmp, file);
  } catch (e) {
    log.error('saveJsonAsync', path.basename(file), e.message);
  }
}

/* Очередь сохранений — не допускает параллельных записей */
class SaveQueue {
  constructor(debounce) {
    this._debounce = debounce;
    this._pending = new Map();
    this._running = false;
    this._timer = null;
  }
  enqueue(key, file, getData) {
    this._pending.set(key, { file, getData });
    this._schedule();
  }
  _schedule() {
    if (this._timer || this._running) return;
    this._timer = setTimeout(() => {
      this._timer = null;
      this._process();
    }, this._debounce);
  }
  async _process() {
    if (this._running || !this._pending.size) return;
    this._running = true;
    const batch = Array.from(this._pending.entries());
    this._pending.clear();
    for (const [key, { file, getData }] of batch) {
      try { await saveJsonAsync(file, getData()); }
      catch (e) { log.error('SaveQueue', key, e.message); }
    }
    this._running = false;
    if (this._pending.size) this._schedule();
  }
  async flush() {
    if (this._timer) { clearTimeout(this._timer); this._timer = null; }
    // Ждём завершения текущего процесса
    while (this._running) await new Promise(r => setTimeout(r, 50));
    // Дописываем оставшееся
    if (this._pending.size) await this._process();
  }
}
const saveQueue = new SaveQueue(SAVE_DEBOUNCE);

/* In-memory состояние */
let users, messages, polls, stories, posts, groups, contacts, blocked, logs, fingerprints;

/* ============================================================
   5. ЗАГРУЗКА ВСЕГО СОСТОЯНИЯ
   ============================================================ */
async function loadAll() {
  const [u, m, pl, st, po, g, c, b, lg, fp] = await Promise.all([
    loadJson(FILES.users, []),
    loadJson(FILES.messages, []),
    loadJson(FILES.polls, []),
    loadJson(FILES.stories, []),
    loadJson(FILES.posts, []),
    loadJson(FILES.groups, []),
    loadJson(FILES.contacts, []),
    loadJson(FILES.blocked, {}),
    loadJson(FILES.logs, []),
    loadJson(FILES.fingerprints, {})
  ]);
  users = u; messages = m; polls = pl; stories = st;
  posts = po; groups = g; contacts = c; blocked = b;
  logs = lg; fingerprints = fp;
}

/* ============================================================
   6. HELPERS
   ============================================================ */
const norm = s => String(s || '').trim().toLowerCase();
const now  = () => new Date().toISOString();
const pub  = u => u ? ({
  id: u.id, username: u.username,
  email: u.email || '', phone: u.phone || '',
  avatar: u.avatar || '', bio: u.bio || '',
  createdAt: u.createdAt, lastSeen: u.lastSeen || null
}) : null;

const findByUsername = u => users.find(x => norm(x.username) === norm(u));
const findById       = id => users.find(x => x.id === id);
const signToken      = u => jwt.sign({ uid: u.id, username: u.username }, JWT_SECRET, { expiresIn: JWT_TTL });
const verifyToken    = t => { try { return jwt.verify(t, JWT_SECRET); } catch { return null; } };

function authMiddleware(req, res, next) {
  const h = req.headers.authorization || '';
  const t = h.startsWith('Bearer ') ? h.slice(7) : (req.query.token || '');
  const payload = verifyToken(t);
  if (!payload) return res.status(401).json({ error: 'Не авторизован' });
  const user = findById(payload.uid);
  if (!user) return res.status(401).json({ error: 'Пользователь не найден' });
  req.user = user;
  next();
}
function requireAdmin(req, res, next) {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== ADMIN_KEY) return res.status(401).json({ error: 'Неверный админ-ключ' });
  next();
}

/* ============================================================
   7. КЭШ ЛИСТИНГА ПАПОК
   ============================================================ */
const dirCache = new Map();   // dir → { files, ts }

async function listDirCached(dir) {
  const hit = dirCache.get(dir);
  if (hit && (Date.now() - hit.ts) < READDIR_TTL) return hit.files;
  const files = await fsp.readdir(dir).catch(() => []);
  dirCache.set(dir, { files, ts: Date.now() });
  return files;
}
function invalidateDirCache(dir) {
  dirCache.delete(dir);
}

/* ============================================================
   8. АСИНХРОННОЕ СОХРАНЕНИЕ BASE64 → ФАЙЛ
   ============================================================ */
async function saveBase64MediaAsync(dataUri, kind, fromUser) {
  if (!dataUri || typeof dataUri !== 'string') return null;

  const m = dataUri.match(/^data:([^;]+);base64,(.+)$/i);
  if (!m) return null;

  const mime   = m[1] || 'application/octet-stream';
  const base64 = m[2];

  // Оценка размера до декодирования
  const estimated = Math.floor(base64.length * 0.75);
  if (estimated > MAX_BASE64_MB * 1024 * 1024) {
    log.warn(`saveBase64Media: слишком большой (~${(estimated / 1024 / 1024).toFixed(1)} МБ)`);
    return null;
  }

  // Yield перед декодированием
  await new Promise(r => setImmediate(r));

  let buf;
  try { buf = Buffer.from(base64, 'base64'); }
  catch { return null; }

  // Yield после декодирования
  await new Promise(r => setImmediate(r));

  let dir, ext;
  if (kind === 'voice' || /^audio\//i.test(mime)) {
    dir = DIRS.voices;
    ext = mime.includes('webm') ? 'webm'
        : mime.includes('mp4')  ? 'm4a'
        : mime.includes('ogg')  ? 'ogg'
        : 'audio';
  } else if (kind === 'photo' || /^image\//i.test(mime)) {
    dir = DIRS.photos;
    ext = mime.includes('png')  ? 'png'
        : mime.includes('gif')  ? 'gif'
        : mime.includes('webp') ? 'webp'
        : 'jpg';
  } else if (kind === 'video' || kind === 'circle' || /^video\//i.test(mime)) {
    dir = DIRS.videos;
    ext = mime.includes('mp4') ? 'mp4' : 'webm';
  } else {
    dir = DIRS.files;
    ext = 'bin';
  }

  const safeUser = String(fromUser || 'anon').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32) || 'anon';
  const filename = `${safeUser}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}.${ext}`;
  const fullPath = path.join(dir, filename);

  try {
    await fsp.writeFile(fullPath, buf);
    invalidateDirCache(dir);
    const rel = path.relative(DIRS.root, fullPath).replace(/\\/g, '/');
    const url = '/' + rel;
    log.info(`💾 ${kind} → ${filename} (${(buf.length / 1024).toFixed(1)} КБ)`);
    return { url, filename, size: buf.length, mime };
  } catch (e) {
    log.error('saveBase64MediaAsync write', e.message);
    return null;
  }
}

/* ============================================================
   9. RATE LIMIT
   ============================================================ */
const rlBuckets = new Map();
function rateLimit(req, res, next) {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const key = ip + ':' + Math.floor(Date.now() / 60000);
  const count = (rlBuckets.get(key) || 0) + 1;
  rlBuckets.set(key, count);
  if (count > 240) return res.status(429).json({ error: 'Слишком много запросов' });
  next();
}
setInterval(() => {
  const cutoff = Math.floor(Date.now() / 60000) - 2;
  for (const k of rlBuckets.keys()) {
    if (Number(k.split(':').pop()) < cutoff) rlBuckets.delete(k);
  }
}, 60000).unref();

/* ============================================================
   10. ЛОГИРОВАНИЕ
   ============================================================ */
function logAction(userId, username, action, data, req) {
  const ip = req
    ? (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim()
    : '';
  const ua = req ? (req.headers['user-agent'] || '') : '';
  logs.push({
    id: 'log_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
    userId: userId || null, username: username || null,
    action, ip, userAgent: ua, at: now(), data: data || null
  });
  if (logs.length > 20000) logs = logs.slice(-20000);
  saveQueue.enqueue('logs', FILES.logs, () => logs.slice(-20000));
}

/* ============================================================
   11. EXPRESS
   ============================================================ */
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '30mb' }));

/* Static — с поддержкой Range и без блокировок */
const staticOpts = { maxAge: '7d', etag: true, lastModified: true };
app.use('/uploads',    express.static(DIRS.uploads,    staticOpts));
app.use('/recordings', express.static(DIRS.recordings, {
  maxAge: '1d', acceptRanges: true, etag: true
}));

app.get('/', (_req, res) => res.json({ ok: true, service: 'huddle-api', version: '2.2.0' }));

/* ---------- AUTH ---------- */
app.post('/api/register', rateLimit, async (req, res) => {
  try {
    const { username, email, password } = req.body || {};
    if (!username || String(username).trim().length < 2)
      return res.status(400).json({ error: 'Имя слишком короткое' });
    if (!password || String(password).length < 6)
      return res.status(400).json({ error: 'Пароль минимум 6 символов' });
    if (findByUsername(username))
      return res.status(409).json({ error: 'Имя занято' });

    const user = {
      id: uuid(),
      username: String(username).trim(),
      email: String(email || '').trim(),
      phone: '',
      passwordHash: await bcrypt.hash(String(password), 10),
      avatar: '', bio: '',
      createdAt: now(), lastSeen: now()
    };
    users.push(user);
    saveQueue.enqueue('users', FILES.users, () => users);
    const token = signToken(user);
    logAction(user.id, user.username, 'register', null, req);
    res.json({ user: pub(user), token });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/login', rateLimit, async (req, res) => {
  try {
    const { username, password } = req.body || {};
    const user = findByUsername(username);
    if (!user) return res.status(401).json({ error: 'Не найден' });
    if (!await bcrypt.compare(String(password || ''), user.passwordHash))
      return res.status(401).json({ error: 'Неверный пароль' });
    user.lastSeen = now();
    saveQueue.enqueue('users', FILES.users, () => users);
    res.json({ user: pub(user), token: signToken(user) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/me', authMiddleware, (req, res) => res.json({ user: pub(req.user) }));

app.put('/api/me', authMiddleware, (req, res) => {
  const { avatar, bio, phone, email } = req.body || {};
  if (typeof avatar === 'string') req.user.avatar = avatar;
  if (typeof bio === 'string')    req.user.bio = bio;
  if (typeof phone === 'string')  req.user.phone = phone;
  if (typeof email === 'string')  req.user.email = email;
  saveQueue.enqueue('users', FILES.users, () => users);
  res.json({ user: pub(req.user) });
});

/* ---------- USERS ---------- */
app.get('/api/users', (req, res) => {
  const q = norm(req.query.q || '');
  const list = users
    .filter(u => !q || norm(u.username).includes(q) || norm(u.email).includes(q))
    .slice(0, 100)
    .map(pub);
  res.json({ users: list, count: list.length });
});

app.get('/api/users/:username', (req, res) => {
  const u = findByUsername(req.params.username);
  if (!u) return res.status(404).json({ error: 'Не найден' });
  res.json({ user: { ...pub(u), online: isUserOnline(u.username) } });
});

/* ============================================================
   12. ЗАГРУЗКА ФАЙЛОВ (multipart, async)
   ============================================================ */
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    let dir = DIRS.files;
    if (/^image\//.test(file.mimetype))      dir = DIRS.photos;
    else if (/^video\//.test(file.mimetype)) dir = DIRS.videos;
    else if (/^audio\//.test(file.mimetype)) dir = DIRS.voices;
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || '').slice(0, 8) || '.bin';
    cb(null, `${Date.now()}_${crypto.randomBytes(6).toString('hex')}${ext}`);
  }
});
const upload = multer({ storage, limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024 } });

app.post('/api/upload', authMiddleware, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Файл не загружен' });
  const rel = path.relative(DIRS.root, req.file.path).replace(/\\/g, '/');
  const url = '/' + rel;
  invalidateDirCache(path.dirname(req.file.path));
  logAction(req.user.id, req.user.username, 'file_upload', {
    filename: req.file.filename, size: req.file.size, mimetype: req.file.mimetype
  }, req);
  res.json({
    ok: true,
    file: {
      filename: req.file.filename,
      originalName: req.file.originalname,
      size: req.file.size,
      mimetype: req.file.mimetype,
      url
    }
  });
});

app.post('/api/avatar', authMiddleware, upload.single('avatar'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Файл не загружен' });
  const rel = path.relative(DIRS.root, req.file.path).replace(/\\/g, '/');
  req.user.avatar = '/' + rel;
  invalidateDirCache(path.dirname(req.file.path));
  saveQueue.enqueue('users', FILES.users, () => users);
  res.json({ ok: true, avatar: req.user.avatar });
});

/* ============================================================
   13. СООБЩЕНИЯ ЧЕРЕЗ REST (async)
   ============================================================ */
app.post('/api/message', authMiddleware, rateLimit, async (req, res) => {
  try {
    const { to, type = 'text', content = '', file_data, duration = 0, media_mime = '' } = req.body || {};
    if (!to) return res.status(400).json({ error: 'Не указан получатель' });

    let mediaUrl = null, mediaSize = 0, mime = media_mime;
    if (file_data && /^data:/i.test(file_data)) {
      const saved = await saveBase64MediaAsync(file_data, type, req.user.username);
      if (!saved) return res.status(413).json({ error: 'Файл слишком большой или невалидный' });
      mediaUrl = saved.url;
      mediaSize = saved.size;
      if (!mime) mime = saved.mime;
    }

    const msg = {
      id: uuid(),
      from: req.user.username,
      to,
      media_type: type, type,
      content: content || (
        type === 'voice'  ? '🎙️ Голосовое' :
        type === 'audio'  ? '🎙️ Голосовое' :
        type === 'photo'  ? '📷 Фото' :
        type === 'video'  ? '🎬 Видео' :
        type === 'circle' ? '⭕ Кружок' : ''
      ),
      media_url: mediaUrl,
      media_mime: mime,
      file_size: mediaSize,
      duration,
      created_at: now(),
      read: false
    };

    messages.push({
      id: msg.id, from: msg.from, to: msg.to,
      type: msg.type, payload: msg, createdAt: msg.created_at
    });
    saveQueue.enqueue('messages', FILES.messages, () => messages.slice(-20000));

    if (msg.to) {
      sendTo(msg.to, { type: 'chat-message', ...msg });
      sendTo(req.user.username, {
        type: 'message-sent',
        id: msg.id, media_url: mediaUrl, file_size: mediaSize
      });
    }

    res.json({ ok: true, message: msg });
  } catch (e) {
    log.error('POST /api/message', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/messages', authMiddleware, (req, res) => {
  const peer  = norm(req.query.with || '');
  const limit = Math.min(Number(req.query.limit) || 500, 2000);
  const me    = req.user.username;
  const list  = messages
    .filter(m => (m.from === me && norm(m.to) === peer) ||
                 (norm(m.from) === peer && m.to === me))
    .slice(-limit)
    .map(m => m.payload);
  res.json({ messages: list, count: list.length });
});

app.delete('/api/messages/:id', authMiddleware, (req, res) => {
  const idx = messages.findIndex(m => m.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Не найдено' });
  if (messages[idx].from !== req.user.username)
    return res.status(403).json({ error: 'Не ваше' });
  messages.splice(idx, 1);
  saveQueue.enqueue('messages', FILES.messages, () => messages.slice(-20000));
  broadcast({ type: 'message-deleted', id: req.params.id, from: req.user.username }, req.user.username);
  res.json({ ok: true });
});

/* ============================================================
   14. ОПРОСЫ
   ============================================================ */
app.post('/api/polls', authMiddleware, (req, res) => {
  const { title, options, multiple } = req.body || {};
  if (!title || !Array.isArray(options) || options.length < 2)
    return res.status(400).json({ error: 'Нужен вопрос и 2+ варианта' });

  const poll = {
    id: 'poll_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    title: String(title).trim(),
    author: req.user.username,
    options: options.map((t, i) => ({ id: 'opt_' + i, text: String(t).trim(), votes: 0 })),
    multiple: !!multiple, votes: {}, totalVotes: 0, createdAt: now()
  };
  polls.unshift(poll);
  saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
  broadcast({ type: 'poll-create', poll, from: poll.author }, poll.author);
  res.json({ poll });
});

app.get('/api/polls', (req, res) => {
  const { limit = 50, author } = req.query;
  let list = polls;
  if (author) list = list.filter(p => p.author === author);
  res.json({ polls: list.slice(0, Number(limit)), count: list.length });
});

app.get('/api/polls/:id', (req, res) => {
  const poll = polls.find(p => p.id === req.params.id);
  if (!poll) return res.status(404).json({ error: 'Не найден' });
  res.json({ poll });
});

app.post('/api/polls/:id/vote', authMiddleware, (req, res) => {
  const { optionId } = req.body || {};
  const poll = polls.find(p => p.id === req.params.id);
  if (!poll) return res.status(404).json({ error: 'Не найден' });
  const opt = poll.options.find(o => o.id === optionId);
  if (!opt) return res.status(404).json({ error: 'Вариант не найден' });

  const me = req.user.username;
  const my = poll.votes[me] || [];
  const voted = my.includes(optionId);

  if (poll.multiple) {
    if (voted) { poll.votes[me] = my.filter(id => id !== optionId); opt.votes = Math.max(0, opt.votes - 1); }
    else { poll.votes[me] = [...my, optionId]; opt.votes++; }
    if (!poll.votes[me].length) delete poll.votes[me];
  } else {
    if (voted) { delete poll.votes[me]; opt.votes = Math.max(0, opt.votes - 1); }
    else {
      my.forEach(oid => { const o = poll.options.find(x => x.id === oid); if (o) o.votes = Math.max(0, o.votes - 1); });
      poll.votes[me] = [optionId]; opt.votes++;
    }
  }
  poll.totalVotes = Object.keys(poll.votes).length;
  saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
  broadcast({ type: 'poll-vote', pollId: poll.id, poll, from: me }, me);
  res.json({ ok: true, poll });
});

app.delete('/api/polls/:id', authMiddleware, (req, res) => {
  const idx = polls.findIndex(p => p.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Не найден' });
  if (polls[idx].author !== req.user.username)
    return res.status(403).json({ error: 'Только автор' });
  polls.splice(idx, 1);
  saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
  broadcast({ type: 'poll-delete', pollId: req.params.id, from: req.user.username }, req.user.username);
  res.json({ ok: true });
});

/* ============================================================
   15. ИСТОРИИ
   ============================================================ */
app.post('/api/stories', authMiddleware, async (req, res) => {
  const { text, media, mediaType } = req.body || {};
  if (!text && !media) return res.status(400).json({ error: 'Нужен текст или медиа' });

  let mediaUrl = null;
  if (media && /^data:/i.test(media)) {
    const saved = await saveBase64MediaAsync(media, mediaType === 'video' ? 'video' : 'photo', req.user.username);
    if (saved) mediaUrl = saved.url;
  }

  const story = {
    id: 's_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    author: req.user.username,
    text: text || '',
    media_url: mediaUrl,
    mediaType: mediaType || null,
    createdAt: now(),
    expiresAt: new Date(Date.now() + 86400000).toISOString()
  };
  stories.push(story);
  saveQueue.enqueue('stories', FILES.stories, () => stories.slice(-5000));
  broadcast({ type: 'story-broadcast', story, from: story.author }, story.author);
  res.json({ story });
});

app.get('/api/stories', (_req, res) => {
  const fresh = stories.filter(s => new Date(s.expiresAt) > new Date());
  const byAuthor = {};
  fresh.forEach(s => { (byAuthor[s.author] = byAuthor[s.author] || []).push(s); });
  res.json({ stories: fresh, byAuthor });
});

/* ============================================================
   16. ПОСТЫ / ЛЕНТА
   ============================================================ */
app.post('/api/posts', authMiddleware, async (req, res) => {
  const { title, content, attachment, poll } = req.body || {};
  if (!title && !content && !attachment && !poll)
    return res.status(400).json({ error: 'Пустой пост' });

  let attach = attachment || null;
  if (attach && attach.src && /^data:/i.test(attach.src)) {
    const saved = await saveBase64MediaAsync(attach.src, attach.type === 'video' ? 'video' : 'photo', req.user.username);
    if (saved) attach = { ...attach, src: saved.url, media_key: saved.filename };
  }

  const post = {
    id: 'p_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    author: req.user.username,
    title: title || '', content: content || '',
    attachment: attach, poll: poll || null,
    likes: [], views: 0, createdAt: now()
  };
  posts.unshift(post);
  saveQueue.enqueue('posts', FILES.posts, () => posts.slice(-5000));
  broadcast({ type: 'post-broadcast', post, from: post.author }, post.author);
  res.json({ post });
});

app.get('/api/posts', (req, res) => {
  const { limit = 50, author } = req.query;
  let list = posts;
  if (author) list = list.filter(p => p.author === author);
  res.json({ posts: list.slice(0, Number(limit)), count: list.length });
});

app.post('/api/posts/:id/like', authMiddleware, (req, res) => {
  const post = posts.find(p => p.id === req.params.id);
  if (!post) return res.status(404).json({ error: 'Не найден' });
  post.likes = post.likes || [];
  const me = req.user.username;
  const i = post.likes.indexOf(me);
  if (i >= 0) post.likes.splice(i, 1); else post.likes.push(me);
  saveQueue.enqueue('posts', FILES.posts, () => posts.slice(-5000));
  res.json({ ok: true, likes: post.likes.length, liked: i < 0 });
});

/* ============================================================
   17. ГРУППЫ
   ============================================================ */
app.post('/api/groups', authMiddleware, (req, res) => {
  const { name, description, members } = req.body || {};
  if (!name || name.trim().length < 2)
    return res.status(400).json({ error: 'Название минимум 2 символа' });

  const group = {
    id: 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
    name: name.trim(), description: description || '',
    owner: req.user.username,
    members: [req.user.username, ...(Array.isArray(members) ? members : [])],
    avatar: '', createdAt: now()
  };
  groups.push(group);
  saveQueue.enqueue('groups', FILES.groups, () => groups);
  group.members.forEach(m => {
    if (m !== req.user.username)
      sendTo(m, { type: 'group-invite', group, from: req.user.username });
  });
  res.json({ group });
});

app.get('/api/groups', authMiddleware, (req, res) => {
  const list = groups.filter(g => (g.members || []).includes(req.user.username));
  res.json({ groups: list });
});

app.post('/api/groups/:id/join', authMiddleware, (req, res) => {
  const g = groups.find(x => x.id === req.params.id);
  if (!g) return res.status(404).json({ error: 'Не найдена' });
  g.members = g.members || [];
  if (!g.members.includes(req.user.username)) g.members.push(req.user.username);
  saveQueue.enqueue('groups', FILES.groups, () => groups);
  broadcast({ type: 'group-join', groupId: g.id, member: req.user.username }, req.user.username);
  res.json({ ok: true, group: g });
});

app.post('/api/groups/:id/leave', authMiddleware, (req, res) => {
  const g = groups.find(x => x.id === req.params.id);
  if (!g) return res.status(404).json({ error: 'Не найдена' });
  g.members = (g.members || []).filter(m => m !== req.user.username);
  saveQueue.enqueue('groups', FILES.groups, () => groups);
  res.json({ ok: true });
});

/* ============================================================
   18. КОНТАКТЫ
   ============================================================ */
app.get('/api/contacts', authMiddleware, (req, res) => {
  res.json({ contacts: contacts[req.user.username] || [] });
});
app.post('/api/contacts', authMiddleware, (req, res) => {
  const { username, name, phone } = req.body || {};
  if (!username) return res.status(400).json({ error: 'username обязателен' });
  if (!contacts[req.user.username]) contacts[req.user.username] = [];
  if (contacts[req.user.username].some(c => c.username === username))
    return res.status(409).json({ error: 'Уже в контактах' });
  contacts[req.user.username].push({
    id: 'c_' + Date.now(), username,
    name: name || username, phone: phone || '', addedAt: now()
  });
  saveQueue.enqueue('contacts', FILES.contacts, () => contacts);
  res.json({ ok: true });
});
app.delete('/api/contacts/:username', authMiddleware, (req, res) => {
  if (!contacts[req.user.username]) return res.json({ ok: true });
  contacts[req.user.username] = contacts[req.user.username].filter(c => c.username !== req.params.username);
  saveQueue.enqueue('contacts', FILES.contacts, () => contacts);
  res.json({ ok: true });
});

/* ============================================================
   19. БЛОКИРОВКИ
   ============================================================ */
app.get('/api/blocked', authMiddleware, (req, res) => {
  res.json({ blocked: blocked[req.user.username] || [] });
});
app.post('/api/blocked/:username', authMiddleware, (req, res) => {
  if (!blocked[req.user.username]) blocked[req.user.username] = [];
  if (!blocked[req.user.username].includes(req.params.username))
    blocked[req.user.username].push(req.params.username);
  saveQueue.enqueue('blocked', FILES.blocked, () => blocked);
  res.json({ ok: true });
});
app.delete('/api/blocked/:username', authMiddleware, (req, res) => {
  if (blocked[req.user.username])
    blocked[req.user.username] = blocked[req.user.username].filter(u => u !== req.params.username);
  saveQueue.enqueue('blocked', FILES.blocked, () => blocked);
  res.json({ ok: true });
});

/* ============================================================
   20. TRACK
   ============================================================ */
app.post('/api/track', (req, res) => {
  try {
    const { username, fingerprint } = req.body || {};
    if (!username || !fingerprint) return res.status(400).json({ error: 'Нет данных' });
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    const ua = req.headers['user-agent'] || '';
    if (!fingerprints[username]) fingerprints[username] = [];
    fingerprints[username].push({
      at: now(), ip, userAgent: ua,
      acceptLanguage: req.headers['accept-language'] || '',
      fingerprint
    });
    if (fingerprints[username].length > 50)
      fingerprints[username] = fingerprints[username].slice(-50);
    saveQueue.enqueue('fingerprints', FILES.fingerprints, () => fingerprints);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ============================================================
   21. ЗАПИСИ ЗВОНКОВ (async + кэш)
   ============================================================ */
app.get('/api/recordings', async (_req, res) => {
  try {
    const files = await listDirCached(DIRS.recordings);
    const list = await Promise.all(
      files.filter(f => /\.(webm|mp4)$/i.test(f)).map(async f => {
        const full = path.join(DIRS.recordings, f);
        const st = await fsp.stat(full).catch(() => null);
        if (!st) return null;
        return {
          id: f.replace(/\.(webm|mp4)$/i, ''),
          filename: f, size: st.size,
          createdAt: st.mtime.toISOString(),
          url: `/recordings/${encodeURIComponent(f)}`
        };
      })
    );
    const clean = list.filter(Boolean).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ recordings: clean, count: clean.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/recordings/:id', requireAdmin, async (req, res) => {
  const id = String(req.params.id).replace(/[^a-zA-Z0-9_.\-]/g, '');
  for (const f of [`${id}.webm`, `${id}.mp4`]) {
    const full = path.join(DIRS.recordings, f);
    try {
      await fsp.access(full);
      await fsp.unlink(full);
      invalidateDirCache(DIRS.recordings);
      return res.json({ ok: true, removed: f });
    } catch {}
  }
  res.status(404).json({ error: 'Не найдена' });
});

/* ============================================================
   22. АДМИН-API (async + кэш)
   ============================================================ */
app.get('/api/admin/stats', requireAdmin, async (_req, res) => {
  const [photos, videos, voices, filesArr, avatars, storiesArr, recordings] = await Promise.all([
    listDirCached(DIRS.photos),
    listDirCached(DIRS.videos),
    listDirCached(DIRS.voices),
    listDirCached(DIRS.files),
    listDirCached(DIRS.avatars),
    listDirCached(DIRS.stories),
    listDirCached(DIRS.recordings)
  ]);
  res.json({
    totalUsers: users.length,
    totalMessages: messages.length,
    totalPolls: polls.length,
    totalStories: stories.length,
    totalPosts: posts.length,
    totalGroups: groups.length,
    files: {
      photos: photos.length, videos: videos.length,
      voices: voices.length, files: filesArr.length,
      avatars: avatars.length, stories: storiesArr.length,
      recordings: recordings.filter(f => /\.(webm|mp4)$/i.test(f)).length
    },
    onlineNow: onlineList(),
    onlineCount: sockets.size,
    serverTime: now(),
    uptimeSec: Math.floor(process.uptime())
  });
});

app.get('/api/admin/users', requireAdmin, (_req, res) => {
  const list = users.map(u => {
    const fps = fingerprints[u.username] || [];
    const lastFp = fps[fps.length - 1] || null;
    return {
      ...pub(u),
      lastIp: lastFp?.ip || null,
      lastUserAgent: lastFp?.userAgent || null,
      lastSeen: lastFp?.at || u.lastSeen || null,
      fingerprintCount: fps.length,
      messagesSent: messages.filter(m => m.from === u.username).length,
      isOnline: isUserOnline(u.username)
    };
  });
  res.json({ users: list, count: list.length });
});

app.get('/api/admin/logs', requireAdmin, (req, res) => {
  const { username, action, limit = 500 } = req.query;
  let list = logs;
  if (username) list = list.filter(l => l.username === username);
  if (action)   list = list.filter(l => l.action === action);
  res.json({ logs: list.slice(-Number(limit)).reverse(), count: list.length });
});

app.get('/api/admin/messages', requireAdmin, (req, res) => {
  const { from, to, limit = 200 } = req.query;
  let list = messages;
  if (from) list = list.filter(m => m.from === from);
  if (to)   list = list.filter(m => m.to === to);
  res.json({ messages: list.slice(-Number(limit)), count: list.length });
});

app.get('/api/admin/polls', requireAdmin, (_req, res) => res.json({ polls }));
app.get('/api/admin/recordings', requireAdmin, async (_req, res) => {
  try {
    const files = await listDirCached(DIRS.recordings);
    const list = await Promise.all(files.filter(f => /\.(webm|mp4)$/i.test(f)).map(async f => {
      const st = await fsp.stat(path.join(DIRS.recordings, f)).catch(() => null);
      return st ? { filename: f, size: st.size, createdAt: st.mtime.toISOString() } : null;
    }));
    res.json({ recordings: list.filter(Boolean).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)), count: list.filter(Boolean).length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/export', requireAdmin, async (_req, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="huddle-export.json"');
  res.setHeader('Content-Type', 'application/json');
  // yield перед stringify больших данных
  await new Promise(r => setImmediate(r));
  res.send(JSON.stringify({
    exportedAt: now(),
    users: users.map(pub),
    messages, logs, fingerprints, polls, stories, posts, groups, contacts, blocked
  }, null, 2));
});

/* ============================================================
   23. 404
   ============================================================ */
app.use((_req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, _req, res, _next) => {
  log.error('express', err.message);
  res.status(err.status || 500).json({ error: err.message || 'Server error' });
});

/* ============================================================
   24. HTTP + WEBSOCKET
   ============================================================ */
const apiServer = http.createServer(app);
const wss = new WebSocketServer({ port: WS_PORT });

const sockets = new Map();       // username → Set<ws>
const wsMeta  = new WeakMap();   // ws → { username }
const activeRecordings = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }
}
function sendTo(username, obj) {
  if (!username) return false;
  const set = sockets.get(norm(username));
  if (!set || !set.size) return false;
  for (const ws of set) send(ws, obj);
  return true;
}
function broadcast(obj, exceptUser) {
  const ex = exceptUser ? norm(exceptUser) : null;
  for (const [u, set] of sockets) {
    if (ex && u === ex) continue;
    for (const ws of set) send(ws, obj);
  }
}
const onlineList   = () => Array.from(sockets.keys());
const isUserOnline = u => sockets.has(norm(u));

function attach(username, ws) {
  const k = norm(username);
  if (!sockets.has(k)) sockets.set(k, new Set());
  sockets.get(k).add(ws);
  wsMeta.set(ws, { username: k });
}
function detach(ws) {
  const meta = wsMeta.get(ws);
  if (!meta) return;
  const set = sockets.get(meta.username);
  if (set) {
    set.delete(ws);
    if (!set.size) {
      sockets.delete(meta.username);
      broadcast({ type: 'peer-offline', username: meta.username }, meta.username);
    }
  }
  wsMeta.delete(ws);
}

/* ============================================================
   25. СТРИМ ЗАПИСИ ЗВОНКА НА ДИСК
   Каждый rec-chunk → в writeStream. RAM = 0.
   ============================================================ */
class RecordingStream {
  constructor(callId, mime, from, to) {
    this.callId = callId;
    this.mime = mime;
    this.from = from;
    this.to = to;
    this.startedAt = Date.now();
    this.lastChunkAt = Date.now();
    this.size = 0;
    this.bytesWritten = 0;
    this.closed = false;
    const ext = /mp4/i.test(mime) ? 'mp4' : 'webm';
    this.filename = `call_${callId}.${ext}`;
    this.fullPath = path.join(DIRS.recordings, this.filename);
    this._pendingFlush = null;
    this._writeStream = fs.createWriteStream(this.fullPath, { flags: 'w' });
    this._writeStream.on('error', (e) => {
      log.error('rec-stream error', this.filename, e.message);
    });
  }

  /** Записать чанк. Возвращает Promise, но НЕ ждём drain — стрим сам буферизует. */
  write(base64Data) {
    if (this.closed) return false;
    let buf;
    try { buf = Buffer.from(base64Data, 'base64'); }
    catch { return false; }

    if (this.size + buf.length > REC_MAX_BYTES) {
      log.warn(`rec-stream: превышен лимит ${this.callId}`);
      this.abort();
      return false;
    }

    this.size += buf.length;
    this.bytesWritten += buf.length;
    this.lastChunkAt = Date.now();

    // write() возвращает false если буфер полон — не блокируем, стрим сам флашит
    this._writeStream.write(buf);
    return true;
  }

  /** Финальный flush + метаданные */
  async finalize() {
    if (this.closed) return null;
    this.closed = true;

    await new Promise((resolve) => {
      this._writeStream.end(resolve);
    });

    if (this.bytesWritten === 0) {
      // Удаляем пустой файл
      await fsp.unlink(this.fullPath).catch(() => {});
      return null;
    }

    return {
      id: this.callId,
      filename: this.filename,
      size: this.bytesWritten,
      url: `/recordings/${encodeURIComponent(this.filename)}`,
      durationMs: Date.now() - this.startedAt,
      from: this.from,
      to: this.to,
      createdAt: now()
    };
  }

  abort() {
    if (this.closed) return;
    this.closed = true;
    try { this._writeStream.end(); } catch {}
    // Асинхронно удаляем файл
    setTimeout(() => {
      fsp.unlink(this.fullPath).catch(() => {});
    }, 200);
  }
}

/* ============================================================
   26. WS HANDLER
   ============================================================ */
wss.on('connection', (ws) => {
  log.info('ws connection');

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); }
    catch { return; }
    if (!m || !m.type) return;

    /* ---- keepalive ---- */
    if (m.type === 'ping') return send(ws, { type: 'pong', t: m.t });
    if (m.type === 'pong') return;

    /* ---- register ---- */
    if (m.type === 'register') {
      const u = m.username || m.from;
      if (!u) return;
      attach(u, ws);
      const usr = findByUsername(u);
      if (usr) { usr.lastSeen = now(); saveQueue.enqueue('users', FILES.users, () => users); }
      send(ws, { type: 'registered', username: u, online: onlineList() });
      broadcast({ type: 'peer-online', username: u }, u);
      const _set = sockets.get(norm(u));
      log.info(`ws + ${u} (${_set ? _set.size : 0})`);
      return;
    }

    if (m.type === 'ping-all') {
      return send(ws, { type: 'pong-all', from: m.from, online: onlineList(), t: Date.now() });
    }

    if (m.type === 'typing' && m.to) {
      return sendTo(m.to, { type: 'typing', from: m.from, isTyping: !!m.isTyping });
    }

    if (m.type === 'read' && m.to) {
      return sendTo(m.to, { type: 'read-receipt', from: m.from, messageIds: m.messageIds || [] });
    }

    if (m.type === 'search-users') {
      const q = norm(m.q || '');
      const found = users
        .filter(u => !q || norm(u.username).includes(q) || norm(u.email).includes(q))
        .slice(0, Number(m.limit) || 50)
        .map(u => ({
          username: u.username, name: u.username,
          phone: u.phone || '', avatar: u.avatar || '',
          online: isUserOnline(u.username)
        }));
      return send(ws, { type: 'search-users-results', q: m.q, reqId: m.reqId, users: found });
    }

    if (m.type === 'search-messages') {
      const q = norm(m.q || ''), me = m.from, peer = m.chatUser || '';
      const found = messages.filter(msg => {
        const between = (msg.from === me && norm(msg.to) === norm(peer)) ||
                        (norm(msg.from) === norm(peer) && msg.to === me);
        if (!between) return false;
        return !q || norm(JSON.stringify(msg.payload || {})).includes(q);
      }).slice(-100);
      return send(ws, {
        type: 'search-messages-results',
        reqId: m.reqId, chatId: m.chatId,
        messages: found.map(x => x.payload)
      });
    }

    /* ============================================================
       CHAT-MESSAGE — async сохранение медиа
       ============================================================ */
    if (m.type === 'chat-message') {
      if (blocked[m.to] && blocked[m.to].includes(m.from)) {
        return send(ws, { type: 'error', message: 'Вы заблокированы получателем' });
      }

      let mediaUrl  = m.media_url || null;
      let mediaSize = m.file_size || 0;
      let mediaMime = m.media_mime || '';
      const hasBase64 = m.file_data && /^data:/i.test(m.file_data);
      const base64Copy = hasBase64 ? m.file_data : null;
      delete m.file_data;   // не таскаем по памяти

      const process = async () => {
        if (hasBase64) {
          const kindMap = { audio: 'voice', voice: 'voice', photo: 'photo', video: 'video', circle: 'video' };
          const kind = kindMap[m.media_type] || 'file';
          const saved = await saveBase64MediaAsync(base64Copy, kind, m.from);
          if (saved) {
            mediaUrl  = saved.url;
            mediaSize = saved.size;
            if (!mediaMime) mediaMime = saved.mime;
            log.info(`📥 ${m.from} → ${m.to}: ${m.media_type} → ${saved.filename}`);
          } else {
            log.warn(`saveBase64Media failed для ${m.media_type} от ${m.from}`);
          }
        }

        const msg = {
          id: m.id || uuid(),
          from: m.from,
          to: m.to,
          fromName: m.fromName || m.from,
          media_type: m.media_type || 'text',
          type: m.media_type || 'text',
          content: m.content || (
            m.media_type === 'audio'  ? '🎙️ Голосовое' :
            m.media_type === 'voice'  ? '🎙️ Голосовое' :
            m.media_type === 'photo'  ? '📷 Фото' :
            m.media_type === 'video'  ? '🎬 Видео' :
            m.media_type === 'circle' ? '⭕ Кружок' : ''
          ),
          media_url: mediaUrl,
          media_mime: mediaMime,
          file_size: mediaSize,
          duration: m.duration || 0,
          reply_to: m.reply_to || null,
          created_at: m.created_at || now()
        };

        messages.push({
          id: msg.id, from: msg.from, to: msg.to,
          type: msg.media_type, payload: msg, createdAt: msg.created_at
        });
        saveQueue.enqueue('messages', FILES.messages, () => messages.slice(-20000));

        if (msg.to) sendTo(msg.to, msg);
        else broadcast(msg, msg.from);

        send(ws, {
          type: 'message-sent',
          id: msg.id,
          media_url: mediaUrl,
          file_size: mediaSize
        });
      };

      process().catch(e => log.error('chat-message async', e.message));
      return;
    }

    /* ---- опросы ---- */
    if (m.type === 'poll-create') {
      const { title, options, multiple, author } = m;
      if (!title || !Array.isArray(options) || options.length < 2) return;
      const poll = {
        id: 'poll_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
        title: String(title).trim(),
        author: String(author || m.from).trim(),
        options: options.map((t, i) => ({ id: 'opt_' + i, text: String(t).trim(), votes: 0 })),
        multiple: !!multiple, votes: {}, totalVotes: 0, createdAt: now()
      };
      polls.unshift(poll);
      saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
      broadcast({ type: 'poll-create', poll, from: poll.author }, poll.author);
      return;
    }

    if (m.type === 'poll-vote') {
      const { pollId, optionId, username } = m;
      const poll = polls.find(p => p.id === pollId);
      if (!poll) return;
      const opt = poll.options.find(o => o.id === optionId);
      if (!opt) return;
      const my = poll.votes[username] || [];
      const voted = my.includes(optionId);

      if (poll.multiple) {
        if (voted) { poll.votes[username] = my.filter(id => id !== optionId); opt.votes = Math.max(0, opt.votes - 1); }
        else { poll.votes[username] = [...my, optionId]; opt.votes++; }
        if (!poll.votes[username].length) delete poll.votes[username];
      } else {
        if (voted) { delete poll.votes[username]; opt.votes = Math.max(0, opt.votes - 1); }
        else {
          my.forEach(oid => { const o = poll.options.find(x => x.id === oid); if (o) o.votes = Math.max(0, o.votes - 1); });
          poll.votes[username] = [optionId]; opt.votes++;
        }
      }
      poll.totalVotes = Object.keys(poll.votes).length;
      saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
      broadcast({ type: 'poll-vote', pollId: poll.id, poll, from: username }, username);
      return;
    }

    if (m.type === 'poll-delete') {
      const { pollId, username } = m;
      const idx = polls.findIndex(p => p.id === pollId);
      if (idx < 0) return;
      if (username && polls[idx].author !== username) return;
      polls.splice(idx, 1);
      saveQueue.enqueue('polls', FILES.polls, () => polls.slice(-2000));
      broadcast({ type: 'poll-delete', pollId, from: username }, username);
      return;
    }

    /* ============================================================
       ЗАПИСЬ ЗВОНКОВ через writeStream (RAM = 0)
       ============================================================ */
    if (m.type === 'rec-start') {
      const callId = m.callId || uuid();
      // Закрываем предыдущую если была
      const old = activeRecordings.get(callId);
      if (old) old.abort();

      const rec = new RecordingStream(callId, m.mime || 'video/webm', m.from, m.to || null);
      activeRecordings.set(callId, rec);
      log.info(`🎥 rec-start ${m.from} → ${m.to || '*'} callId=${callId}`);
      const p = { type: 'rec-started', callId, from: m.from, startedAt: Date.now() };
      if (m.to) sendTo(m.to, p);
      send(ws, p);
      return;
    }

    if (m.type === 'rec-chunk') {
      const r = activeRecordings.get(m.callId);
      if (!r) return send(ws, { type: 'rec-error', callId: m.callId, message: 'Запись не найдена' });
      const ok = r.write(m.data);
      if (!ok) {
        activeRecordings.delete(m.callId);
        return send(ws, { type: 'rec-error', callId: m.callId, message: 'Превышен лимит' });
      }
      if (r.size % (1024 * 1024) < 1024) {
        send(ws, { type: 'rec-progress', callId: m.callId, received: r.size });
      }
      return;
    }

    if (m.type === 'rec-stop') {
      const r = activeRecordings.get(m.callId);
      if (!r) return send(ws, { type: 'rec-error', callId: m.callId, message: 'Не найдено' });
      activeRecordings.delete(m.callId);

      r.finalize()
        .then(info => {
          if (!info) {
            return send(ws, { type: 'rec-error', callId: m.callId, message: 'Пустая запись' });
          }
          invalidateDirCache(DIRS.recordings);
          log.info(`🎥 Запись сохранена: ${info.filename} (${(info.size / 1024 / 1024).toFixed(2)} МБ)`);
          const p = { type: 'rec-ready', callId: m.callId, recording: info };
          send(ws, p);
          if (r.to) {
            sendTo(r.to, p);
            sendTo(r.to, {
              type: 'chat-message',
              id: 'rec_' + m.callId,
              from: r.from, to: r.to, fromName: r.from,
              media_type: 'video', type: 'video',
              content: '🎥 Запись звонка',
              media_url: info.url,
              file_name: info.filename, file_size: info.size,
              media_mime: r.mime,
              created_at: now()
            });
          }
        })
        .catch(e => {
          log.error('rec-stop finalize', e.message);
          send(ws, { type: 'rec-error', callId: m.callId, message: e.message });
        });
      return;
    }

    if (m.type === 'rec-cancel') {
      const r = activeRecordings.get(m.callId);
      if (r) r.abort();
      activeRecordings.delete(m.callId);
      if (m.to) sendTo(m.to, { type: 'rec-cancelled', callId: m.callId });
      send(ws, { type: 'rec-cancelled', callId: m.callId });
      return;
    }

    /* ---- сигналинг звонков ---- */
    if (['call-invite', 'offer', 'answer', 'candidate', 'hangup'].includes(m.type)) {
      if (m.to) sendTo(m.to, m);
      return;
    }

    /* ---- адресные ---- */
    const addressed = new Set([
      'group-invite', 'group-join', 'qr-contact-seen',
      'sync-push', 'sync-request',
      'search-users-results', 'search-messages-results',
      'social-notification', 'message-edited', 'message-deleted',
      'story-viewed', 'block-user', 'unblock-user'
    ]);
    if (addressed.has(m.type)) {
      if (m.to) sendTo(m.to, m);
      else broadcast(m, m.from);
      return;
    }

    /* ---- broadcast ---- */
    if (m.broadcast || ['story-broadcast', 'post-broadcast', 'comment-broadcast'].includes(m.type)) {
      broadcast(m, m.from);
      return;
    }

    /* ---- fallback ---- */
    if (m.to) sendTo(m.to, m);
    else broadcast(m, m.from);
  });

  ws.on('close', () => detach(ws));
  ws.on('error', (e) => log.error('ws', e.message));
});

/* ============================================================
   27. ПЕРИОДИЧЕСКАЯ ОЧИСТКА
   ============================================================ */
setInterval(() => {
  const nowMs = Date.now();
  for (const [callId, rec] of activeRecordings) {
    if (nowMs - rec.lastChunkAt > 3600000) {
      log.warn(`rec-auto-cleanup ${callId}`);
      rec.abort();
      activeRecordings.delete(callId);
    }
  }
  const fresh = stories.filter(s => new Date(s.expiresAt) > new Date());
  if (fresh.length !== stories.length) {
    stories = fresh;
    saveQueue.enqueue('stories', FILES.stories, () => stories.slice(-5000));
  }
}, 60000).unref();

/* ============================================================
   28. GRACEFUL SHUTDOWN
   ============================================================ */
let _shuttingDown = false;
async function shutdown() {
  if (_shuttingDown) return;
  _shuttingDown = true;
  log.info('shutting down…');

  // Форсируем сохранение в очередь и дожидаемся
  saveQueue.enqueue('users', FILES.users, () => users);
  saveQueue.enqueue('messages', FILES.messages, () => messages.slice(-20000));
  saveQueue.enqueue('polls', FILES.polls, () => polls);
  saveQueue.enqueue('stories', FILES.stories, () => stories);
  saveQueue.enqueue('posts', FILES.posts, () => posts);
  saveQueue.enqueue('groups', FILES.groups, () => groups);
  saveQueue.enqueue('contacts', FILES.contacts, () => contacts);
  saveQueue.enqueue('blocked', FILES.blocked, () => blocked);
  saveQueue.enqueue('logs', FILES.logs, () => logs.slice(-20000));
  saveQueue.enqueue('fingerprints', FILES.fingerprints, () => fingerprints);

  // Завершаем активные записи
  for (const [cid, rec] of activeRecordings) {
    try { await rec.finalize(); }
    catch (e) { log.error('shutdown rec', e.message); }
  }
  activeRecordings.clear();

  // Дожидаемся очереди
  try { await saveQueue.flush(); }
  catch (e) { log.error('saveQueue.flush', e.message); }

  try { wss.close(); }       catch {}
  try { apiServer.close(); } catch {}
  setTimeout(() => process.exit(0), 500);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (e) => log.error('uncaught', e.message));
process.on('unhandledRejection', (e) => log.error('unhandledRejection', String(e)));

/* ============================================================
   29. СТАРТ
   ============================================================ */
(async () => {
  try {
    await initDirs();
    await loadAll();

    apiServer.listen(HTTP_PORT, () => log.info(`API  → http://localhost:${HTTP_PORT}`));
    wss.on('listening', () => log.info(`WS   → ws://localhost:${WS_PORT}/ws`));

    log.info('✅ Huddle Server v2.2 (non-blocking) готов.');
    log.info(`   API :        http://localhost:${HTTP_PORT}`);
    log.info(`   WS  :        ws://localhost:${WS_PORT}/ws`);
    log.info(`   Данные:      ${DIRS.root}`);
    log.info(`   Голосовые:   ${DIRS.voices}`);
    log.info(`   Фото:        ${DIRS.photos}`);
    log.info(`   Видео:       ${DIRS.videos}`);
    log.info(`   Файлы:       ${DIRS.files}`);
    log.info(`   Записи:      ${DIRS.recordings}`);

    // Статистика в фоне (не блокирует старт)
    setImmediate(async () => {
      try {
        const [voices, photos, videos, filesArr, recordings] = await Promise.all([
          listDirCached(DIRS.voices),
          listDirCached(DIRS.photos),
          listDirCached(DIRS.videos),
          listDirCached(DIRS.files),
          listDirCached(DIRS.recordings)
        ]);
        log.info(`   Медиа: voice=${voices.length} photo=${photos.length} video=${videos.length} files=${filesArr.length}`);
        log.info(`   Записей: ${recordings.filter(f => /\.(webm|mp4)$/i.test(f)).length}`);
      } catch {}
    });
  } catch (e) {
    log.error('boot', e.message);
    process.exit(1);
  }
})();