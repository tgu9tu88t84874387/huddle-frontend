/**
 * Huddle — signal.js
 * WebSocket-клиент для real-time: сообщения, звонки, печатает, presence,
 * реакции, статусы прочтения. Заменяет BroadcastChannel на реальную сеть.
 */

import { api } from './api.js';

// ============ Состояние ============
const state = {
  ws: null,
  status: 'disconnected', // disconnected | connecting | connected | reconnecting
  attempts: 0,
  maxAttempts: 0,          // 0 = бесконечно
  reconnectTimer: null,
  pingTimer: null,
  pongTimeout: null,
  lastPong: 0,
  latency: 0,
  outbox: [],              // очередь сообщений, пока нет связи
  handlers: new Map(),     // type -> Set<fn>
  userId: null,
  url: null,
  paused: false,
  autoReconnect: true
};

// ============ URL ============
function defaultUrl() {
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  const host = location.hostname || 'localhost';
  return `${proto}${host}:8787/ws`;
}

// ============ Подключение ============
export function connect(opts = {}) {
  state.url = opts.url || localStorage.getItem('huddle_ws_url') || defaultUrl();
  state.userId = opts.userId || null;
  state.paused = false;

  if (state.ws && (state.ws.readyState === 0 || state.ws.readyState === 1)) return;

  state.status = 'connecting';
  emit('status', { status: 'connecting', url: state.url });

  try {
    state.ws = new WebSocket(state.url);
  } catch (e) {
    emit('error', { message: e.message });
    scheduleReconnect();
    return;
  }

  state.ws.onopen = () => {
    state.status = 'connected';
    state.attempts = 0;
    state.lastPong = Date.now();
    emit('status', { status: 'connected', url: state.url });

    // Регистрация
    if (state.userId) {
      send({ type: 'register', username: state.userId });
    }

    startPing();
    flushOutbox();
  };

  state.ws.onmessage = (e) => {
    let msg;
    try { msg = JSON.parse(e.data); }
    catch (err) { return; }

    // Понг — обновляем latency
    if (msg.type === 'pong') {
      state.lastPong = Date.now();
      state.latency = msg.t ? Date.now() - msg.t : 0;
      emit('latency', { ms: state.latency });
      return;
    }

    // Пинг от сервера — отвечаем
    if (msg.type === 'ping') {
      send({ type: 'pong', to: msg.from, t: msg.t });
      return;
    }

    // Раздаём всем подписчикам
    emit('message', msg);
    emit(msg.type, msg);
  };

  state.ws.onclose = (e) => {
    state.status = 'disconnected';
    emit('status', { status: 'disconnected', code: e.code, reason: e.reason });
    stopPing();
    if (state.autoReconnect && !state.paused) scheduleReconnect();
  };

  state.ws.onerror = () => {
    emit('error', { message: 'WebSocket error' });
  };
}

// ============ Отключение ============
export function disconnect() {
  state.paused = true;
  state.autoReconnect = false;
  stopPing();
  clearTimeout(state.reconnectTimer);
  if (state.ws) {
    try { state.ws.close(); } catch (e) {}
    state.ws = null;
  }
  state.status = 'disconnected';
  emit('status', { status: 'disconnected' });
}

// ============ Переподключение ============
function scheduleReconnect() {
  if (state.paused || !state.autoReconnect) return;

  state.attempts++;
  if (state.maxAttempts > 0 && state.attempts >= state.maxAttempts) {
    emit('status', { status: 'failed', attempts: state.attempts });
    return;
  }

  // Экспоненциальная задержка: 1с, 2с, 4с, 8с, 16с, 30с
  const delay = Math.min(1000 * Math.pow(2, state.attempts - 1), 30000);
  state.status = 'reconnecting';
  emit('status', { status: 'reconnecting', attempt: state.attempts, delay });

  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = setTimeout(() => connect({ url: state.url, userId: state.userId }), delay);
}

export function reconnect() {
  state.attempts = 0;
  state.paused = false;
  state.autoReconnect = true;
  if (state.ws) { try { state.ws.close(); } catch (e) {} state.ws = null; }
  connect({ url: state.url, userId: state.userId });
}

export function pause() { state.paused = true; disconnect(); }
export function resume() { state.paused = false; state.autoReconnect = true; connect({ url: state.url, userId: state.userId }); }

// ============ Пинг ============
function startPing() {
  stopPing();
  state.pingTimer = setInterval(() => {
    if (state.ws && state.ws.readyState === 1) {
      send({ type: 'ping', t: Date.now() });
    }
  }, 15000);
}

function stopPing() {
  clearInterval(state.pingTimer);
  state.pingTimer = null;
}

// ============ Отправка ============
export function send(msg) {
  if (!msg) return false;

  if (state.ws && state.ws.readyState === 1) {
    try {
      state.ws.send(JSON.stringify(msg));
      return true;
    } catch (e) {
      emit('error', { message: e.message });
    }
  }

  // В очередь — уйдёт при переподключении
  if (msg.type !== 'ping' && msg.type !== 'pong') {
    state.outbox.push(msg);
    if (state.outbox.length > 200) state.outbox.shift();
  }
  return false;
}

function flushOutbox() {
  if (!state.outbox.length) return;
  const q = state.outbox.splice(0);
  q.forEach((m) => send(m));
}

// ============ Подписка ============
export function on(type, fn) {
  if (!state.handlers.has(type)) state.handlers.set(type, new Set());
  state.handlers.get(type).add(fn);
  return () => off(type, fn);
}

export function off(type, fn) {
  const set = state.handlers.get(type);
  if (set) set.delete(fn);
}

function emit(type, payload) {
  const set = state.handlers.get(type);
  if (set) set.forEach((fn) => { try { fn(payload); } catch (e) { console.error(e); } });

  // Дублируем в CustomEvent, чтобы можно было слушать из HTML
  window.dispatchEvent(new CustomEvent('huddle:' + type, { detail: payload }));
}

// ============ Статус ============
export function getStatus() {
  return {
    status: state.status,
    url: state.url,
    attempts: state.attempts,
    latency: state.latency,
    outbox: state.outbox.length,
    paused: state.paused
  };
}

export function setUrl(url) {
  state.url = url;
  localStorage.setItem('huddle_ws_url', url);
}

export function setMaxAttempts(n) {
  state.maxAttempts = Number(n) || 0;
}

// ============ Обёртки для типовых событий ============
export const signal = {
  // Сообщения
  sendMessage: (payload) => send({ type: 'chat-message', ...payload }),
  sendTyping: (chatId) => send({ type: 'typing', chatId }),
  sendRead: (chatId, messageIds) => send({ type: 'read', chatId, messageIds }),
  sendReaction: (chatId, messageId, emoji) => send({ type: 'reaction', chatId, messageId, emoji }),

  // Звонки
  callInvite: (to, video) => send({ type: 'call-invite', to, video }),
  callAccept: (to) => send({ type: 'call-accept', to }),
  callDecline: (to) => send({ type: 'call-decline', to }),
  callHangup: (to) => send({ type: 'hangup', to }),
  offer: (to, offer) => send({ type: 'offer', to, offer }),
  answer: (to, answer) => send({ type: 'answer', to, answer }),
  candidate: (to, candidate) => send({ type: 'candidate', to, candidate }),

  // Истории
  broadcastStory: (story) => send({ type: 'story-broadcast', story }),

  // Посты
  broadcastPost: (post) => send({ type: 'post-broadcast', post }),

  // Поиск
  searchUsers: (q, reqId) => send({ type: 'search-users', q, reqId }),
  searchMessages: (chatId, q, reqId) => send({ type: 'search-messages', chatId, q, reqId }),

  // Sync
  syncPush: (data) => send({ type: 'sync-push', data }),
  syncRequest: () => send({ type: 'sync-request' }),

  // Ping
  pingAll: () => send({ type: 'ping-all' }),

  // Низкоуровневый
  send,
  on,
  off,
  connect,
  disconnect,
  reconnect,
  pause,
  resume,
  getStatus,
  setUrl,
  setMaxAttempts
};

export default signal;