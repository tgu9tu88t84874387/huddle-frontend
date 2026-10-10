/* ═══════════════════════════════════════════════════════
   HUDDLE Service Worker v4.5.0
   - Кэширование: сайт работает оффлайн
   - Установка как PWA-приложение
   - Автообновление при смене версии
   ═══════════════════════════════════════════════════════ */

const CACHE_NAME = 'huddle-v4.5.0';
const CORE_FILES = [
  './',
  './index.html',
  './manifest.json'
];

/* ─── INSTALL: кэшируем базовые файлы ─── */
self.addEventListener('install', (event) => {
  console.log('🔧 SW install: ' + CACHE_NAME);
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        return Promise.all(
          CORE_FILES.map(file =>
            cache.add(file).catch(err => console.warn('⚠️ Не удалось закэшировать ' + file, err))
          )
        );
      })
      .then(() => self.skipWaiting())
  );
});

/* ─── ACTIVATE: удаляем старые кэши ─── */
self.addEventListener('activate', (event) => {
  console.log('✅ SW activate');
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => {
          console.log('🗑 Удаляю старый кэш:', k);
          return caches.delete(k);
        })
      ))
      .then(() => self.clients.claim())
  );
});

/* ─── FETCH: сеть → если упала, кэш ─── */
self.addEventListener('fetch', (event) => {
  const req = event.request;

  // Только GET
  if (req.method !== 'GET') return;

  // Не трогаем API и WebSocket
  if (req.url.includes('/api/')) return;
  if (req.url.startsWith('ws:') || req.url.startsWith('wss:')) return;

  // Не кэшируем внешние сервисы
  if (req.url.includes('qrserver.com')) return;
  if (req.url.includes('google.com')) return;
  if (req.url.includes('gstatic.com')) return;
  if (req.url.includes('stun.l.google')) return;

  event.respondWith(
    fetch(req)
      .then(resp => {
        // Кэшируем успешные GET-ответы того же origin
        if (resp && resp.status === 200 && (resp.type === 'basic' || resp.type === 'default')) {
          const clone = resp.clone();
          caches.open(CACHE_NAME)
            .then(cache => cache.put(req, clone))
            .catch(() => {});
        }
        return resp;
      })
      .catch(() => {
        // Сеть упала — ищем в кэше
        return caches.match(req).then(cached => {
          if (cached) return cached;

          // Навигация (переходы по страницам) — отдаём index.html
          if (req.mode === 'navigate' || req.destination === 'document') {
            return caches.match('./index.html')
              .then(fallback => fallback || new Response('Оффлайн', {
                status: 503,
                headers: { 'Content-Type': 'text/plain; charset=utf-8' }
              }));
          }

          // Всё остальное — 503
          return new Response('Оффлайн', {
            status: 503,
            statusText: 'Offline',
            headers: { 'Content-Type': 'text/plain; charset=utf-8' }
          });
        });
      })
  );
});

/* ─── MESSAGE: управление из UI ─── */
self.addEventListener('message', (event) => {
  if (!event.data) return;

  // Пропустить ожидание (принудительное обновление)
  if (event.data === 'skipWaiting' || event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  // Очистить весь кэш (например, кнопка «Сбросить»)
  if (event.data === 'clearCache' || event.data.type === 'CLEAR_CACHE') {
    caches.keys().then(keys => keys.forEach(k => caches.delete(k)));
  }
});

console.log('✅ Huddle Service Worker загружен: ' + CACHE_NAME);