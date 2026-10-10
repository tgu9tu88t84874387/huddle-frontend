/* ═══════════════════════════════════════════════════════════
   Huddle Desktop — дополнительная логика для ПК (≥ 900px)
   Создаёт: левый рельс, правую инфо-панель, подсветку чата
   ═══════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ---------- Проверка: ПК или нет ---------- */
  const isDesktop = () => window.matchMedia('(min-width: 900px)').matches;
  const isWide    = () => window.matchMedia('(min-width: 1400px)').matches;

  if (!isDesktop()) {
    console.log('📱 Mobile mode — desktop.js не активен');
    return;
  }

  /* ============================================================
     1. ЛЕВЫЙ РЕЛЬС НАВИГАЦИИ
     ============================================================ */
  function injectRail() {
    if (document.querySelector('.desktop-rail')) return;
    const app = document.getElementById('app');
    if (!app) return;

    const rail = document.createElement('div');
    rail.className = 'desktop-rail';
    rail.innerHTML = `
      <div class="rail-item active" data-tab="chat" title="Чаты">💬</div>
      <div class="rail-item" data-tab="feed" title="Лента">📰</div>
      <div class="rail-item" data-tab="posts" title="Посты">📝</div>
      <div class="rail-item" data-tab="communities" title="Сообщества">🌐</div>
      <div class="rail-item" data-tab="search" title="Поиск">🔍</div>
      <div class="rail-spacer"></div>
      <div class="rail-divider"></div>
      <div class="rail-item" data-action="signal" title="Signal Server">📡</div>
      <div class="rail-item" data-action="theme" title="Сменить тему">🌙</div>
      <div class="rail-avatar" id="rail-avatar" title="Профиль">U</div>
    `;
    app.insertBefore(rail, app.firstChild);

    // Клики по вкладкам
    rail.querySelectorAll('.rail-item[data-tab]').forEach(item => {
      item.onclick = () => {
        const tab = item.getAttribute('data-tab');
        showScreen(tab);
        rail.querySelectorAll('.rail-item').forEach(x => x.classList.remove('active'));
        item.classList.add('active');
      };
    });

    // Клики по действиям
    rail.querySelectorAll('.rail-item[data-action]').forEach(item => {
      item.onclick = () => {
        const act = item.getAttribute('data-action');
        if (act === 'signal') openSignalScreen();
        if (act === 'theme')  toggleTheme();
      };
    });

    // Аватар
    const av = document.getElementById('rail-avatar');
    if (av) av.onclick = () => openProfile();

    updateRailAvatar();
  }

  function updateRailAvatar() {
    const av = document.getElementById('rail-avatar');
    if (!av || !window.currentUser) return;
    const img = localStorage.getItem('huddleAvatar_' + currentUser.username);
    if (img) {
      av.style.backgroundImage = 'url(' + img + ')';
      av.textContent = '';
    } else {
      av.style.backgroundImage = '';
      av.style.background = 'linear-gradient(135deg,#6d5dfc,#a855f7)';
      av.textContent = (currentUser.username[0] || '?').toUpperCase();
    }
  }
  window.updateRailAvatar = updateRailAvatar;

  /* ============================================================
     2. ПРАВАЯ ИНФО-ПАНЕЛЬ
     ============================================================ */
  function injectInfoPanel() {
    if (document.getElementById('desktop-info-panel')) return;
    const app = document.getElementById('app');
    if (!app) return;

    const panel = document.createElement('div');
    panel.id = 'desktop-info-panel';
    panel.innerHTML = `
      <div class="dip-cover"></div>
      <div class="dip-avatar" id="dip-avatar">?</div>
      <div class="dip-body">
        <div class="dip-name" id="dip-name">—</div>
        <div class="dip-handle" id="dip-handle">@—</div>

        <div class="dip-stats">
          <div class="dip-stat"><b id="dip-msg-count">0</b><span>Сообщений</span></div>
          <div class="dip-stat"><b id="dip-media-count">0</b><span>Медиа</span></div>
          <div class="dip-stat"><b id="dip-days">0</b><span>Дней</span></div>
        </div>

        <div class="dip-section-label">Действия</div>
        <div class="dip-row" onclick="openChatSearchModal()">
          <span class="dip-ico">🔍</span><span>Поиск в чате</span>
        </div>
        <div class="dip-row" onclick="openChatMediaModal()">
          <span class="dip-ico">🖼️</span><span>Медиа и файлы</span>
        </div>
        <div class="dip-row" onclick="exportChatHistory()">
          <span class="dip-ico">📤</span><span>Экспорт переписки</span>
        </div>
        <div class="dip-row" onclick="openChatMenu()">
          <span class="dip-ico">⚙️</span><span>Все действия</span>
        </div>

        <div class="dip-section-label">Уведомления</div>
        <div class="dip-row" onclick="toggleMuteChat()">
          <span class="dip-ico">🔕</span><span>Выключить звук</span>
        </div>
        <div class="dip-row" onclick="markChatUnread()">
          <span class="dip-ico">💬</span><span>Пометить непрочитанным</span>
        </div>

        <div class="dip-section-label" style="color:#ee8585">Опасные действия</div>
        <div class="dip-row danger" onclick="blockUser()">
          <span class="dip-ico">🚫</span><span>Заблокировать</span>
        </div>
        <div class="dip-row danger" onclick="deleteChat()">
          <span class="dip-ico">🗑</span><span>Удалить чат</span>
        </div>
      </div>
    `;
    app.appendChild(panel);
  }

  function updateInfoPanel() {
    if (!window.activeChat) {
      document.getElementById('desktop-info-panel')?.classList.remove('visible');
      return;
    }
    const panel = document.getElementById('desktop-info-panel');
    if (!panel) return;

    // Показать только на широких экранах
    if (isWide()) panel.classList.add('visible');
    else panel.classList.remove('visible');

    const name = activeChat.name || '—';
    const username = activeChat.username || activeChat.contactUsername || name;

    // Аватар
    const av = document.getElementById('dip-avatar');
    if (av) {
      const img = localStorage.getItem('huddleAvatar_' + name);
      if (img) {
        av.style.backgroundImage = 'url(' + img + ')';
        av.textContent = '';
      } else {
        av.style.backgroundImage = '';
        av.style.background = 'linear-gradient(135deg,' + colorFrom(name) + ',' + colorFrom(name) + 'dd)';
        av.textContent = name[0].toUpperCase();
      }
    }

    // Имя
    const nm = document.getElementById('dip-name');
    const hd = document.getElementById('dip-handle');
    if (nm) nm.textContent = name;
    if (hd) hd.textContent = '@' + username;

    // Статистика
    try {
      const msgs = getMessages(activeChat.id) || [];
      const mediaCount = msgs.filter(m => m.type === 'photo' || m.type === 'circle' || m.type === 'video').length;
      const firstMsg = msgs[0] && msgs[0].created_at ? new Date(msgs[0].created_at) : new Date();
      const days = Math.max(1, Math.floor((Date.now() - firstMsg.getTime()) / 86400000));

      const mc = document.getElementById('dip-msg-count');
      const md = document.getElementById('dip-media-count');
      const dd = document.getElementById('dip-days');
      if (mc) mc.textContent = msgs.length;
      if (md) md.textContent = mediaCount;
      if (dd) dd.textContent = days;
    } catch (e) {}
  }
  window.updateInfoPanel = updateInfoPanel;

  /* ============================================================
     3. ПОДСВЕТКА АКТИВНОГО ЧАТА
     ============================================================ */
  function highlightActiveChat(chatId) {
    if (!chatId) return;
    document.querySelectorAll('#chat-list .chat-item').forEach(it => {
      it.classList.toggle('active', it.dataset.chatId === chatId);
    });
  }
  window.highlightActiveChat = highlightActiveChat;

  /* ============================================================
     4. ХУКИ НА СУЩЕСТВУЮЩИЕ ФУНКЦИИ
     ============================================================ */

  // --- openChat: показать инфо-панель + подсветить ---
  const _oldOpenChat = window.openChat;
  if (typeof _oldOpenChat === 'function') {
    window.openChat = function (c) {
      const r = _oldOpenChat.apply(this, arguments);
      setTimeout(() => {
        updateInfoPanel();
        highlightActiveChat(c && c.id);
      }, 60);
      return r;
    };
  }

  // --- backToChats: скрыть инфо-панель ---
  const _oldBackToChats = window.backToChats;
  if (typeof _oldBackToChats === 'function') {
    window.backToChats = function () {
      const r = _oldBackToChats.apply(this, arguments);
      document.getElementById('desktop-info-panel')?.classList.remove('visible');
      document.querySelectorAll('#chat-list .chat-item').forEach(it => it.classList.remove('active'));
      return r;
    };
  }

  // --- showScreen: синхронизировать рельс ---
  const _oldShowScreen = window.showScreen;
  if (typeof _oldShowScreen === 'function') {
    window.showScreen = function (name) {
      const r = _oldShowScreen.apply(this, arguments);
      document.querySelectorAll('.desktop-rail .rail-item[data-tab]').forEach(x => {
        x.classList.toggle('active', x.getAttribute('data-tab') === name);
      });
      // Инфо-панель нужна только в чате
      if (name !== 'chat') {
        document.getElementById('desktop-info-panel')?.classList.remove('visible');
      }
      return r;
    };
  }

  // --- pfRefresh: обновить аватар в рельсе ---
  const _oldPfRefresh = window.pfRefresh;
  if (typeof _oldPfRefresh === 'function') {
    window.pfRefresh = function () {
      _oldPfRefresh.apply(this, arguments);
      updateRailAvatar();
    };
  }

  // --- loadChats: после загрузки подсветить активный чат ---
  const _oldLoadChats = window.loadChats;
  if (typeof _oldLoadChats === 'function') {
    window.loadChats = function () {
      const r = _oldLoadChats.apply(this, arguments);
      if (window.activeChat) setTimeout(() => highlightActiveChat(activeChat.id), 50);
      return r;
    };
  }

  // --- renderMessages: обновить статистику в инфо-панели ---
  const _oldRenderMessages = window.renderMessages;
  if (typeof _oldRenderMessages === 'function') {
    window.renderMessages = function () {
      const r = _oldRenderMessages.apply(this, arguments);
      setTimeout(updateInfoPanel, 30);
      return r;
    };
  }

  /* ============================================================
     5. ГОРЯЧИЕ КЛАВИШИ (только ПК)
     ============================================================ */
  document.addEventListener('keydown', (e) => {
    if (!isDesktop()) return;

    // Ctrl+K — поиск
    if (e.ctrlKey && e.key === 'k') {
      e.preventDefault();
      showScreen('search');
      setTimeout(() => document.getElementById('search-input')?.focus(), 100);
      return;
    }
    // Ctrl+F — поиск в чате
    if (e.ctrlKey && e.key === 'f' && activeChat) {
      e.preventDefault();
      if (typeof openChatSearchModal === 'function') openChatSearchModal();
      return;
    }
    // Esc — закрыть всё
    if (e.key === 'Escape') {
      document.querySelectorAll('.modal-overlay.visible').forEach(m => m.classList.remove('visible'));
      document.getElementById('call-menu-sheet')?.classList.remove('visible');
      document.getElementById('search-type-menu')?.classList.remove('open');
    }
    // Ctrl+1..5 — быстрая навигация
    if (e.ctrlKey && ['1', '2', '3', '4', '5'].includes(e.key)) {
      e.preventDefault();
      const map = { '1': 'chat', '2': 'feed', '3': 'posts', '4': 'communities', '5': 'search' };
      showScreen(map[e.key]);
    }
  });

  /* ============================================================
     6. РЕАКЦИЯ НА РЕСАЙЗ ОКНА
     ============================================================ */
  let resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      if (isDesktop()) {
        injectRail();
        injectInfoPanel();
        updateRailAvatar();
        updateInfoPanel();
      } else {
        // Свернули окно до мобильного — убираем десктопные элементы
        document.querySelector('.desktop-rail')?.remove();
        document.getElementById('desktop-info-panel')?.remove();
      }
    }, 200);
  });

  /* ============================================================
     7. ИНИЦИАЛИЗАЦИЯ
     ============================================================ */
  function init() {
    if (!isDesktop()) return;
    injectRail();
    injectInfoPanel();
    if (window.currentUser) updateRailAvatar();
    console.log('🖥️ Desktop layout activated');
  }

  if (document.readyState !== 'loading') {
    setTimeout(init, 400);
  } else {
    document.addEventListener('DOMContentLoaded', () => setTimeout(init, 400));
  }

  // Дополнительный вызов после логина (когда currentUser появляется)
  const _oldCompleteLogin = window.completeLogin;
  if (typeof _oldCompleteLogin === 'function') {
    window.completeLogin = function () {
      const r = _oldCompleteLogin.apply(this, arguments);
      setTimeout(() => {
        injectRail();
        injectInfoPanel();
        updateRailAvatar();
      }, 500);
      return r;
    };
  }

  console.log('✅ Desktop module loaded');
})();