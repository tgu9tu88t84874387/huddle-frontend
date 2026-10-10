/**
 * Huddle — media.js
 * Работа с медиа: загрузка на сервер (S3), скачивание, кэш в IndexedDB,
 * превью, чанковая загрузка для больших файлов, прогресс.
 */

import { api } from './api.js';

// ============ IndexedDB ============
const DB_NAME = 'huddle_media';
const DB_VERSION = 1;
const STORE_BLOBS = 'blobs';   // кэш скачанных файлов
const STORE_KEYS = 'keys';     // ключи шифрования (E2E)

let _dbPromise = null;

function openDb() {
  if (_dbPromise) return _dbPromise;
  _dbPromise = new Promise((resolve, reject) => {
    if (!('indexedDB' in window)) return reject(new Error('IndexedDB not supported'));
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_BLOBS)) {
        const s = db.createObjectStore(STORE_BLOBS, { keyPath: 'key' });
        s.createIndex('createdAt', 'createdAt');
      }
      if (!db.objectStoreNames.contains(STORE_KEYS)) {
        db.createObjectStore(STORE_KEYS, { keyPath: 'key' });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  return _dbPromise;
}

async function idbPut(store, value) {
  const db = await openDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).put(value);
    tx.oncomplete = () => res(true);
    tx.onerror = () => rej(tx.error);
  });
}

async function idbGet(store, key) {
  const db = await openDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readonly');
    const r = tx.objectStore(store).get(key);
    r.onsuccess = () => res(r.result || null);
    r.onerror = () => rej(r.error);
  });
}

async function idbDelete(store, key) {
  const db = await openDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).delete(key);
    tx.oncomplete = () => res(true);
    tx.onerror = () => rej(tx.error);
  });
}

async function idbClear(store) {
  const db = await openDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).clear();
    tx.oncomplete = () => res(true);
    tx.onerror = () => rej(tx.error);
  });
}

async function idbCount(store) {
  const db = await openDb();
  return new Promise((res) => {
    const tx = db.transaction(store, 'readonly');
    const r = tx.objectStore(store).count();
    r.onsuccess = () => res(r.result);
    r.onerror = () => res(0);
  });
}

// ============ Ключи ============
export function genMediaKey() {
  return 'm_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
}

// ============ Кэш blob-URL ============
const _urlCache = new Map(); // key -> { url, size, mime, createdAt }

export function getCachedUrl(key) {
  return _urlCache.get(key)?.url || null;
}

// ============ Скачивание ============
async function fetchWithProgress(url, onProgress) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('HTTP ' + res.status);
  const total = Number(res.headers.get('content-length')) || 0;
  const reader = res.body.getReader();
  const chunks = [];
  let received = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    if (onProgress && total) onProgress(received / total, received, total);
  }
  return new Blob(chunks, { type: res.headers.get('content-type') || 'application/octet-stream' });
}

// ============ Публичный API ============

/**
 * Загрузить файл на сервер.
 * @param {File} file
 * @param {(p:number)=>void} onProgress
 * @param {boolean} encrypt — шифровать (E2E)
 * @returns {Promise<{key:string, url:string, mime:string, size:number}>}
 */
export async function uploadMedia(file, onProgress, encrypt = false) {
  if (!file) throw new Error('No file');

  let blob = file;
  let mime = file.type || 'application/octet-stream';

  // Сжатие фото >1 МБ
  if (/^image\//.test(mime) && file.size > 1024 * 1024) {
    blob = await compressImage(file, 1920, 0.85);
    mime = 'image/jpeg';
  }

  // E2E-шифрование
  if (encrypt) {
    blob = await encryptBlob(blob);
    mime = 'application/octet-stream';
  }

  // Маленькие файлы — одним куском, большие — чанками
  const CHUNK = 5 * 1024 * 1024; // 5 МБ
  let result;

  if (blob.size <= CHUNK) {
    result = await uploadSingle(blob, mime, onProgress);
  } else {
    result = await uploadChunked(blob, mime, onProgress);
  }

  // Кэшируем локально
  const key = result.key;
  const url = URL.createObjectURL(blob);
  _urlCache.set(key, { url, size: blob.size, mime, createdAt: Date.now() });
  await idbPut(STORE_BLOBS, { key, blob, mime, size: blob.size, createdAt: Date.now() });

  return { ...result, size: blob.size, mime };
}

async function uploadSingle(blob, mime, onProgress) {
  const form = new FormData();
  form.append('file', blob, 'file');
  const token = localStorage.getItem('huddle_token');
  const res = await new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', api.baseUrl + '/api/media/upload');
    if (token) xhr.setRequestHeader('Authorization', 'Bearer ' + token);
    xhr.upload.onprogress = (e) => {
      if (onProgress && e.lengthComputable) onProgress(e.loaded / e.total, e.loaded, e.total);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try { resolve(JSON.parse(xhr.responseText)); }
        catch (e) { reject(new Error('Bad JSON')); }
      } else reject(new Error('HTTP ' + xhr.status));
    };
    xhr.onerror = () => reject(new Error('Network error'));
    xhr.send(form);
  });
  return { key: res.key, url: res.url, mime };
}

async function uploadChunked(blob, mime, onProgress) {
  const CHUNK = 5 * 1024 * 1024;
  const total = blob.size;
  const uploadId = genMediaKey();
  const chunks = Math.ceil(total / CHUNK);
  let uploaded = 0;

  // 1. Инициализация
  const init = await api.post('/api/media/multipart/init', {
    uploadId, size: total, mime, chunks
  });

  // 2. Загрузка чанков
  for (let i = 0; i < chunks; i++) {
    const start = i * CHUNK;
    const end = Math.min(start + CHUNK, total);
    const part = blob.slice(start, end);
    const form = new FormData();
    form.append('part', part, 'part' + i);
    form.append('uploadId', uploadId);
    form.append('partNumber', String(i + 1));

    const token = localStorage.getItem('huddle_token');
    await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', api.baseUrl + '/api/media/multipart/part');
      if (token) xhr.setRequestHeader('Authorization', 'Bearer ' + token);
      xhr.upload.onprogress = (e) => {
        const cur = uploaded + e.loaded;
        if (onProgress && total) onProgress(cur / total, cur, total);
      };
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else reject(new Error('HTTP ' + xhr.status));
      };
      xhr.onerror = () => reject(new Error('Network error'));
      xhr.send(form);
    });
    uploaded += (end - start);
  }

  // 3. Завершение
  const done = await api.post('/api/media/multipart/complete', { uploadId, chunks });
  return { key: done.key, url: done.url, mime };
}

/**
 * Получить blob медиа (из кэша или скачать).
 */
export async function getMediaBlob(key, url, onProgress) {
  // 1. Кэш URL
  if (_urlCache.has(key)) return _urlCache.get(key).url;

  // 2. IndexedDB
  const cached = await idbGet(STORE_BLOBS, key);
  if (cached?.blob) {
    const url2 = URL.createObjectURL(cached.blob);
    _urlCache.set(key, { url: url2, size: cached.size, mime: cached.mime, createdAt: cached.createdAt });
    return url2;
  }

  // 3. Скачать
  if (!url) throw new Error('No url for media');
  const blob = await fetchWithProgress(url, onProgress);
  const url3 = URL.createObjectURL(blob);
  _urlCache.set(key, { url: url3, size: blob.size, mime: blob.type, createdAt: Date.now() });
  await idbPut(STORE_BLOBS, { key, blob, mime: blob.type, size: blob.size, createdAt: Date.now() });
  return url3;
}

/**
 * Скачать файл пользователю (диалог сохранения).
 */
export async function downloadMedia(key, url, filename) {
  const blobUrl = await getMediaBlob(key, url);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = filename || 'huddle-file';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

/**
 * Удалить медиа из кэша.
 */
export async function deleteMedia(key) {
  const c = _urlCache.get(key);
  if (c?.url) URL.revokeObjectURL(c.url);
  _urlCache.delete(key);
  await idbDelete(STORE_BLOBS, key);
}

/**
 * Очистить весь кэш медиа.
 */
export async function clearMediaCache() {
  for (const { url } of _urlCache.values()) URL.revokeObjectURL(url);
  _urlCache.clear();
  await idbClear(STORE_BLOBS);
}

/**
 * Сколько файлов в кэше.
 */
export async function mediaCount() {
  return idbCount(STORE_BLOBS);
}

/**
 * Размер кэша в байтах.
 */
export async function mediaCacheSize() {
  const db = await openDb();
  return new Promise((res) => {
    const tx = db.transaction(STORE_BLOBS, 'readonly');
    const r = tx.objectStore(STORE_BLOBS).openCursor();
    let total = 0;
    r.onsuccess = (e) => {
      const c = e.target.result;
      if (c) { total += c.value.size || 0; c.continue(); }
      else res(total);
    };
    r.onerror = () => res(0);
  });
}

// ============ Сжатие изображений ============
export function compressImage(file, maxDim = 1920, quality = 0.85) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (ev) => {
      const img = new Image();
      img.onload = () => {
        let { width: w, height: h } = img;
        if (w > maxDim || h > maxDim) {
          if (w > h) { h = Math.round(h * maxDim / w); w = maxDim; }
          else { w = Math.round(w * maxDim / h); h = maxDim; }
        }
        const canvas = document.createElement('canvas');
        canvas.width = w; canvas.height = h;
        const ctx = canvas.getContext('2d');
        ctx.drawImage(img, 0, 0, w, h);
        canvas.toBlob((blob) => resolve(blob), 'image/jpeg', quality);
      };
      img.onerror = reject;
      img.src = ev.target.result;
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

// ============ Генерация превью ============
export async function generateThumbnail(file, size = 200) {
  if (!/^image\//.test(file.type)) return null;
  return compressImage(file, size, 0.7);
}

export async function generateVideoThumbnail(file, size = 320) {
  return new Promise((resolve) => {
    const video = document.createElement('video');
    video.preload = 'metadata';
    video.muted = true;
    video.src = URL.createObjectURL(file);
    video.onloadeddata = () => {
      video.currentTime = 0.1;
    };
    video.onseeked = () => {
      const canvas = document.createElement('canvas');
      const ratio = video.videoWidth / video.videoHeight;
      canvas.width = size;
      canvas.height = Math.round(size / ratio);
      canvas.getContext('2d').drawImage(video, 0, 0, canvas.width, canvas.height);
      canvas.toBlob((blob) => {
        URL.revokeObjectURL(video.src);
        resolve(blob);
      }, 'image/jpeg', 0.7);
    };
    video.onerror = () => resolve(null);
  });
}

// ============ E2E-шифрование (заглушка) ============
async function encryptBlob(blob) {
  // Реализация в crypto.js
  // Здесь — просто возврат как есть
  return blob;
}

// ============ Экспорт ============
export const media = {
  upload: uploadMedia,
  get: getMediaBlob,
  download: downloadMedia,
  delete: deleteMedia,
  clearCache: clearMediaCache,
  count: mediaCount,
  size: mediaCacheSize,
  compress: compressImage,
  thumb: generateThumbnail,
  videoThumb: generateVideoThumbnail,
  getCachedUrl,
  genKey: genMediaKey
};

export default media;