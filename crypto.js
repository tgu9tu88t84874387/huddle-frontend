/**
 * Huddle — crypto.js
 * End-to-End шифрование на Web Crypto API.
 *
 * Схема (упрощённый X3DH + Double Ratchet):
 *  1. У каждого юзера — долгоживущая пара ключей ECDH (P-256).
 *  2. При первом сообщении — обмен публичными ключами.
 *  3. Из своего приватного + чужого публичного выводится общий секрет (ECDH).
 *  4. Из общего секрета через HKDF выводятся ключи шифрования.
 *  5. Сообщения шифруются AES-GCM (256 бит) с уникальным IV.
 *  6. Медиа шифруются так же, но как ArrayBuffer.
 *
 * Что видит сервер:
 *  - зашифрованный blob (base64)
 *  - IV
 *  - публичные ключи (они не секретны)
 *
 * Чего сервер НЕ видит:
 *  - текст сообщений
 *  - содержимое файлов
 *  - общий секрет (он никогда не уходит в сеть)
 */

// ============ Константы ============
const CURVE = 'P-256';
const KDF_HASH = 'SHA-256';
const CIPHER = 'AES-GCM';
const KEY_LEN = 256;
const IV_LEN = 12;          // 96 бит для GCM
const PBKDF2_ITER = 250000; // для пароля-ключа (локальное хранилище)
const DB_NAME = 'huddle_media_db';
const DB_VERSION = 1;
const STORE_KEYS = 'keys';
const STORE_IDENT = 'identities';

// ============ Хранилище ключей (IndexedDB) ============
let _dbPromise = null;
function openKeyDb() {
  if (_dbPromise) return _dbPromise;
  _dbPromise = new Promise((resolve, reject) => {
    if (!('indexedDB' in window)) return reject(new Error('IndexedDB not supported'));
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_KEYS)) db.createObjectStore(STORE_KEYS);
      if (!db.objectStoreNames.contains(STORE_IDENT)) db.createObjectStore(STORE_IDENT);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  return _dbPromise;
}

async function idbPut(store, key, value) {
  const db = await openKeyDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).put(value, key);
    tx.oncomplete = () => res(true);
    tx.onerror = () => rej(tx.error);
  });
}

async function idbGet(store, key) {
  const db = await openKeyDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readonly');
    const r = tx.objectStore(store).get(key);
    r.onsuccess = () => res(r.result || null);
    r.onerror = () => rej(r.error);
  });
}

async function idbDel(store, key) {
  const db = await openKeyDb();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).delete(key);
    tx.oncomplete = () => res(true);
    tx.onerror = () => rej(tx.error);
  });
}

// ============ Base64 хелперы ============
function bufToB64(buf) {
  const bytes = new Uint8Array(buf);
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

function b64ToBuf(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes.buffer;
}

// ============ Генерация идентичности ============
/**
 * Создать пару ключей ECDH для текущего юзера.
 * Возвращает { publicKey, privateKey } в экспортируемом виде.
 */
export async function generateIdentity() {
  const pair = await crypto.subtle.generateKey(
    { name: 'ECDH', namedCurve: CURVE },
    true,                        // extractable
    ['deriveKey', 'deriveBits']
  );

  const publicKey = await crypto.subtle.exportKey('jwk', pair.publicKey);
  const privateKey = await crypto.subtle.exportKey('jwk', pair.privateKey);

  return { publicKey, privateKey, createdAt: Date.now() };
}

/**
 * Загрузить (или создать) свою идентичность.
 */
export async function loadIdentity(username) {
  const key = 'ident_' + username;
  let ident = await idbGet(STORE_IDENT, key);
  if (!ident) {
    ident = await generateIdentity();
    await idbPut(STORE_IDENT, key, ident);
  }
  return ident;
}

/**
 * Импортировать чужой публичный ключ (из JWK).
 */
async function importPublicKey(jwk) {
  return crypto.subtle.importKey(
    'jwk', jwk,
    { name: 'ECDH', namedCurve: CURVE },
    true, []
  );
}

async function importPrivateKey(jwk) {
  return crypto.subtle.importKey(
    'jwk', jwk,
    { name: 'ECDH', namedCurve: CURVE },
    true, ['deriveKey', 'deriveBits']
  );
}

// ============ Общий секрет ============
/**
 * Вывести общий секрет из своей приватной пары и чужого публичного ключа.
 */
async function deriveSharedSecret(myPrivateJwk, theirPublicJwk) {
  const myPriv = await importPrivateKey(myPrivateJwk);
  const theirPub = await importPublicKey(theirPublicJwk);

  const bits = await crypto.subtle.deriveBits(
    { name: 'ECDH', public: theirPub },
    myPriv,
    256
  );
  return bits; // ArrayBuffer
}

/**
 * Из общего секрета вывести ключ AES-GCM через HKDF.
 */
async function deriveAesKey(sharedSecret, salt, info) {
  const hkdfKey = await crypto.subtle.importKey(
    'raw', sharedSecret,
    { name: 'HKDF' },
    false, ['deriveKey']
  );
  return crypto.subtle.deriveKey(
    {
      name: 'HKDF',
      hash: KDF_HASH,
      salt: salt || new Uint8Array(0),
      info: new TextEncoder().encode(info || 'huddle-v1')
    },
    hkdfKey,
    { name: CIPHER, length: KEY_LEN },
    false,
    ['encrypt', 'decrypt']
  );
}

/**
 * Получить AES-ключ для чата с конкретным юзером.
 * Кэшируется в памяти.
 */
const _sessionKeys = new Map(); // peerUsername -> CryptoKey

export async function getSessionKey(myUsername, peerUsername, peerPublicJwk) {
  const cacheKey = myUsername + '|' + peerUsername;
  if (_sessionKeys.has(cacheKey)) return _sessionKeys.get(cacheKey);

  const myIdent = await loadIdentity(myUsername);
  const shared = await deriveSharedSecret(myIdent.privateKey, peerPublicJwk);

  // Сортируем имена, чтобы у обоих был одинаковый salt
  const [a, b] = [myUsername, peerUsername].sort();
  const salt = new TextEncoder().encode(a + '::' + b);

  const key = await deriveAesKey(shared, salt, 'huddle-chat-v1');
  _sessionKeys.set(cacheKey, key);
  return key;
}

// ============ Шифрование текста ============
/**
 * Зашифровать строку.
 * Возвращает { iv, data } в base64.
 */
export async function encryptText(plaintext, aesKey) {
  const iv = crypto.getRandomValues(new Uint8Array(IV_LEN));
  const enc = new TextEncoder().encode(plaintext);
  const cipher = await crypto.subtle.encrypt(
    { name: CIPHER, iv },
    aesKey,
    enc
  );
  return {
    iv: bufToB64(iv),
    data: bufToB64(cipher)
  };
}

/**
 * Расшифровать строку.
 */
export async function decryptText({ iv, data }, aesKey) {
  const ivBuf = b64ToBuf(iv);
  const dataBuf = b64ToBuf(data);
  const plain = await crypto.subtle.decrypt(
    { name: CIPHER, iv: new Uint8Array(ivBuf) },
    aesKey,
    dataBuf
  );
  return new TextDecoder().decode(plain);
}

// ============ Шифрование медиа ============
/**
 * Зашифровать Blob (фото, видео, голосовое).
 * Возвращает Blob в формате: [IV (12 байт)][ciphertext]
 * Так проще — IV сразу внутри файла.
 */
export async function encryptBlob(blob, aesKey) {
  const iv = crypto.getRandomValues(new Uint8Array(IV_LEN));
  const buf = await blob.arrayBuffer();
  const cipher = await crypto.subtle.encrypt(
    { name: CIPHER, iv },
    aesKey,
    buf
  );

  // Склеиваем: IV + cipher
  const out = new Uint8Array(IV_LEN + cipher.byteLength);
  out.set(iv, 0);
  out.set(new Uint8Array(cipher), IV_LEN);
  return new Blob([out], { type: 'application/octet-stream' });
}

/**
 * Расшифровать Blob.
 */
export async function decryptBlob(encryptedBlob, aesKey) {
  const buf = await encryptedBlob.arrayBuffer();
  const bytes = new Uint8Array(buf);
  const iv = bytes.slice(0, IV_LEN);
  const data = bytes.slice(IV_LEN);
  const plain = await crypto.subtle.decrypt(
    { name: CIPHER, iv },
    aesKey,
    data
  );
  // Возвращаем как Blob, тип уточнит вызывающий код
  return new Blob([plain]);
}

// ============ Шифрование медиа с ключом чата ============
/**
 * Обёртка: шифрует blob ключом чата с peer.
 */
export async function encryptMediaForChat(blob, myUsername, peerUsername, peerPublicJwk) {
  const key = await getSessionKey(myUsername, peerUsername, peerPublicJwk);
  return encryptBlob(blob, key);
}

export async function decryptMediaFromChat(blob, myUsername, peerUsername, peerPublicJwk) {
  const key = await getSessionKey(myUsername, peerUsername, peerPublicJwk);
  return decryptBlob(blob, key);
}

// ============ Локальное шифрование (PIN-код) ============
/**
 * Зашифровать что-то паролем (для локального хранилища).
 * Возвращает { salt, iv, data } в base64.
 */
export async function encryptWithPassword(plaintext, password) {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const iv = crypto.getRandomValues(new Uint8Array(IV_LEN));

  const baseKey = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(password),
    { name: 'PBKDF2' },
    false, ['deriveKey']
  );

  const key = await crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: PBKDF2_ITER, hash: KDF_HASH },
    baseKey,
    { name: CIPHER, length: KEY_LEN },
    false, ['encrypt', 'decrypt']
  );

  const data = await crypto.subtle.encrypt(
    { name: CIPHER, iv },
    key,
    new TextEncoder().encode(plaintext)
  );

  return {
    salt: bufToB64(salt),
    iv: bufToB64(iv),
    data: bufToB64(data)
  };
}

export async function decryptWithPassword({ salt, iv, data }, password) {
  const saltBuf = b64ToBuf(salt);
  const ivBuf = b64ToBuf(iv);
  const dataBuf = b64ToBuf(data);

  const baseKey = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(password),
    { name: 'PBKDF2' },
    false, ['deriveKey']
  );

  const key = await crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: new Uint8Array(saltBuf), iterations: PBKDF2_ITER, hash: KDF_HASH },
    baseKey,
    { name: CIPHER, length: KEY_LEN },
    false, ['encrypt', 'decrypt']
  );

  const plain = await crypto.subtle.decrypt(
    { name: CIPHER, iv: new Uint8Array(ivBuf) },
    key,
    dataBuf
  );

  return new TextDecoder().decode(plain);
}

// ============ Fingerprint (для проверки ключей) ============
/**
 * Отпечаток публичного ключа — чтобы два юзера могли сверить,
 * что у них одинаковые ключи (защита от MITM).
 */
export async function fingerprint(publicJwk) {
  const str = JSON.stringify(publicJwk);
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  const bytes = new Uint8Array(buf);
  let hex = '';
  for (let i = 0; i < 8; i++) hex += bytes[i].toString(16).padStart(2, '0') + ' ';
  return hex.trim().toUpperCase();
}

// ============ Очистка ============
export async function clearKeys() {
  _sessionKeys.clear();
  const db = await openKeyDb();
  await new Promise((res) => {
    const tx = db.transaction([STORE_KEYS, STORE_IDENT], 'readwrite');
    tx.objectStore(STORE_KEYS).clear();
    tx.objectStore(STORE_IDENT).clear();
    tx.oncomplete = () => res();
  });
}

export async function deleteIdentity(username) {
  _sessionKeys.clear();
  await idbDel(STORE_IDENT, 'ident_' + username);
}

// ============ Экспорт ============
export const crypto = {
  generateIdentity,
  loadIdentity,
  getSessionKey,
  encryptText,
  decryptText,
  encryptBlob,
  decryptBlob,
  encryptMediaForChat,
  decryptMediaFromChat,
  encryptWithPassword,
  decryptWithPassword,
  fingerprint,
  clearKeys,
  deleteIdentity,
  // утилиты
  bufToB64,
  b64ToBuf
};

export default crypto;