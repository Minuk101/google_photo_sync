const CLIENT_ID = '232709413830-gjmgctle15h91vcm1i9vtb6h5lnrk84o.apps.googleusercontent.com';
const PHOTO_SCOPE = 'https://www.googleapis.com/auth/photospicker.mediaitems.readonly';
const IMAGE_SIZE = '=w1920-h1080';
const SLIDE_INTERVAL_MS = 5000;
const RETRY_INTERVAL_MS = 3000;
const MIN_SLIDE_GAP_MS = 3000;
const QUEUE_SIZE = 12;
const PREFETCH_AHEAD = 4;
const MAX_MEMORY_BLOBS = 5;
const BULK_CONCURRENCY = 4;
const MANIFEST_POLL_MS = 30000;
const DB_NAME = 'photo_sync_db';
const DB_VERSION = 2;
const GITHUB_RAW = 'https://raw.githubusercontent.com/Minuk101/google_photo_sync/main/photos.json';

let allPhotos = [];
let manifestVersion = 0;
let globalToken = null;
let tokenExpiresAt = 0;
let tokenClient = null;
let tokenRequestPromise = null;
let pendingTokenResolver = null;

const slideQueue = [];
const playedKeys = new Set();
let currentPhotoKey = null;
let advancing = false;
let slideTimer = null;
let lastTransitionAt = Date.now();

const memoryCache = new Map();
const pendingLoads = new Map();
const bulkCompleted = new Set();
const bulkScheduled = new Set();
let bulkActive = 0;
let bulkPausedForAuth = false;
let databasePromise = null;

const loginBtn = document.getElementById('login-btn');
const cacheBar = document.getElementById('cache-bar');

// ---- IndexedDB ----
function openDB() {
  if (databasePromise) return databasePromise;
  databasePromise = new Promise((resolve, reject) => {
    const r = indexedDB.open(DB_NAME, DB_VERSION);
    r.onupgradeneeded = () => {
      const db = r.result;
      if (!db.objectStoreNames.contains('meta')) db.createObjectStore('meta', { keyPath: 'key' });
      if (!db.objectStoreNames.contains('media')) db.createObjectStore('media', { keyPath: 'id' });
    };
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => { databasePromise = null; reject(r.error); };
  });
  return databasePromise;
}
async function dbPutMeta(key, value) {
  const db = await openDB();
  return new Promise((resolve) => { const tx = db.transaction('meta', 'readwrite'); tx.objectStore('meta').put({ key, value }); tx.oncomplete = resolve; });
}
async function dbGetMeta(key) {
  const db = await openDB();
  return new Promise((resolve) => { const tx = db.transaction('meta', 'readonly'); const r = tx.objectStore('meta').get(key); tx.oncomplete = () => resolve(r.result?.value ?? null); });
}
async function dbPutMedia(id, blob) {
  const db = await openDB();
  return new Promise((resolve) => { const tx = db.transaction('media', 'readwrite'); tx.objectStore('media').put({ id, blob }); tx.oncomplete = resolve; });
}
async function dbGetMedia(id) {
  const db = await openDB();
  return new Promise((resolve) => { const tx = db.transaction('media', 'readonly'); const r = tx.objectStore('media').get(id); tx.oncomplete = () => resolve(r.result?.blob ?? null); });
}
async function dbGetAllMediaKeys() {
  const db = await openDB();
  return new Promise((resolve) => { const tx = db.transaction('media', 'readonly'); const r = tx.objectStore('media').getAllKeys(); tx.oncomplete = () => resolve(r.result || []); });
}

// ---- Auth ----
function hasToken() { return Boolean(globalToken && Date.now() < tokenExpiresAt - 30000); }
function applyToken(resp) {
  globalToken = resp.access_token;
  tokenExpiresAt = Date.now() + (resp.expires_in || 3600) * 1000;
  bulkPausedForAuth = false;
  loginBtn.style.display = 'none';
  scheduleBulk();
  pumpSlides();
}
async function waitForGsi() {
  for (let i = 0; i < 100; i++) {
    if (window.google?.accounts?.oauth2) return;
    await new Promise(r => setTimeout(r, 100));
  }
  throw new Error('GSI not ready');
}
function initTokenClient() {
  tokenClient = google.accounts.oauth2.initTokenClient({
    client_id: CLIENT_ID, scope: PHOTO_SCOPE, prompt: '',
    callback: resp => {
      const resolve = pendingTokenResolver; pendingTokenResolver = null;
      if (resp?.access_token) { applyToken(resp); resolve?.(resp.access_token); }
      else resolve?.(null);
    },
    error_callback: err => { console.warn('Auth:', err); const resolve = pendingTokenResolver; pendingTokenResolver = null; resolve?.(null); }
  });
}
async function requestToken() {
  if (hasToken()) return globalToken;
  if (tokenRequestPromise) return tokenRequestPromise;
  tokenRequestPromise = (async () => { await waitForGsi(); if (!tokenClient) initTokenClient(); return new Promise(resolve => { pendingTokenResolver = resolve; tokenClient.requestAccessToken(); }); })();
  try { return await tokenRequestPromise; } finally { tokenRequestPromise = null; }
}
function getToken() {
  if (!hasToken()) throw Object.assign(new Error('Auth required'), { code: 'AUTH_REQUIRED' });
  return globalToken;
}

// ---- Photo loading ----
async function fetchPhotoBlob(photo) {
  const resp = await fetch(photo.baseUrl + IMAGE_SIZE, { headers: { Authorization: `Bearer ${getToken()}` } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const blob = await resp.blob();
  if (!blob.size) throw new Error('Empty');
  return blob;
}
async function getPhotoBlob(photo) {
  const key = photo.id;
  if (memoryCache.has(key)) return memoryCache.get(key);
  if (pendingLoads.has(key)) return pendingLoads.get(key);
  const p = (async () => {
    let blob = await dbGetMedia(key);
    if (!blob) { blob = await fetchPhotoBlob(photo); await dbPutMedia(key, blob); }
    memoryCache.set(key, blob);
    while (memoryCache.size > MAX_MEMORY_BLOBS) memoryCache.delete(memoryCache.keys().next().value);
    return blob;
  })();
  pendingLoads.set(key, p);
  try { return await p; } finally { pendingLoads.delete(key); }
}

// ---- Bulk prefetch with resume ----
function updateCacheUI() {
  const total = allPhotos.length;
  const done = bulkCompleted.size;
  if (total === 0 || done >= total) { cacheBar.style.display = 'none'; return; }
  cacheBar.style.display = 'block';
  if (bulkPausedForAuth) {
    cacheBar.dataset.state = 'action';
    cacheBar.textContent = `로컬 저장 ${done}/${total} · 눌러서 계속`;
  } else {
    cacheBar.dataset.state = 'loading';
    cacheBar.textContent = `로컬 저장 중 ${done}/${total}`;
  }
}
async function restoreCompletedFromDB() {
  const keys = await dbGetAllMediaKeys();
  for (const key of keys) bulkCompleted.add(key);
}
async function scheduleBulk() {
  if (bulkPausedForAuth) return;
  for (const p of allPhotos) {
    if (bulkCompleted.has(p.id) || bulkScheduled.has(p.id)) continue;
    bulkScheduled.add(p.id);
    pumpBulk(p);
  }
  updateCacheUI();
}
async function pumpBulk(photo) {
  if (bulkPausedForAuth || bulkActive >= BULK_CONCURRENCY) { bulkScheduled.delete(photo.id); return; }
  bulkActive++;
  try {
    const cached = await dbGetMedia(photo.id);
    if (cached) { bulkCompleted.add(photo.id); updateCacheUI(); return; }
    const blob = await getPhotoBlob(photo);
    bulkCompleted.add(photo.id);
    updateCacheUI();
  } catch (err) {
    bulkScheduled.delete(photo.id);
    if (err.code === 'AUTH_REQUIRED') { bulkPausedForAuth = true; loginBtn.style.display = 'block'; updateCacheUI(); }
  } finally { bulkActive--; if (!bulkPausedForAuth) scheduleBulk(); }
}

// ---- Manifest ----
async function fetchManifest() {
  const resp = await fetch(GITHUB_RAW + '?t=' + Date.now(), { cache: 'no-store' });
  if (!resp.ok) throw new Error(`Manifest ${resp.status}`);
  return resp.json();
}
function photoKey(p) { return p.id; }
async function applyManifest(manifest) {
  const newVersion = manifest.updated ? new Date(manifest.updated).getTime() : 0;
  if (newVersion <= manifestVersion && allPhotos.length > 0) return false;

  const newPhotos = (manifest.photos || []).filter(p => p.baseUrl);
  const newKeys = new Set(newPhotos.map(photoKey));
  allPhotos = allPhotos.filter(p => newKeys.has(photoKey(p)));
  const existing = new Set(allPhotos.map(photoKey));
  let added = 0;
  for (const p of newPhotos) { if (!existing.has(photoKey(p))) { allPhotos.push(p); added++; } }

  if (added > 0 || newVersion > manifestVersion) {
    manifestVersion = newVersion;
    await dbPutMeta('manifest', { version: manifestVersion, photos: allPhotos });
    slideQueue.length = 0;
    refillQueue();
    scheduleBulk();
    pumpSlides();
  }
  return added > 0;
}

// ---- Slideshow ----
function shuffle(arr) { for (let i = arr.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [arr[i], arr[j]] = [arr[j], arr[i]]; } return arr; }
function refillQueue() {
  const queued = new Set(slideQueue.map(photoKey));
  let candidates = allPhotos.filter(p => !queued.has(photoKey(p)) && !playedKeys.has(photoKey(p)) && photoKey(p) !== currentPhotoKey);
  if (candidates.length === 0) { playedKeys.clear(); candidates = allPhotos.filter(p => !queued.has(photoKey(p)) && photoKey(p) !== currentPhotoKey); }
  shuffle(candidates);
  while (slideQueue.length < QUEUE_SIZE && candidates.length > 0) slideQueue.push(candidates.shift());
}
function pumpSlides() {
  for (const p of slideQueue.slice(0, PREFETCH_AHEAD)) {
    if (bulkCompleted.has(p.id)) continue;
    if (!memoryCache.has(p.id) && !pendingLoads.has(p.id)) getPhotoBlob(p).catch(() => {});
  }
}
async function advanceSlide() {
  if (advancing || allPhotos.length === 0) return;
  if (Date.now() - lastTransitionAt < MIN_SLIDE_GAP_MS) return;
  clearTimeout(slideTimer);
  advancing = true;
  try {
    refillQueue();
    const photo = slideQueue.shift() || allPhotos[Math.floor(Math.random() * allPhotos.length)];
    currentPhotoKey = photoKey(photo);
    playedKeys.add(currentPhotoKey);
    refillQueue(); pumpSlides();
    const blob = await getPhotoBlob(photo);
    displayPhoto(blob);
    lastTransitionAt = Date.now();
    slideTimer = setTimeout(advanceSlide, SLIDE_INTERVAL_MS);
  } catch (err) {
    console.warn('Slide:', err);
    if (err.code === 'AUTH_REQUIRED') { globalToken = null; tokenExpiresAt = 0; bulkPausedForAuth = true; loginBtn.style.display = 'block'; updateCacheUI(); }
    slideTimer = setTimeout(advanceSlide, RETRY_INTERVAL_MS);
  } finally { advancing = false; }
}
function displayPhoto(blob) {
  const img1 = document.getElementById('img1'), img2 = document.getElementById('img2');
  const bg1 = document.getElementById('bg1'), bg2 = document.getElementById('bg2');
  const showing1 = img1.style.opacity !== '0';
  const nextImg = showing1 ? img2 : img1, curImg = showing1 ? img1 : img2;
  const nextBg = showing1 ? bg2 : bg1, curBg = showing1 ? bg1 : bg2;
  const url = URL.createObjectURL(blob);
  nextImg.style.transition = 'none'; nextImg.style.transform = 'scale(1)'; nextImg.style.opacity = '0';
  nextBg.style.transition = 'none'; nextBg.style.opacity = '0';
  nextImg.src = url; nextBg.src = url;
  void nextImg.offsetHeight;
  nextImg.style.transition = 'transform 5s ease-out, opacity 2s'; nextImg.style.transform = 'scale(1.05)'; nextImg.style.opacity = '1';
  nextBg.style.transition = 'opacity 2s'; nextBg.style.opacity = '1';
  curImg.style.transition = 'opacity 2s'; curImg.style.opacity = '0';
  curBg.style.transition = 'opacity 2s'; curBg.style.opacity = '0';
  setTimeout(() => { curImg.removeAttribute('src'); curBg.removeAttribute('src'); URL.revokeObjectURL(url); }, 2200);
}

// ---- Init ----
loginBtn.addEventListener('click', async () => {
  loginBtn.disabled = true;
  try { await requestToken(); } catch { loginBtn.disabled = false; }
});
cacheBar.addEventListener('click', () => {
  if (cacheBar.dataset.state === 'action') loginBtn.click();
});

async function init() {
  await restoreCompletedFromDB();

  try {
    const cached = await dbGetMeta('manifest');
    if (cached?.photos?.length) {
      allPhotos = cached.photos;
      manifestVersion = cached.version || 0;
      refillQueue();
      scheduleBulk();
      advanceSlide();
    }
  } catch (e) { console.warn('Cache:', e); }

  try {
    const manifest = await fetchManifest();
    await applyManifest(manifest);
    if (allPhotos.length > 0) {
      if (!hasToken()) { loginBtn.style.display = 'block'; scheduleBulk(); }
      advanceSlide();
    }
  } catch (err) { console.warn('Manifest:', err); }

  setInterval(async () => {
    try {
      const m = await fetchManifest();
      const changed = await applyManifest(m);
      if (allPhotos.length > 0 && !hasToken()) { loginBtn.style.display = 'block'; scheduleBulk(); }
      if (changed && !slideTimer) advanceSlide();
    } catch {}
  }, MANIFEST_POLL_MS);
}
init();
