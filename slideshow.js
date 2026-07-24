const GITHUB_RAW = 'https://raw.githubusercontent.com/Minuk101/google_photo_sync/main/photos.json';
const CLIENT_ID = '232709413830-gjmgctle15h91vcm1i9vtb6h5lnrk84o.apps.googleusercontent.com';
const PHOTO_SCOPE = 'https://www.googleapis.com/auth/photospicker.mediaitems.readonly';
const IMAGE_SIZE = '=w1920-h1080';
const SLIDE_INTERVAL_MS = 5000;
const RETRY_INTERVAL_MS = 3000;
const QUEUE_SIZE = 12;
const PREFETCH_AHEAD = 4;
const MAX_MEMORY_BLOBS = 5;
const POLL_INTERVAL_MS = 60000;
const DB_NAME = 'photo_sync_db';
const DB_VERSION = 1;

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
let bulkPaused = false;
let databasePromise = null;

const emptyState = document.getElementById('empty-state');
const emptyMsg = document.getElementById('empty-msg');
const slideshow = document.getElementById('slideshow');
const cacheBar = document.getElementById('cache-bar');
const authBtn = document.getElementById('auth-btn');

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
    r.onerror = () => reject(r.error);
  });
  return databasePromise;
}
async function dbPutMeta(key, value) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('meta', 'readwrite');
    tx.objectStore('meta').put({ key, value });
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });
}
async function dbGetMeta(key) {
  const db = await openDB();
  return new Promise((resolve) => {
    const tx = db.transaction('meta', 'readonly');
    const r = tx.objectStore('meta').get(key);
    tx.oncomplete = () => resolve(r.result?.value ?? null);
  });
}
async function dbPutMedia(id, blob) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('media', 'readwrite');
    tx.objectStore('media').put({ id, blob });
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });
}
async function dbGetMedia(id) {
  const db = await openDB();
  return new Promise((resolve) => {
    const tx = db.transaction('media', 'readonly');
    const r = tx.objectStore('media').get(id);
    tx.oncomplete = () => resolve(r.result?.blob ?? null);
  });
}
// ---- End IndexedDB ----

// ---- Auth ----
function hasToken() { return Boolean(globalToken && Date.now() < tokenExpiresAt - 30000); }
function applyToken(resp) {
  globalToken = resp.access_token;
  tokenExpiresAt = Date.now() + (resp.expires_in || 3600) * 1000;
  bulkPaused = false;
  authBtn.classList.add('hidden');
  if (allPhotos.length > 0) { scheduleBulk(); pumpSlides(); }
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
      const resolve = pendingTokenResolver;
      pendingTokenResolver = null;
      if (resp?.access_token) { applyToken(resp); resolve?.(resp.access_token); }
      else resolve?.(null);
    },
    error_callback: err => {
      console.warn('Auth error:', err);
      const resolve = pendingTokenResolver;
      pendingTokenResolver = null;
      resolve?.(null);
    }
  });
}
async function requestToken() {
  if (hasToken()) return globalToken;
  if (tokenRequestPromise) return tokenRequestPromise;
  tokenRequestPromise = (async () => {
    await waitForGsi();
    if (!tokenClient) initTokenClient();
    return new Promise(resolve => {
      pendingTokenResolver = resolve;
      tokenClient.requestAccessToken();
    });
  })();
  try { return await tokenRequestPromise; } finally { tokenRequestPromise = null; }
}
function getToken() {
  if (!hasToken()) throw Object.assign(new Error('Auth required'), { code: 'AUTH_REQUIRED' });
  return globalToken;
}
// ---- End Auth ----

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
// ---- End Photo loading ----

// ---- Bulk prefetch ----
function updateCacheBar() {
  const total = allPhotos.length;
  const done = bulkCompleted.size;
  if (total === 0 || done >= total) { cacheBar.style.display = 'none'; return; }
  cacheBar.style.display = 'block';
  cacheBar.textContent = bulkPaused ? `로컬 저장 ${done}/${total} · 눌러서 계속` : `로컬 저장 중 ${done}/${total}`;
}
function scheduleBulk() {
  if (bulkPaused) return;
  for (const p of allPhotos) {
    if (bulkCompleted.has(p.id) || bulkScheduled.has(p.id)) continue;
    bulkScheduled.add(p.id);
    pumpBulk(p);
  }
  updateCacheBar();
}
function pumpBulk(photo) {
  if (bulkPaused || bulkActive >= 4) return;
  bulkActive++;
  getPhotoBlob(photo).then(() => {
    bulkCompleted.add(photo.id);
    updateCacheBar();
  }).catch(err => {
    bulkScheduled.delete(photo.id);
    if (err.code === 'AUTH_REQUIRED') { bulkPaused = true; authBtn.classList.remove('hidden'); updateCacheBar(); }
  }).finally(() => {
    bulkActive--;
    if (!bulkPaused) scheduleBulk();
  });
}
// ---- End Bulk prefetch ----

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

  // Remove old photos not in new list
  allPhotos = allPhotos.filter(p => newKeys.has(photoKey(p)));

  // Add new photos
  const existing = new Set(allPhotos.map(photoKey));
  let added = 0;
  for (const p of newPhotos) {
    if (!existing.has(photoKey(p))) { allPhotos.push(p); added++; }
  }

  if (added > 0 || newVersion > manifestVersion) {
    manifestVersion = newVersion;
    await dbPutMeta('manifest', { version: manifestVersion, photos: allPhotos });
    slideQueue.length = 0;
    refillQueue();
    scheduleBulk();
    pumpSlides();
    if (allPhotos.length > 0) showSlideshow();
  }
  return added > 0;
}
// ---- End Manifest ----

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
    if (!memoryCache.has(p.id) && !pendingLoads.has(p.id)) getPhotoBlob(p).catch(() => {});
  }
}
async function advanceSlide() {
  if (advancing || allPhotos.length === 0) return;
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
    console.warn('Slide error:', err);
    if (err.code === 'AUTH_REQUIRED') { bulkPaused = true; authBtn.classList.remove('hidden'); updateCacheBar(); }
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
// ---- End Slideshow ----

// ---- Init ----
function showSlideshow() {
  emptyState.classList.add('hidden');
  slideshow.classList.remove('hidden');
}
function showEmpty(msg) {
  emptyState.classList.remove('hidden');
  slideshow.classList.add('hidden');
  emptyMsg.textContent = msg;
}
authBtn.addEventListener('click', async () => {
  authBtn.disabled = true;
  try {
    await requestToken();
    bulkPaused = false;
    scheduleBulk();
    pumpSlides();
    if (!slideTimer) advanceSlide();
  } catch { authBtn.disabled = false; }
});
document.addEventListener('click', e => {
  if (e.target.closest('button')) return;
  if (!document.fullscreenElement && allPhotos.length > 0) document.documentElement.requestFullscreen().catch(() => {});
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && Date.now() - lastTransitionAt > SLIDE_INTERVAL_MS * 2) advanceSlide();
});
async function init() {
  try {
    const cached = await dbGetMeta('manifest');
    if (cached?.photos?.length) {
      allPhotos = cached.photos;
      manifestVersion = cached.version || 0;
      showSlideshow();
      refillQueue();
      scheduleBulk();
      if (!slideTimer) advanceSlide();
    }
  } catch (e) { console.warn('Cache:', e); }

  try {
    if (allPhotos.length === 0) emptyMsg.textContent = 'GitHub에서 사진 목록 확인 중...';
    const manifest = await fetchManifest();
    await applyManifest(manifest);
    if (allPhotos.length > 0) {
      showSlideshow();
      if (!slideTimer) advanceSlide();
    } else {
      showEmpty('저장된 사진이 없습니다. 관리자 페이지에서 사진을 추가해주세요.');
    }
  } catch (err) {
    console.warn('Manifest:', err);
    if (allPhotos.length === 0) {
      showEmpty('사진 목록을 불러올 수 없습니다. 인터넷 연결을 확인해주세요.');
    } else {
      showSlideshow();
      scheduleBulk();
    }
  }
  setInterval(async () => {
    try {
      const m = await fetchManifest();
      await applyManifest(m);
      if (allPhotos.length > 0) showSlideshow();
    } catch {}
  }, POLL_INTERVAL_MS);
}
init();
