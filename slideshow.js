const CLIENT_ID = '232709413830-gjmgctle15h91vcm1i9vtb6h5lnrk84o.apps.googleusercontent.com';
// 푸시할 때마다 이 버전을 올리고 index.html의 ?v= 도 같은 값으로 맞춘다.
// 브라우저 캐시 무효화 + 화면에 로드된 코드 버전 표시용.
const APP_VERSION = 4; // index.html의 ?v= 와 동일하게 유지
// ---- 진단 오버레이 (맨 위 배치: 로드되자마자 확인용) ----
function diag(msg) {
  let el = document.getElementById('diag-bar');
  if (!el) {
    el = document.createElement('div');
    el.id = 'diag-bar';
    el.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:9999;background:rgba(0,0,0,0.85);color:#0f0;font:12px monospace;padding:4px 8px;white-space:pre-wrap;max-height:40%;overflow:auto;';
    if (document.body) document.body.appendChild(el); else window.addEventListener('DOMContentLoaded', () => document.body.appendChild(el));
  }
  const t = new Date().toLocaleTimeString();
  const prev = (el.textContent || '').split('\n');
  prev.push(t + ' ' + msg);
  el.textContent = prev.slice(-8).join('\n');
}
diag('DIAG READY v' + APP_VERSION + '  ' + new Date().toISOString());
// 화면 우하단에 로드된 코드 버전을 항상 표시 (캐시 확인용)
(function showVersionBadge() {
  const badge = document.createElement('div');
  badge.id = 'version-badge';
  badge.textContent = 'v' + APP_VERSION;
  badge.style.cssText = 'position:fixed;right:6px;bottom:6px;z-index:10000;background:rgba(0,0,0,0.5);color:#9f9;font:11px monospace;padding:2px 6px;border-radius:6px;pointer-events:none;opacity:0.7;';
  if (document.body) document.body.appendChild(badge);
  else window.addEventListener('DOMContentLoaded', () => document.body.appendChild(badge));
})();
const PHOTO_SCOPE = 'https://www.googleapis.com/auth/photospicker.mediaitems.readonly';
const API_BASE = 'https://photospicker.googleapis.com/v1';

const DB_NAME = 'photo_sync_db';
const DB_VERSION = 3;
const META_STORE = 'store';
const MEDIA_STORE = 'media';

const IMAGE_SIZE = '=w1920-h1080';
const SLIDE_INTERVAL_MS = 5000;
const RETRY_INTERVAL_MS = 3000;
const IMAGE_FETCH_TIMEOUT_MS = 20000;
const API_FETCH_TIMEOUT_MS = 15000;
const QUEUE_SIZE = 12;
const PREFETCH_AHEAD = 4;
const BULK_PREFETCH_CONCURRENCY = 4;
const MAX_MEMORY_BLOBS = 5;
const DEFAULT_POLL_INTERVAL_MS = 3000;
const DEFAULT_SESSION_TIMEOUT_MS = 10 * 60 * 1000;
// raw.githubusercontent.com은 API rate limit(익명 60회/시)을 타지 않아
// 갤럭시탭이 24/7 폴링해도 막히지 않는다. (캐시버스트 쿼리로 매번 최신 획득)
const GITHUB_RAW = 'https://raw.githubusercontent.com/Minuk101/google_photo_sync/main/photos.json';
const MANIFEST_POLL_MS = 60000;

let allPhotos = [];
let globalToken = null;
let tokenExpiresAt = 0;
let tokenClient = null;
let tokenRequestPromise = null;
let pendingTokenResolver = null;

const pollQueue = [];
let pollInProgress = false;

const slideQueue = [];
const playedPhotoKeys = new Set();
let currentPhotoKey = null;
let slideshowStarted = false;
let advancing = false;
let nextSlideTimer = null;

const memoryBlobCache = new Map();
const pendingBlobLoads = new Map();
const bulkPrefetchQueue = [];
const bulkPrefetchScheduled = new Set();
const bulkPrefetchCompleted = new Set();
let bulkPrefetchActive = 0;
let bulkPrefetchPausedForAuth = false;
let bulkPrefetchRetryTimer = null;
let cacheNeedsAuthorization = false;

let lastTransitionTime = Date.now();
let databasePromise = null;

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseDuration(value, fallback) {
    const seconds = Number.parseFloat(String(value || '').replace(/s$/, ''));
    return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : fallback;
}

function getPhotoKey(photo) {
    return String(photo.id || photo.baseUrl);
}

function normalizePhoto(item) {
    const mediaFile = item.mediaFile || item;
    const baseUrl = mediaFile && mediaFile.baseUrl;
    if (!baseUrl) return null;

    return {
        id: String(item.id || baseUrl),
        baseUrl,
        mimeType: mediaFile.mimeType || item.mimeType || 'image/jpeg'
    };
}

// ---- IndexedDB ----
function openDB() {
    if (databasePromise) return databasePromise;

    databasePromise = new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onupgradeneeded = event => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(META_STORE)) {
                db.createObjectStore(META_STORE, { keyPath: 'id' });
            }
            if (!db.objectStoreNames.contains(MEDIA_STORE)) {
                db.createObjectStore(MEDIA_STORE, { keyPath: 'id' });
            }
        };

        request.onsuccess = event => {
            const db = event.target.result;
            db.onversionchange = () => {
                db.close();
                databasePromise = null;
            };
            db.onclose = () => {
                databasePromise = null;
            };
            resolve(db);
        };
        request.onerror = () => {
            databasePromise = null;
            reject(request.error);
        };
    });

    return databasePromise;
}

async function dbPut(key, value) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(META_STORE, 'readwrite');
        tx.objectStore(META_STORE).put({ id: key, value });
        tx.oncomplete = () => resolve();
        tx.onerror = () => {
            const error = tx.error;
            reject(error);
        };
    });
}

async function dbGet(key) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(META_STORE, 'readonly');
        const request = tx.objectStore(META_STORE).get(key);
        let value = null;

        request.onsuccess = () => {
            value = request.result ? request.result.value : null;
        };
        tx.oncomplete = () => resolve(value);
        tx.onerror = () => reject(tx.error);
    });
}

async function dbPutMedia(id, blob) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(MEDIA_STORE, 'readwrite');
        tx.objectStore(MEDIA_STORE).put({ id, blob });
        tx.oncomplete = () => resolve();
        tx.onerror = () => {
            const error = tx.error;
            reject(error);
        };
    });
}

async function dbGetMedia(id) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(MEDIA_STORE, 'readonly');
        const request = tx.objectStore(MEDIA_STORE).get(id);
        let blob = null;

        request.onsuccess = () => {
            blob = request.result ? request.result.blob : null;
        };
        tx.oncomplete = () => resolve(blob);
        tx.onerror = () => reject(tx.error);
    });
}

async function dbDeleteMedia(id) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(MEDIA_STORE, 'readwrite');
        tx.objectStore(MEDIA_STORE).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

async function dbClearAll() {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([META_STORE, MEDIA_STORE], 'readwrite');
        tx.objectStore(META_STORE).clear();
        tx.objectStore(MEDIA_STORE).clear();
        tx.oncomplete = () => resolve();
        tx.onerror = () => {
            const error = tx.error;
            reject(error);
        };
    });
}
// ---- End IndexedDB ----

// ---- Authentication ----
function hasUsableToken() {
    return Boolean(globalToken && Date.now() < tokenExpiresAt - 30000);
}

function applyToken(response) {
    globalToken = response.access_token;
    tokenExpiresAt = Date.now() + (Number(response.expires_in) || 3600) * 1000;
    cacheNeedsAuthorization = false;
    bulkPrefetchPausedForAuth = false;

    if (allPhotos.length > 0) {
        scheduleBulkPrefetch();
        pumpSlidePrefetch();
    }
    if (pollQueue.length > 0) {
        processQueue();
    }
}

async function waitForGoogleIdentity() {
    for (let attempt = 0; attempt < 100; attempt++) {
        if (window.google && google.accounts && google.accounts.oauth2) return;
        await delay(100);
    }
    throw new Error('Google Identity Services is not ready');
}

function initTokenClient() {
    tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: CLIENT_ID,
        scope: PHOTO_SCOPE,
        prompt: '',
        callback: response => {
            const resolve = pendingTokenResolver;
            pendingTokenResolver = null;

            if (response && response.access_token) {
                applyToken(response);
                if (resolve) resolve(response.access_token);
            } else if (resolve) {
                resolve(null);
            }
        },
        error_callback: error => {
            console.warn('Google authorization error:', error);
            const resolve = pendingTokenResolver;
            pendingTokenResolver = null;
            if (resolve) resolve(null);
        }
    });
}

async function requestAccessToken() {
    if (hasUsableToken()) return globalToken;
    if (tokenRequestPromise) return tokenRequestPromise;

    tokenRequestPromise = (async () => {
        await waitForGoogleIdentity();
        if (!tokenClient) initTokenClient();

        return new Promise(resolve => {
            pendingTokenResolver = resolve;
            tokenClient.requestAccessToken();
        });
    })();

    try {
        return await tokenRequestPromise;
    } finally {
        tokenRequestPromise = null;
    }
}

function authRequiredError() {
    const error = new Error('Authentication is required');
    error.code = 'AUTH_REQUIRED';
    return error;
}

function getValidToken() {
    if (!hasUsableToken()) throw authRequiredError();
    return globalToken;
}
// ---- End Authentication ----

// ---- API and media loading ----
async function fetchOnce(url, init, token, timeoutMs) {
    const controller = timeoutMs ? new AbortController() : null;
    const headers = new Headers(init.headers || {});
    headers.set('Authorization', 'Bearer ' + token);

    const requestInit = { ...init, headers };
    let timeoutId = null;
    if (controller) {
        requestInit.signal = controller.signal;
        timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    }

    try {
        return await fetch(url, requestInit);
    } finally {
        if (timeoutId) clearTimeout(timeoutId);
    }
}

async function authenticatedFetch(url, init = {}, timeoutMs = API_FETCH_TIMEOUT_MS) {
    const token = getValidToken();
    const response = await fetchOnce(url, init, token, timeoutMs);

    if (response.status !== 401) return response;

    globalToken = null;
    tokenExpiresAt = 0;
    throw authRequiredError();
}

async function fetchPhotoBlob(photo) {
    const url = photo.baseUrl + IMAGE_SIZE;
    let dom = '';
    try { dom = new URL(url).hostname; } catch {}
    diag('fetch START ' + dom + '  ' + getPhotoKey(photo).slice(0, 12) + '  t=' + IMAGE_FETCH_TIMEOUT_MS);
    diag('token? ' + (hasUsableToken() ? 'YES(' + Math.round((tokenExpiresAt - Date.now()) / 1000) + 's)' : 'NO') + '  needLogin=' + cacheNeedsAuthorization);
    let response;
    try {
        response = await authenticatedFetch(url, {}, IMAGE_FETCH_TIMEOUT_MS);
    } catch (e) {
        let info = 'name=' + (e && e.name) + ' msg=' + (e && e.message);
        if (e && e.cause) info += ' | cause=' + (e.cause.name || e.cause.code || e.cause.message || e.cause);
        const t = (typeof performance !== 'undefined' ? performance.now() : Date.now());
        diag('fetch THROW ' + dom + '  ' + info + '  at=' + Math.round(t) + 'ms');
        throw e;
    }

    if (!response.ok) {
        diag('fetch HTTP ' + response.status + '  ' + dom);
        throw new Error('Image request failed: HTTP ' + response.status);
    }

    const blob = await response.blob();
    if (!blob.size) throw new Error('Image response was empty');
    return blob;
}

function cacheBlob(key, blob) {
    memoryBlobCache.delete(key);
    memoryBlobCache.set(key, blob);

    while (memoryBlobCache.size > MAX_MEMORY_BLOBS) {
        const oldestKey = memoryBlobCache.keys().next().value;
        memoryBlobCache.delete(oldestKey);
    }
}

async function getPhotoBlob(photo) {
    const key = getPhotoKey(photo);
    if (memoryBlobCache.has(key)) {
        const blob = memoryBlobCache.get(key);
        cacheBlob(key, blob);
        return blob;
    }

    if (pendingBlobLoads.has(key)) {
        return pendingBlobLoads.get(key);
    }

    const loadPromise = (async () => {
        let blob = await dbGetMedia(key);
        if (!blob) {
            blob = await fetchPhotoBlob(photo);
            await dbPutMedia(key, blob);
        }

        cacheBlob(key, blob);
        return blob;
    })();

    pendingBlobLoads.set(key, loadPromise);
    try {
        return await loadPromise;
    } finally {
        pendingBlobLoads.delete(key);
    }
}
// ---- End API and media loading ----

// ---- Persistent background prefetch ----
function getCacheProgress() {
    const photoKeys = new Set(allPhotos.map(getPhotoKey));
    let completed = 0;
    for (const key of photoKeys) {
        if (bulkPrefetchCompleted.has(key)) completed++;
    }
    return { completed, total: photoKeys.size };
}

function updateCacheStatus() {
    const status = document.getElementById('cache-status');
    const { completed, total } = getCacheProgress();

    if (total === 0) {
        status.style.display = 'none';
        return;
    }

    status.style.display = 'block';
    if (completed >= total) {
        cacheNeedsAuthorization = false;
        bulkPrefetchPausedForAuth = false;
        status.dataset.state = 'complete';
        status.textContent = '';
        status.style.display = 'none';
    } else if (cacheNeedsAuthorization) {
        status.dataset.state = 'action';
        status.textContent = `로컬 저장 ${completed}/${total} · 눌러서 계속`;
    } else {
        status.dataset.state = 'loading';
        status.textContent = `로컬 저장 중 ${completed}/${total}`;
    }
}

function markCacheAuthorizationRequired() {
    cacheNeedsAuthorization = true;
    bulkPrefetchPausedForAuth = true;
    updateCacheStatus();
}

function scheduleBulkPrefetchRetry() {
    if (bulkPrefetchRetryTimer || bulkPrefetchPausedForAuth) return;
    bulkPrefetchRetryTimer = setTimeout(() => {
        bulkPrefetchRetryTimer = null;
        scheduleBulkPrefetch();
    }, 15000);
}

function scheduleBulkPrefetch() {
    for (const photo of allPhotos) {
        const key = getPhotoKey(photo);
        if (bulkPrefetchCompleted.has(key) || bulkPrefetchScheduled.has(key)) {
            continue;
        }
        bulkPrefetchScheduled.add(key);
        bulkPrefetchQueue.push(photo);
    }
    updateCacheStatus();
    pumpBulkPrefetch();
}

function pumpBulkPrefetch() {
    while (
        !bulkPrefetchPausedForAuth &&
        bulkPrefetchActive < BULK_PREFETCH_CONCURRENCY &&
        bulkPrefetchQueue.length > 0
    ) {
        const photo = bulkPrefetchQueue.shift();
        const key = getPhotoKey(photo);
        bulkPrefetchActive++;

        getPhotoBlob(photo)
            .then(() => {
                bulkPrefetchCompleted.add(key);
                updateCacheStatus();
            })
            .catch(error => {
                console.warn('Background prefetch failed:', key, error);
                bulkPrefetchScheduled.delete(key);

                if (error.code === 'AUTH_REQUIRED') {
                    markCacheAuthorizationRequired();
                } else {
                    scheduleBulkPrefetchRetry();
                }
            })
            .finally(() => {
                bulkPrefetchActive--;
                pumpBulkPrefetch();
            });
    }
}

function pumpSlidePrefetch() {
    for (const photo of slideQueue.slice(0, PREFETCH_AHEAD)) {
        const key = getPhotoKey(photo);
        if (!memoryBlobCache.has(key) && !pendingBlobLoads.has(key)) {
            getPhotoBlob(photo).catch(error => {
                console.warn('Slide prefetch failed:', key, error);
                if (error.code === 'AUTH_REQUIRED') {
                    markCacheAuthorizationRequired();
                }
            });
        }
    }
}
// ---- End persistent background prefetch ----

// ---- Picker sessions ----
function sessionUrl(sessionId) {
    return API_BASE + '/sessions/' + encodeURIComponent(sessionId);
}

function mediaItemsUrl(sessionId, pageToken) {
    const url = new URL(API_BASE + '/mediaItems');
    url.searchParams.set('sessionId', sessionId);
    if (pageToken) url.searchParams.set('pageToken', pageToken);
    return url.toString();
}

async function createPickerSession() {
    const response = await authenticatedFetch(API_BASE + '/sessions', {
        method: 'POST'
    });

    if (!response.ok) {
        throw new Error('Could not create picker session: HTTP ' + response.status);
    }
    return response.json();
}

function queuePickerSession(session) {
    const timeoutMs = parseDuration(
        session.pollingConfig && session.pollingConfig.timeoutIn,
        DEFAULT_SESSION_TIMEOUT_MS
    );

    pollQueue.push({
        sessionId: session.id,
        deadline: Date.now() + timeoutMs,
        interval: parseDuration(
            session.pollingConfig && session.pollingConfig.pollInterval,
            DEFAULT_POLL_INTERVAL_MS
        )
    });
    processQueue();
}

async function listSessionMediaItems(sessionId) {
    const items = [];
    let pageToken = null;

    do {
        const response = await authenticatedFetch(
            mediaItemsUrl(sessionId, pageToken)
        );
        if (!response.ok) {
            throw new Error('Could not list photos: HTTP ' + response.status);
        }

        const data = await response.json();
        if (Array.isArray(data.mediaItems)) {
            items.push(...data.mediaItems);
        }
        pageToken = data.nextPageToken || null;
    } while (pageToken);

    return items;
}

async function deletePickerSession(sessionId) {
    try {
        await authenticatedFetch(sessionUrl(sessionId), { method: 'DELETE' });
    } catch (error) {
        console.warn('Could not delete picker session:', error);
    }
}

async function mergePhotos(items) {
    const byKey = new Map(allPhotos.map(photo => [getPhotoKey(photo), photo]));
    const byBaseUrl = new Map(allPhotos.map(photo => [photo.baseUrl, photo]));
    let addedCount = 0;
    let changed = false;

    for (const item of items) {
        const photo = normalizePhoto(item);
        if (!photo) continue;

        const existing = byKey.get(getPhotoKey(photo)) || byBaseUrl.get(photo.baseUrl);
        if (existing) {
            changed = changed || existing.baseUrl !== photo.baseUrl || existing.mimeType !== photo.mimeType;
            byBaseUrl.delete(existing.baseUrl);
            existing.baseUrl = photo.baseUrl;
            existing.mimeType = photo.mimeType;
            byBaseUrl.set(existing.baseUrl, existing);
            continue;
        }

        allPhotos.push(photo);
        byKey.set(getPhotoKey(photo), photo);
        byBaseUrl.set(photo.baseUrl, photo);
        addedCount++;
    }

    if (addedCount > 0 || changed) {
        await dbPut('photos', allPhotos);
        if (addedCount > 0 && !slideshowStarted) startSlideshow();
        if (addedCount > 0) refillQueue();
        scheduleBulkPrefetch();
        pumpSlidePrefetch();
    }

    return addedCount;
}

async function processQueue() {
    if (pollInProgress || pollQueue.length === 0) return;

    pollInProgress = true;
    const currentSession = pollQueue[0];
    let nextPollDelay = currentSession.interval;

    try {
        if (Date.now() >= currentSession.deadline) {
            console.warn('Picker session timed out:', currentSession.sessionId);
            pollQueue.shift();
            await deletePickerSession(currentSession.sessionId);
            return;
        }

        const response = await authenticatedFetch(
            sessionUrl(currentSession.sessionId)
        );

        if (response.status === 404) {
            pollQueue.shift();
            return;
        }
        if (!response.ok) {
            throw new Error('Could not poll picker session: HTTP ' + response.status);
        }

        const session = await response.json();
        currentSession.interval = parseDuration(
            session.pollingConfig && session.pollingConfig.pollInterval,
            currentSession.interval
        );
        nextPollDelay = currentSession.interval;

        if (!session.mediaItemsSet) return;

        const items = await listSessionMediaItems(currentSession.sessionId);
        await mergePhotos(items);
        pollQueue.shift();
        await deletePickerSession(currentSession.sessionId);
    } catch (error) {
        console.error('Picker polling failed:', error);
        nextPollDelay = error.code === 'AUTH_REQUIRED' ? 15000 : 5000;
    } finally {
        pollInProgress = false;
        if (pollQueue.length > 0) {
            setTimeout(processQueue, nextPollDelay);
        }
    }
}
// ---- End Picker sessions ----

// ---- Slideshow ----
function shuffle(items) {
    for (let index = items.length - 1; index > 0; index--) {
        const randomIndex = Math.floor(Math.random() * (index + 1));
        [items[index], items[randomIndex]] = [items[randomIndex], items[index]];
    }
    return items;
}

function refillQueue() {
    const queuedKeys = new Set(slideQueue.map(getPhotoKey));
    const buildCandidates = () => allPhotos.filter(photo => {
        const key = getPhotoKey(photo);
        return (
            !queuedKeys.has(key) &&
            !playedPhotoKeys.has(key) &&
            key !== currentPhotoKey
        );
    });

    let candidates = buildCandidates();
    if (candidates.length === 0) {
        playedPhotoKeys.clear();
        candidates = buildCandidates();
    }

    shuffle(candidates);
    while (slideQueue.length < QUEUE_SIZE && candidates.length > 0) {
        const photo = candidates.shift();
        slideQueue.push(photo);
        queuedKeys.add(getPhotoKey(photo));
    }
}

function revokeObjectUrl(url) {
    if (url && url.startsWith('blob:')) {
        URL.revokeObjectURL(url);
    }
}

function displayPhoto(blob, photo) {
    const img1 = document.getElementById('img1');
    const img2 = document.getElementById('img2');
    const bg1 = document.getElementById('bg1');
    const bg2 = document.getElementById('bg2');
    const showingImg1 = img1.style.opacity === '1';

    const nextImg = showingImg1 ? img2 : img1;
    const currentImg = showingImg1 ? img1 : img2;
    const nextBg = showingImg1 ? bg2 : bg1;
    const currentBg = showingImg1 ? bg1 : bg2;
    const oldObjectUrl = currentImg.src;
    const objectUrl = URL.createObjectURL(blob);
    const origins = ['0% 0%', '100% 0%', '0% 100%', '100% 100%', '50% 50%'];
    const origin = origins[Math.floor(Math.random() * origins.length)];

    nextImg.style.transition = 'none';
    nextImg.style.transform = 'scale(1)';
    nextImg.style.transformOrigin = origin;
    nextBg.style.transition = 'none';
    nextBg.style.transform = 'scale(1)';
    nextImg.src = objectUrl;
    nextBg.src = objectUrl;
    void nextImg.offsetHeight;

    nextImg.style.transition = 'transform 5s ease-out, opacity 2s';
    nextImg.style.transform = 'scale(1.05)';
    nextImg.style.opacity = '1';
    nextBg.style.transition = 'opacity 2s';
    nextBg.style.opacity = '1';

    currentImg.style.opacity = '0';
    currentBg.style.opacity = '0';

    setTimeout(() => revokeObjectUrl(oldObjectUrl), 2200);
    currentPhotoKey = getPhotoKey(photo);
}

function scheduleNextSlide(delayMs) {
    if (nextSlideTimer) clearTimeout(nextSlideTimer);
    nextSlideTimer = setTimeout(() => {
        nextSlideTimer = null;
        advanceSlide();
    }, delayMs);
}

async function advanceSlide() {
    if (advancing || !slideshowStarted || allPhotos.length === 0) return;

    advancing = true;
    let photo = null;

    try {
        refillQueue();
        photo = slideQueue.shift();
        if (!photo) {
            photo = allPhotos[Math.floor(Math.random() * allPhotos.length)];
        }

        const key = getPhotoKey(photo);
        playedPhotoKeys.add(key);
        refillQueue();
        pumpSlidePrefetch();

        const blob = await getPhotoBlob(photo);
        displayPhoto(blob, photo);
        lastTransitionTime = Date.now();
        scheduleNextSlide(SLIDE_INTERVAL_MS);
    } catch (error) {
        let dom = '';
        try { if (photo) dom = '  dom=' + new URL(photo.baseUrl).hostname; } catch {}
        let cause = '';
        try { if (error && error.cause) cause = ' | cause=' + (error.cause.name || error.cause.code || error.cause.message || error.cause); } catch {}
        diag('slide/img ERROR: ' + (error.code || error.name || '') + ' ' + (error.message || '') + dom + cause);
        console.error('Slide error:', error);
        if (error.code === 'AUTH_REQUIRED') {
            markCacheAuthorizationRequired();
        }
        if (photo) slideQueue.push(photo);
        scheduleNextSlide(RETRY_INTERVAL_MS);
    } finally {
        advancing = false;
    }
}

function startSlideshow() {
    if (slideshowStarted || allPhotos.length === 0) return;

    slideshowStarted = true;
    document.getElementById('slideshow').style.display = 'block';
    document.getElementById('add-btn').style.display = 'block';
    refillQueue();
    scheduleBulkPrefetch();
    pumpSlidePrefetch();
    advanceSlide();
}
// ---- End Slideshow ----

async function loadFromStorage() {
    try {
        const saved = await dbGet('photos');
        if (!Array.isArray(saved)) return false;

        allPhotos = saved.map(normalizePhoto).filter(Boolean);
        if (allPhotos.length === 0) return false;

        return true;
    } catch (error) {
        console.error('Could not restore photos:', error);
        return false;
    }
}

async function openPicker() {
    requestPersistentStorage();

    try {
        const token = await requestAccessToken();
        if (!token) return;

        const session = await createPickerSession();
        const pickerUri = session.pickerUri.replace(/\/$/, '') + '/autoclose';
        window.open(pickerUri, '_blank');
        document.getElementById('login-btn').style.display = 'none';
        document.getElementById('add-btn').style.display = 'block';
        queuePickerSession(session);
    } catch (error) {
        console.error('Could not open Google Photos Picker:', error);
    }
}

async function addMorePhotos() {
    await openPicker();
}

async function requestPersistentStorage() {
    if (!navigator.storage || !navigator.storage.persist) return;

    try {
        const alreadyPersistent = navigator.storage.persisted
            ? await navigator.storage.persisted()
            : false;
        if (!alreadyPersistent) await navigator.storage.persist();
    } catch (error) {
        console.warn('Persistent storage request failed:', error);
    }
}

async function resumeCacheDownload(event) {
    event.stopPropagation();
    if (!cacheNeedsAuthorization) return;

    try {
        const token = await requestAccessToken();
        if (!token) return;

        cacheNeedsAuthorization = false;
        bulkPrefetchPausedForAuth = false;
        scheduleBulkPrefetch();
        pumpSlidePrefetch();
    } catch (error) {
        console.error('Could not resume local storage:', error);
        markCacheAuthorizationRequired();
    }
}

async function resetPhotos() {
    if (!confirm('Reset all photo list?')) return;

    try {
        await dbClearAll();
        location.reload();
    } catch (error) {
        console.error('Could not reset photos:', error);
    }
}

document.getElementById('login-btn').onclick = openPicker;
document.getElementById('cache-status').onclick = resumeCacheDownload;

// ---- 진단 오버레이 (임시) ----
function diag(msg) {
  let el = document.getElementById('diag-bar');
  if (!el) {
    el = document.createElement('div');
    el.id = 'diag-bar';
    el.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:9999;background:rgba(0,0,0,0.8);color:#0f0;font:12px monospace;padding:4px 8px;white-space:pre-wrap;';
    document.body.appendChild(el);
  }
  const t = new Date().toLocaleTimeString();
  const prev = (el.textContent || '').split('\n');
  prev.push(t + ' ' + msg);
  el.textContent = prev.slice(-5).join('\n');
}

// ---- GitHub sync ----
async function fetchManifest() {
  const url = GITHUB_RAW + '?t=' + Date.now();
  const resp = await fetch(url, { cache: "no-store" });
  if (!resp.ok) throw new Error("Manifest " + resp.status);
  return resp.json();
}
// GitHub photos.json은 "추가할 사진"의 저장소다.
// 뷰어에서 Picker로 로컬 추가한 사진은 GitHub에 없으므로, 통째로 교체하면
// 2분마다 날아가 버린다. 그래서 항상 합집합(union)으로 병합한다.
async function syncFromGitHub() {
  try {
    const manifest = await fetchManifest();
    diag('sync: photos=' + (manifest.photos || []).length + ' updated=' + manifest.updated);
    const remotePhotos = (manifest.photos || []).filter(p => p.baseUrl).map(p => ({ id: String(p.id), baseUrl: p.baseUrl, mimeType: p.mimeType || 'image/jpeg' }));
    const remoteKeys = new Set(remotePhotos.map(p => getPhotoKey(p)));

    // 원격에 있는 사진은 원격 기준(같은 id라도 갱신된 baseUrl로 교체),
    // 원격에 없는 로컬 사진(갤럭시탭에서 Picker로 추가한 것)은 유지 → 합집합(union)
    const localOnly = allPhotos.filter(p => !remoteKeys.has(getPhotoKey(p)));
    const merged = remotePhotos.concat(localOnly);

    const existing = new Map(allPhotos.map(p => [getPhotoKey(p), p]));
    let changed = false;
    for (const p of merged) {
      const key = getPhotoKey(p);
      const cur = existing.get(key);
      if (!cur) {
        allPhotos.push(p);
        existing.set(key, p);
        changed = true;
      } else if (cur.baseUrl !== p.baseUrl || cur.mimeType !== p.mimeType) {
        // 동일 사진이라도 signed URL이 갱신됐을 수 있으므로 최신 baseUrl로 반영
        cur.baseUrl = p.baseUrl;
        cur.mimeType = p.mimeType;
        bulkPrefetchCompleted.delete(key);
        // 옛 blob(만료 이미지 등)이 남으면 안 되니 캐시에서 제거해서 재다운로드
        memoryBlobCache.delete(key);
        try { pendingBlobLoads.delete(key); } catch {}
        dbDeleteMedia(key).catch(() => {});
        changed = true;
      }
    }

    if (changed) {
      try { await dbPut('photos', allPhotos); } catch {}
      refillQueue();
      scheduleBulkPrefetch();
      if (!slideshowStarted && allPhotos.length > 0) startSlideshow();
    }
  } catch (e) { diag('sync ERROR: ' + e.message); console.warn('GitHub sync:', e); }
}
// ---- End GitHub sync ----

window.onload = async () => {
    const restored = await loadFromStorage();
    if (restored) {
        document.getElementById('login-btn').style.display = 'none';
        document.getElementById('add-btn').style.display = 'block';
        startSlideshow();
    }
    syncFromGitHub();
    setInterval(syncFromGitHub, MANIFEST_POLL_MS);
};

setInterval(() => {
    if (
        slideshowStarted &&
        !advancing &&
        Date.now() - lastTransitionTime > 60000
    ) {
        advanceSlide();
    }
}, 10000);
