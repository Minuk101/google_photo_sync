const CLIENT_ID = '232709413830-gjmgctle15h91vcm1i9vtb6h5lnrk84o.apps.googleusercontent.com';
const PHOTO_SCOPE = 'https://www.googleapis.com/auth/photospicker.mediaitems.readonly';
const API_BASE = 'https://photospicker.googleapis.com/v1';
const GITHUB_API = 'https://api.github.com/repos/Minuk101/google_photo_sync/contents/photos.json';

let globalToken = null;
let tokenExpiresAt = 0;
let tokenClient = null;
let allPhotos = [];
let pollTimer = null;

const ghTokenInput = document.getElementById('gh-token');
const saveTokenBtn = document.getElementById('save-token');
const tokenStatus = document.getElementById('token-status');
const loginBtn = document.getElementById('login-btn');
const pickBtn = document.getElementById('pick-btn');
const clearBtn = document.getElementById('clear-btn');
const saveGhBtn = document.getElementById('save-gh');
const authStatus = document.getElementById('auth-status');
const photoList = document.getElementById('photo-list');
const countStatus = document.getElementById('count-status');
const saveStatus = document.getElementById('save-status');

// ---- GitHub Token ----
const loadedToken = localStorage.getItem('gh_pat') || '';
if (loadedToken) { ghTokenInput.value = loadedToken; tokenStatus.textContent = '토큰 저장됨 ✓'; saveGhBtn.disabled = false; }
saveTokenBtn.addEventListener('click', () => {
  const token = ghTokenInput.value.trim();
  if (token) { localStorage.setItem('gh_pat', token); tokenStatus.textContent = '토큰 저장됨 ✓'; }
  else { localStorage.removeItem('gh_pat'); tokenStatus.textContent = '토큰이 제거되었습니다.'; }
  saveGhBtn.disabled = !token || allPhotos.length === 0;
});
function getGhToken() { return localStorage.getItem('gh_pat') || ''; }

// ---- Auth ----
function hasToken() { return Boolean(globalToken && Date.now() < tokenExpiresAt - 30000); }
function applyToken(resp) {
  globalToken = resp.access_token;
  tokenExpiresAt = Date.now() + (resp.expires_in || 3600) * 1000;
  authStatus.textContent = '로그인 완료 ✓';
  loginBtn.disabled = true;
  pickBtn.disabled = false;
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
    callback: resp => { if (resp?.access_token) applyToken(resp); else authStatus.textContent = '인증 실패'; },
    error_callback: err => { console.warn('Auth error:', err); authStatus.textContent = '인증 오류'; }
  });
}
async function requestToken() {
  if (hasToken()) return globalToken;
  await waitForGsi();
  if (!tokenClient) initTokenClient();
  return new Promise((resolve) => {
    const origCallback = tokenClient.callback;
    tokenClient.callback = resp => {
      origCallback(resp);
      resolve(resp?.access_token || null);
    };
    tokenClient.requestAccessToken();
  });
}
function getToken() {
  if (!hasToken()) throw Object.assign(new Error('Auth required'), { code: 'AUTH_REQUIRED' });
  return globalToken;
}
loginBtn.addEventListener('click', async () => {
  loginBtn.disabled = true;
  authStatus.textContent = '로그인 중...';
  try { await requestToken(); } catch { authStatus.textContent = '로그인 실패. 다시 시도해주세요.'; loginBtn.disabled = false; }
});
// ---- End Auth ----

// ---- Picker ----
async function apiFetch(url, init = {}) {
  const token = getToken();
  const resp = await fetch(url, { ...init, headers: { ...init.headers, Authorization: `Bearer ${token}` } });
  if (resp.status === 401) { globalToken = null; tokenExpiresAt = 0; throw Object.assign(new Error('Auth expired'), { code: 'AUTH_REQUIRED' }); }
  return resp;
}
async function createSession() {
  const resp = await apiFetch(API_BASE + '/sessions', { method: 'POST' });
  if (!resp.ok) throw new Error(`Session ${resp.status}`);
  return resp.json();
}
async function pollSession(sessionId) {
  const resp = await apiFetch(API_BASE + '/sessions/' + encodeURIComponent(sessionId));
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`Poll ${resp.status}`);
  return resp.json();
}
async function listMediaItems(sessionId) {
  const items = [];
  let pageToken = null;
  do {
    const url = new URL(API_BASE + '/mediaItems');
    url.searchParams.set('sessionId', sessionId);
    if (pageToken) url.searchParams.set('pageToken', pageToken);
    const resp = await apiFetch(url.toString());
    if (!resp.ok) throw new Error(`List ${resp.status}`);
    const data = await resp.json();
    if (Array.isArray(data.mediaItems)) items.push(...data.mediaItems);
    pageToken = data.nextPageToken || null;
  } while (pageToken);
  return items;
}
async function deleteSession(sessionId) {
  try { await apiFetch(API_BASE + '/sessions/' + encodeURIComponent(sessionId), { method: 'DELETE' }); } catch {}
}
function normalizePhoto(item) {
  const mf = item.mediaFile || item;
  if (!mf?.baseUrl) return null;
  return { id: String(item.id || mf.baseUrl), baseUrl: mf.baseUrl, mimeType: mf.mimeType || 'image/jpeg' };
}
function renderPhotos() {
  photoList.innerHTML = '';
  const shown = allPhotos.slice(-50);
  for (const p of shown) {
    const div = document.createElement('div');
    div.className = 'photo-item';
    const img = document.createElement('img');
    img.src = p.baseUrl + '=w80-h80-c';
    img.loading = 'lazy';
    const span = document.createElement('span');
    span.textContent = p.id.slice(0, 40) + '...';
    div.append(img, span);
    photoList.append(div);
  }
  countStatus.textContent = `총 ${allPhotos.length}장 선택됨`;
  saveGhBtn.disabled = !getGhToken() || allPhotos.length === 0;
}
pickBtn.addEventListener('click', async () => {
  pickBtn.disabled = true;
  authStatus.textContent = 'Picker 실행 중...';
  try {
    const session = await createSession();
    const timeout = (session.pollingConfig?.timeoutIn || '600s').replace('s', '') * 1000;
    const interval = (session.pollingConfig?.pollInterval || '3s').replace('s', '') * 1000;
    const deadline = Date.now() + timeout;

    const poll = async () => {
      if (Date.now() > deadline) { authStatus.textContent = 'Picker 시간 초과'; pickBtn.disabled = false; return; }
      const s = await pollSession(session.id);
      if (!s) { authStatus.textContent = 'Picker 세션 만료'; pickBtn.disabled = false; return; }
      if (s.mediaItemsSet) {
        const items = await listMediaItems(session.id);
        const existing = new Set(allPhotos.map(p => p.id));
        let added = 0;
        for (const item of items) {
          const p = normalizePhoto(item);
          if (p && !existing.has(p.id)) { allPhotos.push(p); existing.add(p.id); added++; }
        }
        await deleteSession(session.id);
        authStatus.textContent = `${added}장 추가됨. 총 ${allPhotos.length}장`;
        renderPhotos();
        pickBtn.disabled = false;
        return;
      }
      setTimeout(poll, interval);
    };
    await poll();
  } catch (err) {
    authStatus.textContent = '오류: ' + err.message;
    pickBtn.disabled = false;
    if (err.code === 'AUTH_REQUIRED') { loginBtn.disabled = false; authStatus.textContent = '인증이 만료되었습니다. 다시 로그인해주세요.'; }
  }
});
clearBtn.addEventListener('click', () => { allPhotos = []; renderPhotos(); countStatus.textContent = '초기화됨'; });
// ---- End Picker ----

// ---- GitHub Save ----
saveGhBtn.addEventListener('click', async () => {
  const token = getGhToken();
  if (!token) { saveStatus.textContent = 'GitHub 토큰을 먼저 저장해주세요.'; saveStatus.className = 'status error'; return; }
  saveGhBtn.disabled = true;
  saveStatus.textContent = '저장 중...';
  saveStatus.className = 'status';

  try {
    const manifest = { version: 1, updated: new Date().toISOString(), photos: allPhotos.map(p => ({ id: p.id, baseUrl: p.baseUrl, mimeType: p.mimeType })) };
    const content = btoa(unescape(encodeURIComponent(JSON.stringify(manifest, null, 2))));

    // Get current SHA
    let sha = null;
    const getResp = await fetch(GITHUB_API, { headers: { Authorization: `Bearer ${token}` } });
    if (getResp.ok) { const f = await getResp.json(); sha = f.sha; }

    const putResp = await fetch(GITHUB_API, {
      method: 'PUT',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: `사진 ${allPhotos.length}장 업데이트`, content, sha })
    });
    if (!putResp.ok) { const err = await putResp.json().catch(() => ({})); throw new Error(err.message || `HTTP ${putResp.status}`); }

    saveStatus.textContent = `저장 완료! ${allPhotos.length}장이 GitHub에 반영되었습니다.`;
  } catch (err) {
    saveStatus.textContent = '저장 실패: ' + err.message;
    saveStatus.className = 'status error';
  } finally { saveGhBtn.disabled = false; }
});
// ---- End GitHub Save ----

// Init
async function init() {
  try {
    const resp = await fetch('https://raw.githubusercontent.com/Minuk101/google_photo_sync/main/photos.json?t=' + Date.now(), { cache: 'no-store' });
    if (resp.ok) {
      const manifest = await resp.json();
      allPhotos = manifest.photos || [];
      renderPhotos();
    }
  } catch {}
}
init();
