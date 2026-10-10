const API_BASE =
  (location.hostname === 'localhost' || location.hostname === '127.0.0.1')
    ? 'http://localhost:8080'
    : location.origin;

async function request(path, options = {}) {
  const token = localStorage.getItem('huddle_token');
  const res = await fetch(API_BASE + path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: 'Bearer ' + token } : {}),
      ...(options.headers || {})
    }
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: 'HTTP ' + res.status }));
    throw new Error(err.error || 'Request failed');
  }
  return res.json();
}

export const api = {
  baseUrl: API_BASE,
  get:  (path)       => request(path),
  post: (path, body) => request(path, { method: 'POST',   body: JSON.stringify(body) }),
  put:  (path, body) => request(path, { method: 'PUT',    body: JSON.stringify(body) }),
  del:  (path)       => request(path, { method: 'DELETE' }),
  request
};

export default api;