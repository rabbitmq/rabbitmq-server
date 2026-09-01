import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_JS_PATH = path.join(__dirname, '../priv/www/js/session.js');
const sessionSrc = fs.readFileSync(SESSION_JS_PATH, 'utf8');

// session.js is a plain browser script (globals + window.* assignments, no
// module exports), so it's loaded into a fresh vm context per test: that
// gives each test a clean copy of its private state (e.g. the heartbeat
// timer) and lets us stub its collaborators (sync_req, with_req, the local
// pref store, the login/logout processor registry).
let localPrefs;
let syncReqCalls;
let syncReqStub;
let withReqStub;
let clearAuthCalls;
let reloadCalls;
let sandbox;

function loadSessionModule() {
  localPrefs = {};
  syncReqCalls = [];
  syncReqStub = () => false;
  withReqStub = () => {};
  clearAuthCalls = 0;
  reloadCalls = 0;

  sandbox = {
    console,
    setInterval,
    clearInterval,
    get_local_pref: (k) => localPrefs[k],
    store_local_pref: (k, v) => { localPrefs[k] = v; },
    clear_local_pref: (k) => { delete localPrefs[k]; },
    sync_req: (...args) => { syncReqCalls.push(args); return syncReqStub(...args); },
    with_req: (...args) => withReqStub(...args),
    clear_auth: () => { clearAuthCalls++; },
    location: { reload: () => { reloadCalls++; } },
    registerLoginProcessor: (_name, fn) => { sandbox.loginProcessor = fn; },
    registerInitFailedProcessor: (_name, fn) => { sandbox.initFailedProcessor = fn; },
    registerLogoutProcessor: (_name, fn) => { sandbox.logoutProcessor = fn; }
  };
  sandbox.window = sandbox;

  vm.createContext(sandbox);
  vm.runInContext(sessionSrc, sandbox, { filename: SESSION_JS_PATH });
  return sandbox;
}

// sync_req's arguments can originate from inside the vm context (e.g. the
// `{}` body literal), which belongs to a different realm than this file's
// objects and so isn't reference-equal for assert's strict deepEqual. Assert
// on the primitive fields instead.
function assertReqCall(call, method, reqPath) {
  assert.equal(call[0], method);
  assert.equal(call[2], reqPath);
}

describe('session.js', () => {
  beforeEach(() => {
    loadSessionModule();
  });

  afterEach(() => {
    // Avoid leaking a real setInterval past the test that started it.
    sandbox.clear_session();
  });

  describe('get_session_id', () => {
    it('returns the id stored under the session_id pref', () => {
      localPrefs.session_id = 'abc-123';
      assert.equal(sandbox.get_session_id(), 'abc-123');
    });

    it('returns undefined when no session is stored', () => {
      assert.equal(sandbox.get_session_id(), undefined);
    });
  });

  describe('clear_session', () => {
    it('deletes the server-side session and the local pref when a session exists', () => {
      localPrefs.session_id = 'abc-123';
      syncReqStub = () => ({ http_status: 204 });

      sandbox.clear_session();

      assert.equal(syncReqCalls.length, 1);
      assertReqCall(syncReqCalls[0], 'DELETE', '/session/abc-123');
      assert.equal(localPrefs.session_id, undefined);
    });

    it('does not call the server when there is no local session', () => {
      sandbox.clear_session();

      assert.deepEqual(syncReqCalls, []);
    });
  });

  describe('check_session', () => {
    it('returns ok and keeps the id when the existing session heartbeats successfully', () => {
      localPrefs.session_id = 'abc-123';
      syncReqStub = () => ({ http_status: 204 });

      assert.equal(sandbox.check_session(), 'ok');
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(syncReqCalls.length, 1);
      assertReqCall(syncReqCalls[0], 'PUT', '/session/abc-123');
    });

    it('replaces a rejected session with a freshly created one', () => {
      localPrefs.session_id = 'stale-id';
      syncReqStub = (method) => method === 'PUT'
        ? { http_status: 401 }
        : { http_status: 201, responseText: JSON.stringify({ session_id: 'new-id' }) };

      assert.equal(sandbox.check_session(), 'ok');
      assert.deepEqual(syncReqCalls.map((c) => [c[0], c[2]]), [
        ['PUT', '/session/stale-id'],
        ['POST', '/session']
      ]);
      assert.equal(sandbox.get_session_id(), 'new-id');
    });

    it('creates a new session when none exists yet', () => {
      syncReqStub = () => ({ http_status: 201, responseText: JSON.stringify({ session_id: 'new-id' }) });

      assert.equal(sandbox.check_session(), 'ok');
      assert.equal(sandbox.get_session_id(), 'new-id');
      assert.equal(syncReqCalls.length, 1);
      assertReqCall(syncReqCalls[0], 'POST', '/session');
    });

    it('returns ok without storing an id when the sessions feature is disabled (404)', () => {
      syncReqStub = () => ({ http_status: 404 });

      assert.equal(sandbox.check_session(), 'ok');
      assert.equal(sandbox.get_session_id(), undefined);
    });

    it('returns limit_reached when the concurrent session limit is hit (403)', () => {
      syncReqStub = () => ({ http_status: 403 });

      assert.equal(sandbox.check_session(), 'limit_reached');
    });

    it('returns error when the server is unreachable', () => {
      syncReqStub = () => false;

      assert.equal(sandbox.check_session(), 'error');
    });
  });

  describe('start_session_heartbeat', () => {
    it('starts the recurring heartbeat when the initial ping succeeds', () => {
      syncReqStub = () => ({ http_status: 204 });

      assert.equal(sandbox.start_session_heartbeat('abc-123', 30), true);
      assert.equal(syncReqCalls.length, 1);
      assertReqCall(syncReqCalls[0], 'PUT', '/session/abc-123');
    });

    it('clears the session and reloads when the initial ping fails', () => {
      localPrefs.session_id = 'abc-123';
      syncReqStub = () => ({ http_status: 401 });

      assert.equal(sandbox.start_session_heartbeat('abc-123', 30), false);
      assert.equal(clearAuthCalls, 1);
      assert.equal(reloadCalls, 1);
      assert.equal(sandbox.get_session_id(), undefined);
    });
  });

  describe('login processor', () => {
    it('is registered under the "session" name', () => {
      assert.equal(typeof sandbox.loginProcessor, 'function');
    });

    it('starts a session and its heartbeat on successful login', () => {
      // check_session's POST creates the session (201); start_session_heartbeat's
      // follow-up PUT then needs to succeed (204) for the session to stick.
      syncReqStub = (method) => method === 'POST'
        ? { http_status: 201, responseText: JSON.stringify({ session_id: 'abc-123' }) }
        : { http_status: 204 };

      const result = sandbox.loginProcessor({ settings: { sessions: { heartbeat_interval: 5 } } });

      assert.equal(result.ok, true);
      assert.equal(sandbox.get_session_id(), 'abc-123');
    });

    it('reports the concurrent session limit as a login error', () => {
      syncReqStub = () => ({ http_status: 403 });

      const result = sandbox.loginProcessor({ settings: {} });

      assert.equal(result.ok, false);
      assert.equal(result.error, 'Concurrent session limit reached');
    });

    it('reports a generic failure when the server is unreachable', () => {
      syncReqStub = () => false;

      const result = sandbox.loginProcessor({ settings: {} });

      assert.equal(result.ok, false);
      assert.equal(result.error, 'Failed to establish session with server');
    });

    it('skips the session check entirely when the sessions feature is disabled', () => {
      sandbox.sessions_enabled = () => false;

      const result = sandbox.loginProcessor({ settings: {} });

      assert.equal(result.ok, true);
      assert.equal(syncReqCalls.length, 0);
    });
  });

  describe('init-failed and logout processors', () => {
    it('clear the session when init fails', () => {
      localPrefs.session_id = 'abc-123';
      syncReqStub = () => ({ http_status: 204 });

      sandbox.initFailedProcessor(new Error('boom'));

      assert.equal(sandbox.get_session_id(), undefined);
      assert.equal(syncReqCalls.length, 1);
      assertReqCall(syncReqCalls[0], 'DELETE', '/session/abc-123');
    });

    it('clear the session on logout', () => {
      localPrefs.session_id = 'abc-123';
      syncReqStub = () => ({ http_status: 204 });

      sandbox.logoutProcessor();

      assert.equal(sandbox.get_session_id(), undefined);
    });
  });
});
