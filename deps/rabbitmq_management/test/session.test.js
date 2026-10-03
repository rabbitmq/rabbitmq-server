// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

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
// timer) and lets us stub its collaborators (the XMLHttpRequest factory, the
// local pref store, the login/logout processor registry).
let localPrefs;
let requests;
let responder;
let clearAuthCalls;
let reloadCalls;
let intervals;
let clearedIntervals;
let sandbox;

class FakeXhr {
  open(method, url, async) {
    this.method = method;
    this.url = url;
    this.async = async;
    this.headers = {};
  }

  setRequestHeader(name, value) {
    this.headers[name] = value;
  }

  send(body) {
    requests.push({ method: this.method, url: this.url, async: this.async, headers: this.headers, body });
    const res = responder(this.method, this.url);
    if (res === 'network-error') {
      throw new Error('network error');
    }
    this.status = res.status;
    this.responseText = res.responseText || '';
    this.readyState = 4;
    if (this.async && this.onreadystatechange) {
      this.onreadystatechange();
    }
  }
}

function loadSessionModule() {
  localPrefs = {};
  requests = [];
  responder = () => ({ status: 0 });
  clearAuthCalls = 0;
  reloadCalls = 0;
  intervals = [];
  clearedIntervals = [];

  sandbox = {
    console,
    setInterval: (fn, ms) => { intervals.push({ fn, ms }); return intervals.length; },
    clearInterval: (id) => { clearedIntervals.push(id); },
    get_local_pref: (k) => localPrefs[k],
    store_local_pref: (k, v) => { localPrefs[k] = v; },
    clear_local_pref: (k) => { delete localPrefs[k]; },
    xmlHttpRequest: () => new FakeXhr(),
    authorization_header: () => 'Basic dGVzdDp0ZXN0',
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

function assertRequest(request, method, url) {
  assert.equal(request.method, method);
  assert.equal(request.url, url);
}

function created(sessionId) {
  return { status: 201, responseText: JSON.stringify({ session_id: sessionId }) };
}

describe('session.js', () => {
  beforeEach(() => {
    loadSessionModule();
  });

  afterEach(() => {
    // Avoid leaking a real setInterval past the test that started it.
    sandbox.clear_session();
  });

  describe('session_req', () => {
    it('sends the authorization header and a JSON body to the session endpoint', () => {
      responder = () => ({ status: 204 });

      sandbox.session_req('PUT', 'abc-123');

      assertRequest(requests[0], 'PUT', 'api/session/abc-123');
      assert.equal(requests[0].async, false);
      assert.equal(requests[0].headers.authorization, 'Basic dGVzdDp0ZXN0');
      assert.equal(requests[0].headers['content-type'], 'application/json');
    });

    it('omits the authorization header when there are no credentials', () => {
      sandbox.authorization_header = () => null;
      responder = () => ({ status: 401 });

      sandbox.session_req('POST');

      assertRequest(requests[0], 'POST', 'api/session');
      assert.equal('authorization' in requests[0].headers, false);
    });

    it('returns the status and body of a failed request', () => {
      responder = () => ({ status: 403, responseText: '{"error":"not_authorized"}' });

      const res = sandbox.session_req('POST');

      assert.equal(res.http_status, 403);
      assert.equal(res.responseText, '{"error":"not_authorized"}');
    });

    it('returns status 0 when the request cannot be sent', () => {
      responder = () => 'network-error';

      assert.equal(sandbox.session_req('POST').http_status, 0);
    });

    it('passes the result to the callback and sends asynchronously when one is given', () => {
      responder = () => ({ status: 204 });
      let result;

      sandbox.session_req('PUT', 'abc-123', (res) => { result = res; });

      assert.equal(requests[0].async, true);
      assert.equal(result.http_status, 204);
    });

    it('passes status 0 to the callback when the request cannot be sent', () => {
      responder = () => 'network-error';
      let result;

      sandbox.session_req('PUT', 'abc-123', (res) => { result = res; });

      assert.equal(result.http_status, 0);
    });
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
      responder = () => ({ status: 204 });

      sandbox.clear_session();

      assert.equal(requests.length, 1);
      assertRequest(requests[0], 'DELETE', 'api/session/abc-123');
      assert.equal(localPrefs.session_id, undefined);
    });

    it('does not call the server when there is no local session', () => {
      sandbox.clear_session();

      assert.deepEqual(requests, []);
    });

    it('sends one DELETE when the request itself triggers another logout', () => {
      localPrefs.session_id = 'abc-123';
      responder = () => { sandbox.clear_session(); return { status: 401 }; };

      sandbox.clear_session();

      assert.equal(requests.length, 1);
    });
  });

  describe('check_session', () => {
    it('returns ok and keeps the id when the existing session heartbeats successfully', () => {
      localPrefs.session_id = 'abc-123';
      responder = () => ({ status: 204 });

      assert.equal(sandbox.check_session(), 'ok');
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(requests.length, 1);
      assertRequest(requests[0], 'PUT', 'api/session/abc-123');
    });

    it('replaces a rejected session with a freshly created one', () => {
      localPrefs.session_id = 'stale-id';
      responder = (method) => method === 'PUT' ? { status: 401 } : created('new-id');

      assert.equal(sandbox.check_session(), 'ok');
      assert.deepEqual(requests.map((r) => [r.method, r.url]), [
        ['PUT', 'api/session/stale-id'],
        ['POST', 'api/session']
      ]);
      assert.equal(sandbox.get_session_id(), 'new-id');
    });

    it('also replaces a session that belongs to another user (403) or is gone (404)', () => {
      for (const status of [403, 404]) {
        localPrefs.session_id = 'stale-id';
        responder = (method) => method === 'PUT' ? { status } : created('new-id');

        assert.equal(sandbox.check_session(), 'ok');
        assert.equal(sandbox.get_session_id(), 'new-id');
      }
    });

    it('keeps the stored id and returns error when the server cannot be reached', () => {
      localPrefs.session_id = 'abc-123';
      responder = () => ({ status: 0 });

      assert.equal(sandbox.check_session(), 'error');
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(requests.length, 1);
    });

    it('creates a new session when none exists yet', () => {
      responder = () => created('new-id');

      assert.equal(sandbox.check_session(), 'ok');
      assert.equal(sandbox.get_session_id(), 'new-id');
      assert.equal(requests.length, 1);
      assertRequest(requests[0], 'POST', 'api/session');
    });

    it('returns limit_reached when the concurrent session limit is hit (403)', () => {
      responder = () => ({ status: 403, responseText: '{"error":"not_authorized"}' });

      assert.equal(sandbox.check_session(), 'limit_reached');
      assert.equal(sandbox.get_session_id(), undefined);
    });

    it('returns error when the sessions endpoint does not exist (404, 405)', () => {
      for (const status of [404, 405]) {
        responder = () => ({ status });

        assert.equal(sandbox.check_session(), 'error');
      }
    });

    it('returns error when a created session has no id', () => {
      responder = () => ({ status: 201, responseText: '{}' });

      assert.equal(sandbox.check_session(), 'error');
    });

    it('returns error when the server is unreachable', () => {
      responder = () => 'network-error';

      assert.equal(sandbox.check_session(), 'error');
    });
  });

  describe('start_session_heartbeat', () => {
    it('schedules the heartbeat without sending a request', () => {
      sandbox.start_session_heartbeat('abc-123', 30);

      assert.equal(requests.length, 0);
      assert.equal(intervals.length, 1);
      assert.equal(intervals[0].ms, 30000);
    });

    it('converts the interval from seconds to milliseconds', () => {
      sandbox.start_session_heartbeat('abc-123', 45);

      assert.equal(intervals[0].ms, 45000);
    });

    it('falls back to 30 seconds for a missing, zero, negative or non-numeric interval', () => {
      for (const interval of [undefined, 0, -5, '60']) {
        intervals.length = 0;

        sandbox.start_session_heartbeat('abc-123', interval);

        assert.equal(intervals[0].ms, 30000, `interval ${interval}`);
      }
    });

    it('replaces a heartbeat that is already running', () => {
      sandbox.start_session_heartbeat('abc-123', 30);
      sandbox.start_session_heartbeat('abc-123', 30);

      assert.equal(intervals.length, 2);
      assert.deepEqual(clearedIntervals, [1]);
    });
  });

  describe('stop_session_heartbeat', () => {
    it('stops the timer and keeps the stored id', () => {
      localPrefs.session_id = 'abc-123';
      sandbox.start_session_heartbeat('abc-123', 30);

      sandbox.stop_session_heartbeat();

      assert.deepEqual(clearedIntervals, [1]);
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(requests.length, 0);
    });

    it('does nothing when no heartbeat is running', () => {
      sandbox.stop_session_heartbeat();

      assert.deepEqual(clearedIntervals, []);
    });
  });

  describe('recurring heartbeat', () => {
    let tick;

    beforeEach(() => {
      responder = () => ({ status: 204 });
      sandbox.start_session_heartbeat('abc-123', 30);
      tick = intervals[0].fn;
    });

    it('sends an asynchronous PUT and keeps the session when it succeeds', () => {
      tick();

      assert.equal(requests.length, 1);
      assertRequest(requests[0], 'PUT', 'api/session/abc-123');
      assert.equal(requests[0].async, true);
      assert.equal(reloadCalls, 0);
    });

    it('stops, clears the credentials and reloads when the server rejects the session', () => {
      for (const status of [401, 403, 404]) {
        reloadCalls = 0;
        clearAuthCalls = 0;
        sandbox.start_session_heartbeat('abc-123', 30);
        tick = intervals[intervals.length - 1].fn;
        clearedIntervals.length = 0;
        responder = () => ({ status });

        tick();

        assert.equal(reloadCalls, 1, `status ${status}`);
        assert.equal(clearAuthCalls, 1, `status ${status}`);
        assert.equal(clearedIntervals.length, 1, `status ${status}`);
      }
    });

    it('keeps the stored id and does not send a DELETE when the session is rejected', () => {
      localPrefs.session_id = 'abc-123';
      responder = () => ({ status: 401 });
      requests.length = 0;

      tick();

      assert.deepEqual(requests.map((r) => r.method), ['PUT']);
      assert.equal(sandbox.get_session_id(), 'abc-123');
    });

    it('ignores failures that do not mean the session is gone', () => {
      for (const status of [0, 500, 503]) {
        responder = () => ({ status });

        tick();
      }

      assert.equal(reloadCalls, 0);
      assert.equal(clearAuthCalls, 0);
    });

    it('reads the credentials again on every beat', () => {
      const headers = [];
      sandbox.authorization_header = () => { const h = 'Bearer ' + (headers.length + 1); headers.push(h); return h; };
      requests.length = 0;

      tick();
      tick();

      assert.deepEqual(requests.map((r) => r.headers.authorization), ['Bearer 1', 'Bearer 2']);
    });
  });

  describe('login processor', () => {
    it('is registered under the "session" name', () => {
      assert.equal(typeof sandbox.loginProcessor, 'function');
    });

    it('creates a session and schedules its heartbeat on a first login', () => {
      responder = () => created('abc-123');

      const result = sandbox.loginProcessor({ settings: { sessions: { enabled: true, heartbeat_interval: 5 } } });

      assert.equal(result.ok, true);
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.deepEqual(requests.map((r) => [r.method, r.url]), [['POST', 'api/session']]);
      assert.equal(intervals.length, 1);
      assert.equal(intervals[0].ms, 5000);
    });

    it('resumes a stored session with a single request', () => {
      localPrefs.session_id = 'abc-123';
      responder = () => ({ status: 204 });

      const result = sandbox.loginProcessor({ settings: { sessions: { enabled: true, heartbeat_interval: 30 } } });

      assert.equal(result.ok, true);
      assert.deepEqual(requests.map((r) => [r.method, r.url]), [['PUT', 'api/session/abc-123']]);
      assert.equal(intervals.length, 1);
    });

    it('does not start a heartbeat when the login fails', () => {
      responder = () => ({ status: 403 });

      sandbox.loginProcessor({ settings: { sessions: { enabled: true } } });

      assert.equal(intervals.length, 0);
    });

    it('reports the concurrent session limit as a login error', () => {
      responder = () => ({ status: 403, responseText: '{"error":"not_authorized"}' });

      const result = sandbox.loginProcessor({ settings: { sessions: { enabled: true } } });

      assert.equal(result.ok, false);
      assert.equal(result.error, 'Concurrent session limit reached');
    });

    it('reports a generic failure when the server is unreachable', () => {
      responder = () => 'network-error';

      const result = sandbox.loginProcessor({ settings: { sessions: { enabled: true } } });

      assert.equal(result.ok, false);
      assert.equal(result.error, 'Failed to establish session with server');
    });

    it('skips the session check when the sessions feature is disabled', () => {
      const result = sandbox.loginProcessor({ settings: { sessions: { enabled: false } } });

      assert.equal(result.ok, true);
      assert.equal(requests.length, 0);
    });

    it('skips the session check when the settings have no sessions entry', () => {
      const result = sandbox.loginProcessor({ settings: {} });

      assert.equal(result.ok, true);
      assert.equal(requests.length, 0);
    });
  });

  describe('init-failed and logout processors', () => {
    it('stop the heartbeat and keep the id when init fails, so the next login resumes the session', () => {
      localPrefs.session_id = 'abc-123';
      sandbox.start_session_heartbeat('abc-123', 30);

      sandbox.initFailedProcessor(new Error('boom'));

      assert.deepEqual(clearedIntervals, [1]);
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(requests.length, 0);
    });

    it('delete the session and the stored id on a voluntary logout', () => {
      localPrefs.session_id = 'abc-123';
      sandbox.start_session_heartbeat('abc-123', 30);
      responder = () => ({ status: 204 });

      sandbox.logoutProcessor();

      assert.deepEqual(clearedIntervals, [1]);
      assert.equal(sandbox.get_session_id(), undefined);
      assert.deepEqual(requests.map((r) => [r.method, r.url]), [['DELETE', 'api/session/abc-123']]);
    });

    it('only stop the heartbeat on an involuntary logout', () => {
      localPrefs.session_id = 'abc-123';
      sandbox.start_session_heartbeat('abc-123', 30);

      sandbox.logoutProcessor({ involuntary: true });

      assert.deepEqual(clearedIntervals, [1]);
      assert.equal(sandbox.get_session_id(), 'abc-123');
      assert.equal(requests.length, 0);
    });
  });
});
