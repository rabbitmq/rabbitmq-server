import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  registerAuthProvider,
  unregisterAuthProvider,
  getAuthProvider,
  set_active_auth_provider,
  set_active_auth_provider_by_name,
  login_flow_provider,
  active_auth_provider
} from '../priv/www/js/auth-providers.js';
import { on_page_load } from '../priv/www/js/main.js';

let calls;

function loadPage(oauthState, href) {
  calls = [];
  const record = (name) => (...args) => calls.push([name, ...args].map(String).join(' '));

  const jq = function() { return { ready: () => {}, on: () => {}, off: () => {} }; };
  jq.fn = { extend: () => {} };
  jq.inArray = () => -1;

  globalThis.console = console;
  globalThis.document = {};
  globalThis.jQuery = jq;
  globalThis.$ = jq;
  globalThis.window = { location: { href: href } };
  globalThis.URL = URL;
  globalThis.oauth = oauthState;
  globalThis.registerInitStep = () => true;
  globalThis.clear_pref = record('clear_pref');
  globalThis.fmt_escape_html = (s) => s;
  globalThis.startWithLoginPage = record('startWithLoginPage');
  globalThis.startWithOAuthLogin = record('startWithOAuthLogin');
  globalThis.renderWarningMessageInLoginStatus = (o, message) => calls.push('warn ' + message);
}

describe('on_page_load: which login flow starts', () => {
  it('starts the oauth flow when oauth is configured', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });

  it('starts the plain login page when oauth is not configured', () => {
    loadPage({ enabled: false }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });

  it('chooses by configuration, not by who logged in previously', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });
});

describe('on_page_load: a failed IdP-initiated login', () => {
  beforeEach(() => {
    loadPage({ enabled: true }, 'https://rabbit.example/?error=access_denied');
  });

  it('reports the error instead of starting a login flow', () => {
    on_page_load();

    assert.ok(calls.includes('warn access_denied'));
    assert.ok(!calls.some((c) => c.startsWith('startWithOAuthLogin')));
    assert.ok(!calls.some((c) => c.startsWith('startWithLoginPage')));
  });

  it('drops the pending-redirect markers it set on the way out', () => {
    on_page_load();

    assert.ok(calls.includes('clear_pref oauth-idp-pending'));
    assert.ok(calls.includes('clear_pref oauth-return-to'));
  });
});

describe('on_page_load: a URL that belongs to nobody', () => {
  it('ignores an unrelated query string and starts the flow normally', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/?something=else');

    on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });

  it('does not treat ?error= as an oauth redirect when oauth is off', () => {
    loadPage({ enabled: false }, 'https://rabbit.example/?error=access_denied');

    on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });
});
