import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WWW_JS = path.join(__dirname, '../priv/www/js');
const mainSrc = fs.readFileSync(path.join(WWW_JS, 'main.js'), 'utf8');
const authProvidersSrc = fs.readFileSync(path.join(WWW_JS, 'auth-providers.js'), 'utf8');

// on_page_load() is what $(document).ready runs. It is a named function
// precisely so a test can call it: an anonymous callback handed to jQuery
// leaves no handle to invoke.
//
// main.js is loaded with the real auth-providers.js so that the provider
// lookup and dispatch are genuinely exercised; only the leaf actions the
// providers take (rendering a login page, warning the user) are recorded.
let sandbox;
let calls;

function loadPage(oauthState, href) {
  calls = [];
  const record = (name) => (...args) => calls.push([name, ...args].map(String).join(' '));

  const jq = function() { return { ready: () => {}, on: () => {}, off: () => {} }; };
  jq.fn = { extend: () => {} };
  jq.inArray = () => -1;

  sandbox = {
    console,
    document: {},
    jQuery: jq,
    $: jq,
    window: { location: { href: href } },
    URL,
    oauth: oauthState,
    registerInitStep: () => true,
    clear_pref: record('clear_pref'),
    fmt_escape_html: (s) => s
  };
  vm.createContext(sandbox);
  // Providers first: main.js calls login_flow_provider() at page load.
  vm.runInContext(authProvidersSrc, sandbox, { filename: 'auth-providers.js' });
  vm.runInContext(mainSrc, sandbox, { filename: 'main.js' });

  // Recorded after loading, not before: main.js declares these itself, and
  // its declarations would otherwise overwrite the stubs. The providers
  // resolve them as globals when called, so replacing them now works.
  sandbox.startWithLoginPage = record('startWithLoginPage');
  sandbox.startWithOAuthLogin = record('startWithOAuthLogin');
  sandbox.renderWarningMessageInLoginStatus = (o, message) => calls.push('warn ' + message);
  return sandbox;
}

describe('on_page_load: which login flow starts', () => {
  it('starts the oauth flow when oauth is configured', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/');

    sandbox.on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });

  it('starts the plain login page when oauth is not configured', () => {
    loadPage({ enabled: false }, 'https://rabbit.example/');

    sandbox.on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });

  it('chooses by configuration, not by who logged in previously', () => {
    // No session exists yet at page load, so the identity-based lookup
    // used elsewhere would send a first-time visitor to the wrong page.
    loadPage({ enabled: true }, 'https://rabbit.example/');
    assert.equal(typeof sandbox.has_auth_resource, 'undefined');

    sandbox.on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });
});

describe('on_page_load: a failed IdP-initiated login', () => {
  beforeEach(() => {
    loadPage({ enabled: true }, 'https://rabbit.example/?error=access_denied');
  });

  it('reports the error instead of starting a login flow', () => {
    sandbox.on_page_load();

    assert.ok(calls.includes('warn access_denied'));
    assert.ok(!calls.some((c) => c.startsWith('startWithOAuthLogin')));
    assert.ok(!calls.some((c) => c.startsWith('startWithLoginPage')));
  });

  it('drops the pending-redirect markers it set on the way out', () => {
    sandbox.on_page_load();

    assert.ok(calls.includes('clear_pref oauth-idp-pending'));
    assert.ok(calls.includes('clear_pref oauth-return-to'));
  });
});

describe('on_page_load: a URL that belongs to nobody', () => {
  it('ignores an unrelated query string and starts the flow normally', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/?something=else');

    sandbox.on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin [object Object]']);
  });

  it('does not treat ?error= as an oauth redirect when oauth is off', () => {
    // Only the oauth module ever produces ?error=, so with oauth disabled
    // this can only be a stale URL: show the login page rather than a
    // blank one.
    loadPage({ enabled: false }, 'https://rabbit.example/?error=access_denied');

    sandbox.on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });
});
