import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const AUTH_PROVIDERS_JS_PATH = path.join(__dirname, '../priv/www/js/auth-providers.js');
const authProvidersSrc = fs.readFileSync(AUTH_PROVIDERS_JS_PATH, 'utf8');

// The provider method bodies call functions that live in main.js and
// friends (replace_content, clear_auth, oauth_initiateLogout, ...). Those
// are resolved as globals at call time, so the test stubs them as
// recorders: this is about which provider gets asked to do the work and
// what it calls, not about what those functions themselves do.
let sandbox;
let calls;

function loadAuthProviders(oauthState) {
  calls = [];
  const record = (name) => (...args) => calls.push([name, ...args]);

  sandbox = {
    console,
    oauth: oauthState,
    replace_content: record('replace_content'),
    fmt_escape_html: (s) => s,
    format_error_response: (response, reason) => `formatted:${reason}`,
    show_popup: record('show_popup'),
    clear_auth: record('clear_auth'),
    go_to_home: record('go_to_home'),
    oauth_initiateLogout: record('oauth_initiateLogout'),
    renderWarningMessageInLoginStatus: record('renderWarningMessageInLoginStatus'),
    initiate_logout: record('initiate_logout')
  };
  vm.createContext(sandbox);
  vm.runInContext(authProvidersSrc, sandbox, { filename: AUTH_PROVIDERS_JS_PATH });
  return sandbox;
}

// Recorded arguments can originate inside the vm, so compare a flattened
// string rather than relying on cross-realm deepEqual.
function callSummary() {
  return calls.map((c) => c.map((v) => String(v)).join(' ')).join(' | ');
}

describe('registry', () => {
  beforeEach(() => { loadAuthProviders({ enabled: false }); });

  it('registers the basic and oauth2 providers out of the box', () => {
    assert.equal(typeof sandbox.getAuthProvider('basic'), 'object');
    assert.equal(typeof sandbox.getAuthProvider('oauth2'), 'object');
  });

  it('rejects a duplicate name', () => {
    assert.equal(sandbox.registerAuthProvider('basic', { presentError: () => {} }), false);
  });

  it('rejects a null or blank name, and a non-object provider', () => {
    assert.equal(sandbox.registerAuthProvider(null, {}), false);
    assert.equal(sandbox.registerAuthProvider('  ', {}), false);
    assert.equal(sandbox.registerAuthProvider('custom', null), false);
    assert.equal(sandbox.registerAuthProvider('custom', 'nope'), false);
  });

  it('accepts an additional mechanism, so a third one is a registration', () => {
    assert.equal(sandbox.registerAuthProvider('mtls', { presentError: () => {} }), true);
    assert.equal(typeof sandbox.getAuthProvider('mtls'), 'object');
  });
});

describe('providerForToken', () => {
  beforeEach(() => { loadAuthProviders({ enabled: true }); });

  it('resolves a bearer token to oauth2', () => {
    assert.equal(sandbox.providerForToken({ type: 'bearer' }), sandbox.getAuthProvider('oauth2'));
  });

  it('resolves any other token to basic', () => {
    assert.equal(sandbox.providerForToken({ type: 'basic' }), sandbox.getAuthProvider('basic'));
    assert.equal(sandbox.providerForToken(null), sandbox.getAuthProvider('basic'));
    assert.equal(sandbox.providerForToken(undefined), sandbox.getAuthProvider('basic'));
  });
});

describe('active_auth_provider', () => {
  it('falls back to oauth2 when oauth is configured and no login has happened', () => {
    loadAuthProviders({ enabled: true });
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('oauth2'));
  });

  it('falls back to basic when oauth is not configured', () => {
    loadAuthProviders({ enabled: false });
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('basic'));
  });

  it('prefers the provider resolved from the token once one is set', () => {
    loadAuthProviders({ enabled: true });
    sandbox.set_active_auth_provider(sandbox.providerForToken({ type: 'basic' }));
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('basic'));
  });
});

describe('basic provider', () => {
  beforeEach(() => { loadAuthProviders({ enabled: false }); });

  it('presents an error in the login-status area', () => {
    sandbox.getAuthProvider('basic').presentError('Denied');
    assert.equal(callSummary(), 'replace_content login-status <p>Denied</p>');
  });

  it('signs out by clearing credentials and returning home', () => {
    sandbox.getAuthProvider('basic').signOut();
    assert.equal(callSummary(), 'clear_auth | go_to_home');
  });

  it('reports a mid-session 401/403 as a popup warning', () => {
    sandbox.getAuthProvider('basic').onUnauthorized({ error: 'not_authorised' }, 'Not authorized');
    assert.equal(callSummary(), 'show_popup warn formatted:Not authorized');
  });
});

describe('oauth2 provider', () => {
  it('presents an error by re-rendering the oauth login page with a warning', () => {
    loadAuthProviders({ enabled: true, logged_in: true });
    sandbox.getAuthProvider('oauth2').presentError('Denied');
    assert.equal(callSummary(), 'renderWarningMessageInLoginStatus [object Object] Denied');
  });

  it('signs out through the identity provider when logged in', () => {
    const oauthState = { enabled: true, logged_in: true };
    loadAuthProviders(oauthState);
    sandbox.getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth | oauth_initiateLogout');
    assert.equal(oauthState.logged_in, false);
  });

  it('just returns home when it was not logged in through the provider', () => {
    loadAuthProviders({ enabled: true, logged_in: false });
    sandbox.getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth | go_to_home');
  });

  it('reports a mid-session 401/403 by restarting the login flow', () => {
    loadAuthProviders({ enabled: true, logged_in: true });
    sandbox.getAuthProvider('oauth2').onUnauthorized({ error: 'not_authorised' }, 'Not authorized');
    assert.equal(callSummary(), 'initiate_logout [object Object] Not authorized');
  });
});
