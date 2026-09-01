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
let authResourcePresent;

function loadAuthProviders(oauthState, options) {
  calls = [];
  // set_auth_resource() is called only by the oauth login flow, so its
  // presence is how a reloaded page tells the two mechanisms apart.
  authResourcePresent = (options || {}).authResourcePresent === true;
  const record = (name) => (...args) => calls.push([name, ...args]);

  sandbox = {
    console,
    oauth: oauthState,
    has_auth_resource: () => authResourcePresent,
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

describe('active_auth_provider', () => {
  it('is the provider the login flow declared, whatever the token looked like', () => {
    // /login returns a bearer token for a basic login when credential
    // encryption is on, so the token type must not decide this.
    loadAuthProviders({ enabled: true });
    sandbox.set_active_auth_provider_by_name('basic');
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('basic'));
  });

  it('resolves oauth2 after a reload when an oauth resource was stored', () => {
    loadAuthProviders({ enabled: true }, { authResourcePresent: true });
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('oauth2'));
  });

  it('resolves basic after a reload when no oauth resource was stored', () => {
    // A deployment can offer both mechanisms, so configuration cannot say
    // how this particular user authenticated.
    loadAuthProviders({ enabled: true }, { authResourcePresent: false });
    assert.equal(sandbox.active_auth_provider(), sandbox.getAuthProvider('basic'));
  });

  it('prefers an explicitly declared provider over the stored marker', () => {
    loadAuthProviders({ enabled: true }, { authResourcePresent: true });
    sandbox.set_active_auth_provider_by_name('basic');
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
