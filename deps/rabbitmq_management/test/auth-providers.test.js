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

let calls;
let authResourcePresent;

function loadAuthProviders(oauthState, options) {
  calls = [];
  authResourcePresent = (options || {}).authResourcePresent === true;
  const record = (name) => (...args) => calls.push([name, ...args]);

  set_active_auth_provider(null);

  globalThis.oauth = oauthState;
  globalThis.has_auth_resource = () => authResourcePresent;
  globalThis.replace_content = record('replace_content');
  globalThis.fmt_escape_html = (s) => s;
  globalThis.format_error_response = (response, reason) => `formatted:${reason}`;
  globalThis.show_popup = record('show_popup');
  globalThis.clear_auth = record('clear_auth');
  globalThis.go_to_home = record('go_to_home');
  globalThis.oauth_initiateLogout = record('oauth_initiateLogout');
  globalThis.renderWarningMessageInLoginStatus = record('renderWarningMessageInLoginStatus');
  globalThis.initiate_logout = record('initiate_logout');
}

function callSummary() {
  return calls.map((c) => c.map((v) => String(v)).join(' ')).join(' | ');
}

describe('registry', () => {
  beforeEach(() => { loadAuthProviders({ enabled: false }); });

  it('registers the basic and oauth2 providers out of the box', () => {
    assert.equal(typeof getAuthProvider('basic'), 'object');
    assert.equal(typeof getAuthProvider('oauth2'), 'object');
  });

  it('rejects a duplicate name', () => {
    assert.equal(registerAuthProvider('basic', { presentError: () => {} }), false);
  });

  it('rejects a null or blank name, and a non-object provider', () => {
    assert.equal(registerAuthProvider(null, {}), false);
    assert.equal(registerAuthProvider('  ', {}), false);
    assert.equal(registerAuthProvider('custom', null), false);
    assert.equal(registerAuthProvider('custom', 'nope'), false);
  });

  it('accepts an additional mechanism, so a third one is a registration', () => {
    assert.equal(registerAuthProvider('mtls', { presentError: () => {} }), true);
    assert.equal(typeof getAuthProvider('mtls'), 'object');
    unregisterAuthProvider('mtls');
  });
});

describe('active_auth_provider', () => {
  it('is the provider the login flow declared, whatever the token looked like', () => {
    loadAuthProviders({ enabled: true });
    set_active_auth_provider_by_name('basic');
    assert.equal(active_auth_provider(), getAuthProvider('basic'));
  });

  it('resolves oauth2 after a reload when an oauth resource was stored', () => {
    loadAuthProviders({ enabled: true }, { authResourcePresent: true });
    assert.equal(active_auth_provider(), getAuthProvider('oauth2'));
  });

  it('resolves basic after a reload when no oauth resource was stored', () => {
    loadAuthProviders({ enabled: true }, { authResourcePresent: false });
    assert.equal(active_auth_provider(), getAuthProvider('basic'));
  });

  it('prefers an explicitly declared provider over the stored marker', () => {
    loadAuthProviders({ enabled: true }, { authResourcePresent: true });
    set_active_auth_provider_by_name('basic');
    assert.equal(active_auth_provider(), getAuthProvider('basic'));
  });
});

describe('basic provider', () => {
  beforeEach(() => { loadAuthProviders({ enabled: false }); });

  it('presents an error in the login-status area', () => {
    getAuthProvider('basic').presentError('Denied');
    assert.equal(callSummary(), 'replace_content login-status <p>Denied</p>');
  });

  it('signs out by clearing credentials and returning home', () => {
    getAuthProvider('basic').signOut();
    assert.equal(callSummary(), 'clear_auth | go_to_home');
  });

  it('reports a mid-session 401/403 as a popup warning', () => {
    getAuthProvider('basic').onUnauthorized({ error: 'not_authorised' }, 'Not authorized');
    assert.equal(callSummary(), 'show_popup warn formatted:Not authorized');
  });
});

describe('oauth2 provider', () => {
  it('presents an error by re-rendering the oauth login page with a warning', () => {
    loadAuthProviders({ enabled: true, logged_in: true });
    getAuthProvider('oauth2').presentError('Denied');
    assert.equal(callSummary(), 'renderWarningMessageInLoginStatus [object Object] Denied');
  });

  it('signs out through the identity provider when logged in', () => {
    const oauthState = { enabled: true, logged_in: true };
    loadAuthProviders(oauthState);
    getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth | oauth_initiateLogout');
    assert.equal(oauthState.logged_in, false);
  });

  it('just returns home when it was not logged in through the provider', () => {
    loadAuthProviders({ enabled: true, logged_in: false });
    getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth | go_to_home');
  });

  it('reports a mid-session 401/403 by restarting the login flow', () => {
    loadAuthProviders({ enabled: true, logged_in: true });
    getAuthProvider('oauth2').onUnauthorized({ error: 'not_authorised' }, 'Not authorized');
    assert.equal(callSummary(), 'initiate_logout [object Object] Not authorized');
  });
});
