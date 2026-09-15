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
// basic-auth.js and oidc-oauth/helper.js self-register with the provider
// registry above as a side effect of being imported - they are no longer
// registered by auth-providers.js itself.
import '../priv/www/js/basic-auth.js';
import '../priv/www/js/oidc-oauth/helper.js';

let calls;
let authResourcePresent;

// presentError/signOut/onUnauthorized now call basic-auth.js's/helper.js's
// own render_login_oauth()/replace_content() directly rather than through a
// swappable bare global (they're defined in the same module as their
// caller), so the jQuery calls those make have to be stubbed instead of the
// higher-level function itself - same reasoning as page-load.test.js's
// getAuthProvider(...).startLoginFlow overrides, one level deeper.
function loadAuthProviders(oauthState, options) {
  calls = [];
  authResourcePresent = (options || {}).authResourcePresent === true;
  const record = (name) => (...args) => calls.push([name, ...args]);

  set_active_auth_provider(null);

  globalThis.oauth = oauthState;
  globalThis.has_auth_resource = () => authResourcePresent;
  globalThis.fmt_escape_html = (s) => s;
  globalThis.format_error_response = (response, reason) => `formatted:${reason}`;
  globalThis.show_popup = record('show_popup');
  globalThis.clear_auth = record('clear_auth');
  globalThis.go_to_home = record('go_to_home');
  globalThis.location = {};

  const jq = function(selector) {
    return {
      html: (content) => record('html')(selector, content),
      appendTo: () => {},
      each: () => {},
      hasClass: () => false,
      on: () => {},
      off: () => {},
      ready: () => {}
    };
  };
  jq.fn = { extend: () => {} };
  jq.inArray = () => -1;
  globalThis.$ = jq;
  globalThis.jQuery = jq;
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
    assert.equal(callSummary(), 'html #login-status <p>Denied</p>');
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
    assert.equal(callSummary(), 'html #outer undefined');
  });

  it('signs out through the identity provider when logged in', () => {
    const oauthState = { enabled: true, logged_in: true, authority: 'https://idp.example/logout' };
    loadAuthProviders(oauthState);
    getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth');
    assert.equal(oauthState.logged_in, false);
    assert.equal(globalThis.location.href, 'https://idp.example/logout');
  });

  it('just returns home when it was not logged in through the provider', () => {
    loadAuthProviders({ enabled: true, logged_in: false });
    getAuthProvider('oauth2').signOut();
    assert.equal(callSummary(), 'clear_auth | go_to_home');
  });

  it('reports a mid-session 401/403 by restarting the login flow', () => {
    loadAuthProviders({ enabled: true, logged_in: true });
    getAuthProvider('oauth2').onUnauthorized({ error: 'not_authorised' }, 'Not authorized');
    assert.equal(callSummary(), 'html #outer undefined');
  });
});
