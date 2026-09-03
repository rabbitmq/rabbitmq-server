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
import { on_page_load } from '../priv/www/js/main.js';

let calls;

function loadPage(oauthState, href) {
  calls = [];
  const record = (name) => (...args) => calls.push([name, ...args].map(String).join(' '));

  // consumeRedirect() renders the oauth login page's warning itself now
  // (render_login_oauth(), defined in the same module) rather than through a
  // swappable bare global, so its jQuery/templating calls have to be
  // stubbed instead - same reasoning as the startLoginFlow overrides below,
  // one level deeper. COMPILED_TEMPLATES is stubbed just enough for
  // format('login_oauth', ...) to succeed instead of throwing, so the
  // warning text actually reaches the recorded html() call.
  const jq = function(selector) {
    return {
      ready: () => {},
      on: () => {},
      off: () => {},
      html: (content) => record('html')(selector, content),
      appendTo: () => {},
      each: () => {},
      hasClass: () => false
    };
  };
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
  globalThis.COMPILED_TEMPLATES = { login_oauth: (json) => JSON.stringify(json.escaped_warnings) };

  // startWithLoginPage/startWithOAuthLogin are implementation details of
  // the basic/oauth2 providers now, not swappable bare globals (one of them
  // is even defined in the same module as its caller) - so stub at the
  // provider's own method instead of trying to intercept the function name.
  getAuthProvider('basic').startLoginFlow = record('startWithLoginPage');
  getAuthProvider('oauth2').startLoginFlow = record('startWithOAuthLogin');
}

describe('on_page_load: which login flow starts', () => {
  it('starts the oauth flow when oauth is configured', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin']);
  });

  it('starts the plain login page when oauth is not configured', () => {
    loadPage({ enabled: false }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });

  it('chooses by configuration, not by who logged in previously', () => {
    loadPage({ enabled: true }, 'https://rabbit.example/');

    on_page_load();

    assert.deepEqual(calls, ['startWithOAuthLogin']);
  });
});

describe('on_page_load: a failed IdP-initiated login', () => {
  beforeEach(() => {
    loadPage({ enabled: true }, 'https://rabbit.example/?error=access_denied');
  });

  it('reports the error instead of starting a login flow', () => {
    on_page_load();

    assert.ok(calls.some((c) => c.startsWith('html #outer') && c.includes('access_denied')));
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

    assert.deepEqual(calls, ['startWithOAuthLogin']);
  });

  it('does not treat ?error= as an oauth redirect when oauth is off', () => {
    loadPage({ enabled: false }, 'https://rabbit.example/?error=access_denied');

    on_page_load();

    assert.deepEqual(calls, ['startWithLoginPage']);
  });
});
