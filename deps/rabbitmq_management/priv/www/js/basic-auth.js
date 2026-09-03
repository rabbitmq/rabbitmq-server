// Basic auth's own login flow and provider registration: rendering its
// login page, submitting credentials to /login to establish the resulting
// session, and registering itself with auth-providers.js's registry as a
// side effect of this module being imported - mirrors how
// oidc-oauth/helper.js holds oauth2's equivalent implementation.
//
// finish_check_login, sync_get, show_popup, start_app_login, clear_auth,
// go_to_home, format_error_response (main.js/global.js) are called as bare
// globals rather than imported: main.js already imports from
// auth-providers.js, and this module imports from auth-providers.js too, so
// an explicit import back to main.js risks a cycle. Each of those modules
// exposes itself on window for exactly this reason.

import { replace_content, format } from './render.js';
import { fmt_escape_html } from './formatters.js';
import { set_current_user } from './global.js';
import { clear_local_pref, SESSION_EXPIRY, set_auth, clear_auth_resource } from './prefs.js';
import { registerAuthProvider, set_active_auth_provider_by_name } from './auth-providers.js';
import { authOptions } from './auth-options.js';

function startWithLoginPage() {
    replace_content('outer', format('login', {}));
    start_app_login();
}

function do_login(username, password) {
    var result = null;
    $.ajax({
        async: false,
        type: 'POST',
        url: 'api/login',
        data: {username: username, password: password},
        success: function(resp) { result = resp; },
        error: function(xhr) {
            try { result = JSON.parse(xhr.responseText); } catch(e) {}
            if (!result) result = {error: 'login_failed', reason: 'Login failed'};
        }
    });
    return result;
}

function login(username, password) {
  var result = do_login(username, password);
  if (!result || result.error) {
    replace_content('login-status', '<p>Login failed</p>');
    if (result && result.reason && typeof result.reason === 'string') {
      show_popup('warn', fmt_escape_html(result.reason));
    }
    return false;
  }
  set_current_user(result.user);
  clear_local_pref(SESSION_EXPIRY);
  var scheme = result.token.type === 'bearer' ? 'Bearer' : 'Basic';
  set_auth(scheme, result.token.value);
  // /login issues a bearer token for basic credentials too when
  // credential encryption is enabled, so the token scheme alone can't tell
  // basic and oauth2 sessions apart - dropping any oauth resource left
  // over from an earlier session is what actually does that, so a later
  // reload does not read this session as an oauth one.
  clear_auth_resource();
  set_active_auth_provider_by_name('basic');

  // Fetch initialization data synchronously
  var rawInitData = sync_get('/init');
  if (rawInitData) {
      var initData = JSON.parse(rawInitData);
      window.app_settings = initData.settings;
      window.app_vhosts = initData.vhosts;
      if (initData.nodes) {
          window.app_nodes = initData.nodes;
      }
  } else {
      console.error("Failed to load /api/init");
  }
  finish_check_login();

  return true;
}

registerAuthProvider('basic', {
    // Nothing in the URL belongs to basic auth.
    consumeRedirect: function(url) {
        return false;
    },
    startLoginFlow: function() {
        startWithLoginPage();
    },
    // Registered regardless of which mechanism is actually driving the
    // login flow: on a combined oauth2+basic deployment, basic's #/login
    // route still has to exist for the fallback form on the oauth2 login
    // page to submit to. Config-gated here rather than by the caller, since
    // only this provider knows what "available" means for basic auth.
    registerRoutes: function(sammy) {
        if (authOptions(oauth).options.some(function(o) { return o.mechanism === 'basic'; })) {
            sammy.put('#/login', function() {
                login(this.params['username'], this.params['password']);
            });
        }
    },
    presentError: function(message) {
        replace_content('login-status', '<p>' + fmt_escape_html(message) + '</p>');
    },
    presentSessionExpired: function() {
        this.presentError('Login failed');
    },
    // Its login form always needs #/login reachable, including as a retry
    // right after presentSessionExpired() re-renders it - unlike oauth2's
    // "not authorized" state, which is just a logout button.
    needsRouterAfterSessionExpired: true,
    // Called after a user changes their own password: for basic, the
    // stored credential (or the token derived from it) is now stale, so
    // logging in again with the new password is what keeps the session
    // alive instead of 401ing on the next request.
    reauthenticate: function(username, password) {
        login(username, password);
    },
    signOut: function() {
        clear_auth();
        go_to_home();
    },
    onUnauthorized: function(response, reason) {
        show_popup('warn', fmt_escape_html(format_error_response(response, reason)));
    }
});

export { startWithLoginPage, do_login, login };
