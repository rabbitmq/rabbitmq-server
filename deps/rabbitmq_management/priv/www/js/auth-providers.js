// One implementation per authentication mechanism, so that the rest of the
// application does not have to ask "is oauth enabled?" at every point where
// the two differ: rendering a login error, signing out, and reacting to a
// 401/403 mid-session.
//
// Which provider is active is resolved once, from the token the login
// produced (a bearer token means oauth2), rather than re-derived from
// configuration at each call site. Before a login has happened there is no
// token yet, so the default falls back to configuration - which is exactly
// what the call sites used to test for themselves.

var authProviderRegistry = new Map();
var activeAuthProvider = null;

function registerAuthProvider(name, provider) {
    if (name == null || (typeof name === 'string' && name.trim() === '')) return false;
    if (provider == null || typeof provider !== 'object') return false;
    if (authProviderRegistry.has(name)) return false;
    authProviderRegistry.set(name, provider);
    return true;
}

function unregisterAuthProvider(name) {
    return authProviderRegistry.delete(name);
}

function getAuthProvider(name) {
    return authProviderRegistry.get(name);
}

// A bearer token can only have come from the oauth2 flow; anything else is
// basic. Mirrors the scheme choice made when the token is stored.
function providerForToken(token) {
    var name = (token != null && token.type === 'bearer') ? 'oauth2' : 'basic';
    return getAuthProvider(name);
}

function set_active_auth_provider(provider) {
    activeAuthProvider = provider;
}

// The provider resolved from the current token, or - before any login - the
// one the deployment is configured for.
function active_auth_provider() {
    if (activeAuthProvider != null) return activeAuthProvider;
    var configured = (typeof oauth !== 'undefined' && oauth && oauth.enabled) ? 'oauth2' : 'basic';
    return getAuthProvider(configured);
}

registerAuthProvider('basic', {
    presentError: function(message) {
        replace_content('login-status', '<p>' + fmt_escape_html(message) + '</p>');
    },
    signOut: function() {
        clear_auth();
        go_to_home();
    },
    onUnauthorized: function(response, reason) {
        show_popup('warn', fmt_escape_html(format_error_response(response, reason)));
    }
});

registerAuthProvider('oauth2', {
    presentError: function(message) {
        renderWarningMessageInLoginStatus(oauth, message);
    },
    signOut: function() {
        clear_auth();
        if (oauth.logged_in) {
            oauth.logged_in = false;
            oauth_initiateLogout();
        } else {
            go_to_home();
        }
    },
    onUnauthorized: function(response, reason) {
        initiate_logout(oauth, reason);
    }
});
