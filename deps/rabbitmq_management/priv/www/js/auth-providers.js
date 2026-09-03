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

function set_active_auth_provider(provider) {
    activeAuthProvider = provider;
}

// Declared by whichever login flow ran: only that flow knows which
// mechanism it is. The token cannot be used to work this out - /login
// issues a "bearer" token for an ordinary basic-auth login whenever
// management.credential_encryption_secret is configured, so the token type
// describes the Authorization scheme to send, not who authenticated.
function set_active_auth_provider_by_name(name) {
    set_active_auth_provider(getAuthProvider(name));
}

// Which mechanism's login flow to start, and so which login page to show.
// That is a question about how the deployment is configured, unlike
// active_auth_provider() below, which asks how the current user
// authenticated - a distinction that matters for a visitor who has not
// logged in yet and therefore has no session to be identified by.
function login_flow_provider() {
    var oauthConfigured = typeof oauth !== 'undefined' && oauth && oauth.enabled;
    return getAuthProvider(oauthConfigured ? 'oauth2' : 'basic');
}

// After a page reload there is no fresh login to declare the mechanism, so
// fall back to the marker the oauth flow persists: set_auth_resource() is
// called only when an oauth login is initiated, and clear_auth() removes
// it. Deployment configuration cannot answer this, because a deployment
// offering both mechanisms says nothing about how THIS user logged in.
function active_auth_provider() {
    if (activeAuthProvider != null) return activeAuthProvider;
    var authenticatedWithOauth =
        typeof has_auth_resource === 'function' && has_auth_resource();
    return getAuthProvider(authenticatedWithOauth ? 'oauth2' : 'basic');
}

registerAuthProvider('basic', {
    // Nothing in the URL belongs to basic auth.
    consumeRedirect: function(url) {
        return false;
    },
    startLoginFlow: function() {
        startWithLoginPage();
    },
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
    // A failed IDP-initiated login redirects back here with ?error=, which
    // this module set itself on the way out, along with the pending-state
    // markers. Drop those so the next successful login is not redirected
    // somewhere confusing.
    consumeRedirect: function(url) {
        var error = url.searchParams.get('error');
        if (!error) return false;
        clear_pref("oauth-idp-pending");
        clear_pref("oauth-return-to");
        renderWarningMessageInLoginStatus(oauth, fmt_escape_html(error));
        return true;
    },
    startLoginFlow: function() {
        startWithOAuthLogin(oauth);
    },
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

export {
    registerAuthProvider,
    unregisterAuthProvider,
    getAuthProvider,
    set_active_auth_provider,
    set_active_auth_provider_by_name,
    login_flow_provider,
    active_auth_provider
};

if (typeof window !== 'undefined') {
    Object.assign(window, {
        registerAuthProvider,
        unregisterAuthProvider,
        getAuthProvider,
        set_active_auth_provider,
        set_active_auth_provider_by_name,
        login_flow_provider,
        active_auth_provider
    });
}
