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

// This module is deliberately mechanism-agnostic: it knows nothing about
// basic auth or oauth2. Each mechanism registers itself as a side effect of
// its own module being imported (see basic-auth.js and oidc-oauth/helper.js),
// so that never importing a mechanism's module is enough to guarantee its
// code never runs - in particular, so that the oauth2 client library is
// never fetched when oauth2 is not configured.

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

// Lets a caller (start_app_login(), building the Sammy app) ask every
// registered mechanism whether it wants to wire up anything, without
// needing to know any mechanism's name - each provider decides for itself,
// from its own registerRoutes(), whether it's actually configured/enabled.
function forEachAuthProvider(fn) {
    authProviderRegistry.forEach(fn);
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

// Bootstraps whichever registered mechanisms need state set up before
// login_flow_provider() can pick between them - e.g. oauth2's initialize()
// populates the global oauth object that login_flow_provider() reads. Only
// mechanisms that were actually imported (and so self-registered) run
// anything here. Called once at page load, from init.js.
function initializeAuthProviders() {
    forEachAuthProvider(function(provider) {
        if (typeof provider.initialize === 'function') provider.initialize();
    });
}

export {
    registerAuthProvider,
    unregisterAuthProvider,
    getAuthProvider,
    forEachAuthProvider,
    set_active_auth_provider,
    set_active_auth_provider_by_name,
    login_flow_provider,
    active_auth_provider,
    initializeAuthProviders
};

