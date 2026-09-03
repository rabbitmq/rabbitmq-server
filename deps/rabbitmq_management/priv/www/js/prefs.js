/// Credential management

const CREDENTIALS = 'credentials'
const AUTH_SCHEME = "auth-scheme"
const SESSION_EXPIRY = 'session_expiry'
const AUTH_RESOURCE = 'auth_resource'

const BASIC_AUTH_SCHEME = "Basic"
const BEARER_AUTH_SCHEME = "Bearer"


function set_auth_resource(resource) {
  store_local_pref(AUTH_RESOURCE, resource)
}
function has_auth_resource() {
    return get_local_pref(AUTH_RESOURCE) != undefined
}
function get_auth_resource() {
  return get_local_pref(AUTH_RESOURCE)
}
function clear_auth_resource() {
  clear_local_pref(AUTH_RESOURCE)
}

// When auth_scheme is undefined, matches any scheme for backwards compatibility.
function has_auth_credentials(auth_scheme) {
    let expiry = get_local_pref(SESSION_EXPIRY);
    let authenticated = get_local_pref(CREDENTIALS) != undefined &&
                        get_local_pref(AUTH_SCHEME) != undefined;
    if (authenticated && expiry != undefined) {
        authenticated = Date.now() < parseInt(expiry, 10);
    }
    return authenticated && (auth_scheme == undefined
        || auth_scheme == get_auth_scheme());
}
function get_auth_credentials() {
    return get_local_pref(CREDENTIALS)
}
function get_auth_scheme() {
    return get_local_pref(AUTH_SCHEME)
}
function clear_auth() {
    clear_local_pref(CREDENTIALS);
    clear_local_pref(AUTH_SCHEME);
    clear_local_pref(SESSION_EXPIRY);
    clear_local_pref(AUTH_RESOURCE);
    if (typeof $ !== 'undefined' && $.ajax) {
        $.ajax({ async: false, type: 'DELETE', url: 'api/login' });
    }
}
function set_token_auth(token) {
    set_auth("Bearer", token)
}
function set_auth(auth_scheme, credentials) {
    clear_local_pref(CREDENTIALS)
    clear_local_pref(AUTH_SCHEME)
    store_local_pref(CREDENTIALS, credentials)
    store_local_pref(AUTH_SCHEME, auth_scheme)
}
const DEFAULT_HARD_LOGIN_SESSION_TIMEOUT = 480; // 8 hours
function set_session_expiry_if_required(login_session_timeout) {
    if (get_local_pref(SESSION_EXPIRY) != undefined) return;
    var timeout = parseInt(login_session_timeout);
    if (isNaN(timeout)) {
        timeout = DEFAULT_HARD_LOGIN_SESSION_TIMEOUT;
    }
    var date = new Date();
    date.setMinutes(date.getMinutes() + timeout);
    store_local_pref(SESSION_EXPIRY, date.getTime());
}
function authorization_header() {
    if (has_auth_credentials()) {
        return get_auth_scheme() + ' ' + get_auth_credentials();
    } else {
        return null;
    }
}

/// End Credential Management

// All preferences and credentials are stored in localStorage.
// The management UI requires localStorage; without it no part of the UI functions.

function store_local_pref(k, v) {
    window.localStorage.setItem('rabbitmq.' + k, v);
}

function clear_local_pref(k) {
    window.localStorage.removeItem('rabbitmq.' + k);
}

function get_local_pref(k) {
    return window.localStorage.getItem('rabbitmq.' + k);
}

function store_pref(k, v) {
    store_local_pref(k, v);
}

function clear_pref(k) {
    clear_local_pref(k);
}

function get_pref(k, defaultValue = undefined) {
    var val = get_local_pref(k);
    return (val == undefined) ?
        (defaultValue != undefined ? defaultValue : default_pref(k)) : val;
}

function section_pref(template, name) {
    return 'visible|' + template + '|' + name;
}

function show_column(mode, column) {
    return get_pref('column-' + mode + '-' + column) == 'true';
}

// ---------------------------------------------------------------------------

function default_pref(k) {
    if (k.substring(0, 11) == 'chart-size-')  return 'small';
    if (k.substring(0, 10) == 'rate-mode-')   return 'chart';
    if (k.substring(0, 11) == 'chart-line-')  return 'true';
    if (k == 'truncate')                      return '100';
    if (k == 'chart-range')                   return '60|5';
    if (k == 'oauth-return-to')               return '';
    if (k.substring(0,  7) == 'column-')
        return default_column_pref(k.substring(7));
    return null;
}

function default_column_pref(key0) {
    var ix = key0.indexOf('-');
    var mode = key0.substring(0, ix);
    var key = key0.substring(ix + 1);
    var columns = typeof COLUMNS !== 'undefined' ? COLUMNS : (typeof window !== 'undefined' ? window.COLUMNS : undefined);
    if (columns && columns[mode]) {
        for (var group in columns[mode]) {
            var options = columns[mode][group];
            for (var i = 0; i < options.length; i++) {
                if (options[i][0] == key) {
                    return '' + options[i][2];
                }
            }
        }
    }
    return 'false';
}

export {
    CREDENTIALS,
    AUTH_SCHEME,
    SESSION_EXPIRY,
    AUTH_RESOURCE,
    BASIC_AUTH_SCHEME,
    BEARER_AUTH_SCHEME,
    DEFAULT_HARD_LOGIN_SESSION_TIMEOUT,
    set_auth_resource,
    has_auth_resource,
    get_auth_resource,
    clear_auth_resource,
    has_auth_credentials,
    get_auth_credentials,
    get_auth_scheme,
    clear_auth,
    set_token_auth,
    set_auth,
    set_session_expiry_if_required,
    authorization_header,
    store_local_pref,
    clear_local_pref,
    get_local_pref,
    store_pref,
    clear_pref,
    get_pref,
    section_pref,
    show_column,
    default_pref,
    default_column_pref
};

// Functions called as bare globals inside .ejs templates (e.g. connections.ejs,
// channels-list.ejs, vhosts.ejs, rate-options.ejs, columns-options.ejs).
// Since EJS templates render dynamically in browser context and are not ES modules,
// they resolve helper functions from window.
if (typeof window !== 'undefined') {
    Object.assign(window, {
        store_pref,
        clear_pref,
        get_pref,
        section_pref,
        show_column
    });
}


