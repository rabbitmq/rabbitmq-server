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

// When auth_scheme is undefined, matches any scheme for backwards compatibility.
function has_auth_credentials(auth_scheme) {
    let expiry = get_local_pref(SESSION_EXPIRY);
    let authenticated = get_local_pref(CREDENTIALS) != undefined &&
                        get_local_pref(AUTH_SCHEME) != undefined &&
                        expiry != undefined &&
                        Date.now() < parseInt(expiry, 10);
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
    clear_local_pref(CREDENTIALS)
    clear_local_pref(AUTH_SCHEME)
    clear_local_pref(SESSION_EXPIRY)
    clear_local_pref(AUTH_RESOURCE)
    $.ajax({ async: false, type: 'DELETE', url: 'api/login' })
}
function set_basic_auth(username, password) {
    set_auth("Basic", b64_encode_utf8(username + ":" + password), default_hard_session_timeout())
}
function set_token_auth(token) {
    set_auth("Bearer", token, default_hard_session_timeout())
}
function set_auth(auth_scheme, credentials, validUntil) {
    clear_local_pref(CREDENTIALS)
    clear_local_pref(AUTH_SCHEME)
    clear_local_pref(SESSION_EXPIRY)
    store_local_pref(CREDENTIALS, credentials)
    store_local_pref(AUTH_SCHEME, auth_scheme)
    store_local_pref(SESSION_EXPIRY, validUntil.getTime())
}

function authorization_header() {
    if (has_auth_credentials()) {
        return get_auth_scheme() + ' ' + get_auth_credentials();
    } else {
        return null;
    }
}
function default_hard_session_timeout() {
  var date  = new Date();
  date.setHours(date.getHours() + 8);
  return date;
}

function update_login_session_timeout(login_session_timeout) {
    if (!has_auth_credentials()) return;
    var date = new Date();
    date.setMinutes(date.getMinutes() + login_session_timeout);
    store_local_pref(SESSION_EXPIRY, date.getTime())
}

function print_logging_session_info (user_login_session_timeout) {
  let authenticated = has_auth_credentials()
  let session_expiry = get_local_pref(SESSION_EXPIRY)
  console.log('user_login_session_timeout: ' + user_login_session_timeout)
  console.log('has_auth_credentials: ' + authenticated)
  console.log('session_expiry: ' + session_expiry)
  console.log('isNaN(user_login_session_timeout): ' + isNaN(user_login_session_timeout))
}


/// End Credential Management

// Our base64 library takes a string that is really a byte sequence,
// and will throw if given a string with chars > 255 (and hence not
// DTRT for chars > 127). So encode a unicode string as a UTF-8
// sequence of "bytes".
function b64_encode_utf8(str) {
    return base64.encode(encode_utf8(str));
}

// encodeURIComponent handles utf-8, unescape does not. Neat!
function encode_utf8(str) {
  return unescape(encodeURIComponent(str));
}

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
    for (var group in COLUMNS[mode]) {
        var options = COLUMNS[mode][group];
        for (var i = 0; i < options.length; i++) {
            if (options[i][0] == key) {
                return '' + options[i][2];
            }
        }
    }
    return 'false';
}

