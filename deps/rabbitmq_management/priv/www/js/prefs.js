function local_storage_available() {
    try {
        return 'localStorage' in window && window['localStorage'] !== null;
    } catch (e) {
        return false;
    }
}

/// Credential management

const CREDENTIALS = 'credentials'
const AUTH_SCHEME = "auth-scheme"
const LOGGED_IN = 'loggedIn'
const LOGIN_SESSION_TIMEOUT = "login_session_timeout"

function has_auth_credentials() {
    return get_local_pref(CREDENTIALS) != undefined && get_local_pref(AUTH_SCHEME) != undefined &&
      get_cookie_value(LOGGED_IN) != undefined
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
    clear_cookie_value(LOGIN_SESSION_TIMEOUT)
    clear_cookie_value(LOGGED_IN)
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
    store_local_pref(CREDENTIALS, credentials)
    store_local_pref(AUTH_SCHEME, auth_scheme)
    store_cookie_value_with_expiration(LOGGED_IN, "true", validUntil) // session marker
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
    //var auth_info = get_cookie_value('auth');
    if (get_cookie_value(LOGIN_SESSION_TIMEOUT) != undefined || !has_auth_credentials()) {
      return;
    }
    var date = new Date();
    date.setMinutes(date.getMinutes() + login_session_timeout);
    store_cookie_value(LOGIN_SESSION_TIMEOUT, login_session_timeout);
    store_cookie_value_with_expiration(LOGGED_IN, "true", date)
}

function print_logging_session_info (user_login_session_timeout) {
  let var_has_auth_cookie_value = has_auth_credentials()
  let login_session_timeout = get_login_session_timeout()
  console.log('user_login_session_timeout: ' + user_login_session_timeout)
  console.log('has_auth_cookie_value: ' + var_has_auth_cookie_value)
  console.log('login_session_timeout: ' + login_session_timeout)
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

function store_cookie_value(k, v) {
    var d = parse_cookie();
    d[short_key(k)] = v;
    store_cookie(d);
}

function store_cookie_value_with_expiration(k, v, expiration_date) {
    var d = parse_cookie();
    d[short_key(k)] = v;
    store_cookie_with_expiration(d, expiration_date);
}

function clear_cookie_value(k) {
    var d = parse_cookie();
    delete d[short_key(k)];
    store_cookie(d);
}

function get_cookie_value(k) {
    var r;
    r = parse_cookie()[short_key(k)];
    return r == undefined ? default_pref(k) : r;
}
function store_local_pref(k, v) {
    if (local_storage_available()) {
        window.localStorage.setItem('rabbitmq.' + k, v);
    }else {
      throw "Local Storage not available. RabbitMQ requires localStorage"
    }
}
function clear_local_pref(k) {
    if (local_storage_available()) {
        window.localStorage.removeItem('rabbitmq.' + k);
    }
}

function store_pref(k, v) {
    if (local_storage_available()) {
        window.localStorage['rabbitmq.' + k] = v;
    }
    else {
        var d = parse_cookie();
        d[short_key(k)] = v;
        store_cookie(d);
    }
}

function clear_pref(k) {
    if (local_storage_available()) {
        window.localStorage.removeItem('rabbitmq.' + k);
    }
    else {
        var d = parse_cookie();
        delete d[short_key(k)];
        store_cookie(d);
    }
}
function get_local_pref(k) {
  if (local_storage_available()) {
      return window.localStorage.getItem('rabbitmq.' + k)
  }else {
    throw "Local Storage not available. RabbitMQ requires localStorage"
  }
}

function get_pref(k) {
    var val;
    if (local_storage_available()) {
        val = window.localStorage['rabbitmq.' + k];
    }
    else {
        val = parse_cookie()[short_key(k)];

    }
    var res = (val == undefined) ? default_pref(k) : val;
    return res;
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

// ---------------------------------------------------------------------------

function parse_cookie() {
    var c = get_cookie('m');
    var items = c.length == 0 ? [] : c.split('|');

    var start = 0;
    var dict = {};
    for (var i in items) {
        var kv = items[i].split(':');
        dict[kv[0]] = unescape(kv[1]);
    }
    return dict;
}

function store_cookie(dict) {
    var date = new Date();
    date.setFullYear(date.getFullYear() + 1);
    store_cookie_with_expiration(dict, date);
}

function store_cookie_with_expiration(dict, expiration_date) {
    var enc = [];
    for (var k in dict) {
        enc.push(k + ':' + escape(dict[k]));
    }
    document.cookie = 'm=' + enc.join('|') + '; expires=' + expiration_date.toUTCString() + "; path=/";
//    console.log("Cookie m expires at " + expiration_date);
}

function get_cookie(key) {
    var cookies = document.cookie.split(';');
    for (var i in cookies) {
        var kv = cookies[i].trim().split('=');
        if (kv[0] == key) return kv[1];
    }
    return '';
}

// Try to economise on space since cookies have limited length.
function short_key(k) {
    var res = Math.abs(k.hashCode() << 16 >> 16);
    res = res.toString(16);
    return res;
}

String.prototype.hashCode = function() {
    var hash = 0;
    if (this.length == 0) return code;
    for (i = 0; i < this.length; i++) {
        char = this.charCodeAt(i);
        hash = 31*hash+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}
