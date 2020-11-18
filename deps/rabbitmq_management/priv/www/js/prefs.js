function local_storage_available() {
    try {
        return 'localStorage' in window && window['localStorage'] !== null;
    } catch (e) {
        return false;
    }
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

function clear_local_pref(k) {
    if (local_storage_available()) {
        window.localStorage.removeItem('rabbitmq.' + k);
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
    document.cookie = 'm=' + enc.join('|') + '; expires=' + expiration_date.toUTCString();
}

function get_cookie(key) {
    var cookies = document.cookie.split(';');
    for (var i in cookies) {
        var kv = jQuery.trim(cookies[i]).split('=');
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
