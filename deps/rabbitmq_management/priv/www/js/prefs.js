// TODO It would be nice to use DOM storage. When that's available.

function store_pref(k, v) {
    var d = parse_cookie();
    d[short_key(k)] = v;
    store_cookie(d);
}

function clear_pref(k) {
    var d = parse_cookie();
    delete d[short_key(k)];
    store_cookie(d);
}

function get_pref(k) {
    var r = parse_cookie()[short_key(k)];
    return r == undefined ? null : r;
}

function section_pref(template, name) {
    return 'visible|' + template + '|' + name;
}

// ---------------------------------------------------------------------------

function parse_cookie() {
    var c = get_cookie();
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
    var enc = [];
    for (var k in dict) {
        enc.push(k + ':' + escape(dict[k]));
    }
    var date = new Date();
    date.setFullYear(date.getFullYear() + 1);
    document.cookie = 'm=' + enc.join('|') + '; expires=' + date.toUTCString();
}

function get_cookie() {
    var c = '';
    if (document.cookie.length > 2) {
        if (document.cookie.substr(0, 2) == 'm=') {
            c = document.cookie.slice(2);
        } else {
            // Old cookie format
            clear_all_cookies();
        }
    }
    return c;
}

var epoch = 'Thu, 01-Jan-1970 00:00:01 GMT';

function clear_all_cookies() {
    var cookies = document.cookie.split(';');
    for (var i in cookies) {
        document.cookie = cookies[i].split('=')[0] + '=; expires=' + epoch;
    }
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
