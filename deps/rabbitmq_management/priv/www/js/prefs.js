// TODO It would be nice to use DOM storage. When that's available.

function store_pref(k, v) {
    var date = new Date();
    date.setFullYear(date.getFullYear() + 1);
    document.cookie = short_key(k) + '=' + escape(v) +
        '; expires=' + date.toUTCString();
    debug(document.cookie);
}

var epoch = 'Thu, 01-Jan-1970 00:00:01 GMT';

function clear_pref(k) {
    document.cookie = short_key(k) + '=; expires=' + epoch;
}

function get_pref(k) {
    k = short_key(k) + '=';
    var start = 0;
    while (start < document.cookie.length) {
        var k_end = start + k.length;
        if (document.cookie.substring(start, k_end) == k) {
            v_end = document.cookie.indexOf (";", k_end);
            if (v_end == -1) v_end = document.cookie.length;
            return unescape(document.cookie.substring(k_end, v_end));
        }
        start = document.cookie.indexOf(" ", start) + 1;
        if (start == 0) break;
    }
    return null;
}

function section_pref(template, name) {
    return 'visible|' + template + '|' + name;
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
