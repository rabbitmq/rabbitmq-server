UNKNOWN_REPR = '<span class="unknown">?</span>';

function fmt_string(str) {
    if (str == undefined) return UNKNOWN_REPR;
    return str;
}

function fmt_bytes(bytes) {
    if (bytes == undefined) return UNKNOWN_REPR;

    function f(n, p) {
        if (n > 1024) return f(n / 1024, p + 1);
        else return [n, p];
    }

    [num, power] = f(bytes, 0);
    var powers = ['B', 'kB', 'MB', 'GB', 'TB'];
    return (power == 0 ? num.toFixed(0) : num.toFixed(1)) + powers[power];
}

function fmt_boolean(b) {
    if (b == undefined) return UNKNOWN_REPR;

    return b ? "&#9679;" : "&#9675;";
}

function fmt_color(r) {
    if (r == undefined) return '';

    if (r > 0.75) return 'red';
    else if (r > 0.5) return 'yellow';
    else return 'green';
}

function alt_rows(i) {
    return (i % 2 == 0) ? ' class="alt"' : '';
}

