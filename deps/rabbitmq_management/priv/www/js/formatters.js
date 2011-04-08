UNKNOWN_REPR = '<span class="unknown">?</span>';
FD_THRESHOLDS=[[0.95, 'red'],
               [0.8, 'yellow']];
SOCKETS_THRESHOLDS=[[1.0, 'red'],
                    [0.8, 'yellow']];
PROCESS_THRESHOLDS=[[0.75, 'red'],
                    [0.5, 'yellow']];

function fmt_string(str) {
    if (str == undefined) return UNKNOWN_REPR;
    return str;
}

function fmt_num(num) {
    if (num == undefined) return UNKNOWN_REPR;
    return num.toFixed(0);
}

function fmt_bytes(bytes) {
    if (bytes == undefined) return UNKNOWN_REPR;

    function f(n, p) {
        if (n > 1024) return f(n / 1024, p + 1);
        else return [n, p];
    }

    var num_power = f(bytes, 0);
    var num = num_power[0];
    var power = num_power[1];
    var powers = ['B', 'kB', 'MB', 'GB', 'TB'];
    return (power == 0 ? num.toFixed(0) : num.toFixed(1)) + powers[power];
}

function fmt_boolean(b) {
    if (b == undefined) return UNKNOWN_REPR;

    return b ? "&#9679;" : "&#9675;";
}

function fmt_date(d) {
    function f(i) {
        return i < 10 ? "0" + i : i;
    }

    return d.getFullYear() + "-" + f(d.getMonth() + 1) + "-" + f(d.getDay()) +
        " " + f(d.getHours()) + ":" + f(d.getMinutes()) + ":" +
        f(d.getSeconds());
}

function fmt_parameters(obj) {
    var res = '';
    if (obj.durable) {
        res += '<acronym title="Durable">D</acronym> ';
    }
    if (obj.auto_delete) {
        res += '<acronym title="Auto-delete">AD</acronym> ';
    }
    if (obj.internal != undefined && obj.internal) {
        res += '<acronym title="Internal">I</acronym> ';
    }
    var args = fmt_table_short(obj.arguments);
    if (args != '') {
        res += args;
    }
    return res;
}

function fmt_channel_mode(ch) {
    if (ch.transactional) {
        return '<acronym title="Transactional">T</acronym>';
    }
    else if (ch.confirm) {
        return '<acronym title="Confirm">C</acronym>';
    }
    else {
        return '';
    }
}

function fmt_color(r, thresholds) {
    if (r == undefined) return '';

    for (var i in thresholds) {
        var threshold = thresholds[i][0];
        var color = thresholds[i][1];

        if (r >= threshold) {
            return color;
        }
    }
    return 'green';
}

function fmt_rate(obj, name, show_total, cssClass) {
    var res = fmt_rate0(obj, name, fmt_num, show_total);
    if (cssClass == undefined || res == '') {
        return res;
    }
    else {
        return '<span class="' + cssClass + '">' + res + '</span>';
    }
}

function fmt_rate_bytes(obj, name) {
    return fmt_rate0(obj, name, fmt_bytes, true);
}

function fmt_rate0(obj, name, fmt, show_total) {
    if (obj == undefined || obj[name] == undefined) return '';
    var res = '';
    if (obj[name + '_details'] != undefined) {
        res = fmt(obj[name + '_details'].rate) + '/s';
    }
    if (show_total) {
        res += '<sub>(' + fmt(obj[name]) + ' total)</sub>';
    }
    return res;
}

function is_stat_empty(obj, name) {
    if (obj == undefined
        || obj[name] == undefined
        || obj[name + '_details'] == undefined
        || obj[name + '_details'].rate < 0.00001) return true;
    return false;
}

function is_col_empty(channels, name) {
    for (var i = 0; i < channels.length; i++) {
        var channel = channels[i];
        if (!is_stat_empty(channel.message_stats, name)) {
            return false;
        }
    }
    return true;
}

function fmt_exchange(name) {
    return name == '' ? '(AMQP default)' : name;
}

function fmt_exchange_url(name) {
    return name == '' ? 'amq.default' : name;
}

function fmt_download_filename(host) {
    var now = new Date();
    return host.replace('@', '_') + "_" + now.getFullYear() + "-" +
        (now.getMonth() + 1) + "-" + now.getDate() + ".json";
}

function fmt_table_short(table) {
    return '<table class="mini">' + fmt_table_body(table, ':') + '</table>';
}

function fmt_table_long(table) {
    return '<table class="facts">' + fmt_table_body(table, '') +
        '</table><span class="br"></span>';
}

function fmt_table_body(table, x) {
    var res = '';
    for (k in table) {
        res += '<tr><th>' + k + x + '</th><td>' + fmt_amqp_value(table[k]) +
            '</td>';
    }
    return res;
}

function fmt_amqp_value(val) {
    if (val instanceof Array) {
        var val2 = new Array();
        for (var i = 0; i < val.length; i++) {
            val2[i] = fmt_amqp_value(val[i]);
        }
        return val2.join("<br/>");
    } else if (val instanceof Object) {
        return fmt_table_short(val);
    } else if (typeof(val) == 'string') {
        return fmt_escape_html(val);
    } else {
        return val;
    }
}

function fmt_uptime(u) {
    var uptime = Math.floor(u / 1000);
    var sec = uptime % 60;
    var min = Math.floor(uptime / 60) % 60;
    var hour = Math.floor(uptime / 3600) % 24;
    var day = Math.floor(uptime / 86400);

    if (day > 0)
        return day + 'd ' + hour + 'h';
    else if (hour > 0)
        return hour + 'h ' + min + 'm';
    else
        return min + 'm ' + sec + 's';
}

function fmt_rabbit_version(applications) {
    for (var i in applications) {
        if (applications[i].name == 'rabbit') {
            return applications[i].version;
        }
    }
    return 'unknown';
}

function fmt_idle(obj) {
    if (obj.idle_since == undefined) {
        return 'Active';
    } else {
        return '<acronym title="Idle since ' + obj.idle_since +
            '">Idle</acronym>';
    }
}

function fmt_idle_long(obj) {
    if (obj.idle_since == undefined) {
        return 'Active';
    } else {
        return 'Idle since<br/>' + obj.idle_since;
    }
}

function fmt_escape_html(txt) {
    return txt.replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\n/g, '<br/>');
}

function fmt_maybe_wrap(txt, encoding) {
    if (encoding == 'string') return fmt_escape_html(txt);

    var WRAP = 120;
    var res = '';
    while (txt != '') {
        var i = txt.indexOf('\n');
        if (i == -1 || i > WRAP) {
            i = Math.min(WRAP, txt.length);
            res += txt.substring(0, i) + '\n';
            txt = txt.substring(i);
        }
        else {
            res += txt.substring(0, i + 1);
            txt = txt.substring(i + 1);
        }
    }
    return fmt_escape_html(res);
}

function fmt_escape_html(txt) {
    return txt.replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function alt_rows(i) {
    return (i % 2 == 0) ? ' class="alt1"' : ' class="alt2"';
}

function esc(str) {
    return encodeURIComponent(str);
}

function link_conn(name) {
    return link_to(name, '#/connections/' + esc(name))
}

function link_channel(name) {
    return link_to(name, '#/channels/' + esc(name))
}

function link_exchange(vhost, name) {
    var url = esc(vhost) + '/' + (name == '' ? 'amq.default' : esc(name));
    return link_to(fmt_exchange(name), '#/exchanges/' + url)
}

function link_queue(vhost, name) {
    return link_to(name, '#/queues/' + esc(vhost) + '/' + esc(name))
}

function link_vhost(name) {
    return link_to(name, '#/vhosts/' + esc(name))
}

function link_user(name) {
    return link_to(name, '#/users/' + esc(name))
}

function link_node(name) {
    return link_to(name, '#/nodes/' + esc(name))
}

function link_to(name, url) {
    return '<a href="' + url + '">' + fmt_escape_html(name) + '</a>';
}

function message_rates(stats) {
    var res = "";
    if (keys(stats).length > 0) {
        var items = [['Publish', 'publish'], ['Confirm', 'confirm'],
                     ['Deliver', 'deliver'], ['Acknowledge', 'ack'],
                     ['Get', 'get'], ['Deliver (noack)', 'deliver_no_ack'],
                     ['Get (noack)', 'get_no_ack'],
                     ['Return (mandatory)', 'return_unroutable'],
                     ['Return (immediate)', 'return_not_delivered']];
        for (var i in items) {
            var name = items[i][0];
            var key = items[i][1] + '_details';
            if (key in stats) {
                res += '<div class="highlight">' + name;
                res += '<strong>' + Math.round(stats[key].rate) + '</strong>';
                res += 'msg/s</div>';
            }
        }

        if (res == "") {
            res = '<p>Waiting for message rates...</p>';
        }
    }
    else {
        res = '<p>Currently idle</p>';
    }

    return res;
}

function maybe_truncate(items) {
    var maximum = 500;
    var str = '';

    if (items.length > maximum) {
        str = '<p class="warning">Only ' + maximum + ' of ' +
            items.length + ' items are shown.</p>';
        items.length = maximum;
    }

    return str;
}

function fmt_sort(display, sort) {
    var prefix = '';
    if (current_sort == sort) {
        prefix = '<span class="arrow">' +
            (current_sort_reverse ? '&#9650; ' : '&#9660; ') +
            '</span>';
    }
    return '<a class="sort" sort="' + sort + '">' + prefix + display + '</a>';
}

function fmt_permissions(obj, permissions, lookup, show, warning) {
    var res = [];
    for (var i in permissions) {
        var permission = permissions[i];
        if (permission[lookup] == obj.name) {
            res.push(permission[show]);
        }
    }
    return res.length == 0 ? warning : res.join(', ');
}

function properties_size(obj) {
    var count = 0;
    for (k in obj) {
        if (obj.hasOwnProperty(k)) count++;
    }
    return count;
}
