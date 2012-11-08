UNKNOWN_REPR = '<span class="unknown">?</span>';
FD_THRESHOLDS=[[0.95, 'red'],
               [0.8, 'yellow']];
SOCKETS_THRESHOLDS=[[1.0, 'red'],
                    [0.8, 'yellow']];
PROCESS_THRESHOLDS=[[0.75, 'red'],
                    [0.5, 'yellow']];

function fmt_string(str, unknown) {
    if (unknown == undefined) unkown = UNKNOWN_REPR;
    if (str == undefined) return unknown;
    return fmt_escape_html("" + str);
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
    var powers = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    return (power == 0 ? num.toFixed(0) : num.toFixed(1)) + powers[power];
}

function fmt_memory(memory, key) {
    return '<div class="memory-key memory_' + key + '"></div>' +
        fmt_bytes(memory[key]);
}

function fmt_boolean(b) {
    if (b == undefined) return UNKNOWN_REPR;

    return b ? "&#9679;" : "&#9675;";
}

function fmt_date(d) {
    function f(i) {
        return i < 10 ? "0" + i : i;
    }

    return d.getFullYear() + "-" + f(d.getMonth() + 1) + "-" +
        f(d.getDate()) + " " + f(d.getHours()) + ":" + f(d.getMinutes()) +
        ":" + f(d.getSeconds());
}

function fmt_time(t, suffix) {
    if (t == undefined || t == 0) return '';
    return t + suffix;
}

function fmt_millis(millis) {
    return Math.round(millis / 1000) + "s";
}

function fmt_parameters(obj) {
    return fmt_table_short(args_to_params(obj));
}

function fmt_parameters_short(obj) {
    var res = '';
    var params = args_to_params(obj);

    for (var k in ALL_ARGS) {
        if (params[k] != undefined) {
            res += '<acronym title="' + k + ': ' + fmt_string(params[k]) +
                '">' + ALL_ARGS[k].short + '</acronym> ';
        }
    }

    if (params.arguments) {
        res += '<acronym title="' + fmt_table_flat(params.arguments) +
        '">Args</acronym>';
    }
    return res;
}

function short_conn(name) {
    var pat = /^(.*)->/;
    var match = pat.exec(name);
    return (match != null && match.length == 2) ? match[1] : name;
}

function short_chan(name) {
    var pat = /^(.*)->.*( \(.*\))/;
    var match = pat.exec(name);
    return (match != null && match.length == 3) ? match[1] + match[2] : name;
}

function args_to_params(obj) {
    var res = {};
    for (var k in obj.arguments) {
        if (k in KNOWN_ARGS) {
            res[k] = obj.arguments[k];
        }
        else {
            if (res.arguments == undefined) res.arguments = {};
            res.arguments[k] = obj.arguments[k];
        }
    }
    if (obj.durable) {
        res['durable'] = true;
    }
    if (obj.auto_delete) {
        res['auto-delete'] = true;
    }
    if (obj.internal != undefined && obj.internal) {
        res['internal'] = true;
    }
    return res;
}

function fmt_mirrors(queue) {
    var synced = queue.synchronised_slave_nodes || [];
    var unsynced = queue.slave_nodes || [];
    unsynced = jQuery.grep(unsynced,
                           function (node, i) {
                               return jQuery.inArray(node, synced) == -1
                           });
    var res = '';
    if (synced.length > 0) {
        res += ' <acronym title="Synchronised mirrors: ' + synced + '">+' +
            synced.length + '</acronym>';
    }
    if (synced.length == 0 && unsynced.length > 0) {
        res += ' <acronym title="There are no synchronised mirrors">+0</acronym>';
    }
    if (unsynced.length > 0) {
        res += ' <acronym class="warning" title="Unsynchronised mirrors: ' +
            unsynced + '">+' + unsynced.length + '</acronym>';
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
    var res = fmt_rate0(obj, name, fmt_rate_num, show_total);
    if (cssClass == undefined || res == '') {
        return res;
    }
    else {
        return '<span class="' + cssClass + '">' + res + '</span>';
    }
}

function fmt_rate_num(num) {
    if (num == undefined) return UNKNOWN_REPR;
    else if (num < 1)     return num.toFixed(2);
    else if (num < 10)    return num.toFixed(1);
    else                  return num.toFixed(0);
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

function fmt_deliver_rate(obj, show_redeliver, cssClass) {
    var res = fmt_rate(obj, 'deliver_get', false, cssClass);
    if (show_redeliver) {
        res += '<sub>' + fmt_rate(obj, 'redeliver') + '</sub>';
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

function is_col_empty(objects, name, accessor) {
    if (accessor == undefined) accessor = function(o) {return o.message_stats;};
    for (var i = 0; i < objects.length; i++) {
        var object = objects[i];
        if (!is_stat_empty(accessor(object), name)) {
            return false;
        }
    }
    return true;
}

function fmt_exchange(name) {
    return name == '' ? '(AMQP default)' : fmt_escape_html(name);
}

function fmt_exchange_type(type) {
    for (var i in exchange_types) {
        if (exchange_types[i].name == type) {
            return fmt_escape_html(type);
        }
    }
    return '<div class="status-red"><acronym title="Exchange type not found. ' +
        'Publishing to this exchange will fail.">' + fmt_escape_html(type) +
        '</acronym></div>';
}

function fmt_exchange_url(name) {
    return name == '' ? 'amq.default' : fmt_escape_html(name);
}

function fmt_download_filename(host) {
    var now = new Date();
    return host.replace('@', '_') + "_" + now.getFullYear() + "-" +
        (now.getMonth() + 1) + "-" + now.getDate() + ".json";
}

function fmt_fd_used(used, total) {
    if (used == 'install_handle_from_sysinternals') {
        return '<p class="c">handle.exe missing <span class="help" id="handle-exe"></span><sub>' + total + ' available</sub></p>';
    }
    else {
        return used;
    }
}

function fmt_table_short(table) {
    return '<table class="mini">' + fmt_table_body(table, ':') + '</table>';
}

function fmt_table_long(table) {
    return '<table class="facts">' + fmt_table_body(table, '') +
        '</table>';
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
    } else {
        var t = typeof(val);
        if (t == 'string') {
            return '<acronym class="type" title="string">' +
                fmt_escape_html(val) + '</acronym>';
        } else {
            return '<acronym class="type" title="' + t + '">' + val + '</acronym>';
        }
    }
}

function fmt_table_flat(table) {
    var res = [];
    for (k in table) {
        res.push(k + ': ' + fmt_amqp_value_flat(table[k]));
    }
    return res.join(', ');
}

function fmt_amqp_value_flat(val) {
    if (val instanceof Array) {
        var val2 = new Array();
        for (var i = 0; i < val.length; i++) {
            val2[i] = fmt_amqp_value_flat(val[i]);
        }
        return '[' + val2.join(",") + ']';
    } else if (val instanceof Object) {
        return '(' + fmt_table_flat(val) + ')';
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
    return fmt_escape_html0(txt).replace(/\n/g, '<br/>');
}

function fmt_escape_html_one_line(txt) {
    return fmt_escape_html0(txt).replace(/\n/g, '');
}

function fmt_escape_html0(txt) {
    return txt.replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;');
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

function fmt_node(node_host) {
    var both = node_host.split('@');
    var node = both.slice(0, 1);
    var host = both.slice(1);
    return '<small>' + node + '@</small>' + host;
}

function fmt_connection_state(conn) {
    if (conn.state == undefined) return '';

    var colour = 'green';
    var text = conn.state;
    var explanation;

    if (conn.last_blocked_by == 'resource' && conn.state == 'blocked') {
        colour = 'red';
        explanation = 'Resource alarm: Connection blocked.';
    }
    else if (conn.state == 'blocking') {
        colour = 'yellow';
        explanation = 'Resource alarm: Connection will block on publish.';
    }
    else if (conn.last_blocked_by == 'flow') {
        var age = conn.last_blocked_age.toFixed();
        if (age < 5) {
            colour = 'yellow';
            text = 'flow';
            explanation = 'Publishing rate recently restricted by server.';
        }
    }

    if (explanation) {
        return '<div class="status-' + colour + '"><acronym title="' +
            explanation + '">' + text + '</acronym></div>';
    }
    else {
        return '<div class="status-' + colour + '">' + text + '</div>';
    }
}

function fmt_resource_bar(used_label, limit_label, ratio, colour, help) {
    var width = 120;

    var res = '';
    var other_colour = colour;
    if (ratio > 1) {
        ratio = 1 / ratio;
        inverted = true;
        colour += '-dark';
    }
    else {
        other_colour += '-dark';
    }
    var offset = Math.round(width * (1 - ratio));

    res += '<div class="status-bar" style="width: ' + width + 'px;">';
    res += '<div class="status-bar-main ' + colour + '" style="background-image: url(img/bg-' + other_colour + '.png); background-position: -' + offset + 'px 0px; background-repeat: no-repeat;">';
    res += used_label;
    if (help != null) {
        res += ' <span class="help" id="' + help + '"></span>';
    }
    res += '</div>'; // status-bar-main
    if (limit_label != null) {
        res += '<sub>' + limit_label + '</sub>';
    }
    res += '</div>'; // status-bar
    return res;
}

function fmt_resource_bar_count(used, total, thresholds) {
    if (typeof used == 'number') {
        return fmt_resource_bar(used, total + ' available', used / total,
                                fmt_color(used / total, thresholds));
    } else {
        return used;
    }
}

function fmt_shortened_uri(uri0) {
    var uri = fmt_escape_html(uri0);
    if (uri.indexOf('?') == -1) {
        return uri;
    }
    else {
        return '<acronym title="' + uri + '">' +
            uri.substr(0, uri.indexOf('?')) + '?...</acronym>';
    }
}

function fmt_client_name(properties) {
    var res = [];
    if (properties.product != undefined) {
        res.push(properties.product);
    }
    if (properties.platform != undefined) {
        res.push(properties.platform);
    }
    res = res.join(" / ");

    if (properties.version != undefined) {
        res += '<sub>' + properties.version + '</sub>';
    }
    return res;
}

function alt_rows(i) {
    return (i % 2 == 0) ? ' class="alt1"' : ' class="alt2"';
}

function esc(str) {
    return encodeURIComponent(str);
}

function link_conn(name, desc) {
    if (desc == undefined) desc = short_conn(name);
    return _link_to(fmt_escape_html(desc), '#/connections/' + esc(name))
}

function link_channel(name) {
    return _link_to(fmt_escape_html(short_chan(name)), '#/channels/' + esc(name))
}

function link_exchange(vhost, name) {
    var url = esc(vhost) + '/' + (name == '' ? 'amq.default' : esc(name));
    return _link_to(fmt_exchange(name), '#/exchanges/' + url)
}

function link_queue(vhost, name) {
    return _link_to(fmt_escape_html(name), '#/queues/' + esc(vhost) + '/' + esc(name))
}

function link_vhost(name) {
    return _link_to(fmt_escape_html(name), '#/vhosts/' + esc(name))
}

function link_user(name) {
    return _link_to(fmt_escape_html(name), '#/users/' + esc(name))
}

function link_node(name) {
    return _link_to(fmt_escape_html(name), '#/nodes/' + esc(name))
}

function link_policy(vhost, name) {
    return _link_to(fmt_escape_html(name), '#/policies/' + esc(vhost) + '/' + esc(name))
}

function _link_to(name, url) {
    return '<a href="' + url + '">' + name + '</a>';
}

function message_rates(stats) {
    var res = "";
    if (keys(stats).length > 0) {
        var items = [['Publish', 'publish'], ['Confirm', 'confirm'],
                     ['Deliver', 'deliver'],
                     ['Redelivered', 'redeliver'],
                     ['Acknowledge', 'ack'],
                     ['Get', 'get'], ['Deliver (noack)', 'deliver_no_ack'],
                     ['Get (noack)', 'get_no_ack'],
                     ['Return', 'return_unroutable']];
        for (var i in items) {
            var name = items[i][0];
            var key = items[i][1] + '_details';
            if (key in stats) {
                res += '<div class="highlight">' + name;
                res += '<strong>' + fmt_rate_num(stats[key].rate) + '</strong>';
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

function queue_length(stats, name, key) {
    var rateMsg = '&nbsp;';
    var detail = stats[key + '_details']
    if (detail != undefined) {
        var rate = detail.rate;
        if (rate > 0)      rateMsg = '+' + fmt_rate_num(rate)  + ' msg/s';
        else if (rate < 0) rateMsg = '-' + fmt_rate_num(-rate) + ' msg/s';
    }

    return '<div class="highlight">' + name +
        '<strong>' + stats[key] + '</strong>' + rateMsg + '</div>';
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
