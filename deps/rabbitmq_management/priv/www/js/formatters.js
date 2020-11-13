const UNKNOWN_REPR = '<span class="unknown">?</span>';
const FD_THRESHOLDS = [[0.95, 'red'],
                       [0.8, 'yellow']];
const SOCKETS_THRESHOLDS = [[1.0, 'red'],
                            [0.8, 'yellow']];
const PROCESS_THRESHOLDS = [[0.75, 'red'],
                            [0.5, 'yellow']];

const TAB_HIGHLIGHTER = "\u2192";
const WHITESPACE_HIGHLIGHTER = "\u23B5";

function fmt_string(str, unknown) {
    if (unknown == undefined) {
        unknown = UNKNOWN_REPR;
    }
    if (str == undefined) {
        return unknown;
    }
    return fmt_escape_html("" + str);
}

function fmt_si_prefix(num0, max0, thousand, allow_fractions) {
    if (num == 0) return 0;

    function f(n, m, p) {
        if (m > thousand) return f(n / thousand, m / thousand, p + 1);
        else return [n, m, p];
    }

    var num_power = f(num0, max0, 0);
    var num = num_power[0];
    var max = num_power[1];
    var power = num_power[2];
    var powers = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    return (((power != 0 || allow_fractions) && max <= 10) ? num.toFixed(1) :
            num.toFixed(0)) + powers[power];
}

function fmt_boolean(b, unknown) {
    if (unknown == undefined) unknown = UNKNOWN_REPR;
    if (b == undefined) return unknown;

    return b ? "&#9679;" : "&#9675;";
}

function fmt_date(d) {
    var res = fmt_date0(d);
    return res[0] + ' ' + res[1];
}

function fmt_date_mini(d) {
    var res = fmt_date0(d);
    return res[1] + '<sub>' + res[0] + '</sub>';
}

function fmt_date0(d) {
    function f(i) {
        return i < 10 ? "0" + i : i;
    }

    return [d.getFullYear() + "-" + f(d.getMonth() + 1) + "-" +
            f(d.getDate()), f(d.getHours()) + ":" + f(d.getMinutes()) +
        ":" + f(d.getSeconds())];
}

function fmt_timestamp(ts) {
    return fmt_date(new Date(ts));
}

function fmt_timestamp_mini(ts) {
    return fmt_date_mini(new Date(ts));
}

function fmt_time(t, suffix) {
    if (t == undefined || t == 0) return '';
    return t + suffix;
}

function fmt_millis(millis) {
    return Math.round(millis / 1000) + "s";
}

function fmt_features(obj) {
    return fmt_table_short(args_to_features(obj));
}

function fmt_policy_short(obj) {
    if (obj.policy != undefined && obj.policy != '') {
        return '<abbr class="policy" title="Policy: ' +
            fmt_escape_html(obj.policy) + '">' +
            link_policy(obj.vhost, obj.policy) + '</abbr> ';
    } else {
        return '';
    }
}

function fmt_op_policy_short(obj) {
    if (obj.operator_policy != undefined && obj.operator_policy != '') {
        return '<abbr class="policy" title="Operator policy: ' +
            fmt_escape_html(obj.operator_policy) + '">' +
            fmt_escape_html(obj.operator_policy) + '</abbr> ';
    } else {
        return '';
    }
}

function fmt_features_short(obj) {
    var res = '';
    var features = args_to_features(obj);

    if (obj.owner_pid_details != undefined) {
        res += '<acronym title="Exclusive queue: click for owning connection">'
            + link_conn(obj.owner_pid_details.name, "Excl") + '</acronym> ';
    }

    for (var k in ALL_ARGS) {
        if (features[k] != undefined) {
            res += '<abbr title="' + k + ': ' + fmt_string(features[k]) +
                '">' + ALL_ARGS[k].short + '</abbr> ';
        }
    }

    if (features.arguments) {
        res += '<abbr title="' + fmt_table_flat(features.arguments) +
        '">Args</abbr> ';
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

function args_to_features(obj) {
    var res = {};
    for (var k in obj.arguments) {
        if (k in KNOWN_ARGS) {
            res[k] = fmt_escape_html(obj.arguments[k]);
        }
        else {
            if (res.arguments == undefined) res.arguments = {};
            res.arguments[fmt_escape_html(k)] = fmt_escape_html(obj.arguments[k]);
        }
    }
    if (obj.exclusive) {
        res['exclusive'] = true;
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
    if (obj.messages_delayed != undefined){
        res['messages delayed'] = obj.messages_delayed;
    }
    return res;
}

function fmt_mirrors(queue) {
    var synced = queue.synchronised_slave_nodes || [];
    var unsynced = queue.slave_nodes || [];
    unsynced = jQuery.grep(unsynced,
                           function (node, i) {
                               return jQuery.inArray(node, synced) == -1;
                           });
    var res = '';
    if (synced.length > 0) {
        res += ' <abbr title="Synchronised mirrors: ' + synced + '">+' +
            synced.length + '</abbr>';
    }
    if (synced.length == 0 && unsynced.length > 0) {
        res += ' <abbr title="There are no synchronised mirrors">+0</abbr>';
    }
    if (unsynced.length > 0) {
        res += ' <abbr class="warning" title="Unsynchronised mirrors: ' +
            unsynced + '">+' + unsynced.length + '</abbr>';
    }
    return res;
}

function fmt_sync_state(queue) {
    var res = '<p><b>Syncing: ';
    res += (queue.messages == 0) ? 100 : Math.round(100 * queue.sync_messages /
                                                    queue.messages);
    res += '%</b></p>';
    return res;
}

function fmt_channel_mode(ch) {
    if (ch.transactional) {
        return '<abbr title="Transactional">T</abbr>';
    }
    else if (ch.confirm) {
        return '<abbr title="Confirm">C</abbr>';
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

function fmt_rate_num(num) {
    if (num == undefined) return UNKNOWN_REPR;
    else if (num < 1)     return num.toFixed(2);
    else if (num < 10)    return num.toFixed(1);
    else                  return fmt_num_thousands(num);
}

function fmt_num_thousands(num) {
    var conv_num = parseFloat(num); // to avoid errors, if someone calls fmt_num_thousands(someNumber.toFixed(0))
    return fmt_num_thousands_unfixed(conv_num.toFixed(0));
}

function fmt_num_thousands_unfixed(num) {
     if (num == undefined) return UNKNOWN_REPR;
     num = '' + num;
     if (num.length < 4) return num;
     res= fmt_num_thousands_unfixed(num.slice(0, -3)) + ',' + num.slice(-3);
     return res;
}

function fmt_percent(num) {
    if (num === '') {
        return 'N/A';
    } else {
        return Math.round(num * 100) + '%';
    }
}

function pick_rate(fmt, obj, name, mode) {
    if (obj == undefined || obj[name] == undefined ||
        obj[name + '_details'] == undefined) return '';
    var details = obj[name + '_details'];
    return fmt(mode == 'avg' ? details.avg_rate : details.rate);
}

function pick_abs(fmt, obj, name, mode) {
    if (obj == undefined || obj[name] == undefined ||
        obj[name + '_details'] == undefined) return '';
    var details = obj[name + '_details'];
    return fmt(mode == 'avg' ? details.avg : obj[name]);
}

function fmt_detail_rate(obj, name, mode) {
    return pick_rate(fmt_rate, obj, name, mode);
}

function fmt_detail_rate_bytes(obj, name, mode) {
    return pick_rate(fmt_rate_bytes, obj, name, mode);
}

// ---------------------------------------------------------------------

// These are pluggable for charts etc

function fmt_plain(num) {
    return num;
}

function fmt_plain_axis(num, max) {
    return fmt_si_prefix(num, max, 1000, true);
}

function fmt_rate(num) {
    return fmt_rate_num(num) + '/s';
}

function fmt_rate_axis(num, max) {
    return fmt_plain_axis(num, max) + '/s';
}

function fmt_bytes(bytes) {
    if (bytes == undefined) return UNKNOWN_REPR;
    return fmt_si_prefix(bytes, bytes, 1024, false) + 'iB';
}

function fmt_bytes_axis(num, max) {
    num = parseInt(num);
    return fmt_bytes(isNaN(num) ? 0 : num);
}

function fmt_rate_bytes(num) {
    return fmt_bytes(num) + '/s';
}

function fmt_rate_bytes_axis(num, max) {
    return fmt_bytes_axis(num, max) + '/s';
}

function fmt_ms(num) {
    return fmt_rate_num(num) + 'ms';
}

// ---------------------------------------------------------------------

function fmt_maybe_vhost(name) {
    return vhosts_interesting ?
        ' in virtual host <b>' + fmt_escape_html(name) + '</b>'
        : '';
}

function fmt_exchange(name) {
    return fmt_escape_html(fmt_exchange0(name));
}

function fmt_exchange0(name) {
    return name == '' ? '(AMQP default)' : name;
}

function fmt_exchange_type(type) {
    for (var i in exchange_types) {
        if (exchange_types[i].name == type) {
            return fmt_escape_html(type);
        }
    }
    return '<div class="status-red"><abbr title="Exchange type not found. ' +
        'Publishing to this exchange will fail.">' + fmt_escape_html(type) +
        '</abbr></div>';
}

function fmt_exchange_url(name) {
    return name == '' ? 'amq.default' : fmt_escape_html(name);
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
        '</table>';
}

function fmt_table_body(table, x) {
    var res = '';
    for (k in table) {
        res += '<tr><th>' + fmt_escape_html(k) + x + '</th>' +
            '<td>' + fmt_amqp_value(table[k]) + '</td>';
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
            return '<abbr class="type" title="string">' +
                fmt_escape_html(val) + '</abbr>';
        } else {
            return '<abbr class="type" title="' + t + '">' + val + '</abbr>';
        }
    }
}

function fmt_table_flat(table) {
    var res = [];
    for (k in table) {
        res.push(fmt_escape_html(k) + ': ' + fmt_amqp_value_flat(table[k]));
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

function fmt_plugins_small(node) {
    if (node.applications === undefined) return '';
    var plugins = [];
    for (var i = 0; i < node.applications.length; i++) {
        var application = node.applications[i];
        if (jQuery.inArray(application.name, node.enabled_plugins) != -1 ) {
            plugins.push(application.name);
        }
    }
    return '<abbr title="Enabled plugins: ' + plugins.join(", ") + '">' +
        plugins.length + '</abbr>';
}

function get_plugins_list(node) {
    var result = [];
    for (var i = 0; i < node.applications.length; i++) {
        var application = node.applications[i];
        if (jQuery.inArray(application.name, node.enabled_plugins) != -1 ) {
            result.push(application);
        }
    }
    return result;
}

function fmt_rabbit_version(applications) {
    for (var i in applications) {
        if (applications[i].name == 'rabbit') {
            return applications[i].version;
        }
    }
    return 'unknown';
}

function fmt_strip_tags(txt) {
    if(txt === null) {
        return "";
    }
    if(txt === undefined) {
        return "";
    }
    return ("" + txt).replace(/<(?:.|\n)*?>/gm, '');
}

function fmt_escape_html(txt) {
    return fmt_escape_html0(txt).replace(/\n/g, '<br/>');
}

function fmt_escape_html_one_line(txt) {
    return fmt_escape_html0(txt).replace(/\n/g, '');
}

function fmt_escape_html0(txt) {
    if(txt === null) {
        return "";
    }
    if(txt === undefined) {
        return "";
    }

    return ("" + txt).replace(/&/g, '&amp;')
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
    return fmt_string(node_host);
}

function fmt_object_state(obj) {
    if (obj.state == undefined) return '';

    var colour = 'green';
    var text = obj.state;
    var explanation;

    if (obj.idle_since !== undefined) {
        colour = 'grey';
        explanation = 'Idle since ' + obj.idle_since;
        text = 'idle';
    }
    // Only connections can be 'blocked' or 'blocking'
    else if (obj.state == 'blocked') {
        colour = 'red';
        explanation = 'Resource alarm: connection blocked.';
    }
    else if (obj.state == 'blocking') {
        colour = 'yellow';
        explanation = 'Resource alarm: connection will block on publish.';
    }
    else if (obj.state == 'flow') {
        colour = 'yellow';
        explanation = 'Publishing rate recently throttled by server.';
    }
    else if (obj.state == 'terminated') {
        colour = 'yellow';
        var terminated_by = "";
        if (obj.terminated_by) {
            terminated_by = " by \"" + String(obj.terminated_by) + "\"";
        }
        explanation = 'The queue is being deleted' + terminated_by + ".";
    }
    else if (obj.state == 'down') {
        colour = 'red';
        explanation = 'The queue is located on a cluster node or nodes that ' +
            'are down.';
    }
    else if (obj.state == 'crashed') {
        colour = 'red';
        explanation = 'The queue has crashed repeatedly and been unable to ' +
            'restart.';
    }
    else if (obj.state == 'stopped') {
        colour = 'red';
        explanation = 'The queue process was stopped by the vhost supervisor.';
    }

    return fmt_state(colour, text, explanation);
}

function fmt_state(colour, text, explanation) {
    var key;
    if (explanation) {
        key = '<abbr class="normal" title="' + explanation + '">' +
            text + '</abbr>';
    }
    else {
        key = text;
    }

    return '<div class="colour-key status-key-' + colour + '"></div>' + key;
}

function fmt_shortened_uri(uri) {
    if (typeof uri == 'object') {
        var res = '';
        for (i in uri) {
            res += fmt_shortened_uri(uri[i]) + '<br/>';
        }
        return res;
    }
    var uri = fmt_escape_html(uri);
    if (uri.indexOf('?') == -1) {
        return uri;
    }
    else {
        return '<abbr title="' + uri + '">' +
            uri.substr(0, uri.indexOf('?')) + '?...</abbr>';
    }
}

function fmt_uri_with_credentials(uri) {
    if (typeof uri == 'object') {
        var res = [];
        for (i in uri) {
            res.push(fmt_uri_with_credentials(uri[i]));
        }
        return res;
    }
    else if (typeof uri == 'string') {
        // mask password
        var mask = /^([a-zA-Z0-9+-.]+):\/\/(.*):(.*)@/;
        return uri.replace(mask, "$1://$2:[redacted]@");
    } else {
        return UNKNOWN_REPR;
    }
}

function fmt_client_name(properties) {
    var res = [];
    if (properties.product != undefined) {
        res.push(fmt_trunc(properties.product, 120));
    }
    if (properties.platform != undefined) {
        res.push(fmt_trunc(properties.platform, 120));
    }
    res = res.join(" / ");

    if (properties.version != undefined) {
        res += '<sub>' + fmt_trunc(properties.version) + '</sub>';
    }

    if (properties.client_id != undefined) {
        res += '<sub>' + fmt_trunc(properties.client_id, 120) + '</sub>';
    }

    return res;
}

function fmt_trunc(str, max_length) {
    return str.length > max_length ?
        ('<abbr class="normal" title="' + fmt_escape_html(str) + '">' +
         fmt_escape_html(str.substring(0, max_length)) + '...</abbr>') :
        fmt_escape_html(str);
}

function alt_rows(i, args) {
    var css = [(i % 2 == 0) ? 'alt1' : 'alt2'];
    if (args != undefined && args['x-internal-purpose'] != undefined) {
        css.push('internal-purpose');
    }
    return ' class="' + css.join(' ') + '"';
}

function esc(str) {
    return encodeURIComponent(str);
}

// Replaces a sequence of characters matched by a regular expression
// group with the given character. Replaced group is combined with the stripped
// original using the provided combine function.
function replace_char_sequence_individually(input, regex, replacement, combine) {
  let ms = input.match(regex);
  if (ms == null) {
    return input;
  } else {
    let n = ms[0].length;
    return combine(replacement.repeat(n), input.replace(regex, ""));
  }
}

// Replaces a leading sequence of characters matched by a regular expression
// group with the given character.
function replace_leading_chars(input, regex, replacement) {
  return replace_char_sequence_individually(input, regex, replacement, function(patch, stripped_input) {
      return patch + stripped_input;
  });
}

// Replaces a trailing sequence of characters matched by a regular expression
// group with the given character.
function replace_trailing_chars(input, regex, replacement) {
    return replace_char_sequence_individually(input, regex, replacement, function(patch, stripped_input) {
      return stripped_input + patch;
  });
}

// Highlights extra (leading and trailing) whitespace and tab characters
// with suitable Unicode characters for improved visiblity.
// Note that mid-word whitespace should not be highighted, which makes
// the implementation trickier than a simple chain of replace/2 calls.
function highlight_extra_whitespace(str) {
  // Highlight leading and trailing whitespace individually but all tabs.
  // This assumes that spaces are reasonable to use in the middle of a name
  // but tabs are not used intentionally and must be highlighted even in the middle.
  return [[replace_trailing_chars, /(\s+)$/g, WHITESPACE_HIGHLIGHTER],
          [replace_leading_chars,  /^(\s+)/g, WHITESPACE_HIGHLIGHTER]].reduce(function(acc, triplet) {
    return triplet[0](acc, triplet[1], triplet[2]);
  }, str.replace(/\t/g, TAB_HIGHLIGHTER));
}

function link_conn(name, desc) {
    if (desc == undefined) {
        return _link_to(short_conn(name), '#/connections/' + esc(name));
    }
    else {
        return _link_to(desc, '#/connections/' + esc(name), false);
    }
}

function link_channel(name) {
    return _link_to(short_chan(name), '#/channels/' + esc(name));
}

function link_exchange(vhost, name, args) {
    var url = esc(vhost) + '/' + (name == '' ? 'amq.default' : esc(name));
    return _link_to(fmt_exchange0(highlight_extra_whitespace(name)), '#/exchanges/' + url, true, args);
}

function link_queue(vhost, name, args) {
    return _link_to(highlight_extra_whitespace(name), '#/queues/' + esc(vhost) + '/' + esc(name), true, args);
}

function link_vhost(name) {
    return _link_to(name, '#/vhosts/' + esc(name));
}

function link_user(name) {
    return _link_to(name, '#/users/' + esc(name));
}

function link_node(name) {
    return _link_to(fmt_node(name), '#/nodes/' + esc(name));
}

function link_policy(vhost, name) {
    return _link_to(name, '#/policies/' + esc(vhost) + '/' + esc(name));
}

function _link_to(name, url, highlight, args) {
    if (highlight == undefined) highlight = true;
    var title = null;
    if (args != undefined && args['x-internal-purpose'] != undefined) {
        var purpose = args['x-internal-purpose'];
        title = 'This is used internally by the ' + purpose + ' mechanism.';
    }
    return '<a href="' + url + '"' +
        (title ? ' title="' + title + '"' : '') + '>' +
        (highlight ? fmt_highlight_filter(name) : fmt_escape_html(name)) +
        '</a>';
}

function fmt_highlight_filter(text) {
    if (current_filter == '') return fmt_escape_html(text);

    var text_to_match = current_filter.toLowerCase();
    if (current_filter_regex) {
        var potential_match = current_filter_regex.exec(text.toLowerCase());
        if (potential_match) {
            text_to_match = potential_match[0];
        }
    }
    var ix = text.toLowerCase().indexOf(text_to_match);
    var l = text_to_match.length;
    if (ix == -1) {
        return fmt_escape_html(text);
    }
    else {
        return fmt_escape_html(text.substring(0, ix)) +
            '<span class="filter-highlight">' +
            fmt_escape_html(text.substring(ix, ix + l)) + '</span>' +
            fmt_escape_html(text.substring(ix + l));
    }
}

function filter_ui_pg(items, truncate, appendselect) {
    var total = items.length;
    if (current_filter != '') {
        var items2 = [];
        for (var i in items) {
            var item = items[i];
            var item_name = item.name.toLowerCase();
            if ((current_filter_regex_on &&
                 current_filter_regex &&
                 current_filter_regex.test(item_name)) ||
                item_name.indexOf(current_filter.toLowerCase()) != -1) {
                items2.push(item);
            }
        }
        items.length = items2.length;
        for (var i in items2) items[i] = items2[i];
    }

    var res = '<div class="filter"><table' +
        (current_filter == '' ? '' : ' class="filter-active"') +
        '><tr><th>Filter:</th>' +
        '<td><input id="filter" type="text" value="' +
        fmt_escape_html(current_filter) + '"/>' +
        '<input type="checkbox" name="filter-regex-mode" id="filter-regex-mode"' +
        (current_filter_regex_on ? ' checked' : '') +
        '/><label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span>' +
        '</td></tr></table>';

    function items_desc(l) {
        return l == 1 ? (l + ' item') : (l + ' items');
    }

    var selected = current_filter == '' ? (items_desc(items.length)) :
        (items.length + ' of ' + items_desc(total) + ' selected');


    selected += appendselect;

    res += '<p id="filter-truncate"><span class="updatable">' + selected +
        '</span>' + truncate + '</p>';
    res += '</div>';

    return res;
}


function filter_ui(items) {
    var current_truncate = parseInt(get_pref('truncate'));
    var truncate_input   = '<input type="text" id="truncate" value="' +
        current_truncate + '">';
     var selected = '';
    if (items.length > current_truncate) {
        selected += '<span id="filter-warning-show"> ' +
            '(only showing first</span> ';
        items.length = current_truncate;
    }
    else {
        selected += ', page size up to ';
    }
   return filter_ui_pg(items, truncate_input, selected);

}

function paginate_header_ui(pages, context){
     var res = '<h2 class="updatable">';
     res += ' All ' + context +' (' + pages.total_count + ((pages.filtered_count != pages.total_count) ?  ', filtered down to ' + pages.filtered_count  : '') +  ')';
     res += '</h2>';
    return res;
}

function paginate_ui(pages, context){
    var res = paginate_header_ui(pages, context);
    res += '<div class="hider">';
    res += '<h3>Pagination</h3>';
    res += '<div class="filter">';
    res += '<table class="updatable">';
    res += '<tr>';
    res += '<th><label for="'+ context +'-page">Page </label> <select id="'+ context +'-page" class="pagination_class pagination_class_select"  >';
    var page =  fmt_page_number_request(context, pages.page);
    if (pages.page_count > 0 &&  page > pages.page_count){
           page = pages.page_count;
           update_pages(context, page);
           return;
      };
        for (var i = 1; i <= pages.page_count; i++) { ;
           if (i == page) {;
    res +=   ' <option selected="selected" value="'+ i + '">' + i + '</option>';
              } else { ;
    res +=    '<option value="' + i + '"> ' + i + '</option>';
             } };
    res += '</select> </th>';
    res += '<th><label for="' + context +'-pageof">of </label>  ' + pages.page_count +'</th>';
    res += '<th><span><label for="'+ context +'-name"> - Filter: </label> <input id="'+ context +'-name"  data-page-start="1"  class="pagination_class pagination_class_input" type="text"';
    res +=   'value = ' + fmt_filter_name_request(context, "") + '>';
    res +=   '</input></th></span>';

    res += '<th> <input type="checkbox" data-page-start="1" class="pagination_class pagination_class_checkbox" id="'+ context +'-filter-regex-mode"' ;

    res += fmt_regex_request(context, "") + '></input> <label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span></th>' ;

    res +=' </table>' ;
    res += '<p id="filter-truncate"><span class="updatable">';
    res += '<span><label for="'+ context +'-pagesize"> Displaying ' + pages.item_count + '  item'+ ((pages.item_count > 1) ? 's' : '' ) + ' , page size up to: </label> ';
    res +=       ' <input id="'+ context +'-pagesize" data-page-start="1" class="pagination_class shortinput pagination_class_input" type="text" ';
    res +=   'value = "' +  fmt_page_size_request(context, pages.page_size) +'"';
    res +=   'onkeypress = "return isNumberKey(event)"> </input></span></p>';
    res += '</tr>';
    res += '</div>';
    res += '</div>';
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
            (current_sort_reverse ? '&#9660; ' : '&#9650; ') +
            '</span>';
    }
    return '<a class="sort" sort="' + sort + '">' + prefix + display + '</a>';
}

function group_count(mode, group, bools) {
    var count = 0;
    for (var i = 0; i < bools.length; i++) {
        if (bools[i]) count++;
    }

    var options = COLUMNS[mode][group];
    for (var i = 0; i < options.length; i++) {
        var column = options[i][0];
        if (show_column(mode, column)) count++;
    }
    return count;
}

function group_heading(mode, group, bools) {
    var count = group_count(mode, group, bools);
    if (count == 0) {
        return '';
    }
    else {
        return '<th colspan="' + count + '">' + group + '</th>';
    }
}

function fmt_permissions(obj, permissions, lookup, show, warning) {
    var res = [];
    for (var i in permissions) {
        var permission = permissions[i];
        if (permission[lookup] == obj.name) {
            res.push(fmt_escape_html(permission[show]));
        }
    }
    return res.length == 0 ? warning : res.join(', ');
}

var radio_id = 0;

function fmt_radio(name, text, value, current) {
    radio_id++;
    return '<label class="radio" for="radio-' + radio_id + '">' +
        '<input type="radio" id="radio-' + radio_id + '" name="' + name +
        '" value="' + value + '"' +
        ((value == current) ? ' checked="checked"' : '') +
        '>' + text + '</label>';
}

function fmt_checkbox(name, text, current) {
    return '<label class="checkbox" for="checkbox-' + name + '">' +
        '<input type="checkbox" id="checkbox-' + name + '" name="' + name +
        '"' + (current ? ' checked="checked"' : '') + '>' + text + '</label>';
}

function properties_size(obj) {
    var count = 0;
    for (k in obj) {
        if (obj.hasOwnProperty(k)) count++;
    }
    return count;
}

function stored_value_or_default(template, defaultValue){
    var stored_value = get_pref(template);
    var result = (((stored_value == null)
      || (stored_value == undefined)
      || (stored_value == '')) ? defaultValue :
    stored_value);

   return ((result == undefined) ? defaultValue : result);
}

function fmt_page_number_request(template, defaultPage){
     if  ((defaultPage == undefined) || (defaultPage <= 0)) {
         defaultPage = 1;
     }
    return stored_value_or_default(template + '_current_page_number', defaultPage);
}
function fmt_page_size_request(template, defaultPageSize){
    if  ((defaultPageSize == undefined) || (defaultPageSize < 0)) {
        defaultPageSize = 100;
    }

    var result = stored_value_or_default(template + '_current_page_size', defaultPageSize);
    if (result > 500) {
        // hard limit
        result = 500;
    }
    return result;
}

function fmt_filter_name_request(template, defaultName){
    return fmt_escape_html(stored_value_or_default(template + '_current_filter_name', defaultName));
}

function fmt_regex_request(template, defaultName){
    return fmt_escape_html(stored_value_or_default(template + '_current_regex', defaultName));
}

function fmt_vhost_state(vhost){
    var cluster_state = vhost.cluster_state;
    var down_nodes = [];
    var ok_count = 0;
    var non_ok_count = 0;
    for(var node in cluster_state){
        var node_state = cluster_state[node];
        if(cluster_state[node] == "stopped" || cluster_state[node] == "nodedown"){
            non_ok_count++;
            down_nodes.push(node);
        } else if(cluster_state[node] == "running"){
            ok_count++;
        }
    }
    if(non_ok_count == 0 ){
        return fmt_state('green', 'running', '');
    } else if(non_ok_count > 0 && ok_count == 0){
        return fmt_state('red', 'stopped', 'Vhost supervisor is not running.');
    } else if(non_ok_count > 0 && ok_count > 0){
        return fmt_state('yellow', 'partial', 'Vhost supervisor is stopped on some cluster nodes: ' + down_nodes.join(', '));
    }
}

function isNumberKey(evt){
    var charCode = (evt.which) ? evt.which : event.keyCode;
    if (charCode > 31 && (charCode < 48 || charCode > 57))
        return false;
    return true;
}
