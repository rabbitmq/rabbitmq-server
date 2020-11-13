//
// Formatting side
//

function message_rates(id, stats) {
    var items = [['Publish', 'publish'],
                 ['Publisher confirm', 'confirm'],
                 ['Publish (In)', 'publish_in'],
                 ['Publish (Out)', 'publish_out'],
                 ['Deliver (manual ack)', 'deliver'],
                 ['Deliver (auto ack)', 'deliver_no_ack'],
                 ['Consumer ack', 'ack'],
                 ['Redelivered', 'redeliver'],
                 ['Get (manual ack)', 'get'],
                 ['Get (auto ack)', 'get_no_ack'],
                 ['Return', 'return_unroutable'],
                 ['Disk read', 'disk_reads'],
                 ['Disk write', 'disk_writes']];
    return rates_chart_or_text(id, stats, items, fmt_rate, fmt_rate_axis, true, 'Message rates', 'message-rates');
}

function queue_lengths(id, stats) {
    var items = [['Ready', 'messages_ready'],
                 ['Unacked', 'messages_unacknowledged'],
                 ['Total', 'messages']];
    return rates_chart_or_text(id, stats, items, fmt_num_thousands, fmt_plain_axis, false, 'Queued messages', 'queued-messages');
}

function data_rates(id, stats) {
    var items = [['From client', 'recv_oct'], ['To client', 'send_oct']];
    return rates_chart_or_text(id, stats, items, fmt_rate_bytes, fmt_rate_bytes_axis, true, 'Data rates');
}

function data_reductions(id, stats) {
    var items = [['Reductions', 'reductions']];
    return rates_chart_or_text(id, stats, items, fmt_rate, fmt_rate_axis, true, 'Reductions (per second)', 'process-reductions');
}

function rates_chart_or_text(id, stats, items, fmt, axis_fmt, chart_rates,
                             heading, heading_help) {
    var prefix = chart_h3(id, heading, heading_help);

    return prefix + rates_chart_or_text_no_heading(
            id, id, stats, items, fmt, axis_fmt, chart_rates);
}

function rates_chart_or_text_no_heading(type_id, id, stats, items,
                                        fmt, axis_fmt, chart_rates) {
    var mode = get_pref('rate-mode-' + type_id);
    var range = get_pref('chart-range');
    var res;
    if (keys(stats).length > 0) {
        if (mode == 'chart') {
            res = rates_chart(
                type_id, id, items, stats, fmt, axis_fmt, 'full', chart_rates);
        }
        else {
            res = rates_text(items, stats, mode, fmt, chart_rates);
        }
        if (res == "") res = '<p>Waiting for data...</p>';
    }
    else {
        res = '<p>Currently idle</p>';
    }
    return res;
}

function chart_h3(id, heading, heading_help) {
    var mode = get_pref('rate-mode-' + id);
    var range = get_pref('chart-range');
    return '<h3>' + heading +
        ' <span class="popup-options-link" title="Click to change" ' +
        'type="rate" for="' + id + '">' + prefix_title(mode, range) +
        '</span>' + (heading_help == undefined ? '' :
         ' <span class="help" id="' + heading_help + '"></span>') +
        '</h3>';
}

function prefix_title(mode, range) {
    var desc = ALL_CHART_RANGES[range];
    if (mode == 'chart') {
        return desc.toLowerCase();
    }
    else if (mode == 'curr') {
        return 'current value';
    }
    else {
        return 'moving average: ' + desc.toLowerCase();
    }
}

function node_stat_count(used_key, limit_key, stats, thresholds) {
    var used = stats[used_key];
    var limit = stats[limit_key];
    if (typeof used == 'number') {
        return node_stat(used_key, 'Used', limit_key, 'available', stats,
                         fmt_plain, fmt_plain_axis,
                         fmt_color(used / limit, thresholds));
    } else {
        return used;
    }
}

function node_stat_count_bar(used_key, limit_key, stats, thresholds) {
    var used = stats[used_key];
    var limit = stats[limit_key];
    if (typeof used == 'number') {
        return node_stat_bar(used_key, limit_key, 'available', stats,
                             fmt_plain_axis,
                             fmt_color(used / limit, thresholds));
    } else {
        return used;
    }
}

function node_stat(used_key, used_name, limit_key, suffix, stats, fmt,
                   axis_fmt, colour, help, invert) {
    if (get_pref('rate-mode-node-stats') == 'chart') {
        var items = [[used_name, used_key], ['Limit', limit_key]];
        add_fake_limit_details(used_key, limit_key, stats);
        return rates_chart('node-stats', 'node-stats-' + used_key, items, stats,
                           fmt, axis_fmt, 'node', false);
    } else {
        return node_stat_bar(used_key, limit_key, suffix, stats, axis_fmt,
                             colour, help, invert);
    }
}

function add_fake_limit_details(used_key, limit_key, stats) {
    var source = stats[used_key + '_details'].samples;
    var limit = stats[limit_key];
    var dest = [];
    for (var i in source) {
        dest[i] = {sample: limit, timestamp: source[i].timestamp};
    }
    stats[limit_key + '_details'] = {samples: dest};
}

function node_stat_bar(used_key, limit_key, suffix, stats, fmt, colour,
                       help, invert) {
    var used = stats[used_key];
    var limit = stats[limit_key];
    var width = 120;

    var res = '';
    var other_colour = colour;
    var ratio = invert ? (limit / used) : (used / limit);
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
    res += fmt(used);
    if (help != null) {
        res += ' <span class="help" id="' + help + '"></span>';
    }
    res += '</div>'; // status-bar-main
    res += '<sub>' + fmt(limit) + ' ' + suffix + '</sub>';
    res += '</div>'; // status-bar

    return res;
}

function node_stats_prefs() {
    return chart_h3('node-stats', 'Node statistics');
}

function rates_chart(type_id, id, items, stats, fmt, axis_fmt, type,
                     chart_rates) {
    function show(key) {
        return get_pref('chart-line-' + id + key) === 'true';
    }

    var size = get_pref('chart-size-' + type_id);
    var legend = [];
    chart_data[id] = {};
    chart_data[id]['data'] = {};
    chart_data[id]['fmt'] = axis_fmt;
    var ix = 0;
    for (var i in items) {
        var name = items[i][0];
        var key = items[i][1];
        var key_details = key + '_details';
        if (key_details in stats) {
            if (show(key)) {
                chart_data[id]['data'][name] = stats[key_details];
                chart_data[id]['data'][name].ix = ix;
            }
            var value = chart_rates ? pick_rate(fmt, stats, key) :
                                      pick_abs(fmt, stats, key);
            legend.push({name:  name,
                         key:   key,
                         value: value,
                         show:  show(key)});
            ix++;
        }
    }
    var html = '<div class="box"><div id="chart-' + id +
        '" class="chart chart-' + type + ' chart-' + size +
        (chart_rates ? ' chart-rates' : '') + '"></div>';
    html += '<table class="legend">';
    for (var i = 0; i < legend.length; i++) {
        if (i % 3 == 0 && i < legend.length - 1) {
            html += '</table><table class="legend">';
        }

        html += '<tr><th><span title="Click to toggle line" ';
        html += 'class="rate-visibility-option';
        html += legend[i].show ? '' : ' rate-visibility-option-hidden';
        html += '" data-pref="chart-line-' + id + legend[i].key + '">';
        html += legend[i].name + '</span></th><td>';
        html += '<div class="colour-key" style="background: ' + chart_colors[type][i];
        html += ';"></div>' + legend[i].value + '</td></tr>'
    }
    html += '</table></div>';
    return legend.length > 0 ? html : '';
}

function rates_text(items, stats, mode, fmt, chart_rates) {
    var res = '';
    for (var i in items) {
        var name = items[i][0];
        var key = items[i][1];
        var key_details = key + '_details';
        if (key_details in stats) {
            var details = stats[key_details];
            res += '<div class="highlight">' + name + '<strong>';
            res += chart_rates ? pick_rate(fmt, stats, key, mode) :
                                 pick_abs(fmt, stats, key, mode);
            res += '</strong></div>';
        }
    }
    return res == '' ? '' : '<div class="box">' + res + '</div>';
}

//
// Rendering side
//

function render_charts() {
    $('.chart').map(function() {
        render_chart($(this));
    });
}

var chart_colors = {full: ['#edc240', '#afd8f8', '#cb4b4b', '#4da74d', '#9440ed', '#666666', '#aaaaaa', 
                           '#7c79c3', '#8e6767', '#67808e', '#e5e4ae', '#4b4a55', '#bba0c1'],
                    node: ['#6ae26a', '#e24545']};

var chart_chrome = {
    series: { lines: { show: true } },
    grid:   { borderWidth: 2, borderColor: "#aaa" },
    xaxis:  { tickColor: "#fff", mode: "time", timezone: "browser" },
    yaxis:  { tickColor: "#eee", min: 0 },
    legend: { show: false }
};

function chart_fill(mode, i) {
    return mode =='node' && i == 0;
}

function render_chart(div) {
    var id = div.attr('id').substring('chart-'.length);
    var rate_mode = div.hasClass('chart-rates');
    var out_data = [];
    var data = chart_data[id]['data'];
    var fmt = chart_data[id]['fmt'];

    var mode = div.hasClass('chart-full') ? 'full': 'node';
    var colors = chart_colors[mode];

    for (var name in data) {
        var series = data[name];
        var samples = series.samples;
        var i = series.ix;
        var d = [];
        for (var j = 1; j < samples.length; j++) {
            var x = samples[j].timestamp;
            var y;
            if (rate_mode) {
                // TODO This doesn't work well if you are looking at
                // stuff in the browser that is finer granularity than
                // the data we have in the DB (and thus we get
                // duplicated entries). Do we care? We should just
                // never allow that...
                y = (samples[j - 1].sample - samples[j].sample) * 1000 /
                    (samples[j - 1].timestamp - samples[j].timestamp);
            }
            else {
                y = samples[j].sample;
            }
            d.push([x, y]);
        }
        out_data.push({data: d, color: colors[i], shadowSize: 0,
                       lines: {show: true, fill: chart_fill(mode, i)}});
    }
    chart_data[id] = {};

    chart_chrome.yaxis.tickFormatter = fmt_y_axis(fmt);
    $.plot(div, out_data, chart_chrome);
}

function fmt_y_axis(fmt) {
    return function (val, axis) {
        // axis.ticks seems to include the bottom value but not the top
        if (axis.max == 1 && axis.ticks.length > 1) {
            var newTicks = [axis.ticks[0]];
            axis.ticks = newTicks;
        }
        return fmt(val, axis.max);
    }
}

function update_rate_options(sammy) {
    var id = sammy.params['id'];
    store_pref('rate-mode-' + id, sammy.params['mode']);
    store_pref('chart-size-' + id, sammy.params['size']);
    store_pref('chart-range', sammy.params['range']);
    partial_update();
}
