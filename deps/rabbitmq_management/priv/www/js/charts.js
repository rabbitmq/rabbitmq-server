function render_charts() {
    $('.chart').map(function() {
        render_chart($(this));
    });
}

var chart_colors = ['#edc240', '#afd8f8', '#cb4b4b', '#4da74d', '#9440ed', '#666666', '#aaaaaa'];

var chart_chrome = {
    series: { lines: { show: true } },
    grid:   { borderWidth: 2, borderColor: "#aaa" },
    xaxis:  { tickColor: "#fff", mode: "time", timezone: "browser" },
    yaxis:  { tickColor: "#eee", min: 0 },
    legend: { show: false }
};

function render_chart(div) {
    var id = div.attr('id').substring('chart-'.length);
    var rate_mode = div.hasClass('chart-rates');

    var out_data = [];
    var i = 0;
    var data = chart_data[id]['data'];
    var fmt = chart_data[id]['fmt'];
    for (var name in data) {
        var series = data[name];
        var samples = series.samples;
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
        out_data.push({data: d, color: chart_colors[i], shadowSize: 0});
        i++;
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
    store_pref('chart-range-' + id, sammy.params['range']);
    partial_update();
}
