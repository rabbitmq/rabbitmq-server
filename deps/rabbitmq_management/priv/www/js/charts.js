function render_charts() {
    $('.chart').map(function() {
        render_chart($(this));
    });
}

var chart_colors = ['#edc240', '#afd8f8', '#cb4b4b', '#4da74d', '#9440ed', '#666666', '#aaaaaa'];

function render_chart(div) {
    var id = div.attr('id').substring('chart-'.length);
    var rate_mode = div.hasClass('chart-rates');

    var chrome = {
        series: { lines: { show: true } },
        grid:   { borderWidth: 2, borderColor: "#aaa" },
        xaxis:  { tickColor: "#fff", mode: "time" },
        yaxis:  { tickColor: "#eee", min: 0 },
        legend: { show: false }
    };

    var out_data = [];
    var i = 0;
    for (var name in chart_data[id]) {
        var data = chart_data[id][name];
        var samples = data.samples;
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
        out_data.push({data: d, color: chart_colors[i]});
        i++;
    }
    chart_data[id] = {};

    $.plot(div, out_data, chrome);
}

function update_rate_options(sammy) {
    var id = sammy.params['id'];
    var range = sammy.params['range'];
    var r = range.split('|');
    current_range_age = r[0];
    current_range_incr = r[1];

    store_pref('rate-mode-' + id, sammy.params['mode']);
    store_pref('chart-size-' + id, sammy.params['size']);
    store_pref('chart-range', range);
    partial_update();
}
