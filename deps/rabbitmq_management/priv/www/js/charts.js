function render_charts() {
    $('.chart').map(function() {
        render_chart($(this));
    });
}

function render_chart(div) {
    var id = div.attr('id').substring('chart-'.length);
    var rate_mode = div.hasClass('chart-rates');

    var chrome = {
        series: { lines: { show: true } },
        grid:   { borderWidth: 2, borderColor: "#aaa" },
        xaxis:  { tickColor: "#fff", mode: "time" },
        yaxis:  { tickColor: "#eee", min: 0 },
        legend: { position: 'se', backgroundOpacity: 0.5 }
    };

    var out_data = [];
    for (var name in chart_data[id]) {
        var data = chart_data[id][name];
        var samples = data.samples;
        var d = [];
        for (var i = 1; i < samples.length; i++) {
            var x = samples[i].timestamp;
            var y;
            if (rate_mode) {
                y = (samples[i - 1].sample - samples[i].sample) * 1000 /
                    (samples[i - 1].timestamp - samples[i].timestamp);
            }
            else {
                y = samples[i].sample;
            }
            d.push([x, y]);
        }
        var suffix;
        if (rate_mode) {
            suffix = " (" + data.rate + " msg/s)";
        }
        else {
            suffix = " (" + samples[0].sample + " msg)";
        }
        out_data.push({label: name + suffix, data: d});
    }
    chart_data[id] = {};

    $.plot(div, out_data, chrome);
}

function update_rate_options(sammy) {
    var id = sammy.params['id'];
    store_pref('rate-mode-' + id, sammy.params['mode']);
    store_pref('chart-size-' + id, sammy.params['size']);
    partial_update();
}
