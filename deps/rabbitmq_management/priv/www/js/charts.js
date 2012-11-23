function render_charts() {
    $('.chart').map(function() {
        render_chart($(this));
    });
}

function render_chart(div) {
    var chrome = {
        series: { lines: { show: true } },
        grid:   { borderWidth: 2, borderColor: "#aaa" },
        xaxis:  { tickColor: "#fff", mode: "time" },
        yaxis:  { tickColor: "#eee", min: 0 },
        legend: { position: 'se', backgroundOpacity: 0.5 }
    };

    var data = [];
    for (var name in chart_data) {
        var samples = chart_data[name].samples;
        var d = [];
        for (var i = 1; i < samples.length; i++) {
            var x = samples[i].timestamp;
            var y = (samples[i - 1].sample - samples[i].sample) * 1000 /
                (samples[i - 1].timestamp - samples[i].timestamp);
            d.push([x, y]);
        }
        data.push({label: name + " (" + chart_data[name].rate + " msg/s)",
                   data: d});
    }
    chart_data = {};

    $.plot(div, data, chrome);
}

function update_rate_options(sammy) {
    store_pref('rate-mode', sammy.params['mode']);
    store_pref('chart-size', sammy.params['size']);
    partial_update();
}
