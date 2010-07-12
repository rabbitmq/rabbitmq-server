$(document).ready(function() {
    update();
    timer = setInterval("update()", 5000);
});

var timer;

function update() {
    with_req('/json/', function(text) {
            var html = format(template_main(), JSON.parse(text));
            $("#main").empty();
            $(html).appendTo("#main");
    });
}

function format(template, json) {
    try {
        var tmpl = jsontemplate.Template(template,
                                         {more_formatters: more_formatters});
        return tmpl.expand(json);
    } catch (err) {
        alert(err['name'] + ": " + err['message']);
    }
}

function more_formatters(name) {
    if (name == 'bytes')
        return format_bytes;
    else
        return null;
}

function format_bytes(bytes) {
    function f(n, p) {
        if (n > 1024) return f(n / 1024, p + 1);
        else return [n, p];
    }

    [num, power] = f(bytes, 0);
    var powers = ['B', 'kB', 'MB', 'GB', 'TB'];
    return (power == 0 ? num : num.toFixed(1)) + powers[power];
}

function with_req(path, fun) {
    var json;
    var req = new XMLHttpRequest();
    req.open( "GET", path, true );
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (req.status == 200) {
                fun(req.responseText);
            }
            else if (req.status == 0) {
                clearInterval(timer);
            }
            else {
                alert("Got response code " + req.status);
                clearInterval(timer);
            }
        }
    };
    req.send(null);
}
