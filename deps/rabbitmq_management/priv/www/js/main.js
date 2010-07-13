$(document).ready(function() {
    var url = this.location.toString();
    url = url.indexOf('#') == -1 ? '#overview' : url;
    apply_url(url);
    timer = setInterval('update()', 5000);
});

var current_page;
var timer;

function apply_url(url) {
    current_page = url.split('#', 2)[1];
    update();
}

function update() {
    with_req('/json/', function(text) {
            var json = JSON.parse(text);
            var template = eval('template_' + current_page + '();');
            var html = format(template, json);
            replace_content('main', html);
            update_status('ok', json['datetime']);
            $('a').removeClass('selected').unbind().click(function() {
                apply_url(this.href);
            });
            $('a[href="#' + current_page + '"]').addClass('selected');
    });
}

function replace_content(id, html) {
    $("#" + id).empty();
    $(html).appendTo("#" + id);
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

function update_status(status, datetime) {
    var text;
    if (status == 'ok')
        text = "Last update: " + datetime;
    else if (status == 'timeout')
        text = "Warning: server reported busy at " + datetime;
    else if (status == 'error')
        text = "Error: could not connect to server at " + datetime;

    var html = format('<p class="status-{status}">{text}</p>', {status: status,
                                                                text: text});
    replace_content('status', html);
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
            else if (req.status == 408) {
                update_status('timeout', new Date());
            }
            else if (req.status == 0) {
                update_status('error', new Date());
            }
            else {
                alert("Got response code " + req.status);
                clearInterval(timer);
            }
        }
    };
    req.send(null);
}


function debug(str) {
    $('<p>' + str + '</p>').appendTo('#debug');
}