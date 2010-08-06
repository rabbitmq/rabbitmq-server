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
    var [reqs, template] = dispatch(current_page);

    with_reqs(reqs, [], function(jsons) {
            var json = merge(jsons, template);
            var html = format(template, json);
            replace_content('main', html);
            update_status('ok', json['datetime']);
            $('a').removeClass('selected').unbind().click(function() {
                apply_url(this.href);
            });
            $('a[href="#' + current_page + '"]').addClass('selected');
        });
}

function with_reqs(reqs, acc, fun) {
    if (reqs.length > 0) {
        // alert(reqs[0]);
        with_req('/json' + reqs[0], function(text) {
                acc.push(jQuery.parseJSON(text));
                reqs.shift();
                with_reqs(reqs, acc, fun);
            });
    }
    else {
        fun(acc);
    }
}

function merge(jsons, template) {
    if (template == 'channels') {
        var stats0 = jsons[0].stats;
        var stats1 = jsons[1].stats;
        for (var i = 0; i < stats0.length; i++) {
            for (var j = 0; j < stats1.length; j++) {
                if (stats0[i].channel == stats1[j].channel) {
                    for (var key in stats1[j].msg_stats) {
                        stats0[i].msg_stats[key] = stats1[j].msg_stats[key];
                    }
                    stats1[j].used = true;
                }
            }
        }
        for (var j = 0; j < stats1.length; j++) {
            if (stats1[j].used == undefined) {
                stats0.push(stats1[j]);
            }
        }
    }

    return jsons[0];
}

function dispatch(page) {
    if (page == 'channel') {
        return [['/stats/channel_queue_stats/?group_by=channel',
                 '/stats/channel_exchange_stats/?group_by=channel'],
                'channels'];
    }
    else if (page == 'connection') {
        return [['/connection/'], 'connections'];
    }
    else if (page == 'queue') {
        return [['/queue/'], 'queues'];
    }
    else {
        return [['/' + page], current_page.split('/', 1)];
    }
}

function replace_content(id, html) {
    $("#" + id).empty();
    $(html).appendTo("#" + id);
}

function format(template, json) {
    try {
        var tmpl = new EJS({url: '/js/tmpl/' + template + '.ejs'});
        return tmpl.render(json);
    } catch (err) {
        clearInterval(timer);
        alert(err['name'] + ": " + err['message']);
    }
}

function update_status(status, datetime) {
    var text;
    if (status == 'ok')
        text = "Last update: " + datetime;
    else if (status == 'timeout')
        text = "Warning: server reported busy at " + datetime;
    else if (status == 'error')
        text = "Error: could not connect to server at " + datetime;

    var html = format('status', {status: status, text: text});
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
            else if (req.status == 404) {
                var html = format('404', {});
                replace_content('main', html);
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