$(document).ready(function() {
    app.run();
    var url = this.location.toString();
    if (url.indexOf('#') == -1) {
        this.location = url + '#/';
    }
});

var app = $.sammy(dispatcher);
function dispatcher() {
    var sammy = this;
    function path(p, r, t) {
        sammy.get(p, function() {
                render(r, t, p);
            });
    }
    path('#/', ['/overview'], 'overview');

    path('#/connections', ['/connections/'], 'connections');
    this.get('#/connections/:name', function() {
            render(['/connections/' + esc(this.params['name'])], 'connection',
                   '#/connections');
        });
    this.del('#/connections', function() {
            sync_delete(this, '/connections/:name');
            go_to('#/connections');
            return false;
        });

    path('#/channels', ['/channels'], 'channels');

    path('#/exchanges', ['/exchanges', '/vhosts'], 'exchanges');
    this.get('#/exchanges/:vhost/:name', function() {
            render(['/exchanges/' + esc(this.params['vhost']) + '/' + esc(this.params['name'])], 'exchange',
                   '#/exchanges');
        });
    this.put('#/exchanges', function() {
            sync_put(this, '/exchanges/:vhost/:name');
            update();
            return false;
        });
    this.del('#/exchanges', function() {
            sync_delete(this, '/exchanges/:vhost/:name');
            go_to('#/exchanges');
            return false;
        });

    path('#/queues', ['/queues', '/vhosts'], 'queues');
    this.get('#/queues/:vhost/:name', function() {
            render(['/queues/' + esc(this.params['vhost']) + '/' + esc(this.params['name'])], 'queue',
                   '#/queues');
        });
    this.put('#/queues', function() {
            sync_put(this, '/queues/:vhost/:name');
            update();
            return false;
        });
    this.del('#/queues', function() {
            sync_delete(this, '/queues/:vhost/:name');
            go_to('#/queues');
            return false;
        });

    path('#/vhosts', ['/vhosts/'], 'vhosts');
    this.get('#/vhosts/:id', function() {
            render(['/vhosts/' + esc(this.params['id'])], 'vhost',
                   '#/vhosts');
        });
    this.put('#/vhosts', function() {
            sync_put(this, '/vhosts/:name');
            update();
            return false;
        });
    this.del('#/vhosts', function() {
            sync_delete(this, '/vhosts/:name');
            go_to('#/vhosts');
            return false;
        });

    path('#/users', ['/users/'], 'users');
    this.get('#/users/:id', function() {
            render(['/users/' + esc(this.params['id']),
                    '/permissions/' + esc(this.params['id']),
                    '/vhosts/'], 'user',
                   '#/users');
        });
    this.put('#/users', function() {
            sync_put(this, '/users/:username');
            update();
            return false;
        });
    this.del('#/users', function() {
            sync_delete(this, '/users/:username');
            go_to('#/users');
            return false;
        });

    this.put('#/permissions', function() {
            sync_put(this, '/permissions/:username/:vhost');
            update();
            return false;
        });
    this.del('#/permissions', function() {
            sync_delete(this, '/permissions/:username/:vhost');
            update();
            return false;
        });
}

function go_to(url) {
    this.location = url;
}

var current_template;
var current_reqs;
var current_highlight;
//var timer;

function render(reqs, template, highlight) {
    current_template = template;
    current_reqs = reqs;
    current_highlight = highlight;
    update();
}

function update() {
    //clearInterval(timer);
    with_reqs(current_reqs, [], function(jsons) {
            var json = merge(jsons, current_template);
            var html = format(current_template, json);
            replace_content('main', html);
            update_status('ok', json['datetime']);
            postprocess();
            //timer = setInterval('update()', 5000);
        });
}

function postprocess() {
    $('a').removeClass('selected');
    $('a[href="' + current_highlight + '"]').addClass('selected');
    // $('input').focus(function() {
    //         clearInterval(timer);
    //         update_status('paused');
    //     });
    $('form.confirm').submit(function() {
            return confirm("Are you sure? This object cannot be recovered " +
                           "after deletion.");
        });
    $('div.section h2, div.section-hidden h2').click(function() {
            $(this).next().toggle(100);
        });
    $('label').map(function() {
            if ($(this).attr('for') == '') {
                var id = 'auto-label-' + Math.floor(Math.random()*1000000000);
                $(this).attr('for', id);
                var input = $(this).parents('tr').first().find('input, select');
                input.attr('id', id);
            }
        });
}

function with_reqs(reqs, acc, fun) {
    if (reqs.length > 0) {
        with_req('/json' + reqs[0], function(text) {
                acc.push(jQuery.parseJSON(text));
                with_reqs(reqs.slice(1), acc, fun);
            });
    }
    else {
        fun(acc);
    }
}

function merge(jsons, template) {
    for (var i = 1; i < jsons.length; i++) {
        for (var k in jsons[i]) {
            jsons[0][k] = jsons[i][k];
        }
    }

    return jsons[0];
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
        //clearInterval(timer);
        debug(err['name'] + ": " + err['message']);
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
    //else if (status == 'paused')
    //    text = "Updating halted due to form interaction.";

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
                debug("Got response code " + req.status);
                //clearInterval(timer);
            }
        }
    };
    req.send(null);
}

function sync_put(sammy, path_template) {
    sync_req('put', sammy, path_template);
}

function sync_delete(sammy, path_template) {
    sync_req('delete', sammy, path_template);
}

function sync_req(type, sammy, path_template) {
    var path = fill_path_template(path_template, sammy.params);
    var req = new XMLHttpRequest();
    req.open(type, '/json' + path, false);
    req.setRequestHeader('content-type', 'application/json');
    req.send(JSON.stringify(sammy.params)); // TODO make portable

    if (req.status >= 400) {
        debug("Got response code " + req.status + " with body " +
              req.responseText);
    }
}

function fill_path_template(template, params) {
    var re = /:[a-zA-Z_]*/g;
    return template.replace(re, function(m) {
            return esc(params[m.substring(1)]);
        });
}

function debug(str) {
    $('<p>' + str + '</p>').appendTo('#debug');
}