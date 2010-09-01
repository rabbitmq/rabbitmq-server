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
    path('#/', {'overview': '/overview'}, 'overview');

    path('#/connections', {'connections': '/connections/'}, 'connections');
    this.get('#/connections/:name', function() {
            render({'connection': '/connections/' + esc(this.params['name'])}, 'connection',
                   '#/connections');
        });
    this.del('#/connections', function() {
            sync_delete(this, '/connections/:name');
            go_to('#/connections');
            return false;
        });

    path('#/channels', {'channels': '/channels'}, 'channels');

    path('#/exchanges', {'exchanges': '/exchanges', 'vhosts': '/vhosts'}, 'exchanges');
    this.get('#/exchanges/:vhost/:name', function() {
            render({'exchange': '/exchanges/' + esc(this.params['vhost']) + '/' + esc(this.params['name'])}, 'exchange',
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

    path('#/queues', {'queues': '/queues', 'vhosts': '/vhosts'}, 'queues');
    this.get('#/queues/:vhost/:name', function() {
            var path = '/queues/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'queue': path,
                    'bindings': path + '/bindings'}, 'queue', '#/queues');
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

    this.post('#/bindings', function() {
            sync_post(this, '/bindings/:vhost/:queue/:exchange');
            update();
            return false;
        });
    this.del('#/bindings', function() {
            sync_delete(this, '/bindings/:vhost/:queue/:exchange/:properties_key');
            update();
            return false;
        });

    path('#/vhosts', {'vhosts': '/vhosts/'}, 'vhosts');
    this.get('#/vhosts/:id', function() {
            render({'vhost': '/vhosts/' + esc(this.params['id'])}, 'vhost',
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

    path('#/users', {'users': '/users/'}, 'users');
    this.get('#/users/:id', function() {
            render({'user': '/users/' + esc(this.params['id']),
                    'permissions': '/permissions/' + esc(this.params['id']),
                    'vhosts': '/vhosts/'}, 'user',
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
    with_reqs(current_reqs, [], function(json) {
            var html = format(current_template, json);
            replace_content('main', html);
            update_status('ok');
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
    var key;
    for (var k in reqs) {
        key = k;
    }
    if (key != undefined) {
        with_req('/api' + reqs[key], function(resp) {
                acc[key] = jQuery.parseJSON(resp.responseText);
                acc['last-modified'] = resp.getResponseHeader('Last-Modified');
                var remainder = {};
                for (var k in reqs) {
                    if (k != key) remainder[k] = reqs[k];
                }
                with_reqs(remainder, acc, fun);
            });
    }
    else {
        fun(acc);
    }
}

function replace_content(id, html) {
    $("#" + id).empty();
    $(html).appendTo("#" + id);
}

function format(template, json) {
    try {
        var tmpl = new EJS({url: 'js/tmpl/' + template + '.ejs'});
        return tmpl.render(json);
    } catch (err) {
        //clearInterval(timer);
        debug(err['name'] + ": " + err['message']);
    }
}

function update_status(status) {
    var text;
    if (status == 'ok')
        text = "Last update: " + new Date();
    else if (status == 'timeout')
        text = "Warning: server reported busy at " + new Date();
    else if (status == 'error')
        text = "Error: could not connect to server at " + new Date();
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
                fun(req);
            }
            else if (req.status == 408) {
                update_status('timeout');
            }
            else if (req.status == 0) {
                update_status('error');
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

function sync_post(sammy, path_template) {
    sync_req('post', sammy, path_template);
}

function sync_req(type, sammy, path_template) {
    var path = fill_path_template(path_template, sammy.params);
    var req = new XMLHttpRequest();
    req.open(type, '/api' + path, false);
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