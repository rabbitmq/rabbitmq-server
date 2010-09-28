var statistics_level;

$(document).ready(function() {
    var overview = JSON.parse(sync_get('/overview'));
    statistics_level = overview.statistics_level;
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
    path('#/', {'overview': '/overview', 'applications': '/applications'}, 'overview');

    path('#/connections', {'connections': '/connections/'}, 'connections');
    this.get('#/connections/:name', function() {
            render({'connection': '/connections/' + esc(this.params['name'])}, 'connection',
                   '#/connections');
        });
    this.del('#/connections', function() {
            if (sync_delete(this, '/connections/:name'))
                go_to('#/connections');
            return false;
        });

    path('#/channels', {'channels': '/channels'}, 'channels');
    this.get('#/channels/:name', function() {
            render({'channel': '/channels/' + esc(this.params['name'])}, 'channel',
                   '#/channels');
        });

    path('#/exchanges', {'exchanges': '/exchanges', 'vhosts': '/vhosts'}, 'exchanges');
    this.get('#/exchanges/:vhost/:name', function() {
            render({'exchange': '/exchanges/' + esc(this.params['vhost']) + '/' + esc(this.params['name'])}, 'exchange',
                   '#/exchanges');
        });
    this.put('#/exchanges', function() {
            if (sync_put(this, '/exchanges/:vhost/:name'))
                update();
            return false;
        });
    this.del('#/exchanges', function() {
            if (sync_delete(this, '/exchanges/:vhost/:name'))
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
            if (sync_put(this, '/queues/:vhost/:name'))
                update();
            return false;
        });
    this.del('#/queues', function() {
            if (sync_delete(this, '/queues/:vhost/:name'))
                go_to('#/queues');
            return false;
        });

    this.post('#/bindings', function() {
            if (sync_post(this, '/bindings/:vhost/:queue/:exchange'))
                update();
            return false;
        });
    this.del('#/bindings', function() {
            if (sync_delete(this, '/bindings/:vhost/:queue/:exchange/:properties_key'))
                update();
            return false;
        });

    path('#/vhosts', {'vhosts': '/vhosts/'}, 'vhosts');
    this.get('#/vhosts/:id', function() {
            render({'vhost': '/vhosts/' + esc(this.params['id'])}, 'vhost',
                   '#/vhosts');
        });
    this.put('#/vhosts', function() {
            if (sync_put(this, '/vhosts/:name'))
                update();
            return false;
        });
    this.del('#/vhosts', function() {
            if (sync_delete(this, '/vhosts/:name'))
                go_to('#/vhosts');
            return false;
        });

    path('#/users', {'users': '/users/'}, 'users');
    this.get('#/users/:id', function() {
            render({'user': '/users/' + esc(this.params['id']),
                    'permissions': '/users/' + esc(this.params['id']) + '/permissions',
                    'vhosts': '/vhosts/'}, 'user',
                   '#/users');
        });
    this.put('#/users', function() {
            if (sync_put(this, '/users/:username'))
                update();
            return false;
        });
    this.del('#/users', function() {
            if (sync_delete(this, '/users/:username'))
                go_to('#/users');
            return false;
        });

    this.put('#/permissions', function() {
            if (sync_put(this, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    this.del('#/permissions', function() {
            if (sync_delete(this, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    this.get('#/import-succeeded', function() {
            render({}, 'import-succeeded', '#/overview');
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
            json.statistics_level = statistics_level;
            var html = format(current_template, json);
            replace_content('main', html);
            update_status('ok');
            postprocess();
            //timer = setInterval('update()', 5000);
        });
}

function error_popup(text) {
    $('body').prepend('<div class="error-message">' + text + '</div>');
    $('.error-message').center().fadeOut(10000)
        .click( function() { $(this).stop().fadeOut('fast') } );
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
            $(this).next().slideToggle(100);
            $(this).toggleClass("toggled");
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
    if (keys(reqs).length > 0) {
        var key = keys(reqs)[0];
        with_req('/api' + reqs[key], function(resp) {
                acc[key] = jQuery.parseJSON(resp.responseText);
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
    var req = xmlHttpRequest();
    req.open( "GET", path, true );
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (req.status == 200) {
                fun(req);
            }
            else if (req.status == 408) {
                update_status('timeout');
            }
            else if (req.status == 0) { // Non-MSIE: could not connect
                update_status('error');
            }
            else if (req.status > 12000) { // MSIE: could not connect
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

function sync_get(path) {
    return sync_req('GET', [], path);
}

function sync_put(sammy, path_template) {
    return sync_req('PUT', sammy.params, path_template);
}

function sync_delete(sammy, path_template) {
    return sync_req('DELETE', sammy.params, path_template);
}

function sync_post(sammy, path_template) {
    return sync_req('POST', sammy.params, path_template);
}

function sync_req(type, params, path_template) {
    var path = fill_path_template(path_template, params);
    var req = xmlHttpRequest();
    req.open(type, '/api' + path, false);
    req.setRequestHeader('content-type', 'application/json');
    try {
        if (params["arguments"] == "") params["arguments"] = []; // TODO
        req.send(JSON.stringify(params));
    }
    catch (e) {
        if (e.number == 0x80004004) {
            // 0x80004004 means "Operation aborted."
            // http://support.microsoft.com/kb/186063
            // MSIE6 appears to do this in response to HTTP 204.
        }
    }

    if (req.status == 400) {
        error_popup(JSON.stringify(JSON.parse(req.responseText).reason));
        return false;
    }

    // 1223 == 204 - see http://www.enhanceie.com/ie/bugs.asp
    // MSIE7 and 8 appear to do this in response to HTTP 204.
    if (req.status >= 400 && req.status != 1223) {
        debug("Got response code " + req.status + " with body " +
              req.responseText);
    }

    if (type == 'GET')
        return req.responseText;
    else
        return true;
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

function keys(obj) {
    var ks = [];
    for (var k in obj) {
        ks.push(k);
    }
    return ks;
}

// Don't use the jQuery AJAX support, it seemss to have trouble reporting
// server-down type errors.
function xmlHttpRequest() {
    var res;
    try {
        res = new XMLHttpRequest();
    }
    catch(e) {
        res = new ActiveXObject("Microsoft.XMLHttp");
    }
    return res;
}

(function($){
    $.fn.extend({
        center: function () {
            return this.each(function() {
                var top = ($(window).height() - $(this).outerHeight()) / 2;
                var left = ($(window).width() - $(this).outerWidth()) / 2;
                $(this).css({margin:0, top: (top > 0 ? top : 0)+'px', left: (left > 0 ? left : 0)+'px'});
            });
        }
    });
})(jQuery);
