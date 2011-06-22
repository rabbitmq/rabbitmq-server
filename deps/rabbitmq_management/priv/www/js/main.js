var statistics_level;
var user_administrator;
var nodes_interesting;
var vhosts_interesting;

$(document).ready(function() {
    statistics_level = JSON.parse(sync_get('/overview')).statistics_level;
    var user = JSON.parse(sync_get('/whoami'));
    replace_content('login', '<p>User: <b>' + user.name + '</b></p>');
    var tags = user.tags.split(",");
    user_administrator = jQuery.inArray("administrator", tags) != -1;
    nodes_interesting = user_administrator &&
        JSON.parse(sync_get('/nodes')).length > 1;
    vhosts_interesting = JSON.parse(sync_get('/vhosts')).length > 1;
    setup_constant_events();
    current_vhost = get_pref('vhost');
    update_vhosts();
    app.run();
    update_interval();
    var url = this.location.toString();
    if (url.indexOf('#') == -1) {
        this.location = url + '#/';
    }
});

function setup_constant_events() {
    $('#update-every').change(function() {
            var interval = $(this).val();
            store_pref('interval', interval);
            if (interval == '')
                interval = null;
            else
                interval = parseInt(interval);
            set_timer_interval(interval);
        });
    $('#show-vhost').change(function() {
            current_vhost = $(this).val();
            store_pref('vhost', current_vhost);
            update();
        });
    if (!vhosts_interesting) {
        $('#vhost-form').hide();
    }
}

function update_vhosts() {
    var vhosts = JSON.parse(sync_get('/vhosts'));
    vhosts_interesting = vhosts.length > 1;
    if (vhosts_interesting)
        $('#vhost-form').show();
    else
        $('#vhost-form').hide();
    var select = $('#show-vhost').get(0);
    select.options.length = vhosts.length + 1;
    var index = 0;
    for (var i = 0; i < vhosts.length; i++) {
        var vhost = vhosts[i].name;
        select.options[i + 1] = new Option(vhost, vhost);
        if (vhost == current_vhost) index = i + 1;
    }
    select.selectedIndex = index;
    current_vhost = select.options[index].value;
    store_pref('vhost', current_vhost);
}

function update_interval() {
    var intervalStr = get_pref('interval');
    var interval;

    if (intervalStr == null)    interval = 5000;
    else if (intervalStr == '') interval = null;
    else                        interval = parseInt(intervalStr);

    if (isNaN(interval)) interval = null; // Prevent DoS if cookie malformed

    set_timer_interval(interval);

    var select = $('#update-every').get(0);
    var opts = select.options;
    for (var i = 0; i < opts.length; i++) {
        if (opts[i].value == intervalStr) {
            select.selectedIndex = i;
            break;
        }
    }
}

var app = $.sammy(dispatcher);
function dispatcher() {
    var sammy = this;
    function path(p, r, t) {
        sammy.get(p, function() {
                render(r, t, p);
            });
    }
    this.get('#/', function() {
            var reqs = {'overview': '/overview'};
            if (user_administrator) {
                reqs['nodes'] = '/nodes';
            }
            render(reqs, 'overview', '#/');
        });
    this.get('#/nodes/:name', function() {
            var name = esc(this.params['name']);
            render({'node': '/nodes/' + name},
                   'node', '');
        });

    path('#/connections', {'connections': '/connections'}, 'connections');
    this.get('#/connections/:name', function() {
            var name = esc(this.params['name']);
            render({'connection': '/connections/' + name,
                    'channels': '/connections/' + name + '/channels'},
                'connection', '#/connections');
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
            var path = '/exchanges/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'exchange': path,
                    'bindings_source': path + '/bindings/source',
                    'bindings_destination': path + '/bindings/destination'},
                'exchange', '#/exchanges');
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
    this.post('#/exchanges/publish', function() {
            publish_msg(this.params);
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
            if (this.params['mode'] == 'delete') {
                if (sync_delete(this, '/queues/:vhost/:name'))
                    go_to('#/queues');
            }
            else if (this.params['mode'] == 'purge') {
                if (sync_delete(this, '/queues/:vhost/:name/contents')) {
                    show_popup('info', "Queue purged");
                    update_partial();
                }
            }
            return false;
        });
    this.post('#/queues/get', function() {
            get_msgs(this.params);
            return false;
        });
    this.put('#/mirrors', function() {
            if (sync_put(this, '/queues/:vhost/:queue/mirrors/:node')) {
                show_popup('info', "Mirror added");
                //update();
            }
            return false;
        });
    this.del('#/mirrors', function() {
            if (sync_delete(this, '/queues/:vhost/:queue/mirrors/:node')) {
                show_popup('info', "Mirror removed");
                //update();
            }
            return false;
        });
    this.post('#/bindings', function() {
            if (sync_post(this, '/bindings/:vhost/e/:source/:destination_type/:destination'))
                update();
            return false;
        });
    this.del('#/bindings', function() {
            if (sync_delete(this, '/bindings/:vhost/e/:source/:destination_type/:destination/:properties_key'))
                update();
            return false;
        });

    path('#/vhosts', {'vhosts': '/vhosts', 'permissions': '/permissions'}, 'vhosts');
    this.get('#/vhosts/:id', function() {
            render({'vhost': '/vhosts/' + esc(this.params['id']),
                    'permissions': '/vhosts/' + esc(this.params['id']) + '/permissions',
                    'users': '/users/'},
                'vhost', '#/vhosts');
        });
    this.put('#/vhosts', function() {
            if (sync_put(this, '/vhosts/:name')) {
                update_vhosts();
                update();
            }
            return false;
        });
    this.del('#/vhosts', function() {
            if (sync_delete(this, '/vhosts/:name')) {
                update_vhosts();
                go_to('#/vhosts');
            }
            return false;
        });

    path('#/users', {'users': '/users', 'permissions': '/permissions'}, 'users');
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
var current_vhost = '';
var current_sort;
var current_sort_reverse = false;
var timer;
var timer_interval;

function set_timer_interval(interval) {
    timer_interval = interval;
    reset_timer();
}

function reset_timer() {
    clearInterval(timer);
    if (timer_interval != null) {
        timer = setInterval('partial_update()', timer_interval);
    }
}

function render(reqs, template, highlight) {
    current_template = template;
    current_reqs = reqs;
    current_highlight = highlight;
    update();
}

function update() {
    clearInterval(timer);
    with_update(function(html) {
            replace_content('main', html);
            postprocess();
            postprocess_partial();
            maybe_scroll();
            reset_timer();
        });
}

var update_counter = 0;

function partial_update() {
    if ($('.updatable').length > 0) {
        if (update_counter >= 200) {
            update_counter = 0;
            full_refresh();
            return;
        }
        with_update(function(html) {
            update_counter++;
            replace_content('scratch', html);
            var befores = $('#main .updatable');
            var afters = $('#scratch .updatable');
            if (befores.length != afters.length) {
                throw("before/after mismatch");
            }
            for (var i = 0; i < befores.length; i++) {
                $(befores[i]).replaceWith(afters[i]);
            }
            replace_content('scratch', '');
            postprocess_partial();
        });
    }
}

function full_refresh() {
    store_pref('position', x_position() + ',' + y_position());
    location.reload();
}

function maybe_scroll() {
    var pos = get_pref('position');
    if (pos) {
        clear_pref('position');
        var xy = pos.split(",");
        window.scrollTo(parseInt(xy[0]), parseInt(xy[1]));
    }
}

function x_position() {
    return window.pageXOffset ?
        window.pageXOffset :
        document.documentElement.scrollLeft ?
        document.documentElement.scrollLeft :
        document.body.scrollLeft;
}

function y_position() {
    return window.pageYOffset ?
        window.pageYOffset :
        document.documentElement.scrollTop ?
        document.documentElement.scrollTop :
        document.body.scrollTop;
}

function with_update(fun) {
    with_reqs(apply_state(current_reqs), [], function(json) {
            json.statistics_level = statistics_level;
            var html = format(current_template, json);
            fun(html);
            update_status('ok');
        });
}

var VHOST_QUERIES = map(['/queues', '/exchanges']);
var SORT_QUERIES  = map(['/connections', '/channels', '/vhosts', '/users',
                         '/queues', '/exchanges']);

function map(list) {
    var res = {};
    for (i in list) {
        res[list[i]] = '';
    }
    return res;
}

function apply_state(reqs) {
    var reqs2 = {};
    for (k in reqs) {
        var req = reqs[k];
        var req2;
        if (req in VHOST_QUERIES && current_vhost != '') {
            req2 = req + '/' + esc(current_vhost);
        }
        else {
            req2 = req;
        }

        var qs = '';
        if (req in SORT_QUERIES && current_sort != null) {
            qs = '?sort=' + current_sort +
                '&sort_reverse=' + current_sort_reverse;
        }

        reqs2[k] = req2 + qs;
    }
    return reqs2;
}

function show_popup(type, text) {
    var cssClass = '.form-popup-' + type;
    function hide() {
        $(cssClass).slideUp(200, function() {
                $(this).remove();
            });
    }

    hide();
    $('h1').after(format('error-popup', {'type': type, 'text': text}));
    $(cssClass).center().slideDown(200);
    $(cssClass + ' span').click(hide);
}

function postprocess() {
    $('a').removeClass('selected');
    $('a[href="' + current_highlight + '"]').addClass('selected');
    $('form.confirm').submit(function() {
            return confirm("Are you sure? This object cannot be recovered " +
                           "after deletion.");
        });
    $('div.section h2, div.section-hidden h2').click(function() {
            toggle_visibility($(this));
        });
    $('label').map(function() {
            if ($(this).attr('for') == '') {
                var id = 'auto-label-' + Math.floor(Math.random()*1000000000);
                var input = $(this).parents('tr').first().find('input, select');
                if (input.attr('id') == '') {
                    $(this).attr('for', id);
                    input.attr('id', id);
                }
            }
        });
    $('#download-configuration').click(function() {
            var path = 'api/all-configuration?download=' +
                esc($('#download-filename').val());
            window.location = path;
            setTimeout('app.run()');
            return false;
        });
    $('input, select').die();
    $('.multifield input').live('blur', function() {
            update_multifields();
        });
    $('.controls-appearance').change(function() {
        var controls = $(this).attr('controls');
        if ($(this).val() == 'true') {
            $('#' + controls + '-yes').slideDown(100);
            $('#' + controls + '-no').slideUp(100);
        } else {
            $('#' + controls + '-yes').slideUp(100);
            $('#' + controls + '-no').slideDown(100);
        }
    });
    setup_visibility();
    $('.help').die().live('click', function() {
        help($(this).attr('id'))
    });
    $('input, select').live('focus', function() {
        update_counter = 0; // If there's interaction, reset the counter.
    });
    if (! user_administrator) {
        $('.administrator-only').remove();
    }
    update_multifields();
}

function postprocess_partial() {
    $('.sort').click(function() {
            var sort = $(this).attr('sort');
            if (current_sort == sort) {
                current_sort_reverse = ! current_sort_reverse;
            }
            else {
                current_sort = sort;
                current_sort_reverse = false;
            }
            update();
        });
    $('.help').html('(?)');
}

function update_multifields() {
    $('.multifield').each(function(index) {
            var largest_id = 0;
            var empty_found = false;
            var name = $(this).attr('id');
            $('#' + name + ' input[name$="_mfkey"]').each(function(index) {
                    var match = $(this).attr('name').
                        match(/[a-z]*_([0-9]*)_mfkey/);
                    var id = parseInt(match[1]);
                    largest_id = Math.max(id, largest_id);
                    var key = $(this).val();
                    var value = $(this).next('input').val();
                    if (key == '' && value == '') {
                        if (empty_found) {
                            $(this).parent().remove();
                        }
                        else {
                            empty_found = true;
                        }
                    }
                });
            if (!empty_found) {
                $(this).append('<p><input type="text" name="' + name + '_' +
                               (largest_id + 1) +
                               '_mfkey" value=""/> = ' +
                               '<input type="text" name="' + name + '_' +
                               (largest_id + 1) +
                               '_mfvalue" value=""/></p>');
            }
        });
}

function setup_visibility() {
    $('div.section,div.section-hidden').each(function(_index) {
        var pref = section_pref(current_template,
                                $(this).children('h2').text());
        var show = get_pref(pref);
        if (show == null) {
            show = $(this).hasClass('section');
        }
        else {
            show = show == 't';
        }
        if (show) {
            $(this).addClass('section-visible');
        }
        else {
            $(this).addClass('section-invisible');
        }
    });
}

function toggle_visibility(item) {
    var hider = item.next();
    var all = item.parent();
    var pref = section_pref(current_template, item.text());
    item.next().slideToggle(100);
    if (all.hasClass('section-visible')) {
        if (all.hasClass('section'))
            store_pref(pref, 'f');
        else
            clear_pref(pref);
        all.removeClass('section-visible');
        all.addClass('section-invisible');
    }
    else {
        if (all.hasClass('section-hidden'))
            store_pref(pref, 't');
        else
            clear_pref(pref);
        all.removeClass('section-invisible');
        all.addClass('section-visible');
    }
}

function publish_msg(params0) {
    var params = params_magic(params0);
    var path = fill_path_template('/exchanges/:vhost/:name/publish', params);
    params['payload_encoding'] = 'string';
    params['properties'] = {};
    params['properties']['delivery_mode'] = parseInt(params['delivery_mode']);
    if (params['headers'] != '')
        params['properties']['headers'] = params['headers'];
    var props = ['content_type', 'content_encoding', 'priority', 'correlation_id', 'reply_to', 'expiration', 'message_id', 'timestamp', 'type', 'user_id', 'app_id', 'cluster_id'];
    for (var i in props) {
        var p = props[i];
        if (params['props'][p] != '')
            params['properties'][p] = params['props'][p];
    }
    with_req('POST', path, JSON.stringify(params), function(resp) {
            var result = jQuery.parseJSON(resp.responseText);
            if (result.routed) {
                show_popup('info', 'Message published.');
            } else {
                show_popup('warn', 'Message published, but not routed.');
            }
        });
}

function get_msgs(params) {
    var path = fill_path_template('/queues/:vhost/:name/get', params);
    with_req('POST', path, JSON.stringify(params), function(resp) {
            var msgs = jQuery.parseJSON(resp.responseText);
            if (msgs.length == 0) {
                show_popup('info', 'Queue is empty');
            } else {
                $('#msg-wrapper').slideUp(200);
                replace_content('msg-wrapper', format('messages', {'msgs': msgs}));
                $('#msg-wrapper').slideDown(200);
            }
        });
}

function with_reqs(reqs, acc, fun) {
    if (keys(reqs).length > 0) {
        var key = keys(reqs)[0];
        with_req('GET', reqs[key], null, function(resp) {
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
    $("#" + id).html(html);
}

function format(template, json) {
    try {
        var tmpl = new EJS({url: 'js/tmpl/' + template + '.ejs'});
        return tmpl.render(json);
    } catch (err) {
        clearInterval(timer);
        debug(err['name'] + ": " + err['message']);
    }
}

var last_successful_connect;

function update_status(status) {
    var text;
    if (status == 'ok')
        text = "Last update: " + fmt_date(new Date());
    else if (status == 'error') {
        var next_try = new Date(new Date().getTime() + timer_interval);
        text = "Error: could not connect to server since " +
            fmt_date(last_successful_connect) + ".<br/>Will retry at " +
            fmt_date(next_try) + ".";
    }
    else
        throw("Unknown status " + status);

    var html = format('status', {status: status, text: text});
    replace_content('status', html);
}

function with_req(method, path, body, fun) {
    var json;
    var req = xmlHttpRequest();
    req.open(method, 'api' + path, true );
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (check_bad_response(req, true)) {
                last_successful_connect = new Date();
                fun(req);
            }
        }
    };
    req.send(body);
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

function sync_req(type, params0, path_template) {
    var params;
    var path;
    try {
        params = params_magic(params0);
        path = fill_path_template(path_template, params);
    } catch (e) {
        show_popup('warn', e);
        return false;
    }
    var req = xmlHttpRequest();
    req.open(type, 'api' + path, false);
    req.setRequestHeader('content-type', 'application/json');
    try {
        if (type == 'GET')
            req.send(null);
        else
            req.send(JSON.stringify(params));
    }
    catch (e) {
        if (e.number == 0x80004004) {
            // 0x80004004 means "Operation aborted."
            // http://support.microsoft.com/kb/186063
            // MSIE6 appears to do this in response to HTTP 204.
        }
    }

    if (check_bad_response(req, false)) {
        if (type == 'GET')
            return req.responseText;
        else
            return true;
    }
    else {
        return false;
    }
}

function check_bad_response(req, full_page_404) {
    // 1223 == 204 - see http://www.enhanceie.com/ie/bugs.asp
    // MSIE7 and 8 appear to do this in response to HTTP 204.
    if ((req.status >= 200 && req.status < 300) || req.status == 1223) {
        return true;
    }
    else if (req.status == 404 && full_page_404) {
        var html = format('404', {});
        replace_content('main', html);
    }
    else if (req.status >= 400 && req.status <= 404) {
        var reason = JSON.parse(req.responseText).reason;
        if (typeof(reason) != 'string') reason = JSON.stringify(reason);
        show_popup('warn', reason);
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
    else {
        debug("Got response code " + req.status + " with body " +
              req.responseText);
        clearInterval(timer);
    }

    return false;
}

function fill_path_template(template, params) {
    var re = /:[a-zA-Z_]*/g;
    return template.replace(re, function(m) {
            var str = esc(params[m.substring(1)]);
            if (str == '') {
                throw(m.substring(1) + " is required");
            }
            return str;
        });
}

function params_magic(params) {
    return check_password(
             add_known_arguments(
               maybe_remove_fields(
                 collapse_multifields(params))));
}

function collapse_multifields(params0) {
    var params = {};
    for (key in params0) {
        var match = key.match(/([a-z]*)_([0-9]*)_mfkey/);
        var match2 = key.match(/[a-z]*_[0-9]*_mfvalue/);
        if (match == null && match2 == null) {
            params[key] = params0[key];
        }
        else if (match == null) {
            // Do nothing, value is handled below
        }
        else {
            var name = match[1];
            var id = match[2];
            if (params[name] == undefined) {
                params[name] = {};
            }
            if (params0[key] != "") {
                var k = params0[key];
                var v = params0[name + '_' + id + '_mfvalue'];
                params[name][k] = v;
            }
        }
    }
    return params;
}

KNOWN_ARGS = {'alternate-exchange': {'short': 'AE',  'type': 'string'},
              'x-message-ttl':      {'short': 'TTL', 'type': 'int'},
              'x-expires':          {'short': 'Exp', 'type': 'int'},
              'x-mirror':           {'short': 'M',   'type': 'array'}};

IMPLICIT_ARGS = {'durable':         {'short': 'D',   'type': 'boolean'},
                 'auto-delete':     {'short': 'AD',  'type': 'boolean'},
                 'internal':        {'short': 'I',   'type': 'boolean'}};

ALL_ARGS = {};
for (var k in KNOWN_ARGS)    ALL_ARGS[k] = KNOWN_ARGS[k];
for (var k in IMPLICIT_ARGS) ALL_ARGS[k] = IMPLICIT_ARGS[k];

function add_known_arguments(params) {
    for (var k in KNOWN_ARGS) {
        var v = params[k];
        if (v != undefined && v != '') {
            var type = KNOWN_ARGS[k].type;
            if (type == 'int') {
                v = parseInt(v);
            }
            else if (type == 'array' && typeof(v) == 'string') {
                v = v.split(' ');
            }
            params.arguments[k] = v;
        }
        delete params[k];
    }

    return params;
}

function check_password(params) {
    if (params['password'] != undefined) {
        if (params['password'] != params['password_confirm']) {
            throw("Passwords do not match.");
        }
        delete params['password_confirm'];
    }

    return params;
}

function maybe_remove_fields(params) {
    $('.controls-appearance').each(function(index) {
        if ($(this).val() == 'false') {
            delete params[$(this).attr('param-name')];
            delete params[$(this).attr('name')];
        }
    });
    return params;
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