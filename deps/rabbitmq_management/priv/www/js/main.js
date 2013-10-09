$(document).ready(function() {
    replace_content('outer', format('login', {}));
    try_uri_login();
    start_app_login();
});

function dispatcher_add(fun) {
    dispatcher_modules.push(fun);
    if (dispatcher_modules.length == extension_count) {
        start_app();
    }
}

function dispatcher() {
    for (var i in dispatcher_modules) {
        dispatcher_modules[i](this);
    }
}

function try_uri_login() {
    var location = this.location.href;
    location = location.substr(3 + location.indexOf("://"));
    var authority = location.substr(0, location.indexOf("/"));
    if (authority.indexOf("@") !== -1) {
        var userinfo = authority.substr(0, authority.indexOf("@"));
        if (userinfo.split(":").length === 2) {
            uri_auth_used = true;
            set_auth_cookie(decodeURIComponent(userinfo));
        }
    }
}

function set_auth_cookie(userinfo) {
    var b64 = b64_encode_utf8(decodeURIComponent(userinfo));
    document.cookie = 'auth=' + encodeURIComponent(b64);
}

function start_app_login() {
    app = new Sammy.Application(function () {
        this.put('#/login', function() {
            username = this.params['username'];
            password = this.params['password'];
            set_auth_cookie(username + ':' + password);
            check_login();
        });
    });
    app.run();
    if (get_cookie('auth') != '') {
        check_login();
    }
}

function check_login() {
    var user = JSON.parse(sync_get('/whoami'));
    if (user == false) {
        document.cookie = 'auth=; expires=Thu, 01 Jan 1970 00:00:00 GMT';
        replace_content('login-status', '<p>Login failed</p>');
    }
    else {
        replace_content('outer',
                        format('layout', {uri_auth_used: uri_auth_used}));
        setup_global_vars(user);
        setup_constant_events();
        update_vhosts();
        update_interval();
        setup_extensions();
    }
}

function start_app() {
    if (app !== undefined) {
        app.unload();
    }
    // Oh boy. Sammy uses various different methods to determine if
    // the URL hash has changed. Unsurprisingly this is a native event
    // in modern browsers, and falls back to an icky polling function
    // in MSIE. But it looks like there's a bug. The polling function
    // should get installed when the app is started. But it's guarded
    // behind if (Sammy.HashLocationProxy._interval != null). And of
    // course that's not specific to the application; it's pretty
    // global. So we need to manually clear that in order for links to
    // work in MSIE.
    // Filed as https://github.com/quirkey/sammy/issues/171
    //
    // Note for when we upgrade: HashLocationProxy has become
    // DefaultLocationProxy in later versions, but otherwise the issue
    // remains.
    Sammy.HashLocationProxy._interval = null;
    app = new Sammy.Application(dispatcher);
    app.run();
    var url = this.location.toString();
    if (url.indexOf('#') == -1) {
        this.location = url + '#/';
    }
}

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

function setup_extensions() {
    var extensions = JSON.parse(sync_get('/extensions'));
    extension_count = extensions.length;
    for (var i in extensions) {
        var extension = extensions[i];
        dynamic_load(extension.javascript);
    }
}

function dynamic_load(filename) {
    var element = document.createElement('script');
    element.setAttribute('type', 'text/javascript');
    element.setAttribute('src', 'js/' + filename);
    document.getElementsByTagName("head")[0].appendChild(element);
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

function go_to(url) {
    this.location = url;
}

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

function update_manual(div, query) {
    var path;
    var template;
    if (query == 'memory') {
        path = current_reqs['node'] + '?memory=true';
        template = 'memory';
    }
    var data = JSON.parse(sync_get(path));

    replace_content(div, format(template, data));
    postprocess_partial();
}

function render(reqs, template, highlight) {
    current_template = template;
    current_reqs = reqs;
    current_highlight = highlight;
    update();
}

function update() {
    replace_content('debug', '');
    clearInterval(timer);
    with_update(function(html) {
            update_navigation();
            replace_content('main', html);
            postprocess();
            postprocess_partial();
            render_charts();
            maybe_scroll();
            reset_timer();
        });
}

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
                $(befores[i]).empty().append($(afters[i]).contents());
            }
            replace_content('scratch', '');
            postprocess_partial();
            render_charts();
        });
    }
}

function update_navigation() {
    var l1 = '';
    var l2 = '';
    var descend = null;

    for (var k in NAVIGATION) {
        var val = NAVIGATION[k];
        var path = val;
        while (!leaf(path)) {
            path = first_showable_child(path);
        }
        var selected = false;
        if (contains_current_highlight(val)) {
            selected = true;
            if (!leaf(val)) {
                descend = nav(val);
            }
        }
        if (show(path)) {
            l1 += '<li><a href="' + nav(path) + '"' +
                (selected ? ' class="selected"' : '') + '>' + k + '</a></li>';
        }
    }

    if (descend) {
        l2 = obj_to_ul(descend);
        $('#main').addClass('with-rhs');
    }
    else {
        $('#main').removeClass('with-rhs');
    }

    replace_content('tabs', l1);
    replace_content('rhs', l2);
}

function nav(pair) {
    return pair[0];
}

function show(pair) {
    return jQuery.inArray(pair[1], user_tags) != -1;
}

function leaf(pair) {
    return typeof(nav(pair)) == 'string';
}

function first_showable_child(pair) {
    var items = pair[0];
    var ks = keys(items);
    for (var i = 0; i < ks.length; i++) {
        var child = items[ks[i]];
        if (show(child)) return child;
    }
    return items[ks[0]]; // We'll end up not showing it anyway
}

function contains_current_highlight(val) {
    if (leaf(val)) {
        return current_highlight == nav(val);
    }
    else {
        var b = false;
        for (var k in val) {
            b |= contains_current_highlight(val[k]);
        }
        return b;
    }
}

function obj_to_ul(val) {
    var res = '<ul>';
    for (var k in val) {
        var obj = val[k];
        if (show(obj)) {
            res += '<li>';
            if (leaf(obj)) {
                res += '<a href="' + nav(obj) + '"' +
                    (current_highlight == nav(obj) ? ' class="selected"' : '') +
                    '>' + k + '</a>';
            }
            else {
                res += obj_to_ul(nav(obj));
            }
            res += '</li>';
        }
    }
    return res + '</ul>';
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

function apply_state(reqs) {
    var reqs2 = {};
    for (k in reqs) {
        var req = reqs[k];
        var options = {};
        if (typeof(req) == "object") {
            options = req.options;
            req = req.path;
        }
        var req2;
        if (options['vhost'] != undefined && current_vhost != '') {
            req2 = req + '/' + esc(current_vhost);
        }
        else {
            req2 = req;
        }

        var qs = [];
        if (options['sort'] != undefined && current_sort != null) {
            qs.push('sort=' + current_sort);
            qs.push('sort_reverse=' + current_sort_reverse);
        }
        if (options['ranges'] != undefined) {
            for (i in options['ranges']) {
                var type = options['ranges'][i];
                var range = get_pref('chart-range-' + type).split('|');
                var prefix;
                if (type.substring(0, 8) == 'lengths-') {
                    prefix = 'lengths';
                }
                else if (type.substring(0, 10) == 'msg-rates-') {
                    prefix = 'msg_rates';
                }
                else if (type.substring(0, 11) == 'data-rates-') {
                    prefix = 'data_rates';
                }
                qs.push(prefix + '_age=' + parseInt(range[0]));
                qs.push(prefix + '_incr=' + parseInt(range[1]));
            }
        }
        qs = qs.join('&');
        if (qs != '') qs = '?' + qs;

        reqs2[k] = req2 + qs;
    }
    return reqs2;
}

function show_popup(type, text, mode) {
    var cssClass = '.form-popup-' + type;
    function hide() {
        if (mode == 'fade') {
            $(cssClass).fadeOut(200, function() {
                $(this).remove();
            });
        }
        else {
            $(cssClass).slideUp(200, function() {
                $(this).remove();
            });
        }
    }

    hide();
    $('h1').after(format('error-popup', {'type': type, 'text': text}));
    if (mode == 'fade') {
        $(cssClass).fadeIn(200);
    }
    else {
        $(cssClass).center().slideDown(200);
    }
    $(cssClass + ' span').click(function () {
        $('.popup-owner').removeClass('popup-owner');
        hide();
    });
}

function postprocess() {
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
    $('#download-definitions').click(function() {
            var path = 'api/definitions?download=' +
                esc($('#download-filename').val()) +
                '&auth=' + get_cookie('auth');
            window.location = path;
            setTimeout('app.run()');
            return false;
        });
    $('.update-manual').click(function() {
            update_manual($(this).attr('for'), $(this).attr('query'));
        });
    $('input, select').die();
    $('.multifield input').live('keyup', function() {
            update_multifields();
        });
    $('.multifield select').live('change', function() {
            update_multifields();
        });
    $('.controls-appearance').change(function() {
        var controls = $(this).attr('controls-divs');
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
    $('.rate-options').die().live('click', function() {
        var remove = $('.popup-owner').length == 1 &&
                     $('.popup-owner').get(0) == $(this).get(0);
        $('.popup-owner').removeClass('popup-owner');
        if (remove) {
            $('.form-popup-rate-options').fadeOut(200, function() {
                $(this).remove();
            });
        }
        else {
            $(this).addClass('popup-owner');
            show_popup('rate-options', format('rate-options', {span: $(this)}),
                       'fade');
        }
    });
    $('input, select').live('focus', function() {
        update_counter = 0; // If there's interaction, reset the counter.
    });
    $('.tag-link').click(function() {
        $('#tags').val($(this).attr('tag'));
    });
    $('form.auto-submit select, form.auto-submit input').live('click', function(){
        $(this).parents('form').submit();
    });
    $('#filter').die().live('keyup', debounce(update_filter, 500));
    $('#truncate').die().live('keyup', debounce(update_truncate, 500));
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
    // TODO remove this hack when we get rid of "updatable"
    if ($('#filter-warning-show').length > 0) {
        $('#filter-truncate').addClass('filter-warning');
    }
    else {
        $('#filter-truncate').removeClass('filter-warning');
    }
}

function update_multifields() {
    $('div.multifield').each(function(index) {
        update_multifield($(this), true);
    });
}

function update_multifield(multifield, dict) {
    var largest_id = 0;
    var empty_found = false;
    var name = multifield.attr('id');
    $('#' + name + ' *[name$="_mftype"]').each(function(index) {
        var re = new RegExp(name + '_([0-9]*)_mftype');
        var match = $(this).attr('name').match(re);
        if (!match) return;
        var id = parseInt(match[1]);
        largest_id = Math.max(id, largest_id);
        var prefix = name + '_' + id;
        var type = $(this).val();
        var input = $('#' + prefix + '_mfvalue');
        if (type == 'list') {
            if (input.size() == 1) {
                input.replaceWith('<div class="multifield-sub" id="' + prefix +
                                  '"></div>');
            }
            update_multifield($('#' + prefix), false);
        }
        else {
            if (input.size() == 1) {
                var key = dict ? $('#' + prefix + '_mfkey').val() : '';
                var value = input.val();
                if (key == '' && value == '') {
                    if (empty_found) {
                        $(this).parent().remove();
                    }
                    empty_found = true;
                }
            }
            else {
                $('#' + prefix).replaceWith(multifield_input(prefix, 'value',
                                                             'text'));
            }
        }
    });
    if (!empty_found) {
        var prefix = name + '_' + (largest_id + 1);
        var t = multifield.hasClass('string-only') ? 'hidden' : 'select';
        var val_type = multifield_input(prefix, 'value', 'text') + ' ' +
            multifield_input(prefix, 'type', t);

        if (dict) {
            multifield.append('<table><tr><td>' +
                              multifield_input(prefix, 'key', 'text') +
                              '</td><td class="equals"> = </td><td>' +
                              val_type + '</td></tr></table>');
        }
        else {
            multifield.append('<div>' + val_type + '</div>');
        }
    }
}

function multifield_input(prefix, suffix, type) {
    if (type == 'hidden' ) {
        return '<input type="hidden" id="' + prefix + '_mf' + suffix +
            '" name="' + prefix + '_mf' + suffix + '" value="string"/>';
    }
    else if (type == 'text' ) {
        return '<input type="text" id="' + prefix + '_mf' + suffix +
            '" name="' + prefix + '_mf' + suffix + '" value=""/>';
    }
    else if (type == 'select' ) {
        return '<select id="' + prefix + '_mf' + suffix + '" name="' + prefix +
            '_mf' + suffix + '">' +
            '<option value="string">String</option>' +
            '<option value="number">Number</option>' +
            '<option value="boolean">Boolean</option>' +
            '<option value="list">List</option>' +
            '</select>';
    }
}

function update_filter() {
    current_filter = $(this).val();
    var table = $(this).parents('table').first();
    table.removeClass('filter-active');
    if ($(this).val() != '') {
        table.addClass('filter-active');
    }
    partial_update();
}

function update_truncate() {
    var current_truncate_str =
        $(this).val().replace(new RegExp('\\D', 'g'), '');
    if (current_truncate_str == '')
        current_truncate_str = '0';
    if ($(this).val() != current_truncate_str)
        $(this).val(current_truncate_str);
    current_truncate = parseInt(current_truncate_str, 10);
    store_pref('truncate', current_truncate);
    partial_update();
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

var ejs_cached = {};

function format(template, json) {
    try {
        var cache = true;
        if (!(template in ejs_cached)) {
            ejs_cached[template] = true;
            cache = false;
        }
        var tmpl = new EJS({url: 'js/tmpl/' + template + '.ejs', cache: cache});
        return tmpl.render(json);
    } catch (err) {
        clearInterval(timer);
        debug(err['name'] + ": " + err['message']);
    }
}

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

function auth_header() {
    return "Basic " + decodeURIComponent(get_cookie('auth'));
}

function with_req(method, path, body, fun) {
    var json;
    var req = xmlHttpRequest();
    req.open(method, 'api' + path, true );
    req.setRequestHeader('authorization', auth_header());
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

function sync_delete(sammy, path_template, options) {
    return sync_req('DELETE', sammy.params, path_template, options);
}

function sync_post(sammy, path_template) {
    return sync_req('POST', sammy.params, path_template);
}

function sync_req(type, params0, path_template, options) {
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
    req.setRequestHeader('authorization', auth_header());

    if (options != undefined || options != null) {
	if (options.headers != undefined || options.headers != null) {
	    jQuery.each(options.headers, function (k, v) {
		req.setRequestHeader(k, v);
	    });
	}
    }

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
    else if (req.status == 503) { // Proxy: could not connect
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
    function set(x) { return x != '' && x != undefined }

    var params = {};
    var ks = keys(params0);
    var ids = [];
    for (i in ks) {
        var key = ks[i];
        var match = key.match(/([a-z]*)_([0-9_]*)_mftype/);
        var match2 = key.match(/[a-z]*_[0-9_]*_mfkey/);
        var match3 = key.match(/[a-z]*_[0-9_]*_mfvalue/);
        if (match == null && match2 == null && match3 == null) {
            params[key] = params0[key];
        }
        else if (match == null) {
            // Do nothing, value is handled below
        }
        else {
            var name = match[1];
            var id = match[2];
            ids.push([name, id]);
        }
    }
    ids.sort();
    var id_map = {};
    for (i in ids) {
        var name = ids[i][0];
        var id = ids[i][1];
        if (params[name] == undefined) {
            params[name] = {};
            id_map[name] = {};
        }
        var id_parts = id.split('_');
        var k = params0[name + '_' + id_parts[0] + '_mfkey'];
        var v = params0[name + '_' + id + '_mfvalue'];
        var t = params0[name + '_' + id + '_mftype'];
        var val = null;
        if (t == 'list') {
            val = [];
            id_map[name][id] = val;
        }
        else if (set(k) || set(v)) {
            if (t == 'boolean') {
                if (v != 'true' && v != 'false')
                    throw(k + ' must be "true" or "false"; got ' + v);
                val = (v == 'true');
            }
            else if (t == 'number') {
                var n = parseFloat(v);
                if (isNaN(n))
                    throw(k + ' must be a number; got ' + v);
                val = n;
            }
            else {
                val = v;
            }
        }
        if (val != null) {
            if (id_parts.length == 1) {
                params[name][k] = val;
            }
            else {
                var prefix = id_parts.slice(0, id_parts.length - 1).join('_');
                id_map[name][prefix].push(val);
            }
        }
    }
    return params;
}

function add_known_arguments(params) {
    for (var k in KNOWN_ARGS) {
        var v = params[k];
        if (v != undefined && v != '') {
            var type = KNOWN_ARGS[k].type;
            if (type == 'int') {
                v = parseInt(v);
                if (isNaN(v)) {
                    throw(k + " must be an integer.");
                }
            }
            params.arguments[k] = v;
        }
        delete params[k];
    }

    return params;
}

function check_password(params) {
    if (params['password'] != undefined) {
        if (params['password'] == '') {
            throw("Please specify a password.");
        }
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

function put_parameter(sammy, mandatory_keys, num_keys, bool_keys,
                       arrayable_keys) {
    for (var i in sammy.params) {
        if (i === 'length' || !sammy.params.hasOwnProperty(i)) continue;
        if (sammy.params[i] == '' && jQuery.inArray(i, mandatory_keys) == -1) {
            delete sammy.params[i];
        }
        else if (jQuery.inArray(i, num_keys) != -1) {
            sammy.params[i] = parseInt(sammy.params[i]);
        }
        else if (jQuery.inArray(i, bool_keys) != -1) {
            sammy.params[i] = sammy.params[i] == 'true';
        }
        else if (jQuery.inArray(i, arrayable_keys) != -1) {
            sammy.params[i] = sammy.params[i].split(' ');
            if (sammy.params[i].length == 1) {
                sammy.params[i] = sammy.params[i][0];
            }
        }
    }
    var params = {"component": sammy.params.component,
                  "vhost":     sammy.params.vhost,
                  "name":      sammy.params.name,
                  "value":     params_magic(sammy.params)};
    delete params.value.vhost;
    delete params.value.component;
    delete params.value.name;
    sammy.params = params;
    if (sync_put(sammy, '/parameters/:component/:vhost/:name')) update();
}

function put_policy(sammy, mandatory_keys, num_keys, bool_keys) {
    for (var i in sammy.params) {
        if (i === 'length' || !sammy.params.hasOwnProperty(i)) continue;
        if (sammy.params[i] == '' && jQuery.inArray(i, mandatory_keys) == -1) {
            delete sammy.params[i];
        }
        else if (jQuery.inArray(i, num_keys) != -1) {
            sammy.params[i] = parseInt(sammy.params[i]);
        }
        else if (jQuery.inArray(i, bool_keys) != -1) {
            sammy.params[i] = sammy.params[i] == 'true';
        }
    }
    if (sync_put(sammy, '/policies/:vhost/:name')) update();
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

// Our base64 library takes a string that is really a byte sequence,
// and will throw if given a string with chars > 255 (and hence not
// DTRT for chars > 127). So encode a unicode string as a UTF-8
// sequence of "bytes".
function b64_encode_utf8(str) {
    return base64.encode(encode_utf8(str));
}

// encodeURIComponent handles utf-8, unescape does not. Neat!
function encode_utf8(str) {
  return unescape(encodeURIComponent(str));
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

function debounce(f, delay) {
    var timeout = null;

    return function() {
        var obj = this;
        var args = arguments;

        function delayed () {
            f.apply(obj, args);
            timeout = null;
        }
        if (timeout) clearTimeout(timeout);
        timeout = setTimeout(delayed, delay);
    }
}
