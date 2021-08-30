$(document).ready(function() {
    if (enable_uaa) {
        get(uaa_location + "/info", "application/json", function(req) {
            if (req.status !== 200) {
                replace_content('outer', format('login_uaa', {}));
                replace_content('login-status', '<p class="warning">' + uaa_location + " does not appear to be a running UAA instance or may not have a trusted SSL certificate"  + '</p> <button id="loginWindow" onclick="uaa_login_window()">Single Sign On</button>');
            } else {
                replace_content('outer', format('login_uaa', {}));
            }
        });
    } else {
        replace_content('outer', format('login', {}));
        start_app_login();
    }
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

function set_auth_pref(userinfo) {
    // clear a local storage value used by earlier versions
    clear_local_pref('auth');

    var b64 = b64_encode_utf8(userinfo);
    var date  = new Date();
    var login_session_timeout = get_login_session_timeout();

    if (login_session_timeout) {
        date.setMinutes(date.getMinutes() + login_session_timeout);
    } else {
        // 8 hours from now
        date.setHours(date.getHours() + 8);
    }
    store_cookie_value_with_expiration('auth', encodeURIComponent(b64), date);
}

function getParameterByName(name) {
    var match = RegExp('[#&]' + name + '=([^&]*)').exec(window.location.hash);
    return match && decodeURIComponent(match[1].replace(/\+/g, ' '));
}

function getAccessToken() {
    return getParameterByName('access_token');
}

function start_app_login() {
    app = new Sammy.Application(function () {
        this.get('#/', function() {});
        this.put('#/login', function() {
            username = this.params['username'];
            password = this.params['password'];
            set_auth_pref(username + ':' + password);
            check_login();
        });
    });
    if (enable_uaa) {
        var token = getAccessToken();
        if (token != null) {
            set_auth_pref(uaa_client_id + ':' + token);
            store_pref('uaa_token', token);
            check_login();
        } else if(has_auth_cookie_value()) {
            check_login();
        };
    } else {
        app.run();
        if (get_cookie_value('auth') != null) {
            check_login();
        }
    }
}


function uaa_logout_window() {
    uaa_invalid = true;
    uaa_login_window();
}

function uaa_login_window() {
    var redirect;
    if (window.location.hash != "") {
        redirect = window.location.href.split(window.location.hash)[0];
    } else {
        redirect = window.location.href
    };
    var loginRedirectUrl;
    if (uaa_invalid) {
        loginRedirectUrl = Singular.properties.uaaLocation + '/logout.do?client_id=' + Singular.properties.clientId + '&redirect=' + redirect;
    } else {
        loginRedirectUrl = Singular.properties.uaaLocation + '/oauth/authorize?response_type=token&client_id=' + Singular.properties.clientId + '&redirect_uri=' + redirect;
    };
    window.open(loginRedirectUrl, "LOGIN_WINDOW");
}

function check_login() {
    user = JSON.parse(sync_get('/whoami'));
    if (user == false) {
        // clear a local storage value used by earlier versions
        clear_pref('auth');
        clear_pref('uaa_token');
        clear_cookie_value('auth');
        if (enable_uaa) {
            uaa_invalid = true;
            replace_content('login-status', '<button id="loginWindow" onclick="uaa_login_window()">Log out</button>');
        } else {
            replace_content('login-status', '<p>Login failed</p>');
        }
    }
    else {
        hide_popup_warn();
        replace_content('outer', format('layout', {}));
        var user_login_session_timeout = parseInt(user.login_session_timeout);
        // Update auth login_session_timeout if changed
        if (has_auth_cookie_value() && !isNaN(user_login_session_timeout) &&
            user_login_session_timeout !== get_login_session_timeout()) {

            update_login_session_timeout(user_login_session_timeout);
        }
        setup_global_vars();
        setup_constant_events();
        update_vhosts();
        update_interval();
        setup_extensions();
    }
}

function get_login_session_timeout() {
    parseInt(get_cookie_value('login_session_timeout'));
}

function update_login_session_timeout(login_session_timeout) {
    var auth_info = get_cookie_value('auth');
    var date  = new Date();
    // `login_session_timeout` minutes from now
    date.setMinutes(date.getMinutes() + login_session_timeout);
    store_cookie_value('login_session_timeout', login_session_timeout);
    store_cookie_value_with_expiration('auth', auth_info, date);
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

    // updated to the version  0.7.6 this _interval = null is fixed
    // just leave the history here.
    //Sammy.HashLocationProxy._interval = null;

    app = new Sammy.Application(dispatcher);
    app.run();

    var url = this.location.toString();
    var hash = this.location.hash;
    var pathname = this.location.pathname;
    if (url.indexOf('#') == -1) {
        this.location = url + '#/';
    } else if (hash.indexOf('#token_type') != - 1 && pathname == '/') {
        // This is equivalent to previous `if` clause when uaa authorisation is used.
        // Tokens are passed in the url hash, so the url always contains a #.
        // We need to check the current path is `/` and token is present,
        // so we can redirect to `/#/`
        this.location = url.replace(/#token_type.+/gi, "#/");
    }
}

function setup_constant_events() {
    $('#update-every').on('change', function() {
            var interval = $(this).val();
            store_pref('interval', interval);
            if (interval == '')
                interval = null;
            else
                interval = parseInt(interval);
            set_timer_interval(interval);
        });
    $('#show-vhost').on('change', function() {
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
    extension_count = 0;
    for (var i in extensions) {
        var extension = extensions[i];
        if ($.isPlainObject(extension) && extension.hasOwnProperty("javascript")) {
            dynamic_load(extension.javascript);
            extension_count++;
        }
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
        timer = setInterval(partial_update, timer_interval);
    }
}

function update_manual(div, query) {
    var path;
    var template;
    if (query == 'memory' || query == 'binary') {
        path = current_reqs['node']['path'] + '?' + query + '=true';
        template = query;
    }

    var data = JSON.parse(sync_get(path));

    replace_content(div, format(template, data));
    postprocess_partial();
}

function render(reqs, template, highlight) {
    var old_template = current_template;
    current_template = template;
    current_reqs = reqs;
    for (var i in outstanding_reqs) {
        outstanding_reqs[i].abort();
    }
    outstanding_reqs = [];
    current_highlight = highlight;
    if (old_template !== current_template) {
        window.scrollTo(0, 0);
    }
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
    if (!$(".pagination_class").is(":focus")) {
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
                    console.log("before/after mismatch! Doing a full reload...");
                    full_refresh();
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
    if(outstanding_reqs.length > 0){
        return false;
    }
    with_reqs(apply_state(current_reqs), [], function(json) {
            var html = format(current_template, json);
            fun(html);
            update_status('ok');
        });
    return true;
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
            var indexPage = req.indexOf("?page=");
            if (indexPage >- 1) {
				pageUrl = req.substr(indexPage);
				req2 = req.substr(0,indexPage) + '/' + esc(current_vhost) + pageUrl;
            } else

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
                var range = get_pref('chart-range').split('|');
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
                else if (type == 'node-stats') {
                    prefix = 'node_stats';
                }
                qs.push(prefix + '_age=' + parseInt(range[0]));
                qs.push(prefix + '_incr=' + parseInt(range[1]));
            }
        }
        /* Unknown options are used as query parameters as is. */
        Object.keys(options).forEach(function (key) {
          /* Skip known keys we already handled and undefined parameters. */
          if (key == 'vhost' || key == 'sort' || key == 'ranges')
            return;
          if (!key || options[key] == undefined)
            return;

          qs.push(esc(key) + '=' + esc(options[key]));
        });
        qs = qs.join('&');
        if (qs != '')
            if (req2.indexOf("?page=") >- 1)
            qs = '&' + qs;
             else
            qs = '?' + qs;

        reqs2[k] = req2 + qs;
    }
    return reqs2;
}

function show_popup(type, text, _mode) {
    var cssClass = '.form-popup-' + type;
    function hide() {
        $(cssClass).fadeOut(100, function() {
            $(this).remove();
        });
    }
    hide();
    $('#outer').after(format('popup', {'type': type, 'text': text}));
    $(cssClass).fadeIn(100);
    $(cssClass + ' span').on('click', function () {
        $('.popup-owner').removeClass('popup-owner');
        hide();
    });
}

function hide_popup_warn() {
    var cssClass = '.form-popup-warn';
    $('.popup-owner').removeClass('popup-owner');
    $(cssClass).fadeOut(100, function() {
        $(this).remove();
    });
}

function submit_import(form) {
    if (form.file.value) {
        var confirm_upload = confirm('Are you sure you want to import a definitions file? Some entities (vhosts, users, queues, etc) may be overwritten!');
        if (confirm_upload === true) {
            var file = form.file.files[0]; // FUTURE: limit upload file size (?)
            var vhost_upload = $("select[name='vhost-upload'] option:selected");
            var vhost_selected = vhost_upload.index() > 0;

            var vhost_name = null;
            if (vhost_selected) {
                vhost_name = vhost_upload.val();
            }

            var vhost_part = '';
            if (vhost_name) {
                vhost_part = '/' + esc(vhost_name);
            }

            if (enable_uaa) {
                var form_action = "/definitions" + vhost_part + '?token=' + get_pref('uaa_token');
            } else {
                var form_action = "/definitions" + vhost_part + '?auth=' + get_cookie_value('auth');
            };
            var fd = new FormData();
            fd.append('file', file);
            with_req('POST', form_action, fd, function(resp) {
                show_popup('info', 'Your definitions were imported successfully.');
            });
        }
    }
    return false;
};

function postprocess() {
    $('form.confirm-queue').on('submit', function() {
        return confirm("Are you sure? The queue is going to be deleted. " +
                       "Messages cannot be recovered after deletion.");
        });

    $('form.confirm-purge-queue').on('submit', function() {
        return confirm("Are you sure? Messages cannot be recovered after purging.");
        });

    $('form.confirm').on('submit', function() {
            return confirm("Are you sure? This object cannot be recovered " +
                           "after deletion.");
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

    $('#download-definitions').on('click', function() {
            var idx = $("select[name='vhost-download'] option:selected").index();
            var vhost = ((idx <=0 ) ? "" : "/" + esc($("select[name='vhost-download'] option:selected").val()));
        if (enable_uaa) {
            var path = 'api/definitions' + vhost + '?download=' +
                esc($('#download-filename').val()) +
                '&token=' + get_pref('uaa_token');
            } else {
                var path = 'api/definitions' + vhost + '?download=' +
                    esc($('#download-filename').val()) +
                    '&auth=' + get_cookie_value('auth');
            };
            window.location = path;
            setTimeout('app.run()');
            return false;
        });

    $('.update-manual').on('click', function() {
            update_manual($(this).attr('for'), $(this).attr('query'));
        });

    $(document).on('keyup', '.multifield input', function() {
            update_multifields();
        });

    $(document).on('change', '.multifield select', function() {
            update_multifields();
        });

    $('.controls-appearance').on('change', function() {
        var params = $(this).get(0).options;
        var selected = $(this).val();

        for (i = 0; i < params.length; i++) {
            var param = params[i].value;
            if (param == selected) {
                $('#' + param + '-div').slideDown(100);
            } else {
                $('#' + param + '-div').slideUp(100);
            }
        }
    });

    $(document).on('click', '.help', function() {
      show_popup('help', HELP[$(this).attr('id')]);
    });

    $(document).on('click', '.popup-options-link', function() {
        $('.popup-owner').removeClass('popup-owner');
        $(this).addClass('popup-owner');
        var template = $(this).attr('type') + '-options';
        show_popup('options', format(template, {span: $(this)}), 'fade');
    });

    $(document).on('click', '.rate-visibility-option', function() {
        var k = $(this).attr('data-pref');
        var show = get_pref(k) !== 'true';
        store_pref(k, '' + show);
        partial_update();
    });

    $(document).on('focus', 'input, select', function() {
        update_counter = 0; // If there's interaction, reset the counter.
    });

    $('.tag-link').on('click', function() {
        $('#tags').val($(this).attr('tag'));
    });

    $('.argument-link').on('click', function() {
        var field = $(this).attr('field');
        var row = $('#' + field).find('.mf').last();
        var key = row.find('input').first();
        var value = row.find('input').last();
        var type = row.find('select').last();
        key.val($(this).attr('key'));
        value.val($(this).attr('value'));
        type.val($(this).attr('type'));
        update_multifields();
    });

    $(document).on('click', 'form.auto-submit select, form.auto-submit input', function(){
        $(this).parents('form').submit();
    });

    $('#filter').on('keyup', debounce(update_filter, 500));

    $('#filter-regex-mode').on('change', update_filter_regex_mode);

    $('#truncate').on('keyup', debounce(update_truncate, 500));

    if (! user_administrator) {
        $('.administrator-only').remove();
    }

    update_multifields();
}

function url_pagination_template_context(template, context, defaultPage, defaultPageSize){
    var page_number_request = fmt_page_number_request(context, defaultPage);
    var page_size = fmt_page_size_request(context, defaultPageSize);
    var name_request = fmt_filter_name_request(context, "");
    var use_regex = fmt_regex_request(context, "") == "checked";
    if (use_regex) {
        name_request = esc(name_request);
    }
    return  '/' + template +
        '?page=' +  page_number_request +
        '&page_size=' + page_size +
        '&name=' + name_request +
        '&use_regex=' + use_regex;
}

function url_pagination_template(template, defaultPage, defaultPageSize){
    return url_pagination_template_context(template, template, defaultPage, defaultPageSize);
}

function stored_page_info(template, page_start){
    var pageSize = fmt_strip_tags($('#' + template+'-pagesize').val());
    var filterName = fmt_strip_tags($('#' + template+'-name').val());

    store_pref(template + '_current_page_number', page_start);
    if (filterName != null && filterName != undefined) {
        store_pref(template + '_current_filter_name', filterName);
    }
    var regex_on =  $("#" + template + "-filter-regex-mode").is(':checked');

    if (regex_on != null && regex_on != undefined) {
        store_pref(template + '_current_regex', regex_on ? "checked" : " " );
    }

    if (pageSize != null && pageSize != undefined) {
        store_pref(template + '_current_page_size', pageSize);
    }
}

function update_pages(template, page_start){
     stored_page_info(template, page_start);
     switch (template) {
         case 'queues' : renderQueues(); break;
         case 'exchanges' : renderExchanges(); break;
         case 'connections' : renderConnections(); break;
         case 'channels' : renderChannels(); break;
         default:
             renderCallback = RENDER_CALLBACKS[template];
             if (renderCallback != undefined) {
                 renderCallback();
             }
             break;
     }
}

function renderQueues() {
    ensure_queues_chart_range();
    render({'queues': {
        path: url_pagination_template('queues', 1, 100),
        options: {
            sort: true,
            vhost: true,
            pagination: true
        }
    }, 'vhosts': '/vhosts'}, 'queues', '#/queues');
}

function renderExchanges() {
    render({'exchanges': {path: url_pagination_template('exchanges', 1, 100),
                          options: {sort:true, vhost:true, pagination:true}},
                         'vhosts': '/vhosts'}, 'exchanges', '#/exchanges');
}

function renderConnections() {
    render({'connections': {path:  url_pagination_template('connections', 1, 100),
                            options: {sort:true}}},
                            'connections', '#/connections');
}

function renderChannels() {
    render({'channels': {path:  url_pagination_template('channels', 1, 100),
                        options: {sort:true}}},
                        'channels', '#/channels');
}

function update_pages_from_ui(sender) {
    var val = $(sender).val();
    var raw = !!$(sender).attr('data-page-start') ? $(sender).attr('data-page-start') : val;
    var s   = fmt_escape_html(fmt_strip_tags(raw));
    update_pages(current_template, s);
}

function postprocess_partial() {
    $('.pagination_class_input').on('keypress', function(e) {
        if (e.keyCode == 13) {
            update_pages_from_ui(this);
        }
    });

    $('.pagination_class_checkbox').on('click', function(e) {
        update_pages_from_ui(this);
    });

    $('.pagination_class_select').on('change', function(e) {
        update_pages_from_ui(this);
    });

    setup_visibility();

    $('#main').off('click', 'div.section h2, div.section-hidden h2');
    $('#main').on('click', 'div.section h2, div.section-hidden h2', function() {
            toggle_visibility($(this));
        });

    $('.sort').on('click', function() {
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
    var type_inputs = $('#' + name + ' *[name$="_mftype"]');
    type_inputs.each(function(index) {
        var re = new RegExp(name + '_([0-9]*)_mftype');
        var match = $(this).attr('name').match(re);
        if (!match) return;
        var id = parseInt(match[1]);
        largest_id = Math.max(id, largest_id);
        var prefix = name + '_' + id;
        var type = $(this).val();
        var input = $('#' + prefix + '_mfvalue');
        if (type == 'list') {
            if (input.length == 1) {
                input.replaceWith('<div class="multifield-sub" id="' + prefix +
                                  '"></div>');
            }
            update_multifield($('#' + prefix), false);
        }
        else {
            if (input.length == 1) {
                var key = dict ? $('#' + prefix + '_mfkey').val() : '';
                var value = input.val();
                if (key == '' && value == '') {
                    if (index == type_inputs.length - 1) {
                        empty_found = true;
                    }
                    else {
                        $(this).parents('.mf').first().remove();
                    }
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
            multifield.append('<table class="mf"><tr><td>' +
                              multifield_input(prefix, 'key', 'text') +
                              '</td><td class="equals"> = </td><td>' +
                              val_type + '</td></tr></table>');
        }
        else {
            multifield.append('<div class="mf">' + val_type + '</div>');
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

function update_filter_regex(jElem) {
    current_filter_regex = null;
    jElem.parents('.filter').children('.status-error').remove();
    if (current_filter_regex_on && $.trim(current_filter).length > 0) {
        try {
            current_filter_regex = new RegExp(current_filter,'i');
        } catch (e) {
            jElem.parents('.filter').append('<p class="status-error">' +
                                            fmt_escape_html(e.message) + '</p>');
        }
    }
}

function update_filter_regex_mode() {
    current_filter_regex_on = $(this).is(':checked');
    update_filter_regex($(this));
    partial_update();
}

function update_filter() {
    current_filter = $(this).val();
    var table = $(this).parents('table').first();
    table.removeClass('filter-active');
    if ($(this).val() != '') {
        table.addClass('filter-active');
    }
    update_filter_regex($(this));
    partial_update();
}

function update_truncate() {
    var current_truncate_str =
        $(this).val().replace(new RegExp('\\D', 'g'), '');
    if (current_truncate_str == '') {
        current_truncate_str = '0';
    }
    if ($(this).val() != current_truncate_str) {
        $(this).val(current_truncate_str);
    }
    var current_truncate = parseInt(current_truncate_str, 10);
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
            // Workaround for... something. Although div.hider is
            // display:block anyway, not explicitly setting this
            // prevents the first slideToggle() from animating
            // successfully; instead the element just vanishes.
            $(this).find('.hider').attr('style', 'display:block;');
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
        if (all.hasClass('section-hidden')) {
            store_pref(pref, 't');
        } else {
            clear_pref(pref);
        }
        all.removeClass('section-invisible');
        all.addClass('section-visible');
    }
}

function publish_msg(params0) {
    try {
        var params = params_magic(params0);
        publish_msg0(params);
    } catch (e) {
        show_popup('warn', fmt_escape_html(e));
        return false;
    }
}

function publish_msg0(params) {
    var path = fill_path_template('/exchanges/:vhost/:name/publish', params);
    params['payload_encoding'] = 'string';
    params['properties'] = {};
    params['properties']['delivery_mode'] = parseInt(params['delivery_mode']);
    if (params['headers'] != '')
        params['properties']['headers'] = params['headers'];
    var props = [['content_type',     'str'],
                 ['content_encoding', 'str'],
                 ['correlation_id',   'str'],
                 ['reply_to',         'str'],
                 ['expiration',       'str'],
                 ['message_id',       'str'],
                 ['type',             'str'],
                 ['user_id',          'str'],
                 ['app_id',           'str'],
                 ['cluster_id',       'str'],
                 ['priority',         'int'],
                 ['timestamp',        'int']];
    for (var i in props) {
        var name = props[i][0];
        var type = props[i][1];
        if (params['props'][name] != undefined && params['props'][name] != '') {
            var value = params['props'][name];
            if (type == 'int') value = parseInt(value);
            params['properties'][name] = value;
        }
    }
    with_req('POST', path, JSON.stringify(params), function(resp) {
            var result = JSON.parse(resp.responseText);
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
            var msgs = JSON.parse(resp.responseText);
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
                acc[key] = JSON.parse(resp.responseText);
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
        console.log("Uncaught error: " + err);
        console.log("Stack: " + err['stack']);
        debug(err['name'] + ": " + err['message'] + "\n" + err['stack'] + "\n");
    }
}

function update_status(status) {
    var text;
    if (status == 'ok')
        text = "Refreshed " + fmt_date(new Date());
    else if (status == 'error') {
        var next_try = new Date(new Date().getTime() + timer_interval);
        text = "Error: could not connect to server since " +
            fmt_date(last_successful_connect) + ". Will retry at " +
            fmt_date(next_try) + ".";
    }
    else
        throw("Unknown status " + status);

    var html = format('status', {status: status, text: text});
    replace_content('status', html);
}

function has_auth_cookie_value() {
    return get_cookie_value('auth') != null;
}

function auth_header() {
    if(has_auth_cookie_value() && enable_uaa) {
        return "Bearer " + decodeURIComponent(get_pref('uaa_token'));
    } else {
        if(has_auth_cookie_value()) {
            return "Basic " + decodeURIComponent(get_cookie_value('auth'));
        } else {
            return null;
        }
    }
}

function with_req(method, path, body, fun) {
    if(!has_auth_cookie_value()) {
        // navigate to the login form
        location.reload();
        return;
    }

    var json;
    var req = xmlHttpRequest();
    req.open(method, 'api' + path, true );
    var header = auth_header();
    if (header !== null) {
        req.setRequestHeader('authorization', header);
    }
    req.setRequestHeader('x-vhost', current_vhost);
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            var ix = jQuery.inArray(req, outstanding_reqs);
            if (ix != -1) {
                outstanding_reqs.splice(ix, 1);
            }
            if (check_bad_response(req, true)) {
                last_successful_connect = new Date();
                fun(req);
            }
        }
    };
    outstanding_reqs.push(req);
    req.send(body);
}

function get(url, accept, callback) {
  var req = new XMLHttpRequest();
  req.open("GET", url);
  req.setRequestHeader("Accept", accept);
  req.send();

  req.onreadystatechange = function() {
    if (req.readyState == XMLHttpRequest.DONE) {
      callback(req);
    }
  };
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
        show_popup('warn', fmt_escape_html(e));
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
            // https://support.microsoft.com/kb/186063
            // MSIE6 appears to do this in response to HTTP 204.
        }
    }

    if (check_bad_response(req, false)) {
        if (type == 'GET')
            return req.responseText;
        else
            // rabbitmq/rabbitmq-management#732
            // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
            return {result: true, http_status: req.status, req_params: params};
    }
    else {
        return false;
    }
}

function check_bad_response(req, full_page_404) {
    // 1223 == 204 - see https://www.enhanceie.com/ie/bugs.asp
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

        var error = JSON.parse(req.responseText).error;
        if (typeof(error) != 'string') error = JSON.stringify(error);

        if (error == 'bad_request' || error == 'not_found' || error == 'not_authorised') {
            show_popup('warn', fmt_escape_html(reason));
        } else if (error == 'page_out_of_range') {
            var seconds = 60;
            if (last_page_out_of_range_error > 0)
                    seconds = (new Date().getTime() - last_page_out_of_range_error.getTime())/1000;
            if (seconds > 3) {
                 Sammy.log('server reports page is out of range, redirecting to page 1');
                 var contexts = ["queues", "exchanges", "connections", "channels"];
                 var matches = /api\/(.*)\?/.exec(req.responseURL);
                 if (matches != null && matches.length > 1) {
                     contexts.forEach(function(item) {
                         if (matches[1].indexOf(item) == 0) {update_pages(item, 1)};
                     });
                 } else update_pages(current_template, 1);

                 last_page_out_of_range_error = new Date();
            }
        }
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
        debug("Management API returned status code " + req.status + " - <strong>" + fmt_escape_html_one_line(req.responseText) + "</strong>");
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
    return check_password(maybe_remove_fields(collapse_multifields(params)));
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
        var top_level = id_parts.length == 1;
        if (t == 'list') {
            val = [];
            id_map[name][id] = val;
        }
        else if ((set(k) && top_level) || set(v)) {
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
            if (top_level) {
                params[name][k] = val;
            }
            else {
                var prefix = id_parts.slice(0, id_parts.length - 1).join('_');
                id_map[name][prefix].push(val);
            }
        }
    }
    if (params.hasOwnProperty('queuetype')) {
        delete params['queuetype'];
        params['arguments']['x-queue-type'] = queue_type;
        if (queue_type == 'quorum' ||
            queue_type == 'stream') {
            params['durable'] = true;
            params['auto_delete'] = false;
        }
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
        var options = $(this).get(0).options;
        var selected = $(this).val();

        for (i = 0; i < options.length; i++) {
            var option = options[i].value;
            if (option != selected) {
                delete params[option];
            }
        }
        delete params[$(this).attr('name')];
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

function put_cast_params(sammy, path, mandatory_keys, num_keys, bool_keys) {
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
    if (sync_put(sammy, path)) update();
}

function update_column_options(sammy) {
    var mode = sammy.params['mode'];
    for (var group in COLUMNS[mode]) {
        var options = COLUMNS[mode][group];
        for (var i = 0; i < options.length; i++) {
            var key = options[i][0];
            var value = sammy.params[mode + '-' + key] != undefined;
            store_pref('column-' + mode + '-' + key, value);
        }
    }

    partial_update();
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

// Don't use the jQuery AJAX support, it seems to have trouble reporting
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

function rename_multifield(params, from, to) {
    var new_params = {};
    for(var key in params){
        var match = key.match("^" + from + "_[0-9_]*_mftype$");
        var match2 = key.match("^" + from +  "_[0-9_]*_mfkey$");
        var match3 = key.match("^" + from + "_[0-9_]*_mfvalue$");
        if (match != null) {
            new_params[match[0].replace(from, to)] = params[match];
        }
        else if (match2 != null) {
            new_params[match2[0].replace(from, to)] = params[match2];
        }
        else if (match3 != null) {
            new_params[match3[0].replace(from, to)] = params[match3];
        }
        else {
            new_params[key] = params[key]
        }
    }
    return new_params;
}

function select_queue_type(queuetype) {
    queue_type = queuetype.value;
    update();
}

function is_quorum(queue) {
    if (queue["arguments"]) {
        if (queue["arguments"]["x-queue-type"]) {
            return queue["arguments"]["x-queue-type"] === "quorum";
        } else {
            return false;
        }
    } else {
        return false;
    }
}

function is_stream(queue) {
    if (queue["arguments"]) {
        if (queue["arguments"]["x-queue-type"]) {
            return queue["arguments"]["x-queue-type"] === "stream";
        } else {
            return false;
        }
    } else {
        return false;
    }
}

function is_classic(queue) {
    if (queue["arguments"]) {
        if (queue["arguments"]["x-queue-type"]) {
            return queue["arguments"]["x-queue-type"] === "classic";
        } else {
            return true;
        }
    } else {
        return true;
    }
}

function ensure_queues_chart_range() {
    var range = get_pref('chart-range');
    // Note: the queues page uses the 'basic' range type
    var fixup_range;
    var valid_range = false;
    var range_type = get_chart_range_type('queues');
    var chart_periods = CHART_RANGES[range_type];
    for (var i = 0; i < chart_periods.length; ++i) {
        var data = chart_periods[i];
        var val = data[0];
        if (range === val) {
            valid_range = true;
            break;
        }
        // If the range needs to be adjusted, use the last
        // valid one
        fixup_range = val;
    }
    if (!valid_range) {
        store_pref('chart-range', fixup_range);
    }
}

function get_chart_range_type(arg) {
   /*
    * 'arg' can be:
    * lengths-over for the Overview page
    * lengths-q for the per-queue page
    * queues for setting up the queues range
    */
    if (arg === 'lengths-over') {
        return 'global';
    }
    if (arg === 'msg-rates-over') {
        return 'global';
    }
    if (arg === 'lengths-q') {
        return 'basic';
    }
    if (arg === 'msg-rates-q') {
        return 'basic';
    }
    if (arg === 'queues') {
        return 'basic';
    }
    if (arg === 'queue-churn') {
        return 'basic';
    }
    if (arg === 'channel-churn') {
        return 'basic';
    }
    if (arg === 'connection-churn') {
        return 'basic';
    }

    console.log('[WARNING]: range type not found for arg: ' + arg);
    return 'basic';
}
