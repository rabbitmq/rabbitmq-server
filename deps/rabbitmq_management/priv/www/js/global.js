function map(list) {
    var res = {};
    for (i in list) {
        res[list[i]] = '';
    }
    return res;
}

var VHOST_QUERIES = map(['/queues', '/exchanges']);
var SORT_QUERIES  = map(['/connections', '/channels', '/vhosts', '/users',
                         '/queues', '/exchanges']);

var KNOWN_ARGS = {'alternate-exchange': {'short': 'AE',  'type': 'string'},
                  'x-message-ttl':      {'short': 'TTL', 'type': 'int'},
                  'x-expires':          {'short': 'Exp', 'type': 'int'},
                  'x-ha-policy':        {'short': 'M',   'type': 'string'}};

var IMPLICIT_ARGS = {'durable':         {'short': 'D',   'type': 'boolean'},
                     'auto-delete':     {'short': 'AD',  'type': 'boolean'},
                     'internal':        {'short': 'I',   'type': 'boolean'}};

var ALL_ARGS = {};
for (var k in KNOWN_ARGS)    ALL_ARGS[k] = KNOWN_ARGS[k];
for (var k in IMPLICIT_ARGS) ALL_ARGS[k] = IMPLICIT_ARGS[k];

var statistics_level;
var user_administrator;
var user_monitor;
var nodes_interesting;
var vhosts_interesting;
var dispatcher_modules = [];
var extension_count;
var app;

function setup_global_vars() {
    statistics_level = JSON.parse(sync_get('/overview')).statistics_level;
    var user = JSON.parse(sync_get('/whoami'));
    replace_content('login', '<p>User: <b>' + user.name + '</b></p>');
    var tags = user.tags.split(",");
    user_administrator = jQuery.inArray("administrator", tags) != -1;
    user_monitor = user_administrator ||
        jQuery.inArray("monitoring", tags) != -1;
    nodes_interesting = user_administrator &&
        JSON.parse(sync_get('/nodes')).length > 1;
    vhosts_interesting = JSON.parse(sync_get('/vhosts')).length > 1;
    current_vhost = get_pref('vhost');
}

var current_template;
var current_reqs;
var current_highlight;
var current_vhost = '';
var current_sort;
var current_sort_reverse = false;
var timer;
var timer_interval;
var last_successful_connect;
var update_counter = 0;

