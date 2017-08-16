///////////////////////
//                   //
// Genuine constants //
//                   //
///////////////////////

// Just used below
function map(list) {
    var res = {};
    for (i in list) {
        res[list[i]] = '';
    }
    return res;
}

// Extension arguments that we know about and present specially in the UI.
var KNOWN_ARGS = {'alternate-exchange':        {'short': 'AE',  'type': 'string'},
                  'x-message-ttl':             {'short': 'TTL', 'type': 'int'},
                  'x-expires':                 {'short': 'Exp', 'type': 'int'},
                  'x-max-length':              {'short': 'Lim', 'type': 'int'},
                  'x-max-length-bytes':        {'short': 'Lim B', 'type': 'int'},
                  'x-dead-letter-exchange':    {'short': 'DLX', 'type': 'string'},
                  'x-dead-letter-routing-key': {'short': 'DLK', 'type': 'string'},
                  'x-queue-master-locator':    {'short': 'ML', 'type': 'string'},
                  'x-max-priority':            {'short': 'Pri', 'type': 'int'}};

// Things that are like arguments that we format the same way in listings.
var IMPLICIT_ARGS = {'durable':         {'short': 'D',    'type': 'boolean'},
                     'auto-delete':     {'short': 'AD',   'type': 'boolean'},
                     'exclusive':       {'short': 'Excl', 'type': 'boolean'},
                     'internal':        {'short': 'I',    'type': 'boolean'}};

// Both the above
var ALL_ARGS = {};
for (var k in IMPLICIT_ARGS) ALL_ARGS[k] = IMPLICIT_ARGS[k];
for (var k in KNOWN_ARGS)    ALL_ARGS[k] = KNOWN_ARGS[k];

var NAVIGATION = {'Overview':    ['#/',            "management"],
                  'Connections': ['#/connections', "management"],
                  'Channels':    ['#/channels',    "management"],
                  'Exchanges':   ['#/exchanges',   "management"],
                  'Queues':      ['#/queues',      "management"],
                  'Admin':
                    [{'Users':         ['#/users',    "administrator"],
                      'Virtual Hosts': ['#/vhosts',   "administrator"],
                      'Policies':      ['#/policies', "management"]},
                     "management"]
                 };

var CHART_PERIODS = {'60|5':       'Last minute',
                     '600|5':      'Last ten minutes',
                     '3600|60':    'Last hour',
                     '28800|600':  'Last eight hours',
                     '86400|1800': 'Last day'};

var COLUMNS =
    {'exchanges' :
     {'Overview': [['type',                 'Type',                   true],
                   ['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false]],
      'Message rates': [['rate-in',         'rate in',                true],
                        ['rate-out',        'rate out',               true]]},
     'queues' :
     {'Overview': [['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false],
                   ['consumers',            'Consumer count',         false],
                   ['consumer_utilisation', 'Consumer utilisation',   false],
                   ['state',                'State',                  true]],
      'Messages': [['msgs-ready',      'Ready',          true],
                   ['msgs-unacked',    'Unacknowledged', true],
                   ['msgs-ram',        'In memory',      false],
                   ['msgs-persistent', 'Persistent',     false],
                   ['msgs-total',      'Total',          true]],
      'Message bytes': [['msg-bytes-ready',      'Ready',          false],
                        ['msg-bytes-unacked',    'Unacknowledged', false],
                        ['msg-bytes-ram',        'In memory',      false],
                        ['msg-bytes-persistent', 'Persistent',     false],
                        ['msg-bytes-total',      'Total',          false]],
      'Message rates': [['rate-incoming',  'incoming',      true],
                        ['rate-deliver',   'deliver / get', true],
                        ['rate-redeliver', 'redelivered',   false],
                        ['rate-ack',       'ack',           true]]},
     'channels' :
     {'Overview': [['user',  'User name', true],
                   ['mode',  'Mode',      true],
                   ['state', 'State',     true]],
      'Details': [['msgs-unconfirmed', 'Unconfirmed', true],
                  ['prefetch',         'Prefetch',    true],
                  ['msgs-unacked',     'Unacked',     true]],
      'Transactions': [['msgs-uncommitted', 'Msgs uncommitted', false],
                       ['acks-uncommitted', 'Acks uncommitted', false]],
      'Message rates': [['rate-publish',   'publish',            true],
                        ['rate-confirm',   'confirm',            true],
                        ['rate-return',    'return (mandatory)', false],
                        ['rate-deliver',   'deliver / get',      true],
                        ['rate-redeliver', 'redelivered',        false],
                        ['rate-ack',       'ack',                true]]},
     'connections':
     {'Overview': [['user',   'User name', true],
                   ['state',  'State',     true]],
      'Details': [['ssl',            'SSL / TLS',      true],
                  ['ssl_info',       'SSL Details',    false],
                  ['protocol',       'Protocol',       true],
                  ['channels',       'Channels',       true],
                  ['channel_max',    'Channel max',    false],
                  ['frame_max',      'Frame max',      false],
                  ['auth_mechanism', 'Auth mechanism', false],
                  ['client',         'Client',         false]],
      'Network': [['from_client',  'From client',  true],
                  ['to_client',    'To client',    true],
                  ['heartbeat',    'Heartbeat',    false],
                  ['connected_at', 'Connected at', false]]},

     'vhosts':
     {'Messages': [['msgs-ready',      'Ready',          true],
                   ['msgs-unacked',    'Unacknowledged', true],
                   ['msgs-total',      'Total',          true]],
      'Network': [['from_client',  'From client',  true],
                  ['to_client',    'To client',    true]],
      'Message rates': [['rate-publish', 'publish',       true],
                        ['rate-deliver', 'deliver / get', true]]},
     'overview':
     {'Statistics': [['file_descriptors',   'File descriptors',   true],
                     ['socket_descriptors', 'Socket descriptors', true],
                     ['erlang_processes',   'Erlang processes',   true],
                     ['memory',             'Memory',             true],
                     ['disk_space',         'Disk space',         true]],
      'General': [['uptime',     'Uptime',     false],
                  ['rates_mode', 'Rates mode', true],
                  ['info',       'Info',       true]]}};

///////////////////////////////////////////////////////////////////////////
//                                                                       //
// Mostly constant, typically get set once at startup (or rarely anyway) //
//                                                                       //
///////////////////////////////////////////////////////////////////////////

// All these are to do with hiding UI elements if
var rates_mode;                  // ...there are no fine stats
var user_administrator;          // ...user is not an admin
var is_user_policymaker;         // ...user is not a policymaker
var user_monitor;                // ...user cannot monitor
var nodes_interesting;           // ...we are not in a cluster
var vhosts_interesting;          // ...there is only one vhost
var rabbit_versions_interesting; // ...all cluster nodes run the same version

// Extensions write to this, the dispatcher maker reads it
var dispatcher_modules = [];

// We need to know when all extension script files have loaded
var extension_count;

// The dispatcher needs access to the Sammy app
var app;

// Used for the new exchange form, and to display broken exchange types
var exchange_types;

// Used for access control
var user_tags;
var user;

// Set up the above vars
function setup_global_vars() {
    var overview = JSON.parse(sync_get('/overview'));
    rates_mode = overview.rates_mode;
    user_tags = expand_user_tags(user.tags.split(","));
    user_administrator = jQuery.inArray("administrator", user_tags) != -1;
    is_user_policymaker = jQuery.inArray("policymaker", user_tags) != -1;
    user_monitor = jQuery.inArray("monitoring", user_tags) != -1;
    replace_content('login-details',
                    '<p>User: <b>' + fmt_escape_html(user.name) + '</b></p>' +
                    '<p>Cluster: <b>' + fmt_escape_html(overview.cluster_name) + '</b> ' +
                    (user_administrator ?
                     '(<a href="#/cluster-name">change</a>)' : '') + '</p>' +
                    '<p>RabbitMQ ' + fmt_escape_html(overview.rabbitmq_version) +
                    ', <acronym class="normal" title="' +
                    fmt_escape_html(overview.erlang_full_version) + '">Erlang ' +
                    fmt_escape_html(overview.erlang_version) + '</acronym></p>');
    nodes_interesting = false;
    rabbit_versions_interesting = false;
    if (user_monitor) {
        var nodes = JSON.parse(sync_get('/nodes'));
        if (nodes.length > 1) {
            nodes_interesting = true;
            var v = '';
            for (var i = 0; i < nodes.length; i++) {
                var v1 = fmt_rabbit_version(nodes[i].applications);
                if (v1 != 'unknown') {
                    if (v != '' && v != v1) rabbit_versions_interesting = true;
                    v = v1;
                }
            }
        }
    }
    vhosts_interesting = JSON.parse(sync_get('/vhosts')).length > 1;
    current_vhost = get_pref('vhost');
    exchange_types = overview.exchange_types;
}

function expand_user_tags(tags) {
    var new_tags = [];
    for (var i = 0; i < tags.length; i++) {
        var tag = tags[i];
        new_tags.push(tag);
        switch (tag) { // Note deliberate fall-through
            case "administrator": new_tags.push("monitoring");
                                  new_tags.push("policymaker");
            case "monitoring":    new_tags.push("management");
                                  break;
            case "policymaker":   new_tags.push("management");
            default:              break;
        }
    }
    return new_tags;
}

////////////////////////////////////////////////////
//                                                //
// Change frequently (typically every "new page") //
//                                                //
////////////////////////////////////////////////////

// Which top level template we're showing
var current_template;

// Which JSON requests do we need to populate it
var current_reqs;

// And which of those have yet to return (so we can cancel them when
// changing current_template).
var outstanding_reqs = [];

// Which tab is highlighted
var current_highlight;

// Which vhost are we looking at
var current_vhost = '';

// What is our current sort order
var current_sort;
var current_sort_reverse = false;

var current_filter = '';
var current_filter_regex_on = false;

var current_filter_regex;
var current_truncate;

// The timer object for auto-updates, and how often it goes off
var timer;
var timer_interval;

// When did we last connect successfully (for the "could not connect" error)
var last_successful_connect;

// Every 200 updates without user interaction we do a full refresh, to
// work around memory leaks in browser DOM implementations.
// TODO: maybe we don't need this any more?
var update_counter = 0;

// Holds chart data in between writing the div in an ejs and rendering
// the chart.
var chart_data = {};

// whenever a UI requests a page that doesn't exist
// because things were deleted between refreshes
var last_page_out_of_range_error = 0;
