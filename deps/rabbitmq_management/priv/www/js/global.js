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
                  'x-dead-letter-exchange':    {'short': 'DLX', 'type': 'string'},
                  'x-dead-letter-routing-key': {'short': 'DLK', 'type': 'string'}};

// Things that are like arguments that we format the same way in listings.
var IMPLICIT_ARGS = {'durable':         {'short': 'D',   'type': 'boolean'},
                     'auto-delete':     {'short': 'AD',  'type': 'boolean'},
                     'internal':        {'short': 'I',   'type': 'boolean'}};

// Both the above
var ALL_ARGS = {};
for (var k in KNOWN_ARGS)    ALL_ARGS[k] = KNOWN_ARGS[k];
for (var k in IMPLICIT_ARGS) ALL_ARGS[k] = IMPLICIT_ARGS[k];

var NAVIGATION = {'Overview':    ['#/',            "management"],
                  'Connections': ['#/connections', "management"],
                  'Channels':    ['#/channels',    "management"],
                  'Exchanges':   ['#/exchanges',   "management"],
                  'Queues':      ['#/queues',      "management"],
                  'Admin':
                    [{'Users':         ['#/users',    "administrator"],
                      'Virtual Hosts': ['#/vhosts',   "administrator"],
                      'Policies':      ['#/policies', "policymaker"]},
                     "policymaker"]
                 };

var CHART_PERIODS = {'60|5':       'Last minute',
                     '600|5':      'Last ten minutes',
                     '3600|60':    'Last hour',
                     '28800|600':  'Last eight hours',
                     '86400|1800': 'Last day'};

///////////////////////////////////////////////////////////////////////////
//                                                                       //
// Mostly constant, typically get set once at startup (or rarely anyway) //
//                                                                       //
///////////////////////////////////////////////////////////////////////////

// All these are to do with hiding UI elements if
var statistics_level;            // ...there are no fine stats
var user_administrator;          // ...user is not an admin
var user_monitor;                // ...user cannot monitor
var nodes_interesting;           // ...we are not in a cluster
var vhosts_interesting;          // ...there is only one vhost
var rabbit_versions_interesting; // ...all cluster nodes run the same version
var uri_auth_used = false;       // ...it's impossible to log out

// Extensions write to this, the dispatcher maker reads it
var dispatcher_modules = [];

// We need to know when all extension script files have loaded
var extension_count;

// The dispatcher needs access to the Sammy app
var app;

// Used for the new exchange form, and to display broken exchange types
var exchange_types;

// Used for access control to the menu
var user_tags;

// Set up the above vars
function setup_global_vars(user) {
    var overview = JSON.parse(sync_get('/overview'));
    statistics_level = overview.statistics_level;
    replace_content('login-details',
                    '<p>User: <b>' + user.name + '</b></p>' +
                    '<p>RabbitMQ ' + overview.rabbitmq_version +
                    ', <acronym class="normal" title="' +
                    overview.erlang_full_version + '">Erlang ' +
                    overview.erlang_version + '</acronym></p>');
    user_tags = expand_user_tags(user.tags.split(","));
    user_administrator = jQuery.inArray("administrator", user_tags) != -1;
    user_monitor = jQuery.inArray("monitoring", user_tags) != -1;
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

// Which tab is highlighted
var current_highlight;

// Which vhost are we looking at
var current_vhost = '';

// What is our current sort order
var current_sort;
var current_sort_reverse = false;

var current_filter = '';
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
