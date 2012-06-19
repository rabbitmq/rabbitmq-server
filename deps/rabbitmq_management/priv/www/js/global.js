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

// Which queries need to have the current vhost appended (if there is one)?
var VHOST_QUERIES = map(['/queues', '/exchanges']);

// Which queries need to have the current sort appended (if there is one)?
var SORT_QUERIES  = map(['/connections', '/channels', '/vhosts', '/users',
                         '/queues', '/exchanges']);

// Extension arguments that we know about and present specially in the UI.
var KNOWN_ARGS = {'alternate-exchange':        {'short': 'AE',  'type': 'string'},
                  'x-message-ttl':             {'short': 'TTL', 'type': 'int'},
                  'x-expires':                 {'short': 'Exp', 'type': 'int'},
                  'x-ha-policy':               {'short': 'HA',  'type': 'string'},
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

var NAVIGATION = {'Overview':    ['#/',                          false],
                  'Connections': ['#/connections',               false],
                  'Channels':    ['#/channels',                  false],
                  'Exchanges':   ['#/exchanges',                 false],
                  'Queues':      ['#/queues',                    false],
                  'Admin':       [{'Users':         ['#/users',  true],
                                   'Virtual Hosts': ['#/vhosts', true]}, true]
                 };

///////////////////////////////////////////////////////////////////////////
//                                                                       //
// Mostly constant, typically get set once at startup (or rarely anyway) //
//                                                                       //
///////////////////////////////////////////////////////////////////////////

// All these are to do with hiding UI elements if
var statistics_level;   // ...there are no fine stats
var user_administrator; // ...user is not an admin
var user_monitor;       // ...user cannot monitor
var nodes_interesting;  // ...we are not in a cluster
var vhosts_interesting; // ...there is only one vhost

// Extensions write to this, the dispatcher maker reads it
var dispatcher_modules = [];

// We need to know when all extension script files have loaded
var extension_count;

// The dispatcher needs access to the Sammy app
var app;

var exchange_types;

// Set up the above vars
function setup_global_vars() {
    var overview = JSON.parse(sync_get('/overview'));
    statistics_level = overview.statistics_level;
    var user = JSON.parse(sync_get('/whoami'));
    replace_content('login', '<p>User: <b>' + user.name + '</b></p>');
    var tags = user.tags.split(",");
    user_administrator = jQuery.inArray("administrator", tags) != -1;
    user_monitor = user_administrator ||
        jQuery.inArray("monitoring", tags) != -1;
    nodes_interesting = user_monitor &&
        JSON.parse(sync_get('/nodes')).length > 1;
    vhosts_interesting = JSON.parse(sync_get('/vhosts')).length > 1;
    current_vhost = get_pref('vhost');
    exchange_types = overview.exchange_types;
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

// The timer object for auto-updates, and how often it goes off
var timer;
var timer_interval;

// When did we last connect successfully (for the "could not connect" error)
var last_successful_connect;

// Every 200 updates without user interaction we do a full refresh, to
// work around memory leaks in browser DOM implementations.
// TODO: maybe we don't need this any more?
var update_counter = 0;

