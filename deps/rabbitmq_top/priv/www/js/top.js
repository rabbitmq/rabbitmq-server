// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

dispatcher_add(function(sammy) {
    sammy.get('#/top', function() {
            var nodes = JSON.parse(sync_get('/nodes'));
            go_to('#/top/' + nodes[0].name + "/20");
        });
    sammy.get('#/top/ets', function() {
            var nodes = JSON.parse(sync_get('/nodes'));
            go_to('#/top/ets/' + nodes[0].name + "/20");
        });
    sammy.get('#/top/:node/:row_count', function() {
            render({'top':   {path:    '/top/' + esc(this.params['node']),
                              options: {sort: true,
                                        row_count: this.params['row_count']}},
                    'nodes': '/nodes'},
                    'processes', '#/top');
        });
    sammy.get('#/top/ets/:node/:row_count', function() {
            render({'top': {path:    '/top/ets/' + esc(this.params['node']),
                            options: {sort: true,
                                      row_count: this.params['row_count']}},
                    'nodes': '/nodes'},
                    'ets_tables', '#/top/ets');
        });
    sammy.get('#/process/:pid', function() {
            render({'process': '/process/' + esc(this.params['pid'])},
                    'process', '#/top');
        });
});

NAVIGATION['Admin'][0]['Top Processes']  = ['#/top', 'administrator'];
NAVIGATION['Admin'][0]['Top ETS Tables'] = ['#/top/ets', 'administrator'];

HELP['gen-server2-buffer'] = "The processes with a <strong>gen_server2 buffer</strong> value of <code>\>= 0</code> are of type gen_server2. " +
"They drain their Erlang mailbox into an internal queue as an optimisation. " +
"In this context, \"queue\" refers to an internal data structure and must not be confused with a RabbitMQ queue.";

$(document).on('change', 'select#top-node', function() {
    var url='#/top/' + $(this).val() + "/" + $('select#row-count').val();
    go_to(url);
});

$(document).on('change', 'select#top-node-ets', function() {
    var url='#/top/ets/' + $(this).val() + "/" + $('select#row-count-ets').val();
    go_to(url);
});

$(document).on('change', 'select#row-count', function() {
    go_to('#/top/' + $('select#top-node').val() + "/" + $(this).val());
});

$(document).on('change', 'select#row-count-ets', function() {
    go_to('#/top/ets/' + $('select#top-node-ets').val() + "/" + $(this).val());
});

function link_pid(name) {
    return _link_to(name, '#/process/' + esc(name));
}

// fmt_sort assumes ascending ordering by default and
// relies on two global variables, current_sort and current_sort_reverse.
// rabbitmq-top, however, wants to sort things in descending order by default
// because otherwise it is useless on initial page load.
//
// Thsi discrepancy means that either in the management UI or in the top
// tables the sorting indicator (a triangle) will be reversed.
//
// Changing the global variables has side effects, so we
// copy fmt_sort here and flip the arrow to meet top's needs.
function fmt_sort_desc_by_default(display, sort) {
    var prefix = '';
    if (current_sort == sort) {
        prefix = '<span class="arrow">' +
            (current_sort_reverse ? '&#9650; ' : '&#9660; ') +
            '</span>';
    }
    return '<a class="sort" sort="' + sort + '">' + prefix + display + '</a>';
}

function fmt_process_name(process) {
    if (process == undefined) return '';
    var name = process.name;

    if (name.supertype != undefined) {
        if (name.supertype == 'channel') {
            return link_channel(name.connection_name + ' (' +
                                name.channel_number + ')');
        }
        else if (name.supertype == 'queue') {
            return link_queue(name.vhost, name.queue_name);
        }
        else if (name.supertype == 'connection') {
            return link_conn(name.connection_name);
        }
    }
    else {
        return '<b>' + name.name + '</b>';
    }
}

function fmt_remove_rabbit_prefix(name) {
    if (name == 'rabbit_amqqueue_process') return 'queue';

    if (name.substring(0, 7) == 'rabbit_') {
        return name.substring(7);
    }
    else {
        return name;
    }
}

function fmt_pids(pids) {
    var txt = '';
    for (var i = 0; i < pids.length; i++) {
        txt += link_pid(pids[i]) + ' ';
    }

    return txt;
}

function fmt_reduction_delta(delta) {
    return Math.round(delta / 5); // gen_server updates every 5s
}
