import { dispatcher_add, sync_get, go_to, render } from './main.js';
import { NAVIGATION, HELP, current_sort, current_sort_reverse } from './global.js';
import { _link_to, esc, fmt_escape_html, link_channel, link_queue, link_conn } from './formatters.js';

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

if (typeof $ !== 'undefined') {
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
}

function link_pid(name) {
    return _link_to(name, '#/process/' + esc(name));
}

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
        return '<b>' + fmt_escape_html(name.name) + '</b>';
    }
}

function fmt_remove_rabbit_prefix(name) {
    if (name == 'rabbit_amqqueue_process') return 'queue';

    if (name.substring(0, 7) == 'rabbit_') {
        return fmt_escape_html(name.substring(7));
    }
    else {
        return fmt_escape_html(name);
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
    return Math.round(delta / 5);
}

export {
    link_pid,
    fmt_sort_desc_by_default,
    fmt_process_name,
    fmt_remove_rabbit_prefix,
    fmt_pids,
    fmt_reduction_delta
};

if (typeof window !== 'undefined') {
    Object.assign(window, {
        link_pid,
        fmt_sort_desc_by_default,
        fmt_process_name,
        fmt_remove_rabbit_prefix,
        fmt_pids,
        fmt_reduction_delta
    });
}
