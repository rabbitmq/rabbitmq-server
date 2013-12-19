dispatcher_add(function(sammy) {
    sammy.get('#/top', function() {
            var nodes = JSON.parse(sync_get('/nodes'));
            go_to('#/top/' + nodes[0].name);
        });
    sammy.get('#/top/:node', function() {
            render({'processes': {path:    '/top/' + esc(this.params['node']),
                                  options: {sort:true}},
                    'nodes':     '/nodes'},
                    'processes', '#/top');
        });
    sammy.get('#/process/:pid', function() {
            render({'process': '/process/' + esc(this.params['pid'])},
                    'process', '#/top');
        });
});

NAVIGATION['Admin'][0]['Top Processes'] = ['#/top', 'administrator'];

function link_pid(name) {
    return _link_to(name, '#/process/' + esc(name))
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
