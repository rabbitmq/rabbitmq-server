dispatcher_add(function(sammy) {
        sammy.get('#/top', function() {
                render({'top': {path:    '/top',
                                options: {sort:true}}},
                       'top', '#/top');
            });
});

NAVIGATION['Admin'][0]['Top'] = ['#/top', 'administrator'];

function fmt_process_name(process) {
    if (process == undefined) return '';

    debug(JSON.stringify(process));

    if (process.registered_name != '') {
        return '<b>' + process.registered_name +
            '</b><sub>registered</sub>';
    }
    else if (process.process_name != undefined) {
        var proc = process.process_name;
        var txt;
        if (proc.supertype == 'channel') {
            txt = link_channel(proc.connection_name + ' (' +
                               proc.channel_number + ')');
        }
        else if (proc.supertype == 'queue') {
            txt = link_queue(proc.vhost, proc.queue_name);
        }
        else if (proc.supertype == 'connection') {
            txt = link_conn(proc.connection_name);
        }

        return txt + '<sub>' + fmt_remove_rabbit_prefix(proc.type) + '</sub>';
    }
    else {
        return fmt_string(process.pid) + '<sub>unidentified</sub>';
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