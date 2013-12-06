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

    if (process.registered_name != '') {
        return '<b>' + process.registered_name +
            '</b><sub>registered</sub>';
    }
    else if (process.rabbit_process_name != undefined) {
        var proc = process.rabbit_process_name;
        var txt;
        if (proc.type == 'channel' || proc.type == 'writer') {
            txt = link_channel(proc.connection_name + ' (' +
                               proc.channel_number + ')');
        }
        else if (proc.type == 'queue' || proc.type == 'queue_slave') {
            txt = link_queue(proc.vhost, proc.queue_name);
        }
        else if (proc.type == 'reader') {
            txt = link_conn(proc.connection_name);
        }

        return txt + '<sub>' + proc.type + '</sub>';
    }
    else {
        return process.initial_call + '<sub>unidentified</sub>';
    }
}
