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

    var name = process.name;

    var txt;
    if (name.supertype != undefined) {
        if (name.supertype == 'channel') {
            txt = link_channel(name.connection_name + ' (' +
                               name.channel_number + ')');
        }
        else if (name.supertype == 'queue') {
            txt = link_queue(name.vhost, name.queue_name);
        }
        else if (name.supertype == 'connection') {
            txt = link_conn(name.connection_name);
        }
    }
    else {
        txt = name.name;
    }

    return txt + '<sub>' + fmt_remove_rabbit_prefix(name.type) + '</sub>';
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