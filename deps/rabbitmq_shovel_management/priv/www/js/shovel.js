dispatcher_add(function(sammy) {
    sammy.get('#/shovels', function() {
            render({'shovels': '/shovels'}, 'shovels', '#/shovels');
        });
    sammy.get('#/dynamic-shovels', function() {
            render({'shovels': {path:   '/parameters/shovel',
                            options:{vhost:true}},
                    'vhosts': '/vhosts'},
                   'dynamic-shovels', '#/dynamic-shovels');
        });
    sammy.get('#/dynamic-shovels/:vhost/:id', function() {
            render({'shovel': '/parameters/shovel/' + esc(this.params['vhost']) + '/' + esc(this.params['id'])},
                   'dynamic-shovel', '#/dynamic-shovels');
        });
    sammy.put('#/shovel-parameters', function() {
            var num_keys = ['prefetch-count', 'reconnect-delay'];
            var bool_keys = [];
            var arrayable_keys = ['from-uri', 'to-uri'];
            put_parameter(this, [], num_keys, bool_keys, arrayable_keys);
            return false;
        });
    sammy.del('#/shovel-parameters', function() {
            if (sync_delete(this, '/parameters/:component/:vhost/:name'))
                go_to('#/dynamic-shovels');
            return false;
        });
});


NAVIGATION['Admin'][0]['Shovel Status'] = ['#/shovels', "administrator"];
NAVIGATION['Admin'][0]['Shovel Management'] = ['#/dynamic-shovels', "administrator"];

function link_shovel(vhost, name) {
    return _link_to(fmt_escape_html(name), '#/dynamic-shovels/' + esc(vhost) + '/' + esc(name));
}

function fmt_shovel_endpoint(point) {
    if (point == undefined) return '';

    if (point.node) {
        delete point.heartbeat;
        delete point.frame_max;
        delete point.channel_max;
        delete point.port;
    }
    delete point.client_properties;
    return fmt_table_short(point);
}
