dispatcher_add(function(sammy) {
        sammy.get('#/shovels', function() {
                render({'shovels': '/shovels'},
                       'shovels', '#/shovels');
            });
});

NAVIGATION['Admin'][0]['Shovel Status'] = ['#/shovels', true];

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