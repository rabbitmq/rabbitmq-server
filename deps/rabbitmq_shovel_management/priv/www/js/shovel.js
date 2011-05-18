dispatcher_add(function(sammy) {
        sammy.get('#/shovel-status', function() {
                render({'shovels': '/shovel-status'},
                       'shovel-status', '#/shovel-status');
            });
});

$("#tabs").append('<li class="administrator-only"><a href="#/shovel-status">Shovel Status</a></li>');

function fmt_shovel_endpoint(point) {
    if (point.node) {
        delete point.heartbeat;
        delete point.frame_max;
        delete point.channel_max;
        delete point.port;
    }
    delete point.client_properties;
    return fmt_table_short(point);
}