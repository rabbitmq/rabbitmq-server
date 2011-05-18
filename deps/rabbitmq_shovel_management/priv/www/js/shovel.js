dispatcher_add(function(sammy) {
        sammy.get('#/shovels', function() {
                render({'shovels': '/shovels'},
                       'shovels', '#/shovels');
            });
});

$("#tabs").append('<li class="administrator-only"><a href="#/shovels">Shovels</a></li>');

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