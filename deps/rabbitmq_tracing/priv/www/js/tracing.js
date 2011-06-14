dispatcher_add(function(sammy) {
    sammy.get('#/traces', function() {
            render({'traces': '/traces', 'vhosts': '/vhosts'},
                   'traces', '#/traces');
        });
    sammy.get('#/traces/:vhost/:name', function() {
            var path = '/traces/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'trace': path},
                'trace', '#/traces');
        });
    sammy.put('#/traces', function() {
            if (sync_put(this, '/traces/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/traces', function() {
            if (sync_delete(this, '/traces/:vhost/:name'))
                go_to('#/traces');
            return false;
        });
});

$("#tabs").append('<li class="administrator-only"><a href="#/traces">Tracing</a></li>');
