dispatcher_add(function(sammy) {
    function path(p, r, t) {
        sammy.get(p, function() {
                render(r, t, p);
            });
    }
    sammy.get('#/', function() {
            var reqs = {'overview': '/overview'};
            if (user_administrator) {
                reqs['nodes'] = '/nodes';
            }
            render(reqs, 'overview', '#/');
        });
    sammy.get('#/nodes/:name', function() {
            var name = esc(sammy.params['name']);
            render({'node': '/nodes/' + name},
                   'node', '');
        });

    path('#/connections', {'connections': '/connections'}, 'connections');
    sammy.get('#/connections/:name', function() {
            var name = esc(sammy.params['name']);
            render({'connection': '/connections/' + name,
                    'channels': '/connections/' + name + '/channels'},
                'connection', '#/connections');
        });
    sammy.del('#/connections', function() {
            if (sync_delete(sammy, '/connections/:name'))
                go_to('#/connections');
            return false;
        });

    path('#/channels', {'channels': '/channels'}, 'channels');
    sammy.get('#/channels/:name', function() {
            render({'channel': '/channels/' + esc(sammy.params['name'])}, 'channel',
                   '#/channels');
        });

    path('#/exchanges', {'exchanges': '/exchanges', 'vhosts': '/vhosts'}, 'exchanges');
    sammy.get('#/exchanges/:vhost/:name', function() {
            var path = '/exchanges/' + esc(sammy.params['vhost']) + '/' + esc(sammy.params['name']);
            render({'exchange': path,
                    'bindings_source': path + '/bindings/source',
                    'bindings_destination': path + '/bindings/destination'},
                'exchange', '#/exchanges');
        });
    sammy.put('#/exchanges', function() {
            if (sync_put(sammy, '/exchanges/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/exchanges', function() {
            if (sync_delete(sammy, '/exchanges/:vhost/:name'))
                go_to('#/exchanges');
            return false;
        });
    sammy.post('#/exchanges/publish', function() {
            publish_msg(sammy.params);
            return false;
        });

    path('#/queues', {'queues': '/queues', 'vhosts': '/vhosts'}, 'queues');
    sammy.get('#/queues/:vhost/:name', function() {
            var path = '/queues/' + esc(sammy.params['vhost']) + '/' + esc(sammy.params['name']);
            render({'queue': path,
                    'bindings': path + '/bindings'}, 'queue', '#/queues');
        });
    sammy.put('#/queues', function() {
            if (sync_put(sammy, '/queues/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/queues', function() {
            if (sammy.params['mode'] == 'delete') {
                if (sync_delete(sammy, '/queues/:vhost/:name'))
                    go_to('#/queues');
            }
            else if (sammy.params['mode'] == 'purge') {
                if (sync_delete(sammy, '/queues/:vhost/:name/contents')) {
                    show_popup('info', "Queue purged");
                    update_partial();
                }
            }
            return false;
        });
    sammy.post('#/queues/get', function() {
            get_msgs(sammy.params);
            return false;
        });
    sammy.post('#/bindings', function() {
            if (sync_post(sammy, '/bindings/:vhost/e/:source/:destination_type/:destination'))
                update();
            return false;
        });
    sammy.del('#/bindings', function() {
            if (sync_delete(sammy, '/bindings/:vhost/e/:source/:destination_type/:destination/:properties_key'))
                update();
            return false;
        });

    path('#/vhosts', {'vhosts': '/vhosts', 'permissions': '/permissions'}, 'vhosts');
    sammy.get('#/vhosts/:id', function() {
            render({'vhost': '/vhosts/' + esc(sammy.params['id']),
                    'permissions': '/vhosts/' + esc(sammy.params['id']) + '/permissions',
                    'users': '/users/'},
                'vhost', '#/vhosts');
        });
    sammy.put('#/vhosts', function() {
            if (sync_put(sammy, '/vhosts/:name')) {
                update_vhosts();
                update();
            }
            return false;
        });
    sammy.del('#/vhosts', function() {
            if (sync_delete(sammy, '/vhosts/:name')) {
                update_vhosts();
                go_to('#/vhosts');
            }
            return false;
        });

    path('#/users', {'users': '/users', 'permissions': '/permissions'}, 'users');
    sammy.get('#/users/:id', function() {
            render({'user': '/users/' + esc(sammy.params['id']),
                    'permissions': '/users/' + esc(sammy.params['id']) + '/permissions',
                    'vhosts': '/vhosts/'}, 'user',
                   '#/users');
        });
    sammy.put('#/users', function() {
            if (sync_put(sammy, '/users/:username'))
                update();
            return false;
        });
    sammy.del('#/users', function() {
            if (sync_delete(sammy, '/users/:username'))
                go_to('#/users');
            return false;
        });

    sammy.put('#/permissions', function() {
            if (sync_put(sammy, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    sammy.del('#/permissions', function() {
            if (sync_delete(sammy, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    sammy.get('#/import-succeeded', function() {
            render({}, 'import-succeeded', '#/overview');
        });
});
