dispatcher_add(function(sammy) {
    function path(p, r, t) {
        sammy.get(p, function() {
                render(r, t, p);
            });
    }
    sammy.get('#/', function() {
            var reqs = {'overview': {path:    '/overview',
                                     options: {ranges: ['lengths-over',
                                                        'msg-rates-over']}}};
            if (user_monitor) {
                reqs['nodes'] = '/nodes';
            }
            render(reqs, 'overview', '#/');
        });
    sammy.get('#/nodes/:name', function() {
            var name = esc(this.params['name']);
            render({'node': '/nodes/' + name},
                   'node', '');
        });

    path('#/connections',
         {'connections': {path: '/connections', options: {sort:true}}},
        'connections');
    sammy.get('#/connections/:name', function() {
            var name = esc(this.params['name']);
            render({'connection': {path:    '/connections/' + name,
                                   options: {ranges: ['data-rates-conn']}},
                    'channels': '/connections/' + name + '/channels'},
                'connection', '#/connections');
        });
    sammy.del('#/connections', function() {
            var options = {headers: {
              'X-Reason': this.params['reason']
            }};
            if (sync_delete(this, '/connections/:name', options)) {
              go_to('#/connections');
            }

           return false;
        });

    path('#/channels', {'channels': {path: '/channels', options: {sort:true}}},
         'channels');
    sammy.get('#/channels/:name', function() {
            render({'channel': {path:   '/channels/' + esc(this.params['name']),
                                options:{ranges:['msg-rates-ch']}}},
                   'channel', '#/channels');
        });

    path('#/exchanges', {'exchanges':  {path:    '/exchanges',
                                        options: {sort:true,vhost:true}},
                         'vhosts': '/vhosts'}, 'exchanges');
    sammy.get('#/exchanges/:vhost/:name', function() {
            var path = '/exchanges/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'exchange': {path:    path,
                                 options: {ranges:['msg-rates-x']}},
                    'bindings_source': path + '/bindings/source',
                    'bindings_destination': path + '/bindings/destination'},
                'exchange', '#/exchanges');
        });
    sammy.put('#/exchanges', function() {
            if (sync_put(this, '/exchanges/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/exchanges', function() {
            if (sync_delete(this, '/exchanges/:vhost/:name'))
                go_to('#/exchanges');
            return false;
        });
    sammy.post('#/exchanges/publish', function() {
            publish_msg(this.params);
            return false;
        });

    path('#/queues', {'queues':  {path:    '/queues',
                                  options: {sort:true,vhost:true}},
                      'vhosts': '/vhosts'}, 'queues');
    sammy.get('#/queues/:vhost/:name', function() {
            var path = '/queues/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'queue': {path:    path,
                              options: {ranges:['lengths-q', 'msg-rates-q']}},
                    'bindings': path + '/bindings'}, 'queue', '#/queues');
        });
    sammy.put('#/queues', function() {
            if (sync_put(this, '/queues/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/queues', function() {
            if (this.params['mode'] == 'delete') {
                if (sync_delete(this, '/queues/:vhost/:name'))
                    go_to('#/queues');
            }
            else if (this.params['mode'] == 'purge') {
                if (sync_delete(this, '/queues/:vhost/:name/contents')) {
                    show_popup('info', "Queue purged");
                    update_partial();
                }
            }
            return false;
        });
    sammy.post('#/queues/get', function() {
            get_msgs(this.params);
            return false;
        });
    sammy.post('#/queues/actions', function() {
            if (sync_post(this, '/queues/:vhost/:name/actions'))
                // We can't refresh fast enough, it's racy. So grey
                // the button and wait for a normal refresh.
                $('#action-button').addClass('wait').prop('disabled', true);
            return false;
        });
    sammy.post('#/bindings', function() {
            if (sync_post(this, '/bindings/:vhost/e/:source/:destination_type/:destination'))
                update();
            return false;
        });
    sammy.del('#/bindings', function() {
            if (sync_delete(this, '/bindings/:vhost/e/:source/:destination_type/:destination/:properties_key'))
                update();
            return false;
        });

    path('#/vhosts', {'vhosts':  {path:    '/vhosts',
                                  options: {sort:true}},
                      'permissions': '/permissions'}, 'vhosts');
    sammy.get('#/vhosts/:id', function() {
            render({'vhost': {path:    '/vhosts/' + esc(this.params['id']),
                              options: {ranges: ['lengths-vhost',
                                                 'msg-rates-vhost',
                                                 'data-rates-vhost']}},
                    'permissions': '/vhosts/' + esc(this.params['id']) + '/permissions',
                    'users': '/users/'},
                'vhost', '#/vhosts');
        });
    sammy.put('#/vhosts', function() {
            if (sync_put(this, '/vhosts/:name')) {
                update_vhosts();
                update();
            }
            return false;
        });
    sammy.del('#/vhosts', function() {
            if (sync_delete(this, '/vhosts/:name')) {
                update_vhosts();
                go_to('#/vhosts');
            }
            return false;
        });

    path('#/users', {'users': {path:    '/users',
                               options: {sort:true}},
                     'permissions': '/permissions'}, 'users');
    sammy.get('#/users/:id', function() {
            render({'user': '/users/' + esc(this.params['id']),
                    'permissions': '/users/' + esc(this.params['id']) + '/permissions',
                    'vhosts': '/vhosts/'}, 'user',
                   '#/users');
        });
    sammy.put('#/users-add', function() {
            if (sync_put(this, '/users/:username'))
                update();
            return false;
        });
    sammy.put('#/users-modify', function() {
            if (sync_put(this, '/users/:username'))
                go_to('#/users');
            return false;
        });
    sammy.del('#/users', function() {
            if (sync_delete(this, '/users/:username'))
                go_to('#/users');
            return false;
        });

    sammy.put('#/permissions', function() {
            if (sync_put(this, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    sammy.del('#/permissions', function() {
            if (sync_delete(this, '/permissions/:vhost/:username'))
                update();
            return false;
        });
    path('#/policies', {'policies': '/policies',
                        'vhosts':   '/vhosts'}, 'policies');
    sammy.get('#/policies/:vhost/:id', function() {
            render({'policy': '/policies/' + esc(this.params['vhost'])
                        + '/' + esc(this.params['id'])},
                'policy', '#/policies');
        });
    sammy.put('#/policies', function() {
            put_policy(this, ['name', 'pattern', 'policy'], ['priority'], []);
            return false;
        });
    sammy.del('#/policies', function() {
            if (sync_delete(this, '/policies/:vhost/:name'))
                go_to('#/policies');
            return false;
        });

    sammy.put('#/logout', function() {
            document.cookie = 'auth=; expires=Thu, 01 Jan 1970 00:00:00 GMT';
            location.reload();
        });

    sammy.get('#/import-succeeded', function() {
            render({}, 'import-succeeded', '#/overview');
        });
    sammy.put('#/rate-options', function() {
            update_rate_options(this);
        });
});
