dispatcher_add(function(sammy) {
    function path(p, r, t) {
        sammy.get(p, function() {
                render(r, t, p);
            });
    }
    sammy.get('#/', function() {
            var reqs = {'overview': {path:    '/overview',
                                     options: {ranges: ['lengths-over',
                                                        'msg-rates-over']}},
                        'vhosts': '/vhosts'};
            if (user_monitor) {
                reqs['nodes'] = '/nodes';
            }
            render(reqs, 'overview', '#/');
        });
    sammy.get('#/', function() {
        var reqs = {'overview': {path:    '/overview',
                                 options: {ranges: ['lengths-over',
                                                    'msg-rates-over']}},
                    'vhosts': '/vhosts'};
        if (user_monitor) {
            reqs['nodes'] = '/nodes';
        }
        render(reqs, 'overview', '#/');
    });
    path('#/cluster-name', {'cluster_name': '/cluster-name'}, 'cluster-name');
    sammy.put('#/cluster-name', function() {
            if (sync_put(this, '/cluster-name')) {
                setup_global_vars();
                update();
            }
            return false;
        });

    sammy.get('#/nodes/:name', function() {
            var name = esc(this.params['name']);
            render({'node': {path:    '/nodes/' + name,
                             options: {ranges: ['node-stats']}}},
                   'node', '');
            });

    if (ac.canAccessVhosts()) {
      sammy.get('#/connections', function() {
            renderConnections();
        });
      sammy.get('#/connections/:name', function() {
            var name = esc(this.params['name']);
            var connectionPath = '/connections/' + name;
            var reqs = {
                'connection': {
                    path: connectionPath,
                    options: { ranges: ['data-rates-conn'] }
                }
            };
            // First, get the connection details to check the protocol
            var connectionDetails = JSON.parse(sync_get(connectionPath));
            if (connectionDetails.protocol === 'AMQP 1-0' ||
                connectionDetails.protocol === 'Web AMQP 1-0') {
                reqs['sessions'] = connectionPath + '/sessions';
            } else {
                reqs['channels'] = connectionPath + '/channels';
            }
            render(reqs, 'connection', '#/connections');
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
      sammy.get('#/channels', function() {
            renderChannels();
        });
      sammy.get('#/channels/:name', function() {
            render({'channel': {path:   '/channels/' + esc(this.params['name']),
                                options:{ranges:['data-rates-ch','msg-rates-ch']}}},
                   'channel', '#/channels');
        });
      sammy.get('#/exchanges', function() {
            renderExchanges();
        });
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

      sammy.get('#/queues', function() {
            renderQueues();
        });

      sammy.get('#/queues/:vhost/:name', function() {
            var vhost = this.params['vhost'];
            var queue = this.params['name'];
            var path = '/queues/' + esc(vhost) + '/' + esc(queue);
            var requests = {'queue': {path:    path,
                                      options: {ranges:['lengths-q', 'msg-rates-q', 'data-rates-q']}},
                            'bindings': path + '/bindings'};
            // we add extra requests that can be added by code plugged on the extension point
            for (var i = 0; i < QUEUE_EXTRA_CONTENT_REQUESTS.length; i++) {
                var extra = QUEUE_EXTRA_CONTENT_REQUESTS[i](vhost, queue);
                for (key in extra) {
                    if(extra.hasOwnProperty(key)){
                        requests[key] = extra[key];
                    }
                }
            }
            render(requests, 'queue', '#/queues');
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
                    partial_update();
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
                    'topic_permissions': '/vhosts/' + esc(this.params['id']) + '/topic-permissions',
                    'users': '/users/',
                    'exchanges' : '/exchanges/' + esc(this.params['id'])},
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

      sammy.get('#/users', function() {
        renderUsers();
      });
      sammy.get('#/users/:id', function() {
        var vhosts = JSON.parse(sync_get('/vhosts'));
        const current_vhost = get_pref('vhost');
        var index_vhost = 0;
        for (var i = 0; i < vhosts.length; i++) {
            if (vhosts[i].name === current_vhost) {
                index_vhost = i;
                break;
            }
        }
        render({'user': '/users/' + esc(this.params['id']),
                    'permissions': '/users/' + esc(this.params['id']) + '/permissions',
                    'topic_permissions': '/users/' + esc(this.params['id']) + '/topic-permissions',
                    'vhosts': '/vhosts/',
                    'exchanges': '/exchanges/' + esc(vhosts[index_vhost].name)},
           'user','#/users');
      });
      sammy.put('#/users-add', function() {
            res = sync_put(this, '/users/:username');
            if (res) {
                if (res.http_status === 204) {
                    username = fmt_escape_html(res.req_params.username);
                    show_popup('warn', "Updated an existing user: '" + username + "'");
                }
                update();
            }
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
      sammy.put('#/topic-permissions', function() {
            if (sync_put(this, '/topic-permissions/:vhost/:username'))
                update();
            return false;
        });
      sammy.del('#/topic-permissions', function() {
            if (sync_delete(this, '/topic-permissions/:vhost/:username/:exchange'))
                update();
            return false;
        });
      path('#/policies', {'policies': '/policies',
                        'operator_policies': '/operator-policies',
                        'vhosts':   '/vhosts'}, 'policies');
      sammy.get('#/policies/:vhost/:id', function() {
            render({'policy': '/policies/' + esc(this.params['vhost'])
                        + '/' + esc(this.params['id'])},
                'policy', '#/policies');
        });
      sammy.put('#/policies', function() {
            put_cast_params(this, '/policies/:vhost/:name',
                            ['name', 'pattern', 'policy'], ['priority'], []);
            return false;
        });
      sammy.del('#/policies', function() {
            if (sync_delete(this, '/policies/:vhost/:name'))
                go_to('#/policies');
            return false;
        });
      sammy.put('#/operator_policies', function() {
            this.params = rename_multifield(this.params, "definitionop", "definition");
            put_cast_params(this, '/operator-policies/:vhost/:name',
                            ['name', 'pattern', 'policy'], ['priority'], []);
            return false;
        });
      sammy.del('#/operator_policies', function() {
            if (sync_delete(this, '/operator-policies/:vhost/:name'))
                update();
        });
      let datamodel = {
        'limits': '/vhost-limits',
        'user_limits': '/user-limits',
        'vhosts': '/vhosts'
      }
      if (ac.isAdministratorUser()) {
        datamodel['users'] = '/users'
      }
      path('#/limits', datamodel, 'limits');

      sammy.put('#/limits', function() {
          var valAsInt = parseInt(this.params.value);
          if (isNaN(valAsInt)) {
              var e = 'Validation failed\n\n' +
                  this.params.name + ' should be a number, actually was "' +
                  this.params.value + '"';
              show_popup('warn', fmt_escape_html(e));
          } else {
              this.params.value = valAsInt;
              if (sync_put(this, '/vhost-limits/:vhost/:name')) {
                  update();
              }
          }
      })
      sammy.put('#/user-limits', function() {
          var valAsInt = parseInt(this.params.value);
          if (isNaN(valAsInt)) {
              var e = 'Validation failed\n\n' +
                  this.params.name + ' should be a number, actually was "' +
                  this.params.value + '"';
              show_popup('warn', fmt_escape_html(e));
          } else {
              this.params.value = valAsInt;
              if (sync_put(this, '/user-limits/:user/:name')) {
                  update();
              }
          }
      })
      sammy.post('#/restart_vhost', function(){
          if(sync_post(this, '/vhosts/:vhost/start/:node')) update();
      })
      sammy.del('#/limits', function() {
          if (sync_delete(this, '/vhost-limits/:vhost/:name')) update();
      })
      sammy.del('#/user-limits', function() {
          if (sync_delete(this, '/user-limits/:user/:name')) update();
      })
    }
    path('#/feature-flags', {'feature_flags': {path:    '/feature-flags',
                                           options: {sort:true}},
                         'permissions': '/permissions'}, 'feature-flags');
    sammy.put('#/feature-flags-enable', function() {
        if (sync_put(this, '/feature-flags/:name/enable'))
            update();
        return false;
    });
    path('#/deprecated-features', {'deprecated_features': {path:    '/deprecated-features',
                                                           options: {sort:true}},
                                   'used_deprecated_features': '/deprecated-features/used'}, 'deprecated-features');
    sammy.put('#/logout', function() {
        // clear a local storage value used by earlier versions
        clear_auth()
        if (oauth.logged_in) {
            oauth.logged_in = false;
            oauth_initiateLogout();
        }else {
          go_to_home()
        }
    });

    sammy.put('#/rate-options', function() {
            update_rate_options(this);
        });
    sammy.put('#/column-options', function() {
            update_column_options(this);
        });

    sammy.del("#/reset", function(){
            if(sync_delete(this, '/reset')){
                update();
            }
        });
    sammy.del("#/reset_node", function(){
            if(sync_delete(this, '/reset/:node')){
                update();
            }
        });
});
