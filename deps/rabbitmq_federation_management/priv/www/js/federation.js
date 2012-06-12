dispatcher_add(function(sammy) {
    sammy.get('#/federation', function() {
            render({'links':       '/federation-links',
                    'connections': '/parameters/federation_connection',
                    'globals':     '/parameters/federation'},
                'federation', '#/federation');
        });
    sammy.get('#/federation-connection/:id', function() {
            render({'connection': '/parameters/federation_connection/' + esc(this.params['id'])},
                'federation-connection', '#/federation');
        });
    sammy.put('#/fed-globals', function() {
            if (this.params.value == '') this.params.value = null;

            if (sync_put(this, '/parameters/:component/:key'))
                update();
            return false;
        });
    sammy.put('#/fed-parameters', function() {
            var num_keys = ['expires', 'message_ttl', 'max_hops',
                            'prefetch_count', 'reconnect_delay'];
            for (var i in this.params) {
                if (i === 'length' || !this.params.hasOwnProperty(i)) continue;
                if (this.params[i] == '') {
                    delete this.params[i];
                }
                else if (num_keys.indexOf(i) != -1) {
                    this.params[i] = parseInt(this.params[i]);
                }
            }
            this.params = {"component": this.params.component,
                           "key":       this.params.key,
                           "value":     this.params};
            delete this.params.value.component;
            delete this.params.value.key;
            if (sync_put(this, '/parameters/:component/:key'))
                update();
            return false;
        });
    sammy.del('#/fed-parameters', function() {
            if (sync_delete(this, '/parameters/:component/:key'))
                go_to('#/federation');
            return false;
        });
});

NAVIGATION['Admin'][0]['Federation'] = ['#/federation', true];

VHOST_QUERIES["/federation-links"] = "";
SORT_QUERIES["/federation-links"] = "";

HELP['federation-explicit-identity'] =
    'Each broker in a federated set of brokers needs a name. If you leave this blank RabbitMQ will generate one. If you set this, ensure that each broker has a unique name. Note that you <b>must</b> set this when using federation combined with clustering.';

HELP['federation-local-username'] =
    'The name of a local user which can be used to publish messages received over federated links.';

HELP['federation-expires'] =
    'Time in milliseconds that the upstream should remember about this node for. After this time all upstream state will be removed. Leave this blank to mean "forever".';

HELP['federation-ttl'] =
    'Time in milliseconds that undelivered messages should be held upstream when there is a network outage or backlog. Leave this blank to mean "forever".';

HELP['federation-max-hops'] =
    'Maximum number of federation links that messages can traverse before being dropped. Defaults to 1 if not set.';

HELP['federation-prefetch'] =
    'Maximum number of unacknowledged messages that may be in flight over a federation link at one time. Defaults to 1000 if not set.';

HELP['federation-reconnect'] =
    'Time in seconds to wait after a network link goes down before attempting reconnection. Defaults to 1 if not set.';

HELP['federation-ha-policy'] =
    'Federation declares a queue at the upstream node to buffer messages waiting to be sent. Use this to set the x-ha-policy argument for this queue.';

function link_fed_conn(name) {
    return _link_to(fmt_escape_html(name), '#/federation-connection/' + esc(name))
}
