dispatcher_add(function(sammy) {
    sammy.get('#/federation', function() {
            render({'links':     {path:   '/federation-links',
                                  options:{vhost:true}},
                    'vhosts':    '/vhosts',
                    'globals':   '/parameters/federation'},
                'federation', '#/federation');
        });
    sammy.get('#/federation-upstreams', function() {
            render({'upstreams': {path:   '/parameters/federation-upstream',
                                  options:{vhost:true}},
                    'vhosts': '/vhosts'},
                   'federation-upstreams', '#/federation-upstreams');
        });
    sammy.get('#/federation-upstreams/:vhost/:id', function() {
            render({'upstream': '/parameters/federation-upstream/' + esc(this.params['vhost']) + '/' + esc(this.params['id'])},
                   'federation-upstream', '#/federation');
        });
    sammy.put('#/fed-globals', function() {
            if (this.params.value == '') this.params.value = null;

            if (sync_put(this, '/parameters/:component/:vhost/:name'))
                update();
            return false;
        });
    sammy.put('#/fed-parameters', function() {
            var num_keys = ['expires', 'message-ttl', 'max-hops',
                            'prefetch-count', 'reconnect-delay'];
            var bool_keys = ['trust-user-id'];
            var arrayable_keys = ['uri'];
            put_parameter(this, [], num_keys, bool_keys, arrayable_keys);
            return false;
        });
    sammy.del('#/fed-parameters', function() {
            if (sync_delete(this, '/parameters/:component/:vhost/:name'))
                go_to('#/federation-upstreams');
            return false;
        });
});

NAVIGATION['Admin'][0]['Federation Status'] = ['#/federation', true];
NAVIGATION['Admin'][0]['Federation Upstreams'] = ['#/federation-upstreams', true];

HELP['federation-explicit-identity'] =
    'Each broker in a federated set of brokers needs a name. If you leave this blank RabbitMQ will generate one. If you set this, ensure that each broker has a unique name. Note that you <b>must</b> set this when using federation combined with clustering.';

HELP['federation-local-username'] =
    'The name of a local user which can be used to publish messages received over federated links.';

HELP['federation-uri'] =
    'URI to connect to. If upstream is a cluster and can have several URIs, you can enter them here separated by spaces.';

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

HELP['federation-ack-mode'] =
    '<dl>\
       <dt><code>on-confirm</code></dt>\
       <dd>Messages are acknowledged to the upstream broker after they have been confirmed downstream. Handles network errors and broker failures without losing messages. The slowest option, and the default.</dd>\
       <dt><code>on-publish</code></dt>\
       <dd>Messages are acknowledged to the upstream broker after they have been published downstream. Handles network errors without losing messages, but may lose messages in the event of broker failures.</dd>\
       <dt><code>no-ack</code></dt>\
       <dd>Message acknowledgements are not used. The fastest option, but may lose messages in the event of network or broker failures.</dd>\
</dl>';

HELP['federation-trust-user-id'] =
    'Set "Yes" to preserve the "user-id" field across a federation link, even if the user-id does not match that used to republish the message. Set to "No" to clear the "user-id" field when messages are federated. Only set this to "Yes" if you trust the upstream broker not to forge user-ids.';

function link_fed_conn(vhost, name) {
    return _link_to(fmt_escape_html(name), '#/federation-upstreams/' + esc(vhost) + '/' + esc(name));
}
