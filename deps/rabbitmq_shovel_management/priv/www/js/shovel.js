dispatcher_add(function(sammy) {
    sammy.get('#/shovels', function() {
            render({'shovels': {path:    '/shovels',
                                options: {vhost:true}}},
                    'shovels', '#/shovels');
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
    sammy.put('#/shovel-parameters-move-messages', function() {
            var num_keys = ['src-prefetch-count', 'reconnect-delay'];
            var bool_keys = ['dest-add-forward-headers'];
            var arrayable_keys = ['src-uri', 'dest-uri'];
            var redirect = this.params['redirect'];
            if (redirect !== undefined) {
                delete this.params['redirect'];
            }
            var stream_offset = this.params['src-consumer-args-stream-offset'];
            var src_consumer_args = {'x-stream-offset': stream_offset};
            delete this.params['src-consumer-args-stream-offset'];
            this.params['src-consumer-args'] = src_consumer_args;
            put_parameter(this, [], num_keys, bool_keys, arrayable_keys);
            if (redirect !== undefined) {
                go_to(redirect);
            }
            return false;
        });
    sammy.put('#/shovel-parameters', function() {
            // patch up the protocol selectors
            var src_proto = this.params['src-protocol-selector'];
            this.params['src-protocol'] = src_proto.substr(0, src_proto.indexOf('-'));
            var dest_proto = this.params['dest-protocol-selector'];
            this.params['dest-protocol'] = dest_proto.substr(0, dest_proto.indexOf('-'));

            //remove fields not required by the selected protocol
            if (this.params['src-protocol'] == 'amqp10') {
                remove_params_with(this, 'amqp091-src');
            } else {
                remove_params_with(this, 'amqp10-src');
            }
            if (this.params['dest-protocol'] == 'amqp10') {
                remove_params_with(this, 'amqp091-dest');
            } else {
                remove_params_with(this, 'amqp10-dest');
            }

            var trimProtoPrefix = function (x) {
                if(x.startsWith('amqp10-') || x.startsWith('amqp091-')) {
                    return x.substr(x.indexOf('-') + 1, x.length);
                }
                return x;
            };

            rekey_params(this, trimProtoPrefix);

            var num_keys = ['src-prefetch-count', 'reconnect-delay'];

            //copy the correct delete-after value
            if (this.params['src-delete-after-selector'] == 'never') {
                this.params['src-delete-after'] = 'never';
            } else if (this.params['src-delete-after-selector'] == 'number') {
                num_keys.push('src-delete-after');
            }

            delete this.params['src-delete-after-selector'];

            var bool_keys = ['dest-add-forward-headers'];
            var arrayable_keys = ['src-uri', 'dest-uri'];
            var redirect = this.params['redirect'];
            if (redirect !== undefined) {
                delete this.params['redirect'];
            }
            put_parameter(this, [], num_keys, bool_keys, arrayable_keys);
            if (redirect !== undefined) {
                go_to(redirect);
            }
            return false;
        });
    sammy.del('#/shovel-parameters', function() {
            if (sync_delete(this, '/shovels/vhost/:vhost/:name')) {
                go_to('#/dynamic-shovels');
            } else {
                show_popup('warn', 'Shovel was not able to be deleted');
                return false;
            }
        });
    sammy.del("#/shovel-restart-link", function(){
            if (sync_delete(this, '/shovels/vhost/:vhost/:name/restart')) {
                update();
            } else {
                show_popup('warn', 'Shovel was not able to be restarted');
                return false;
            }
        });
});


NAVIGATION['Admin'][0]['Shovel Status'] = ['#/shovels', "monitoring"];
NAVIGATION['Admin'][0]['Shovel Management'] = ['#/dynamic-shovels', "policymaker"];

HELP['shovel-uri'] =
    'Both source and destination can be either a local or remote broker. See the "URI examples" pane for examples of how to construct URIs. If connecting to a cluster, you can enter several URIs here separated by spaces.';

HELP['shovel-amqp10-address'] =
    'The AMQP 1.0 address representing the source or target node.'

HELP['shovel-queue-exchange'] =
    'You can set both source and destination as either a queue or an exchange. If you choose "queue", it will be declared beforehand; if you choose "exchange" it will not, but an appropriate binding and queue will be created when the source is an exchange.';

HELP['shovel-prefetch'] =
    'Maximum number of unacknowledged messages that may be in flight over a shovel at one time. Defaults to 1000 if not set.';

HELP['shovel-reconnect'] =
    'Time in seconds to wait after a shovel goes down before attempting reconnection. Defaults to 1 if not set.';

HELP['shovel-forward-headers'] =
    'Whether to add headers to the shovelled messages indicating where they have been shovelled from and to. Defaults to false if not set.';

HELP['shovel-ack-mode'] =
    '<dl>\
       <dt><code>on-confirm</code></dt>\
       <dd>Messages are acknowledged at the source after they have been confirmed at the destination. Handles network errors and broker failures without losing messages. The slowest option, and the default.</dd>\
       <dt><code>on-publish</code></dt>\
       <dd>Messages are acknowledged at the source after they have been published at the destination. Handles network errors without losing messages, but may lose messages in the event of broker failures.</dd>\
       <dt><code>no-ack</code></dt>\
       <dd>Message acknowledgements are not used. The fastest option, but may lose messages in the event of network or broker failures.</dd>\
</dl>';

HELP['shovel-amqp091-auto-delete'] =
    '<dl>\
       <dt><code>Never</code></dt>\
       <dd>The shovel never deletes itself; it will persist until it is explicitly removed.</dd>\
       <dt><code>After initial length transferred</code></dt>\
       <dd>The shovel will check the length of the queue when it starts up. It will transfer that many messages, and then delete itself.</dd>\
</dl>';

HELP['shovel-amqp10-auto-delete'] =
    '<dl>\
       <dt><code>Never</code></dt>\
       <dd>The shovel never deletes itself; it will persist until it is explicitly removed.</dd>\
       <dt><code>After num messages</code></dt>\
       <dd>The shovel will delete itself after the given number of messages have been transferred.</dd>\
</dl>';
function remove_params_with(sammy, prefix) {
    for (var i in sammy.params) {
        if(i.startsWith(prefix)) {
            delete sammy.params[i];
        }
    }
}

function rekey_params(sammy, func) {
    for (var i in sammy.params) {
        var k = func(i);
        if(i != k) {
            var v = sammy.params[i];
            delete sammy.params[i];
            sammy.params[k] = v;
        }
    }
}
function link_shovel(vhost, name) {
    return _link_to(name, '#/dynamic-shovels/' + esc(vhost) + '/' + esc(name));
}

function fmt_shovel_endpoint(prefix, shovel) {
    var txt = '';
    if(shovel[prefix + "protocol"] == 'amqp10') {
        txt += fmt_string(shovel[prefix + 'address']);
    } else {
        if (shovel[prefix + 'queue']) {
            txt += fmt_string(shovel[prefix + 'queue']) + '<sub>queue</sub>';
        } else {
            if (shovel[prefix + 'exchange']) {
                txt += fmt_string(shovel[prefix + 'exchange']);
            } else {
                txt += '<i>as published</i>';
            }
            if (shovel[prefix + 'exchange_key']) {
                txt += ' : ' + fmt_string(shovel[prefix + 'exchange_key']);
            }
            txt += '<sub>exchange</sub>';
        }
    }

    return txt;
}

function fallback_value(shovel, key1, key2) {
    var v = shovel.value[key1];
    return (v !== undefined ? v : shovel.value[key2]);
}
