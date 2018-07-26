dispatcher_add(function(sammy) {
    sammy.get('#/traces', function() {
            var nodes = JSON.parse(sync_get('/nodes'));
            go_to('#/traces/' + nodes[0].name);
        });
    sammy.get('#/traces/:node', function() {
            render({'traces': '/traces/n/' + esc(this.params['node']),
                    'vhosts': '/vhosts',
                    'node': '/nodes/' + esc(this.params['node']),
                    'nodes': '/nodes',
                    'files': '/trace-files/n/' + esc(this.params['node'])},
                   'traces', '#/traces')
    });
    sammy.get('#/traces/n/:node/:vhost/:name', function() {
            var path = '/traces/n/' + esc(this.params['node']) + '/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'trace': path},
                'trace', '#/traces');
        });
    sammy.put('#/traces/n/:node', function() {
            if (this.params['max_payload_bytes'] === '') {
                delete this.params['max_payload_bytes'];
            }
            else {
                this.params['max_payload_bytes'] =
                    parseInt(this.params['max_payload_bytes']);
            }
        if (sync_put(this, '/traces/n/' + esc(this.params['node']) + '/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/traces/n/:node', function() {
        if (sync_delete(this, '/traces/n/' + esc(this.params['node'])
                        + '/:vhost/:name'))
                partial_update();
            return false;
        });
    sammy.del('#/trace-files/n/:node', function() {
        if (sync_delete(this, '/trace-files/n/' + esc(this.params['node']) + '/:name'))
                partial_update();
            return false;
        });
});

NAVIGATION['Admin'][0]['Tracing'] = ['#/traces', 'administrator'];

HELP['tracing-max-payload'] =
    'Maximum size of payload to log, in bytes. Payloads larger than this limit will be truncated. Leave blank to prevent truncation. Set to 0 to prevent logging of payload altogether.';

$(document).on('change', 'select#traces-node', function() {
    var url='#/traces/' + $(this).val();
    go_to(url);
});

function link_trace(node, name) {
    return _link_to(name, 'api/trace-files/n/' + esc(node) + '/' + esc(name));
}

function link_trace_queue(trace) {
    return _link_to('(queue)',  '#/queues/' + esc(trace.vhost) + '/' + esc(trace.queue.name));
}
