dispatcher_add(function(sammy) {
    sammy.get('#/traces', function() {
            render({'traces': '/traces',
                    'vhosts': '/vhosts',
                    'files': '/trace-files'},
                   'traces', '#/traces');
        });
    sammy.get('#/traces/:vhost/:name', function() {
            var path = '/traces/' + esc(this.params['vhost']) + '/' + esc(this.params['name']);
            render({'trace': path},
                'trace', '#/traces');
        });
    sammy.put('#/traces', function() {
            if (this.params['max_payload_bytes'] === '') {
                delete this.params['max_payload_bytes'];
            }
            else {
                this.params['max_payload_bytes'] =
                    parseInt(this.params['max_payload_bytes']);
            }
            if (sync_put(this, '/traces/:vhost/:name'))
                update();
            return false;
        });
    sammy.del('#/traces', function() {
            if (sync_delete(this, '/traces/:vhost/:name'))
                partial_update();
            return false;
        });
    sammy.del('#/trace-files', function() {
            if (sync_delete(this, '/trace-files/:name'))
                partial_update();
            return false;
        });
});

NAVIGATION['Admin'][0]['Tracing'] = ['#/traces', 'administrator'];

HELP['tracing-max-payload'] =
    'Maximum size of payload to log, in bytes. Payloads larger than this limit will be truncated. Leave blank to prevent truncation. Set to 0 to prevent logging of payload altogether.';

function link_trace(name) {
    return _link_to(name, 'api/trace-files/' + esc(name));
}

function link_trace_queue(trace) {
    return _link_to('(queue)',  '#/queues/' + esc(trace.vhost) + '/' + esc(trace.queue.name));
}
