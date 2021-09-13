dispatcher_add(function(sammy) {
    sammy.get('#/stream/connections', function() {
            renderStreamConnections();
        });
    sammy.get('#/stream/connections/:vhost/:name', function() {
                var vhost = esc(this.params['vhost']);
                var name = esc(this.params['name']);
                render({'connection': {path:    '/stream/connections/'+ vhost + '/' + name,
                                       options: {ranges: ['data-rates-conn']}},
                        'consumers': '/stream/connections/' + vhost + '/' + name + '/consumers',
                        'publishers': '/stream/connections/' + vhost + '/' + name + '/publishers'},
                        'streamConnection', '#/stream/connections');
            });
    // not exactly dispatcher stuff, but we have to make sure this is called before
    // HTTP requests are made in case of refresh of the queue page
    QUEUE_EXTRA_CONTENT_REQUESTS.push(function(vhost, queue) {
        return {'extra_stream_publishers' : '/stream/publishers/' + esc(vhost) + '/' + esc(queue)};
    });
    QUEUE_EXTRA_CONTENT.push(function(queue, extraContent) {
        if (is_stream(queue)) {
            var publishers = extraContent['extra_stream_publishers'];
            if (publishers !== undefined) {
                return '<div class="section"><h2>Stream publishers</h2><div class="hider updatable">' +
                    format('streamPublishersList', {'publishers': publishers}) +
                    '</div></div>';
            } else {
                return '';
            }
        } else {
            return '';
        }
    });
});

NAVIGATION['Stream'] = ['#/stream/connections', "monitoring"];

var ALL_STREAM_CONNECTION_COLUMNS =
     {'Overview': [['user',   'User name', true],
                   ['state',  'State',     true]],
      'Details': [['ssl',            'TLS',      true],
                  ['ssl_info',       'TLS details',    false],
                  ['protocol',       'Protocol',       true],
                  ['frame_max',      'Frame max',      false],
                  ['auth_mechanism', 'Auth mechanism', false],
                  ['client',         'Client',         false]],
      'Network': [['from_client',  'From client',  true],
                  ['to_client',    'To client',    true],
                  ['heartbeat',    'Heartbeat',    false],
                  ['connected_at', 'Connected at', false]]};

var DISABLED_STATS_STREAM_CONNECTION_COLUMNS =
     {'Overview': [['user',   'User name', true],
                   ['state',  'State',     true]]};

COLUMNS['streamConnections'] = disable_stats?DISABLED_STATS_STREAM_CONNECTION_COLUMNS:ALL_STREAM_CONNECTION_COLUMNS;

function renderStreamConnections() {
  render({'connections': {path: url_pagination_template_context('stream/connections', 'streamConnections', 1, 100),
                          options: {sort:true}}},
                          'streamConnections', '#/stream/connections');
}

function link_stream_conn(vhost, name) {
  return _link_to(short_conn(name), '#/stream/connections/' + esc(vhost) + '/' + esc(name));
}

RENDER_CALLBACKS['streamConnections'] = function() { renderStreamConnections() };

CONSUMER_OWNER_FORMATTERS.push({
    order: 0, formatter: function(consumer) {
        if (consumer.consumer_tag.startsWith('stream.subid-')) {
            return link_stream_conn(
                consumer.queue.vhost,
                consumer.channel_details.connection_name
            );
        } else {
            return undefined;
        }
    }
});

CONSUMER_OWNER_FORMATTERS.sort(CONSUMER_OWNER_FORMATTERS_COMPARATOR);

