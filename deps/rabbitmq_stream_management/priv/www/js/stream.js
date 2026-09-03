import { dispatcher_add, render } from './main.js';
import {
    NAVIGATION,
    HELP,
    COLUMNS,
    ALL_COLUMNS,
    RENDER_CALLBACKS,
    QUEUE_EXTRA_CONTENT_REQUESTS,
    QUEUE_EXTRA_CONTENT
} from './global.js';
import {
    _link_to,
    esc,
    fmt_bytes,
    fmt_detail_rate,
    fmt_rate,
    CONSUMER_OWNER_FORMATTERS,
    CONSUMER_OWNER_FORMATTERS_COMPARATOR
} from './formatters.js';

dispatcher_add(function(sammy) {
    sammy.get('#/stream-connections', function() {
        render({}, 'stream-connections', '#/stream-connections');
    });
    sammy.get('#/stream-connections/:name', function() {
            render({'connection': '/stream/connections/' + esc(this.params['name'])},
                   'stream-connection', '#/stream-connections');
        });
});

if (!NAVIGATION['Explore']) {
    NAVIGATION['Explore'] = [{}, 'management', true];
}
NAVIGATION['Explore'][0]['Stream Connections'] = ['#/stream-connections', 'user'];

HELP['stream-publisher-count'] = 'Total number of stream publishers using this connection.';
HELP['stream-consumer-count'] = 'Total number of stream consumers using this connection.';
HELP['stream-credits'] = 'Number of credits granted by the consumer.';

function link_stream_conn(name) {
    return _link_to(name, '#/stream-connections/' + esc(name));
}

const ALL_STREAM_CONNECTION_COLUMNS = {
    'overview': [
        {name: 'node', path: 'node', type: 'string'},
        {name: 'name', path: 'name', type: 'string'},
        {name: 'publishers', path: 'publishers', type: 'number'},
        {name: 'consumers', path: 'consumers', type: 'number'},

        {name: 'rate-in', path: 'stats.publish_rate_details.rate', type: 'number'},
        {name: 'rate-out', path: 'stats.command_out_rate_details.rate', type: 'number'}
    ]
};

const DISABLED_STATS_STREAM_CONNECTION_COLUMNS = {
    'overview': [
        {name: 'node', path: 'node', type: 'string'},
        {name: 'name', path: 'name', type: 'string'},
        {name: 'publishers', path: 'publishers', type: 'number'},
        {name: 'consumers', path: 'consumers', type: 'number'}
    ]
};

function renderStreamConnections(sammy) {
    const sammyCurrentParams = sammy.params;
    let cols = ALL_STREAM_CONNECTION_COLUMNS['overview'];

    if(!sammy.app.isStatsEnabled()) {
        cols = DISABLED_STATS_STREAM_CONNECTION_COLUMNS['overview'];
    }

    render(
        {
            'connections': {
                path: '/stream/connections',
                options: {
                    sort: true,
                    page: true,
                    pagination_params: sammyCurrentParams,
                }
            }
        },
        'stream-connections',
        '#/stream-connections',
        function(items) {
            sammy.app.storePaginationParams('stream-connections', items);
        },
        cols
    );
}

if (typeof COLUMNS !== 'undefined' && COLUMNS) {
    COLUMNS['streamConnections'] = ALL_STREAM_CONNECTION_COLUMNS;
} else {
    ALL_COLUMNS['streamConnections'] = ALL_STREAM_CONNECTION_COLUMNS;
}

RENDER_CALLBACKS['streamConnections'] = function(sammy) {
    renderStreamConnections(sammy);
};

QUEUE_EXTRA_CONTENT_REQUESTS.push(function(vhost, name) {
    return {
        'stream_publishers': '/queues/' + esc(vhost) + '/' + esc(name) + '/stream-publishers',
        'stream_consumers': '/queues/' + esc(vhost) + '/' + esc(name) + '/stream-consumers'
    };
});

QUEUE_EXTRA_CONTENT.push(function(queue, extra_data) {
    if(!extra_data.stream_publishers || extra_data.stream_publishers.length === 0) {
        delete extra_data.stream_publishers;
    }
    if(!extra_data.stream_consumers || extra_data.stream_consumers.length === 0) {
        delete extra_data.stream_consumers;
    }
    return format_stream_publishers_and_consumers(queue, extra_data);
});

function format_stream_publishers_and_consumers(queue, extra_data) {
    if(!extra_data.stream_publishers && !extra_data.stream_consumers) {
        return '';
    }
    const sammy = {
        params: {
            vhost: queue.vhost,
            name: queue.name
        }
    };
    let res = '';
    res += format_stream_publishers(extra_data.stream_publishers);
    res += format_stream_consumers(extra_data.stream_consumers);

    return res;
}

function format_stream_publishers(publishers) {
    let res = '';
    if (publishers && publishers.length > 0) {
        res += '<h3>Stream Publishers</h3>';
        res += '<table class="list">';
        res += '<thead><tr>';
        res += '<th>Connection</th>';
        res += '<th>Publisher ID</th>';
        res += '<th>Reference</th>';
        res += '<th>Rate</th>';
        res += '</tr></thead>';
        res += '<tbody>';
        for (let i = 0; i < publishers.length; i++) {
            const pub = publishers[i];
            res += '<tr>';
            res += '<td>' + link_stream_conn(pub.connection_name) + '</td>';
            res += '<td>' + esc(pub.publisher_id) + '</td>';
            res += '<td>' + esc(pub.reference) + '</td>';
            res += '<td>' + fmt_detail_rate(pub, 'rate') + '</td>';
            res += '</tr>';
        }
        res += '</tbody>';
        res += '</table>';
    }
    return res;
}

function format_stream_consumers(consumers) {
    let res = '';
    if (consumers && consumers.length > 0) {
        res += '<h3>Stream Consumers</h3>';
        res += '<table class="list">';
        res += '<thead><tr>';
        res += '<th>Connection</th>';
        res += '<th>Subscription ID</th>';
        res += '<th>Credits</th>';
        res += '<th>Active Single Active Consumer</th>';
        res += '<th>Active Single Active Consumer Property</th>';
        res += '<th>Rate</th>';
        res += '</tr></thead>';
        res += '<tbody>';
        for (let i = 0; i < consumers.length; i++) {
            const cons = consumers[i];
            res += '<tr>';
            res += '<td>' + link_stream_conn(cons.connection_name) + '</td>';
            res += '<td>' + esc(cons.subscription_id) + '</td>';
            res += '<td>' + esc(cons.credits) + '</td>';
            res += '<td>' + esc(cons.active_single_active_consumer) + '</td>';
            res += '<td>' + esc(cons.active_single_active_consumer_property) + '</td>';
            res += '<td>' + fmt_detail_rate(cons, 'rate') + '</td>';
            res += '</tr>';
        }
        res += '</tbody>';
        res += '</table>';
    }
    return res;
}

CONSUMER_OWNER_FORMATTERS.push({
    order: 20,
    formatter: function(consumer) {
        const cd = consumer.channel_details;
        if (cd && cd.name && cd.name.startsWith('stream: ')) {
            const conn_name = cd.name.substring('stream: '.length);
            let owner = link_stream_conn(conn_name);
            if (consumer.subscription_id) {
                owner += ' (sub id: ' + esc(consumer.subscription_id) + ')';
            }
            return owner;
        }
        return undefined;
    }
});

export {
    renderStreamConnections,
    link_stream_conn,
    ALL_STREAM_CONNECTION_COLUMNS,
    DISABLED_STATS_STREAM_CONNECTION_COLUMNS
};

if (typeof window !== 'undefined') {
    Object.assign(window, {
        renderStreamConnections,
        link_stream_conn,
        ALL_STREAM_CONNECTION_COLUMNS,
        DISABLED_STATS_STREAM_CONNECTION_COLUMNS
    });
}
