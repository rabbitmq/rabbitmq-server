///////////////////////
//                   //
// Genuine constants //
//                   //
///////////////////////

// Just used below
function map(list) {
    var res = {};
    for (i in list) {
        res[list[i]] = '';
    }
    return res;
}

// Extension arguments that we know about and present specially in the UI.
var KNOWN_ARGS = {'alternate-exchange':        {'short': 'AE',  'type': 'string'},
                  'x-message-ttl':             {'short': 'TTL', 'type': 'int'},
                  'x-expires':                 {'short': 'Exp', 'type': 'int'},
                  'x-max-length':              {'short': 'Lim', 'type': 'int'},
                  'x-max-length-bytes':        {'short': 'Lim B', 'type': 'int'},
                  'x-delivery-limit':          {'short': 'DlL', 'type': 'int'},
                  'x-overflow':                {'short': 'Ovfl', 'type': 'string'},
                  'x-dead-letter-exchange':    {'short': 'DLX', 'type': 'string'},
                  'x-dead-letter-routing-key': {'short': 'DLK', 'type': 'string'},
                  'x-queue-master-locator':    {'short': 'ML', 'type': 'string'},
                  'x-queue-leader-locator':    {'short': 'LL', 'type': 'string'},
                  'x-max-priority':            {'short': 'Pri', 'type': 'int'},
                  'x-single-active-consumer':  {'short': 'SAC', 'type': 'boolean'}};

// Things that are like arguments that we format the same way in listings.
var IMPLICIT_ARGS = {'durable':         {'short': 'D',    'type': 'boolean'},
                     'auto-delete':     {'short': 'AD',   'type': 'boolean'},
                     'internal':        {'short': 'I',    'type': 'boolean'},
                     'exclusive':       {'short': 'Excl', 'type': 'boolean'},
                     'messages delayed':{'short': 'DM',   'type': 'int'}};

// Both the above
var ALL_ARGS = {};
for (var k in IMPLICIT_ARGS) ALL_ARGS[k] = IMPLICIT_ARGS[k];
for (var k in KNOWN_ARGS)    ALL_ARGS[k] = KNOWN_ARGS[k];

var NAVIGATION = {'Overview':    ['#/',            "management"],
                  'Connections': ['#/connections', "management", true],
                  'Channels':    ['#/channels',    "management", true],
                  'Exchanges':   ['#/exchanges',   "management", true],
                  'Queues and Streams': ['#/queues',      "management", true],
                  'Admin':
                    [{'Users':         ['#/users',              "administrator"],
                      'Virtual Hosts': ['#/vhosts',             "administrator"],
                      'Feature Flags': ['#/feature-flags',      "administrator"],
                      'Deprecated Features': ['#/deprecated-features',      "administrator"],
                      'Policies':      ['#/policies',           "management"],
                      'Limits':        ['#/limits',             "management"],
                      'Cluster':       ['#/cluster-name',       "administrator"]},
                     "management",
                     true
                     ]
                 };

var CHART_RANGES = {'global': [], 'basic': []};
var ALL_CHART_RANGES = {};

var ALL_COLUMNS =
    {'exchanges' :
     {'Overview': [['type',                 'Type',                   true],
                   ['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false]],
      'Message rates': [['rate-in',         'rate in',                true],
                        ['rate-out',        'rate out',               true]]},
     'queues' :
     {'Overview': [['type',                 'Type',                   true],
                   ['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false],
                   ['consumers',            'Consumer count',         false],
                   ['consumer_capacity',    'Consumer capacity',      false],
                   ['state',                'State',                  true]],
      'Messages': [['msgs-ready',      'Ready',          true],
                   ['msgs-unacked',    'Unacknowledged', true],
                   ['msgs-ram',        'In memory',      false],
                   ['msgs-persistent', 'Persistent',     false],
                   ['msgs-total',      'Total',          true]],
      'Message bytes': [['msg-bytes-ready',      'Ready',          false],
                        ['msg-bytes-unacked',    'Unacknowledged', false],
                        ['msg-bytes-ram',        'In memory',      false],
                        ['msg-bytes-persistent', 'Persistent',     false],
                        ['msg-bytes-total',      'Total',          false]],
      'Message rates': [['rate-incoming',  'incoming',      true],
                        ['rate-deliver',   'deliver / get', true],
                        ['rate-redeliver', 'redelivered',   false],
                        ['rate-ack',       'ack',           true]]},
     'channels' :
     {'Overview': [['user',  'User name', true],
                   ['mode',  'Mode',      true],
                   ['state', 'State',     true]],
      'Details': [['msgs-unconfirmed', 'Unconfirmed', true],
                  ['consumer-count',   'Consumer count', false],
                  ['prefetch',         'Prefetch',    true],
                  ['msgs-unacked',     'Unacked',     true]],
      'Transactions': [['msgs-uncommitted', 'Msgs uncommitted', false],
                       ['acks-uncommitted', 'Acks uncommitted', false]],
      'Message rates': [['rate-publish',   'publish',            true],
                        ['rate-confirm',   'confirm',            true],
                        ['rate-unroutable-drop',    'unroutable (drop)', true],
                        ['rate-unroutable-return',    'unroutable (return)', false],
                        ['rate-deliver',   'deliver / get',      true],
                        ['rate-redeliver', 'redelivered',        false],
                        ['rate-ack',       'ack',                true]]},
     'connections':
     {'Overview': [['container_id', 'Container ID', true],
                   ['user',   'User name', true],
                   ['state',  'State',     true]],
      'Details': [['ssl',            'TLS',      true],
                  ['ssl_info',       'TLS details',    false],
                  ['protocol',       'Protocol',       true],
                  ['channels',       'Channels',       true],
                  ['channel_max',    'Channel max',    false],
                  ['frame_max',      'Frame max',      false],
                  ['auth_mechanism', 'SASL auth mechanism', false],
                  ['client',         'Client',         false]],
      'Network': [['from_client',  'From client',  true],
                  ['to_client',    'To client',    true],
                  ['heartbeat',    'Heartbeat',    false],
                  ['connected_at', 'Connected at', false]]},

     'vhosts':
     {'Overview': [['default-queue-type', 'Default queue type', false],
                   ['cluster-state',   'Cluster state',  false],
                   ['description',   'Description',  false],
                   ['tags',   'Tags',  false]],
      'Messages': [['msgs-ready',      'Ready',          true],
                   ['msgs-unacked',    'Unacknowledged', true],
                   ['msgs-total',      'Total',          true]],
      'Network': [['from_client',  'From client',  true],
                  ['to_client',    'To client',    true]],
      'Message rates': [['rate-publish', 'publish',       true],
                        ['rate-deliver', 'deliver / get', true]]},
     'overview':
     {'Statistics': [['file_descriptors',   'File descriptors',   true],
                     ['erlang_processes',   'Erlang processes',   true],
                     ['memory',             'Memory',             true],
                     ['disk_space',         'Disk space',         true]],
      'General': [['uptime',    'Uptime',       true],
                  ['cores',     'Cores',        true],
                  ['info',      'Info',         true],
                  ['reset_stats',     'Reset stats',        true]]}};

var DISABLED_STATS_COLUMNS =
    {'exchanges' :
     {'Overview': [['type',                 'Type',                   true],
                   ['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false]]},
     'queues' :
     {'Overview': [['type',                 'Type',                   true],
                   ['features',             'Features (with policy)', true],
                   ['features_no_policy',   'Features (no policy)',   false],
                   ['policy',               'Policy',                 false],
                   ['state',                'State',                  true]],
      'Messages': [['msgs-ready',      'Ready',          true],
                   ['msgs-unacked',    'Unacknowledged', true],
                   ['msgs-ram',        'In memory',      false],
                   ['msgs-persistent', 'Persistent',     false],
                   ['msgs-total',      'Total',          true]]},
     'connections':
     {'Overview': [['user',   'User name', true],
                   ['state',  'State',     true]]},

     'vhosts':
     {'Overview': [['cluster-state',   'Cluster state',  false]]}};

var COLUMNS;

var RENDER_CALLBACKS = {};

const QUEUE_EXTRA_CONTENT = [];
const QUEUE_EXTRA_CONTENT_REQUESTS = [];

// All help ? popups
var HELP = {
    'delivery-limit':
      'The number of allowed unsuccessful delivery attempts. Once a message has been delivered unsuccessfully more than this many times it will be dropped or dead-lettered, depending on the queue configuration. The default is always 20. A value of -1 or lower sets the limit to "unlimited".',

    'exchange-auto-delete':
      'If yes, the exchange will delete itself after at least one queue or exchange has been bound to this one, and then all queues or exchanges have been unbound.',

    'exchange-internal':
      'If yes, clients cannot publish to this exchange directly. It can only be used with exchange to exchange bindings.',

    'exchange-alternate':
      'If messages to this exchange cannot otherwise be routed, send them to the alternate exchange named here.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/ae.html">alternate-exchange</a>" argument.)',

    'queue-message-ttl':
    'How long a message published to a queue can live before it is discarded (milliseconds).<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/ttl.html#per-queue-message-ttl">x-message-ttl</a>" argument.)',

    'queue-consumer-timeout':
    'If a consumer does not ack its delivery for more than the <a href="https://www.rabbitmq.com/consumers.html#acknowledgement-timeout">timeout value</a> (30 minutes by default), its channel will be closed with a <code>PRECONDITION_FAILED</code> channel exception.',

    'queue-expires':
      'How long a queue can be unused for before it is automatically deleted (milliseconds).<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/ttl.html#queue-ttl">x-expires</a>" argument.)',

    'queue-max-length':
      'How many (ready) messages a queue can contain before it starts to drop them from its head.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/maxlength.html">x-max-length</a>" argument.)',

    'queue-max-length-bytes':
      'Total body size for ready messages a queue can contain before it starts to drop them from its head.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/maxlength.html">x-max-length-bytes</a>" argument.)',

    'queue-max-age':
      'How long a message published to a stream queue can live before it is discarded.',

    'queue-stream-filter-size-bytes':
      'Size of the filter data attached to each stream chunk.<br/>(Sets the x-stream-filter-size-bytes argument.)',

    'queue-auto-delete':
      'If yes, the queue will delete itself after at least one consumer has connected, and then all consumers have disconnected.',

    'queue-dead-letter-exchange':
      'Optional name of an exchange to which messages will be republished if they are rejected or expire.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/dlx.html">x-dead-letter-exchange</a>" argument.)',

    'queue-dead-letter-routing-key':
      'Optional replacement routing key to use when a message is dead-lettered. If this is not set, the message\'s original routing key will be used.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/dlx.html">x-dead-letter-routing-key</a>" argument.)',

    'queue-dead-letter-strategy':
      'Valid values are <code>at-most-once</code> or <code>at-least-once</code>. It defaults to <code>at-most-once</code>. If <code>at-least-once</code> is set, <code>Overflow behaviour</code> must be set to <code>reject-publish</code>. Otherwise, dead letter strategy will fall back to <code>at-most-once</code>.',

    'queue-single-active-consumer':
      'If set, makes sure only one consumer at a time consumes from the queue and fails over to another registered consumer in case the active one is cancelled or dies.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/consumers.html#single-active-consumer">x-single-active-consumer</a>" argument.)',

    'queue-max-priority':
      'Maximum number of priority levels for the queue to support; if not set, the queue will not support message priorities.<br/>(Sets the "<a target="_blank" href="https://rabbitmq.com/priority.html">x-max-priority</a>" argument.)',

    'queue-max-age':
      'Sets the data retention for stream queues in time units </br>(Y=Years, M=Months, D=Days, h=hours, m=minutes, s=seconds).<br/>E.g. "1h" configures the stream to only keep the last 1 hour of received messages.</br></br>(Sets the x-max-age argument.)',

    'queue-overflow':
      'Sets the <a target="_blank" href="https://www.rabbitmq.com/maxlength.html#overflow-behaviour">queue overflow behaviour</a>. This determines what happens to messages when the maximum length of a queue is reached. Valid values are <code>drop-head</code>, <code>reject-publish</code> or <code>reject-publish-dlx</code>',

    'quorum-queue-overflow':
      'Sets the <a target="_blank" href="https://www.rabbitmq.com/maxlength.html#overflow-behaviour">queue overflow behaviour</a>. This determines what happens to messages when the maximum length of a queue is reached. Valid values for quorum queues are <code>drop-head</code> and <code>reject-publish</code>.',

    'queue-master-locator':
       'Deprecated: please use `queue-leader-locator` instead. <a target="_blank" href="https://www.rabbitmq.com/docs/clustering#replica-placement">Controls which node the queue will be running on.</a>',

    'queue-leader-locator':
       'Set the rule by which the queue leader is located when declared on a cluster of nodes. Valid values are <code>client-local</code> (default) and <code>balanced</code>.',

    'queue-initial-cluster-size':
       'Set the queue initial cluster size.',

    'queue-type':
       'Set the queue type, determining the type of queue to use: raft-based high availability or classic queue. Valid values are <code>quorum</code> or <code>classic</code>. It defaults to <code>classic</code>. <br/>',

    'queue-messages':
      '<p>Message counts.</p><p>Note that some messages can be in memory and on disk at the same time.',

    'queue-messages-stream':
      '<p>Approximate message counts.</p><p>Note that streams store some entries that are not user messages such as offset tracking data which is included in this count. Thus this value will never be completely correct.</p>',

    'queue-dead-lettered':
      'Applies to messages dead-lettered with dead-letter-strategy <code>at-least-once</code>.',

    'queue-delivery-limit':
      'The number of times a message can be returned to this queue before it is dead-lettered (if configured) or dropped.',

    'queue-message-body-bytes':
      '<p>The sum total of the sizes of the message bodies in this queue. This only counts message bodies; it does not include message properties (including headers) or metadata used by the queue.</p><p>Note that some messages can be in memory and on disk at the same time.</p><p>For classic queues, if a message larger than <code>queue_index_embed_msgs_below</code> (4KB by default) is routed to multiple queues, its body will be stored only once and shared between queues. The value shown here does not take this optimization into account.</p>',

    'queue-process-memory':
      'Total memory used by this queue process. This does not include in-memory message bodies (which may be shared between queues and will appear in the global "binaries" memory) but does include everything else.',

    'queue-consumer-capacity':
      'Fraction of the time that the queue is able to immediately deliver messages to consumers. Will be 0 for queues that have no consumers. If this number is less than 100% you may be able to deliver messages faster if: \
        <ul> \
          <li>There were more consumers or</li> \
          <li>The consumers were faster or</li> \
          <li>The consumer channels used a higher prefetch count</li> \
        </ul>',

    'internal-users-only':
      'Only users within the internal RabbitMQ database are shown here. Other users (e.g. those authenticated over LDAP) will not appear.',

    'export-definitions':
    'The definitions consist of users, virtual hosts, permissions, parameters, exchanges, queues, policies and bindings. They do not include the contents of queues. Exclusive queues will not be exported.',

    'export-definitions-vhost':
    'The definitions exported for a single virtual host consist of exchanges, queues, bindings and policies.',

    'import-definitions':
      'The definitions that are imported will be merged with the current definitions. If an error occurs during import, any changes made will not be rolled back.',

    'import-definitions-vhost':
    'For a single virtual host, only exchanges, queues, bindings and policies are imported.',

    'exchange-rates-incoming':
      'The incoming rate is the rate at which messages are published directly to this exchange.',

    'exchange-rates-outgoing':
      'The outgoing rate is the rate at which messages enter queues, having been published directly to this exchange.',

    'channel-mode':
      'Channel guarantee mode. Can be one of the following, or neither:<br/> \
      <dl> \
        <dt><abbr title="Confirm">C</abbr> &ndash; <a target="_blank" href="https://www.rabbitmq.com/confirms.html">confirm</a></dt> \
        <dd>Channel will send streaming publish confirmations.</dd> \
        <dt><abbr title="Transactional">T</abbr> &ndash; <a target="_blank" href="https://www.rabbitmq.com/amqp-0-9-1-reference.html#class.tx">transactional</a></dt> \
        <dd>Channel is transactional.</dd> \
      </dl>',

    'channel-prefetch':
      'Channel prefetch count.\
       <p> \
         Each channel can have a prefetch count. The prefetch is the number of messages that will be held \
         by the client. Setting a value of 0 will result in an unlimited prefetch. \
       </p> \
       ',

    'file-descriptors':
      '<p>File descriptor count and limit, as reported by the operating \
      system. The count includes network sockets and file handles.</p> \
      <p>To optimize disk access RabbitMQ uses as many file descriptors as \
      needed, so the limit must be high enough for safe operation.</p> \
      <p>To change the limit on Unix / Linux, use "ulimit -n". To change \
      the limit on Windows, set the ERL_MAX_PORTS environment variable</p> \
      <p>To report used file handles on Windows, handle.exe from \
      sysinternals must be installed in your path. You can download it \
      <a target="_blank" href="https://technet.microsoft.com/en-us/sysinternals/bb896655">here</a>.</p>',

    'memory-alarm':
      '<p>The <a target="_blank" href="https://www.rabbitmq.com/memory.html#memsup">memory \
      alarm</a> for this node has gone off. It will block \
      incoming network traffic until the memory usage drops below \
      the watermark.</p>\
      <p>Note that the pale line in this case indicates the high watermark \
      in relation to how much memory is used in total. </p>',

    'disk-free-alarm':
      'The <a target="_blank" href="https://www.rabbitmq.com/memory.html#diskfreesup">disk \
      free space alarm</a> for this node has gone off. It will block \
      incoming network traffic until the amount of free space exceeds \
      the limit.',

    'message-get-requeue':
      '<p>Clicking "Get Message(s)" will consume messages from the queue. \
      If requeue is set the message will be put back into the queue in place, \
      but "redelivered" will be set on the message.</p> \
      <p>If requeue is not set messages will be removed from the queue.</p> \
      <p>Furthermore, message payloads will be truncated to 50000 bytes.</p>',

    'message-publish-headers':
      'Headers can have any name. Only long string headers can be set here.',

    'message-publish-properties':
      '<p>You can set other message properties here (delivery mode and headers \
      are pulled out as the most common cases).</p>\
      <p>Invalid properties will be ignored. Valid properties are:</p>\
      <ul>\
      <li>content_type</li>\
      <li>content_encoding</li>\
      <li>priority</li>\
      <li>correlation_id</li>\
      <li>reply_to</li>\
      <li>expiration</li>\
      <li>message_id</li>\
      <li>timestamp</li>\
      <li>type</li>\
      <li>user_id</li>\
      <li>app_id</li>\
      <li>cluster_id</li>\
      </ul>',

    'string-base64':
    '<p>AMQP message payloads can contain any binary content. They can \
     therefore be difficult to display in a browser. The options here \
     have the following meanings:</p> \
     <dl> \
       <dt>Auto string / base64</dt> \
       <dd>If the message payload can be interpreted as a string in UTF-8 \
           encoding, do so. Otherwise return the payload encoded as \
           base64.</dd> \
       <dt>base64</dt> \
       <dd>Return the payload encoded as base64 unconditionally.</dd> \
     </dl>',

    'user-tags':
      'Comma-separated list of tags to apply to the user. Currently \
       <a target="_blank" href="https://www.rabbitmq.com/management.html#permissions">supported \
       by the management plugin</a>: \
      <dl> \
        <dt>management</dt> \
        <dd> \
          User can access the management plugin \
        </dd> \
        <dt>policymaker</dt> \
        <dd> \
          User can access the management plugin and manage policies and \
          parameters for the vhosts they have access to. \
        </dd> \
        <dt>monitoring</dt> \
        <dd> \
          User can access the management plugin and see all connections and \
          channels as well as node-related information. \
        </dd> \
        <dt>administrator</dt> \
        <dd> \
          User can do everything monitoring can do, manage users, \
          vhosts and permissions, close other user\'s connections, and manage \
          policies and parameters for all vhosts. \
        </dd> \
      </dl> \
      <p> \
        Note that you can set any tag here; the links for the above four \
        tags are just for convenience. \
      </p>',

    'queued-messages':
      '<dl>                          \
        <dt>Ready</dt>\
        <dd>Number of messages that are available to be delivered now.</dd>\
        <dt>Unacknowledged</dt>\
        <dd>Number of messages for which the server is waiting for acknowledgement.</dd>\
        <dt>Total</dt>\
        <dd>The total of these two numbers.</dd>\
      </dl>',

    'message-rates':
    'Only rates for which some activity is taking place will be shown.\
      <dl>\
        <dt>Publish</dt>\
        <dd>Rate at which messages are entering the server.</dd>\
        <dt>Publisher confirm</dt>\
        <dd>Rate at which the server is confirming publishes.</dd>\
        <dt>Deliver (manual ack)</dt>\
        <dd>Rate at which messages are delivered to consumers that use manual acknowledgements.</dd>\
        <dt>Deliver (auto ack)</dt>\
        <dd>Rate at which messages are delivered to consumers that use automatic acknowledgements.</dd>\
        <dt>Consumer ack</dt>\
        <dd>Rate at which messages are being acknowledged by consumers.</dd>\
        <dt>Redelivered</dt>\
        <dd>Rate at which messages with the \'redelivered\' flag set are being delivered. Note that these messages will <b>also</b> be counted in one of the delivery rates above.</dd>\
        <dt>Get (manual ack)</dt>\
        <dd>Rate at which messages requiring acknowledgement are being delivered in response to basic.get.</dd>\
        <dt>Get (auto ack)</dt>\
        <dd>Rate at which messages not requiring acknowledgement are being delivered in response to basic.get.</dd>\
        <dt>Get (empty)</dt>\
        <dd>Rate at which empty queues are hit in response to basic.get.</dd>\
        <dt>Return</dt>\
        <dd>Rate at which basic.return is sent to publishers for unroutable messages published with the \'mandatory\' flag set.</dd>\
      </dl>\
      <p>\
        Note that the last two items originate in queues rather than \
        channels; they may therefore be slightly out of sync with other \
        statistics.\
      </p>',

    'disk-monitoring-no-watermark' : 'There is no <a target="_blank" href="https://www.rabbitmq.com/memory.html#diskfreesup">disk space low watermark</a> set. RabbitMQ will not take any action to avoid running out of disk space.',

    'resource-counts' : 'Shows total number of objects for all virtual hosts the current user has access to.',

    'memory-use' : '<p>Note that the memory details shown here are only updated on request - they could be too expensive to calculate every few seconds on a busy server.</p><p><a target="_blank" href="https://www.rabbitmq.com/memory-use.html">Read more</a> on memory use.</p>',

    'memory-calculation-strategy-breakdown' : '<p>The setting <code>vm_memory_calculation_strategy</code> defines which of the below memory values is used to check if the memory usage reaches the watermark or paging to disk is required.</p><p><a target="_blank" href="https://www.rabbitmq.com/memory-use.html">Read more</a> on memory use.</p>',

    'memory-calculation-strategy' : '<p>This value can be calculated using different strategies, see the <code>vm_memory_calculation_strategy</code> configuration setting.</p><p><a target="_blank" href="https://www.rabbitmq.com/memory-use.html">Read more</a> on memory use.</p>',

    'binary-use' : '<p>Binary accounting is not exact; binaries are shared between processes (and thus the same binary might be counted in more than one section), and the VM does not allow us to track binaries that are not associated with processes (so some binary use might not appear at all).</p>',

    'policy-federation-upstream-set' :
    'A string; only if the federation plugin is enabled. Chooses the name of a set of upstreams to use with federation, or "all" to use all upstreams. Incompatible with <code>federation-upstream</code>.',

    'policy-federation-upstream' :
    'A string; only if the federation plugin is enabled. Chooses a specific upstream set to use for federation. Incompatible with <code>federation-upstream-set</code>.',

    'handle-exe' : 'In order to monitor the number of file descriptors in use on Windows, RabbitMQ needs the <a href="https://technet.microsoft.com/en-us/sysinternals/bb896655" target="_blank">handle.exe command line tool from Microsoft</a>. Download it and place it in the path (e.g. in C:\Windows).',

    'filter-regex' :
    'Whether to enable regular expression matching. Both string literals \
    and regular expressions are matched in a case-insensitive manner.<br/><br/>',

    'consumer-active' :
    'Whether the consumer is active or not, i.e. whether the consumer can get messages from the queue. \
    When single active consumer is enabled for the queue, only one consumer at a time is active. \
    When single active consumer is disabled for the queue, consumers are active by default. \
    For a quorum queue, a consumer can be inactive because its owning node is suspected down. <br/><br/> \
    (<a href="https://www.rabbitmq.com/consumers.html#active-consumer" target="_blank">Documentation</a>)',

    'consumer-owner' :
    '<a href="https://www.rabbitmq.com/consumers.html">AMQP 0-9-1 consumers</a> belong to a channel, \
    and <a href="https://www.rabbitmq.com/stream.html">stream consumers</a> belong to a stream connection.',

    'plugins' :
    'Note that only plugins which are both explicitly enabled and running are shown here.',

    'io-operations':
    'Rate of I/O operations. Only operations performed by the message \
      persister are shown here (e.g. changes in the schema data store or writes \
      to the log files are not shown).\
      <dl>\
        <dt>Read</dt>\
        <dd>Rate at which data is read from the disk.</dd>\
        <dt>Write</dt>\
        <dd>Rate at which data is written to the disk.</dd>\
        <dt>Seek</dt>\
        <dd>Rate at which the broker switches position while reading or \
         writing to disk.</dd>\
        <dt>Sync</dt>\
        <dd>Rate at which the node invokes <code>fsync()</code> to ensure \
         data is flushed to disk.</dd>\
        <dt>Reopen</dt>\
        <dd>Rate at which the node recycles file handles in order to support \
         more queues than it has file handles. If this operation is occurring \
         frequently you may get a performance boost from increasing the number \
         of file handles available.</dd>\
      </dl>',

    'mnesia-transactions':
    'Rate at which schema data store transactions are initiated on this node (this node \
     will also take part in the transactions initiated on other nodes).\
      <dl>\
        <dt>RAM only</dt>\
        <dd>Rate at which RAM-only schema data store transactions take place (e.g. creation or \
            deletion of transient queues).</dd>\
        <dt>Disk</dt>\
        <dd>Rate at which all schema data store transactions take place (e.g. \
            creation or deletion of durable queues).</dd>\
      </dl>',

    'persister-operations-msg':
    'Rate at which per-message persister operations take place on this node. See \
     <a href="https://www.rabbitmq.com/persistence-conf.html" target="_blank">here</a> \
     for more information on the persister. \
      <dl>\
        <dt>QI Journal</dt>\
        <dd>Rate at which message information (publishes, deliveries and \
            acknowledgements) is written to queue index journals.</dd>\
        <dt>Store Read</dt>\
        <dd>Rate at which messages are read from the message store.</dd>\
        <dt>Store Write</dt>\
        <dd>Rate at which messages are written to the message store.</dd>\
      </dl>',

    'persister-operations-bulk':
    'Rate at which whole-file persister operations take place on this node. See \
     <a href="https://www.rabbitmq.com/persistence-conf.html" target="_blank">here</a> \
     for more information on the persister. \
      <dl>\
        <dt>QI Read</dt>\
        <dd>Rate at which queue index segment files are read.</dd>\
        <dt>QI Write</dt>\
        <dd>Rate at which queue index segment files are written. </dd>\
      </dl>',

    'gc-operations':
    'Rate at which garbage collection operations take place on this node.',

    'gc-bytes':
    'Rate at which memory is reclaimed by the garbage collector on this node.',

    'context-switches-operations':
    'Rate at which runtime context switching takes place on this node.',

    'process-reductions':
    'Rate at which reductions take place on this process.',

    'connection-operations':
    ' <dl>\
        <dt>Created</dt>\
        <dd>Rate at which connections are created.</dd>\
        <dt>Closed</dt>\
        <dd>Rate at which connections are closed.</dd>\
      </dl> ',

    'channel-operations':
    ' <dl>\
        <dt>Created</dt>\
        <dd>Rate at which channels are created.</dd>\
        <dt>Closed</dt>\
        <dd>Rate at which channels are closed.</dd>\
      </dl> ',

    'queue-operations':
    ' <dl>\
        <dt>Declared</dt>\
        <dd>Rate at which queues are declared by clients.</dd>\
        <dt>Created</dt>\
        <dd>Rate at which queues are created. Declaring a queue that already exists counts for a "Declared" event, but not for a "Created" event. </dd>\
        <dt>Deleted</dt>\
        <dd>Rate at which queues are deleted.</dd>\
     </dl> ',

    'container-id':
      'Name of the client application as sent from client to RabbitMQ in the "container-id" field of the AMQP 1.0 <a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-open">open</a> frame.',

    'incoming-links':
      '<a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#section-links">Links</a> where the client is the sender/publisher and RabbitMQ is the receiver of messages.',

    'outgoing-links':
      '<a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#section-links">Links</a> where the client is the receiver/consumer and RabbitMQ is the sender of messages.',

    'target-address':
      'The "address" field of the link <a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-target">target</a>.',

    'source-address':
      'The "address" field of the link <a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-source">source</a>.',

    'amqp-source-queue':
      'The client receives messages from this queue.',

    'amqp-unconfirmed-messages':
      'Number of messages that have been sent to queues but have not been confirmed by all queues.',

    'snd-settle-mode':
      '<a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-sender-settle-mode">Sender Settle Mode</a>',

    'sender-settles':
      '"true" if the sender sends all deliveries settled to the receiver. "false" if the sender sends all deliveries initially unsettled to the receiver.',

    'outgoing-unsettled-deliveries':
      'Number of messages that have been sent to consumers but have not yet been settled/acknowledged.',

    'amqp-filter':
      'Filters are predicates that define which messages RabbitMQ sends to the receiver. Each filter in the <a target="_blank" href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-filter-set">Filter Set</a> has a name displayed in the boxes below. Hovering over a box will display the filter descriptor and the filter definition.'
};

///////////////////////////////////////////////////////////////////////////
//                                                                       //
// Mostly constant, typically get set once at startup (or rarely anyway) //
//                                                                       //
///////////////////////////////////////////////////////////////////////////

// All these are to do with hiding UI elements if
var rates_mode;                  // ...there are no fine stats
var user_administrator;          // ...user is not an admin
var is_user_policymaker;         // ...user is not a policymaker
var user_monitor;                // ...user cannot monitor
var nodes_interesting;           // ...we are not in a cluster
var vhosts_interesting;          // ...there is only one vhost
var is_op_policy_updating_enabled;      // ...editing operator policies is enabled
var queue_type;
var rabbit_versions_interesting; // ...all cluster nodes run the same version
var disable_stats;               // ...disable all stats, management only mode

// Extensions write to this, the dispatcher maker reads it
var dispatcher_modules = [];

// We need to know when all extension script files have loaded
var extension_count;

// The dispatcher needs access to the Sammy app
var app;

// Used for the new exchange form, and to display broken exchange types
var exchange_types;

// Used for access control
var user_tags;
var user;
var ac = new AccessControl();
var display = new DisplayControl();

var ui_data_model = {
  vhosts: [],
  nodes: [],

};

// Access control

function AccessControl() {

  this.update = function(user, ui_data_model) {
    this.user = user;
    this.user_tags = expand_user_tags(user.tags);
    this.ui_data_model = ui_data_model;
  };
  this.isMonitoringUser = function() {
    if (this.user_tags)
      return this.user_tags.includes("monitoring");
    else return false;
  };
  this.isAdministratorUser = function() {
    if (this.user_tags)
      return this.user_tags.includes("administrator");
    else return false;
  };
  this.isPolicyMakerUser = function() {
    if (this.user_tags)
      return this.user_tags.includes("policymaker");
    else return false;
  };
  this.canAccessVhosts = function() {
    if (this.ui_data_model)
      return this.ui_data_model.vhosts.length > 0;
    else return false;
  };
  this.canListNodes = function() {
    if (this.ui_data_model)
      return this.isMonitoringUser() && this.ui_data_model.nodes.length > 1;
    else return false;
  };

};

function DisplayControl() {
  this.nodes = false
  this.vhosts = false
  this.rabbitmqVersions = false

  this.update = function(overview, ui_data_model) {    
    this.nodes = ac.canListNodes() && ui_data_model.nodes.length > 1
    this.vhosts = ac.canAccessVhosts()
    this.rabbitmqVersions = false
    var v = '';
    for (var i = 0; i < ui_data_model.nodes.length; i++) {
        var v1 = fmt_rabbit_version(ui_data_model.nodes[i].applications);
        if (v1 != 'unknown') {
            if (v != '' && v != v1) this.rabbitmqVersions = true;
            v = v1;
        }
    }
    this.data = ui_data_model;
  }

}


// Set up the above vars
function setup_global_vars(overview) {
    rates_mode = overview.rates_mode;
    is_op_policy_updating_enabled = overview.is_op_policy_updating_enabled;
    user_tags = expand_user_tags(user.tags);
    user_administrator = jQuery.inArray("administrator", user_tags) != -1;
    is_user_policymaker = jQuery.inArray("policymaker", user_tags) != -1;
    user_monitor = jQuery.inArray("monitoring", user_tags) != -1;
    exchange_types = overview.exchange_types.map(function(xt) { return xt.name; });

    cluster_name = fmt_escape_html(overview.cluster_name);
    $('#logout').before(
      '<li>Cluster ' + (user_administrator ?  '<a href="#/cluster-name">' + cluster_name + '</a>' : cluster_name) + '</li>'
    );

    user_name = fmt_escape_html(user.name); 
    $('#header #logout').prepend(
      'User ' + (user_administrator && user.is_internal_user ?  '<a href="#/users/' + user_name + '">' + user_name + '</a>' : user_name)
    );

    var product = overview.rabbitmq_version;
    if (overview.product_name && overview.product_version) {
        product = overview.product_name + ' ' + overview.product_version;
    }

    $('#versions').html(
      '<abbr title="Available exchange types: ' + exchange_types.join(", ") + '">' + fmt_escape_html(product) + '</abbr>' +
      '<abbr title="' + fmt_escape_html(overview.erlang_full_version) + '">Erlang ' + fmt_escape_html(overview.erlang_version) + '</abbr>'
    );
    nodes_interesting = false;
    rabbit_versions_interesting = false;
    if (user_monitor) {
        var nodes = JSON.parse(sync_get('/nodes'));
        if (nodes.length > 1) {
            nodes_interesting = true;
            var v = '';
            for (var i = 0; i < ui_data_model.nodes.length; i++) {
                var v1 = fmt_rabbit_version(ui_data_model.nodes[i].applications);
                if (v1 != 'unknown') {
                    if (v != '' && v != v1) rabbit_versions_interesting = true;
                    v = v1;
                }
            }
        }
    }
    vhosts_interesting = ui_data_model.vhosts.length > 1;

    queue_type = "default";
    current_vhost = get_pref('vhost');
    exchange_types = overview.exchange_types;

    disable_stats = overview.disable_stats;
    enable_queue_totals = overview.enable_queue_totals;
    COLUMNS = disable_stats?DISABLED_STATS_COLUMNS:ALL_COLUMNS;

    setup_chart_ranges(overview.sample_retention_policies);
}

function setup_chart_ranges(srp) {
    var range_types = ['global', 'basic'];
    var default_ranges = {
        60:    ['60|5', 'Last minute'],
        600:   ['600|5', 'Last ten minutes'],
        3600:  ['3600|60', 'Last hour'],
        28800: ['28800|600', 'Last eight hours'],
        86400: ['86400|1800', 'Last day']
    };

    for (var range in default_ranges) {
        var data = default_ranges[range];
        var range = data[0];
        var desc = data[1];
        ALL_CHART_RANGES[range] = desc;
    }

    for (var i = 0; i < range_types.length; ++i) {
        var range_type = range_types[i];
        if (srp.hasOwnProperty(range_type)) {
            var srp_range_types = srp[range_type];
            var last_minute_added = false;
            for (var j = 0; j < srp_range_types.length; ++j) {
                var srp_range = srp_range_types[j];
                if (default_ranges.hasOwnProperty(srp_range)) {
                    if (srp_range === 60) {
                        last_minute_added = true;
                    }
                    var v = default_ranges[srp_range];
                    CHART_RANGES[range_type].push(v);
                }
            }
            if (!last_minute_added) {
                var last_minute = default_ranges[60];
                CHART_RANGES[range_type].unshift(last_minute);
            }
        }
    }
}

function expand_user_tags(tags) {
    var new_tags = [];
    for (var i = 0; i < tags.length; i++) {
        var tag = tags[i];
        new_tags.push(tag);
        switch (tag) { // Note deliberate fall-through
            case "administrator": new_tags.push("monitoring");
                                  new_tags.push("policymaker");
            case "monitoring":    new_tags.push("management");
                                  break;
            case "policymaker":   new_tags.push("management");
            default:              break;
        }
    }
    return new_tags;
}

////////////////////////////////////////////////////
//                                                //
// Change frequently (typically every "new page") //
//                                                //
////////////////////////////////////////////////////

// Which top level template we're showing
var current_template;

// Which JSON requests do we need to populate it
var current_reqs;

// And which of those have yet to return (so we can cancel them when
// changing current_template).
var outstanding_reqs = [];

// Which tab is highlighted
var current_highlight;

// Which vhost are we looking at
var current_vhost = '';

// What is our current sort order
var current_sort;
var current_sort_reverse = false;

var current_filter = '';
var current_filter_regex_on = false;

var current_filter_regex;
var current_truncate;

// The timer object for auto-updates, and how often it goes off
var timer;
var timer_interval;

// When did we last connect successfully (for the "could not connect" error)
var last_successful_connect;

// Every 200 updates without user interaction we do a full refresh, to
// work around memory leaks in browser DOM implementations.
// TODO: maybe we don't need this any more?
var update_counter = 0;

// Holds chart data in between writing the div in an ejs and rendering
// the chart.
var chart_data = {};

// whenever a UI requests a page that doesn't exist
// because things were deleted between refreshes
var last_page_out_of_range_error = 0;

var oauth;


///////////////////////////////////////////////////////////////////////////
//                                                                       //
// Queue types                                                           //
//                                                                       //
///////////////////////////////////////////////////////////////////////////

/// this queue types are very well known to the server, at the very least
/// this collection must be validated in terms of matching server queue
/// types registry. I hope I will have time for this.
var QUEUE_TYPE = function (queue) {
    return QUEUE_TYPE[get_queue_type(queue)];
}
// TODO: while this allows for custom queues
// the proper way is to follow single source of truth
// and generate most of this on the server from queue type metadata
// including replacing tmpl's with data-driven generators
// For example server knows policy_apply_to for each queue
// and it knows what extra agruments each queue type accepts.
// So for the latter case we dont' need a template that lists
// queue args. We need iterator over server-supplied object.
QUEUE_TYPE["default"] = {
    label: "Default",
    params: {},
    policy_apply_to: "classic_queue",
    actions: {
        get_message: true,
        purge: true
    },
    tmpl: {
        "arguments"    : "classic-queue-arguments",
        // TODO: this must be generated from js objects of course.
        // and then those objects must be rendered by the server
        "user_policy_arguments": "classic-queue-user-policy-arguments",
        "operator_policy_arguments": "classic-queue-operator-policy-arguments",
        "list"   : "classic-queue-list",
        "stats"  : "classic-queue-stats",
        "node_details" : "classic-queue-node-details"
    }
};

QUEUE_TYPE["classic"] = {
    label: "Classic",
    params: {},
    policy_apply_to: "classic_queue",
    actions: {
        get_message: true,
        purge: true
    },
    tmpl: {
        "arguments"    : "classic-queue-arguments",
        "user_policy_arguments": "classic-queue-user-policy-arguments",
        "operator_policy_arguments": "classic-queue-operator-policy-arguments",
        "list"   : "classic-queue-list",
        "stats"  : "classic-queue-stats",
        "node_details" : "classic-queue-node-details"
    }
};

QUEUE_TYPE["quorum"] = {
    label: "Quorum",
    params: {
        'durable': true,
        'auto_delete': false
    },
    policy_apply_to: "quorum_queues",
    actions: {
        get_message: true,
        purge: true
    },
    tmpl: {
        "arguments"    : "quorum-queue-arguments",
        "user_policy_arguments": "quorum-queue-user-policy-arguments",
        "operator_policy_arguments": "quorum-queue-operator-policy-arguments",
        "list"   : "quorum-queue-list",
        "stats": "quorum-queue-stats",
        "node_details" : "quorum-queue-node-details"
    }
};

QUEUE_TYPE["stream"] = {
    label: "Stream",
    params: {
        'durable': true,
        'auto_delete': false
    },
    policy_apply_to: "streams",
    actions: {
        get_message: false,
        purge: false
    },
    tmpl: {
        "arguments"    : "stream-queue-arguments",
        "user_policy_arguments": "quorum-queue-user-policy-arguments",
        "operator_policy_arguments": "stream-queue-operator-policy-arguments",
        "list"   : "stream-queue-list",
        "stats"  : "stream-queue-stats",
        "node_details" : "stream-queue-node-details"
    }
};

// here I'll shortcut for now and let it be like that
// other queue types can inject themlves where they want.
// since the 'sections' object will likely keep key insertion
// order custom keys for queue type will be coming last.

// maybe add helper functions?
var MEMORY_STATISTICS = {
    sections: {'queue_procs'         : ['classic',  'Classic queues'],
               'quorum_queue_procs'  : ['quorum', 'Quorum queues'],
               'quorum_queue_dlx_procs'      : ['quorum', 'Dead letter workers'],
               'stream_queue_procs'          : ['stream',  'Stream queues'],
               'stream_queue_replica_reader_procs'  : ['stream',  'Stream queues (replica reader)'],
               'stream_queue_coordinator_procs'  : ['stream',  'Stream queues (coordinator)'],
               'binary'              : ['binary', 'Binaries'],
               'connection_readers'  : ['conn',   'Connection readers'],
               'connection_writers'  : ['conn',   'Connection writers'],
               'connection_channels' : ['conn',   'Connection channels'],
               'connection_other'    : ['conn',   'Connections (other)'],
               'mnesia'              : ['table',  'Mnesia'],
               'msg_index'           : ['table',  'Message store index'],
               'mgmt_db'             : ['table',  'Management database'],
               'quorum_ets'          : ['table',  'Quorum queue ETS tables'],
               'other_ets'           : ['table',  'Other ETS tables'],
               'plugins'             : ['proc',   'Plugins'],
               'other_proc'          : ['proc',   'Other process memory'],
               'code'                : ['system', 'Code'],
               'atom'                : ['system', 'Atoms'],
               'other_system'        : ['system', 'Other system'],
               'allocated_unused'    : ['unused', 'Allocated unused'],
               'reserved_unallocated': ['unused', 'Unallocated reserved by the OS']},
    keys: [[{name: 'Classic Queues', colour: 'classic',
             keys: [['queue_procs',       'queues']]},
            {name: 'Quorum Queues', colour: 'quorum',
             keys: [['quorum_queue_procs','quorum'],
                    ['quorum_queue_dlx_procs', 'dead letter workers']]},
            {name: 'Streams', colour: 'stream',
             keys: [['stream_queue_procs',                'stream'],
                    ['stream_queue_replica_reader_procs', 'stream replica reader'],
                    ['stream_queue_coordinator_procs',    'stream coordinator']]},
            {name: 'Binaries', colour: 'binary',
             keys: [['binary', '']]}],

           [{name: 'Connections', colour: 'conn',
             keys: [['connection_readers',  'readers'],
                    ['connection_writers',  'writers'],
                    ['connection_channels', 'channels'],
                    ['connection_other',    'other']]}],

           [{name: 'Tables', colour: 'table',
             keys: [['mnesia',              'internal database tables'],
                    ['msg_index',           'message store index'],
                    ['mgmt_db',             'management database'],
                    ['quorum_ets',          'quorum queue tables'],
                    ['other_ets',           'other']]}],

           [{name: 'Processes', colour: 'proc',
             keys: [['plugins',             'plugins'],
                    ['metadata_store',      'metadata store'],
                    ['other_proc',          'other']]},
            {name: 'System', colour: 'system',
             keys: [['code',                'code'],
                    ['atom',                'atoms'],
                    ['other_system',        'other']
                   ]}],

           [{name: 'Preallocated memory', colour: 'unused',
             keys: [['allocated_unused',     'preallocated by runtime, unused'],
                    ['reserved_unallocated', 'unallocated, reserved by the OS']]}]]
}

var BINARY_STATISTICS = {
    sections: {'queue_procs'         : ['classic',  'Classic queues'],
               'quorum_queue_procs'  : ['quorum',  'Quorum queues'],
               'quorum_queue_dlx_procs'      : ['quorum', 'Dead letter workers'],
               'stream_queue_procs'          : ['stream',  'Stream queues'],
               'stream_queue_replica_reader_procs'  : ['stream',  'Stream queues (replica reader)'],
               'stream_queue_coordinator_procs'  : ['stream',  'Stream queues (coordinator)'],
               'connection_readers'  : ['conn',   'Connection readers'],
               'connection_writers'  : ['conn',   'Connection writers'],
               'connection_channels' : ['conn',   'Connection channels'],
               'connection_other'    : ['conn',   'Connections (other)'],
               'msg_index'           : ['table',  'Message store index'],
               'mgmt_db'             : ['table',  'Management database'],
               'plugins'             : ['proc',   'Plugins'],
               'metadata_store'      : ['metadata_store',  'Metadata store'],
               'other'               : ['system', 'Other binary references']},
    key: [[{name: 'Classic Queues', colour: 'classic',
            keys: [['queue_procs',                       'queues']]},
           {name: 'Quorum Queues', colour: 'quorum',
            keys: [['quorum_queue_procs',                'quorum'],
                   ['quorum_queue_dlx_procs',            'dead letter workers']]},
           {name: 'Streams', colour: 'stream',
            keys: [['stream_queue_procs',                'stream'],
                   ['stream_queue_replica_reader_procs', 'stream replica reader'],
                   ['stream_queue_coordinator_procs',    'stream coordinator']]}],

          [{name: 'Connections', colour: 'conn',
            keys: [['connection_readers',  'readers'],
                   ['connection_writers',  'writers'],
                   ['connection_channels', 'channels'],
                   ['connection_other',    'other']]}],

          [{name: 'Tables', colour: 'table',
            keys: [['msg_index',           'message store index'],
                   ['mgmt_db',             'management database']]}],

          [{name: 'Processes', colour: 'proc',
            keys: [['plugins',             'plugins'],
                   ['metadata_store',      'metadata store']]},
           {name: 'System', colour: 'system',
            keys: [['other',               'other']]}]]
};
