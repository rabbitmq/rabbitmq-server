HELP = {
    'exchange-auto-delete':
      'If yes, the exchange will delete itself after at least one queue or exchange has been bound to this one, and then all queues or exchanges have been unbound.',

    'exchange-internal':
      'If yes, clients cannot publish to this exchange directly. It can only be used with exchange to exchange bindings.',

    'exchange-alternate':
      'If messages to this exchange cannot otherwise be routed, send them to the alternate exchange named here.<br/>(Sets the "alternate-exchange" argument.)',

    'queue-message-ttl':
    'How long a message published to a queue can live before it is discarded (milliseconds).<br/>(Sets the "x-message-ttl" argument.)',

    'queue-expires':
      'How long a queue can be unused for before it is automatically deleted (milliseconds).<br/>(Sets the "x-expires" argument.)',

    'queue-auto-delete':
      'If yes, the queue will delete itself after at least one consumer has connected, and then all consumers have disconnected.',

    'queue-dead-letter-exchange':
      'Optional name of an exchange to which messages will be republished if the queue is deleted or purged, or the message is rejected or expires.',

    'internal-users-only':
      'Only users within the internal RabbitMQ database are shown here. Other users (e.g. those authenticated over LDAP) will not appear.',

    'export-definitions':
    'The definitions consist of users, virtual hosts, permissions, exchanges, queues and bindings. They do not include the contents of queues. Exclusive queues will not be exported.',

    'import-definitions':
      'The definitions that are imported will be merged with the current definitions. If an error occurs during import, any changes made will not be rolled back.',

    'exchange-rates-incoming':
      'The incoming rate is the rate at which messages are published directly to this exchange.',

    'exchange-rates-outgoing':
      'The outgoing rate is the rate at which messages enter queues, having been published directly to this exchange.',

    'channel-mode':
      'Channel guarantee mode. Can be one of the following, or neither:<br/> \
      <dl> \
        <dt><acronym title="Confirm">C</acronym> &ndash; confirm</dt> \
        <dd>Channel will send streaming publish confirmations.</dd> \
        <dt><acronym title="Transactional">T</acronym> &ndash; transactional</dt> \
        <dd>Channel is transactional.</dd> \
      <dl>',

    'file-descriptors':
      '<p>File descriptor count and limit, as reported by the operating \
      system. The count includes network sockets and file handles.</p> \
      <p>To optimize disk access RabbitMQ uses as many free descriptors as are \
      available, so the count may safely approach the limit. \
      However, if most of the file descriptors are used by sockets then \
      persister performance will be negatively impacted.</p> \
      <p>To change the limit on Unix / Linux, use "ulimit -n". To change \
      the limit on Windows, set the ERL_MAX_PORTS environment variable</p> \
      <p>To report used file handles on Windows, handle.exe from \
      sysinternals must be installed in your path. You can download it \
      <a href="http://technet.microsoft.com/en-us/sysinternals/bb896655">here</a>.</p>',

    'socket-descriptors':
      'The network sockets count and limit managed by RabbitMQ.<br/> \
      When the limit is exhausted RabbitMQ will stop accepting new \
      network connections.',

    'memory-alarm':
      'The memory alarm for this node has gone off. It will block \
      incoming network traffic until the memory usage drops below \
      the watermark.',

    'message-get-requeue':
      '<p>Clicking "Get Message(s)" will consume messages from the queue. \
      If requeue is set the message will be re-added to the queue, \
      but ordering will not be preserved and "redelivered" will be set.</p> \
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
      'Comma-separated list of tags to apply to the user. Currently supported \
       by the management plugin: \
      <dl> \
        <dt>management</dt> \
        <dd> \
          User can access the management plugin \
        </dd> \
        <dt>monitoring</dt> \
        <dd> \
          User can access the management plugin and see all connections and \
          channels as well as node-related information. \
        </dd> \
        <dt>administrator</dt> \
        <dd> \
          User can do everything monitoring can do, manage users, \
          vhosts and permissions, and close other user\'s connections. \
        </dd> \
      </dl>',

    'queued-messages':
    'Total messages in all queues:\
      <dl>\
        <dt>Ready</dt>\
        <dd>Number of messages that are available to be delivered now.</dd>\
        <dt>Unacknowledged</dt>\
        <dd>Number of messages for which the server is waiting for acknowledgement.</dd>\
        <dt>Total</dt>\
        <dd>The total of these two numbers.</dd>\
      </dl>\
    Note that the rate of change of total queued messages does \
    <b>not</b> include messages removed due to queue deletion.',

    'message-rates':
    'Total rates for all queues. Only rates for which some activity is taking place will be shown.\
      <dl>\
        <dt>Publish</dt>\
        <dd>Rate at which messages are entering the server.</dd>\
        <dt>Confirm</dt>\
        <dd>Rate at which the server is confirming publishes.</dd>\
        <dt>Deliver</dt>\
        <dd>Rate at which messages requiring acknowledgement are being delivered in response to basic.consume.</dd>\
        <dt>Deliver (noack)</dt>\
        <dd>Rate at which messages not requiring acknowledgement are being delivered in response to basic.consume.</dd>\
        <dt>Get</dt>\
        <dd>Rate at which messages requiring acknowledgement are being delivered in response to basic.get.</dd>\
        <dt>Get (noack)</dt>\
        <dd>Rate at which messages not requiring acknowledgement are being delivered in response to basic.get.</dd>\
        <dt>Acknowledge</dt>\
        <dd>Rate at which messages are being acknowledged.</dd>\
        <dt>Redelivered</dt>\
        <dd>Rate at which messages with the \'redelivered\' flag set are being delivered. Note that these messages will <b>also</b> be counted in one of the delivery rates above.</dd>\
        <dt>Return (mandatory)</dt>\
        <dd>Rate at which basic.return is sent to publishers for unroutable  messages published with the \'mandatory\' flag set.</dd>\
        <dt>Return (immediate)</dt>\
        <dd>Rate at which basic.return is sent to publishers for undeliverable  messages published with the \'immediate\' flag set.</dd>\
      </dl>',

    'foo': 'foo' // No comma.
};

function help(id) {
    show_popup('help', HELP[id]);
}