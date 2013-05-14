HELP = {
    'exchange-auto-delete':
      'If yes, the exchange will delete itself after at least one queue or exchange has been bound to this one, and then all queues or exchanges have been unbound.',

    'exchange-internal':
      'If yes, clients cannot publish to this exchange directly. It can only be used with exchange to exchange bindings.',

    'exchange-alternate':
      'If messages to this exchange cannot otherwise be routed, send them to the alternate exchange named here.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/ae.html">alternate-exchange</a>" argument.)',

    'queue-message-ttl':
    'How long a message published to a queue can live before it is discarded (milliseconds).<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/ttl.html#per-queue-message-ttl">x-message-ttl</a>" argument.)',

    'queue-expires':
      'How long a queue can be unused for before it is automatically deleted (milliseconds).<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/ttl.html#queue-ttl">x-expires</a>" argument.)',

    'queue-max-length':
      'How many (ready) messages a queue can contain before it starts to drop them from its head.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/maxlength.html">x-max-length</a>" argument.)',

    'queue-auto-delete':
      'If yes, the queue will delete itself after at least one consumer has connected, and then all consumers have disconnected.',

    'queue-dead-letter-exchange':
      'Optional name of an exchange to which messages will be republished if they are rejected or expire.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/dlx.html">x-dead-letter-exchange</a>" argument.)',

    'queue-dead-letter-routing-key':
      'Optional replacement routing key to use when a message is dead-lettered. If this is not set, the message\'s original routing key will be used.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/dlx.html">x-dead-letter-routing-key</a>" argument.)',

    'internal-users-only':
      'Only users within the internal RabbitMQ database are shown here. Other users (e.g. those authenticated over LDAP) will not appear.',

    'export-definitions':
    'The definitions consist of users, virtual hosts, permissions, parameters, exchanges, queues and bindings. They do not include the contents of queues. Exclusive queues will not be exported.',

    'import-definitions':
      'The definitions that are imported will be merged with the current definitions. If an error occurs during import, any changes made will not be rolled back.',

    'exchange-rates-incoming':
      'The incoming rate is the rate at which messages are published directly to this exchange.',

    'exchange-rates-outgoing':
      'The outgoing rate is the rate at which messages enter queues, having been published directly to this exchange.',

    'channel-mode':
      'Channel guarantee mode. Can be one of the following, or neither:<br/> \
      <dl> \
        <dt><acronym title="Confirm">C</acronym> &ndash; <a target="_blank" href="http://www.rabbitmq.com/confirms.html">confirm</a></dt> \
        <dd>Channel will send streaming publish confirmations.</dd> \
        <dt><acronym title="Transactional">T</acronym> &ndash; <a target="_blank" href="http://www.rabbitmq.com/amqp-0-9-1-reference.html#class.tx">transactional</a></dt> \
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
      <a target="_blank" href="http://technet.microsoft.com/en-us/sysinternals/bb896655">here</a>.</p>',

    'socket-descriptors':
      'The network sockets count and limit managed by RabbitMQ.<br/> \
      When the limit is exhausted RabbitMQ will stop accepting new \
      network connections.',

    'memory-alarm':
      '<p>The <a target="_blank" href="http://www.rabbitmq.com/memory.html#memsup">memory \
      alarm</a> for this node has gone off. It will block \
      incoming network traffic until the memory usage drops below \
      the watermark.</p>\
      <p>Note that the pale line in this case indicates the high watermark \
      in relation to how much memory is used in total. </p>',

    'disk-free-alarm':
      'The <a target="_blank" href="http://www.rabbitmq.com/memory.html#diskfreesup">disk \
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
       <a target="_blank" href="http://www.rabbitmq.com/management.html#permissions">supported \
       by the management plugin</a>: \
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
      </dl> \
      <p> \
        Note that you can set any tag here; the links for the above three \
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
      </dl>\
    Note that the rate of change of total queued messages does \
    <b>not</b> include messages removed due to queue deletion.',

    'message-rates':
    'Only rates for which some activity is taking place will be shown.\
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
        <dt>Return</dt>\
        <dd>Rate at which basic.return is sent to publishers for unroutable messages published with the \'mandatory\' flag set.</dd>\
      </dl>',

    'disk-monitoring-no-watermark' : 'There is no <a target="_blank" href="http://www.rabbitmq.com/memory.html#diskfreesup">disk space low watermark</a> set. RabbitMQ will not take any action to avoid running out of disk space.',

    'resource-counts' : 'Shows total number of objects for all virtual hosts the current user has access to.',

    'memory-use' : '<p>Note that the memory details shown here are only updated on request - they could be too expensive to calculate every few seconds on a busy server.</p><p><a target="_blank" href="http://www.rabbitmq.com/memory-use.html">Read more</a> on memory use.</p>',

    'policy-definitions' : '<dl>\
<dt><code>ha-mode</code></dt>\
  <dd>\
    One of <code>all</code>, <code>exactly</code>\
    or <code>nodes</code> (the latter currently not supported by\
    web UI).\
  </dd>\
  <dt><code>ha-params</code></dt>\
  <dd>\
    Absent if <code>ha-mode</code> is <code>all</code>, a number\
    if <code>ha-mode</code> is <code>exactly</code>, or an array\
    of strings if <code>ha-mode</code> is <code>nodes</code>.\
  </dd>\
  <dt><code>ha-sync-mode</code></dt>\
  <dd>\
    One of <code>manual</code> or <code>automatic</code>.\
  </dd>\
  <dt><code>federation-upstream-set</code></dt>\
  <dd>\
    A string; only if the federation plugin is enabled.\
  </dd>\
</dl>',

    'handle-exe' : 'In order to monitor the number of file descriptors in use on Windows, RabbitMQ needs the <a href="http://technet.microsoft.com/en-us/sysinternals/bb896655" target="_blank">handle.exe command line tool from Microsoft</a>. Download it and place it in the path (e.g. in C:\Windows).',

    'foo': 'foo' // No comma.
};

function help(id) {
    show_popup('help', HELP[id]);
}
