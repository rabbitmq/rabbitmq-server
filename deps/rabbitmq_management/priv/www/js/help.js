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

    'queue-max-length-bytes':
      'Total body size for ready messages a queue can contain before it starts to drop them from its head.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/maxlength.html">x-max-length-bytes</a>" argument.)',

    'queue-auto-delete':
      'If yes, the queue will delete itself after at least one consumer has connected, and then all consumers have disconnected.',

    'queue-dead-letter-exchange':
      'Optional name of an exchange to which messages will be republished if they are rejected or expire.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/dlx.html">x-dead-letter-exchange</a>" argument.)',

    'queue-dead-letter-routing-key':
      'Optional replacement routing key to use when a message is dead-lettered. If this is not set, the message\'s original routing key will be used.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/dlx.html">x-dead-letter-routing-key</a>" argument.)',

    'queue-max-priority':
      'Maximum number of priority levels for the queue to support; if not set, the queue will not support message priorities.<br/>(Sets the "<a target="_blank" href="http://rabbitmq.com/priority.html">x-max-priority</a>" argument.)',

    'queue-lazy':
      'Set the queue into lazy mode, keeping as many messages as possible on disk to reduce RAM usage; if not set, the queue will keep an in-memory cache to deliver messages as fast as possible.<br/>(Sets the "<a target="_blank" href="https://www.rabbitmq.com/lazy-queues.html">x-queue-mode</a>" argument.)',

    'queue-master-locator':
       'Set the queue into master location mode, determining the rule by which the queue master is located when declared on a cluster of nodes.<br/>(Sets the "<a target="_blank" href="https://www.rabbitmq.com/ha.html">x-queue-master-locator</a>" argument.)',

    'queue-messages':
      '<p>Message counts.</p><p>Note that "in memory" and "persistent" are not mutually exclusive; persistent messages can be in memory as well as on disc, and transient messages can be paged out if memory is tight. Non-durable queues will consider all messages to be transient.</p>',

    'queue-message-body-bytes':
      '<p>The sum total of the sizes of the message bodies in this queue. This only counts message bodies; it does not include message properties (including headers) or metadata used by the queue.</p><p>Note that "in memory" and "persistent" are not mutually exclusive; persistent messages can be in memory as well as on disc, and transient messages can be paged out if memory is tight. Non-durable queues will consider all messages to be transient.</p><p>If a message is routed to multiple queues on publication, its body will be stored only once (in memory and on disk) and shared between queues. The value shown here does not take account of this effect.</p>',

    'queue-process-memory':
      'Total memory used by this queue process. This does not include in-memory message bodies (which may be shared between queues and will appear in the global "binaries" memory) but does include everything else.',

    'queue-consumer-utilisation':
      'Fraction of the time that the queue is able to immediately deliver messages to consumers. If this number is less than 100% you may be able to deliver messages faster if: \
        <ul> \
          <li>There were more consumers or</li> \
          <li>The consumers were faster or</li> \
          <li>The consumers had a higher prefetch count</li> \
        </ul>',

    'internal-users-only':
      'Only users within the internal RabbitMQ database are shown here. Other users (e.g. those authenticated over LDAP) will not appear.',

    'export-definitions':
    'The definitions consist of users, virtual hosts, permissions, parameters, exchanges, queues and bindings. They do not include the contents of queues or the cluster name. Exclusive queues will not be exported.',

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
        <dt><acronym title="Confirm">C</acronym> &ndash; <a target="_blank" href="http://www.rabbitmq.com/confirms.html">confirm</a></dt> \
        <dd>Channel will send streaming publish confirmations.</dd> \
        <dt><acronym title="Transactional">T</acronym> &ndash; <a target="_blank" href="http://www.rabbitmq.com/amqp-0-9-1-reference.html#class.tx">transactional</a></dt> \
        <dd>Channel is transactional.</dd> \
      </dl>',

    'channel-prefetch':
      'Channel prefetch counts. \
       <p> \
         Each channel can have two prefetch counts: A per-consumer count, which \
         will limit each new consumer created on the channel, and a global \
         count, which is shared between all consumers on the channel.\
       </p> \
       <p> \
         This column shows one, the other, or both limits if they are set. \
       </p>',

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
        <dt>Return</dt>\
        <dd>Rate at which basic.return is sent to publishers for unroutable messages published with the \'mandatory\' flag set.</dd>\
        <dt>Disk read</dt>\
        <dd>Rate at which queues read messages from disk.</dd>\
        <dt>Disk write</dt>\
        <dd>Rate at which queues write messages to disk.</dd>\
      </dl>\
      <p>\
        Note that the last two items are originate in queues rather than \
        channels; they may therefore be slightly out of sync with other \
        statistics.\
      </p>',

    'disk-monitoring-no-watermark' : 'There is no <a target="_blank" href="http://www.rabbitmq.com/memory.html#diskfreesup">disk space low watermark</a> set. RabbitMQ will not take any action to avoid running out of disk space.',

    'resource-counts' : 'Shows total number of objects for all virtual hosts the current user has access to.',

    'memory-use' : '<p>Note that the memory details shown here are only updated on request - they could be too expensive to calculate every few seconds on a busy server.</p><p><a target="_blank" href="http://www.rabbitmq.com/memory-use.html">Read more</a> on memory use.</p>',

    'binary-use' : '<p>Binary accounting is not exact; binaries are shared between processes (and thus the same binary might be counted in more than one section), and the VM does not allow us to track binaries that are not associated with processes (so some binary use might not appear at all).</p>',

    'policy-ha-mode' : 'One of <code>all</code> (mirror to all nodes in the cluster), <code>exactly</code> (mirror to a set number of nodes) or <code>nodes</code> (mirror to an explicit list of nodes). If you choose one of the latter two, you must also set <code>ha-params</code>.',

    'policy-ha-params' : 'Absent if <code>ha-mode</code> is <code>all</code>, a number\
    if <code>ha-mode</code> is <code>exactly</code>, or a list\
    of strings if <code>ha-mode</code> is <code>nodes</code>.',

    'policy-ha-sync-mode' : 'One of <code>manual</code> or <code>automatic</code>.',

    'policy-federation-upstream-set' :
    'A string; only if the federation plugin is enabled. Chooses the name of a set of upstreams to use with federation, or "all" to use all upstreams. Incompatible with <code>federation-upstream</code>.',

    'policy-federation-upstream' :
    'A string; only if the federation plugin is enabled. Chooses a specific upstream set to use for federation. Incompatible with <code>federation-upstream-set</code>.',

    'handle-exe' : 'In order to monitor the number of file descriptors in use on Windows, RabbitMQ needs the <a href="http://technet.microsoft.com/en-us/sysinternals/bb896655" target="_blank">handle.exe command line tool from Microsoft</a>. Download it and place it in the path (e.g. in C:\Windows).',

    'filter-regex' :
    'Whether to enable regular expression matching. Both string literals \
    and regular expressions are matched in a case-insensitive manner.<br/></br/> \
    (<a href="https://developer.mozilla.org/en/docs/Web/JavaScript/Guide/Regular_Expressions" target="_blank">Regular expression reference</a>)',

    'plugins' :
    'Note that only plugins which are both explicitly enabled and running are shown here.',

    'io-operations':
    'Rate of I/O operations. Only operations performed by the message \
      persister are shown here (e.g. metadata changes in Mnesia or writes \
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
        <dd>Rate at which the broker invokes <code>fsync()</code> to ensure \
         data is flushed to disk.</dd>\
        <dt>Reopen</dt>\
        <dd>Rate at which the broker recycles file handles in order to support \
         more queues than it has file handles. If this operation is occurring \
         frequently you may get a performance boost from increasing the number \
         of file handles available.</dd>\
      </dl>',

    'mnesia-transactions':
    'Rate at which Mnesia transactions are initiated on this node (this node \
     will also take part in Mnesia transactions initiated on other nodes).\
      <dl>\
        <dt>RAM only</dt>\
        <dd>Rate at which RAM-only transactions take place (e.g. creation / \
            deletion of transient queues).</dd>\
        <dt>Disk</dt>\
        <dd>Rate at which disk (and RAM) transactions take place (.e.g \
            creation / deletion of durable queues).</dd>\
      </dl>',

    'persister-operations-msg':
    'Rate at which per-message persister operations take place on this node. See \
     <a href="http://www.rabbitmq.com/persistence-conf.html" target="_blank">here</a> \
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
     <a href="http://www.rabbitmq.com/persistence-conf.html" target="_blank">here</a> \
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

    'foo': 'foo' // No comma.
};

function help(id) {
    show_popup('help', HELP[id]);
}
