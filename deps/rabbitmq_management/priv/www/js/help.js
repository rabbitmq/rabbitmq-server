HELP = {
    'exchange-arguments':
      'Supported arguments:<br/> \
      <dl><dt>alternate-exchange</dt> \
      <dd>Alternate exchange to which to route messages which would otherwise not be routed.</dd><dl>',

    'queue-arguments':
      'Supported arguments:<br/> \
      <dl> \
        <dt>x-message-ttl</dt> \
        <dd>How long a message published to a queue can live before it is discarded (milliseconds).</dd> \
        <dt>x-expires</dt> \
        <dd>How long a queue can be unused before it is automatically deleted (milliseconds).</dd> \
      <dl>',

    'mnesia-storage':
      'Mnesia storage types:<br/> \
      <dl> \
        <dt>disc</dt> \
        <dd>Node configuration is held on disc.</dd> \
        <dt>ram</dt> \
        <dd>Node configuration is held in ram. Messages will still be written to disc if necessary.</dd> \
      <dl>',

    'internal-users-only':
      'Only users within the internal RabbitMQ database are managed here. Other users (e.g. those authenticated over LDAP) will not appear.',

    'export-config':
    'The configuration consists of users, virtual hosts, permissions, exchanges, queues and bindings. It does not include the contents of queues. Exclusive queues will not be exported.',

    'import-config':
      'The configuration that is imported will be merged with the current configuration. If an error occurs during import, any configuration changes made will not be rolled back.',

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
      'File descriptor count and limit, as reported by the operating system. \
      The count includes network sockets and file handlers.<br/> \
      To optimize disk access RabbitMQ uses as many free descriptors as are \
      available, so the count may safely approach the limit. \
      However, if most of the file descriptors are used by sockets then \
      persister performance will be negatively impacted.',

    'socket-descriptors':
      'The network sockets count and limit managed by RabbitMQ.<br/> \
      When the limit is exhausted RabbitMQ will stop accepting new \
      network connections.',

    'memory-alarm':
      'The memory alarm for this node has gone off. It will block \
      incoming network traffic until the memory usage drops below \
      the watermark.',

    'foo': 'foo' // No comma.
};

function help(id) {
    show_popup('help', HELP[id]);
}