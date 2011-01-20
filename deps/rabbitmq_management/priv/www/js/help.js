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

    'foo': 'foo' // No comma.
};

function help(id) {
    show_popup('help', HELP[id]);
}