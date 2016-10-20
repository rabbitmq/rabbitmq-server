## Testing RabbitMQ upgrades.

This tool can be used to test upgrade of RabbitMQ to a branch version.
`rabbitmq-server` dependency should be checked out to the tested branch.

### How it works:

This tool use GNU Make to run following steps:

 - install generic unix release for specified version to `rabbitmq_r_<version>`
 - start a server with configuration from `config` directory and data directory in `rabbitmq_server_upgrade_data`
 - set up vhost, user, policies, exchange (fanout), queues (transient and durable)
 - publish messages to queues (persistent to queue index, persistent to storage, not persistent)
 - stop the server
 - start local branch server using `run-background-broker`
 - verify everything is in place (transient queues and messages are expected to be lost)
 - stop local server

By default it will also clean up the test data.

### Custom targets and configuration

Make targets:

 - `run-release-for-upgrade` - download release and run it in `rabbitmq_r_<version>` with data in `rabbitmq_server_upgrade_data`
 - `setup-release-for-upgrade` - run previous step and set up test data
 - `prepare-release-for-upgrade` - run previous step and stop the server (can be used to build "pre upgrade" state)
 - `run-broker-upgrade` - run previous step and run a current branch server (can be used to observe "after upgrade" state for manual verification)
 - `verify-upgrade` - run previous step and verification script (that will consume published messages and delete some vhosts)
 - `verify-and-stop` - run previous stap and stop the server
 - `verify-and-cleanup` - run previous step and delete test data **this is the default step**

Additional targets:

 - `clean` - stop server (if running) and delete temporary data
 - `distclean` - same as `clean`, but also removes downloaded release package

Environment:

Following environment parameters can be used to configure upgrade validation:

| parameter                   | default | description                                                    |
|-----------------------------|---------|----------------------------------------------------------------|
| UPGRADE_FROM                | 3.6.5   | Release version to upgrade from                                |
| QUEUES_COUNT_TRANSIENT      | 1       | Number of transient queues                                     |
| QUEUES_COUNT_DURABLE        | 1       | Number of durable queues                                       |
| MSGS_COUNT_NON_PERSISTENT   | 10      | Number of transient messages to publish                        |
| MSGS_COUNT_PERSISTENT_INDEX | 10      | Number of persistent messages to publish to queue index        |
| MSGS_COUNT_PERSISTENT_STORE | 10      | Number of persistent messages to publish to message store      |
| INDEX_MSG_SIZE              | 50      | Message size to fit queue index (depends on configuration)     |
| STORE_MSG_SIZE              | 150     | Message size to not fit queue index (depends on configuration) |

`INDEX_MSG_SIZE` and `STORE_MSG_SIZE` should be set to be more and less than `queue_index_embed_msgs_below` setting in `config/rabbitmq.config` file respectively.

Unsafe. Do not change without need:

| parameter | default | description |
|-----------|---------|-------------|
| UPGRADE_FROM_SCRIPT | 3.5   | Script to use for data setup. It's only `3.5` now used for both `3.5.x` and `3.6.x` releases |
| UPGRADE_TO_SCRIPT   | 3.6   | Script to use for verification. Should correspond with branch version  |
| RELEASE_ARCHIVE     | rabbitmq-server-generic-unix-$(UPGRADE_FROM).tar.xz | Filename for release archive |
| RELEASE_FOR_UPGRADE_URL | http://www.rabbitmq.com/releases/rabbitmq-server/v$(UPGRADE_FROM)/$(RELEASE_ARCHIVE) | URL to load $RELEASE_ARCHIVE from. Should point to directly accessible (via wget) generic unix archive |


