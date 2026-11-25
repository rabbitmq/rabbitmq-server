# RabbitMQ threat model

RabbitMQ is a multi-protocol messaging and streaming broker written in Erlang, with
CLI tools written in Elixir. Clients authenticate and then use the virtual hosts and
resources they have permissions for. MQTT, STOMP, the Stream Protocol, the HTTP API
and most other features are plugins.

## Where to focus

Most important: `deps/rabbit` (core broker, AMQP 0-9-1 and AMQP 1.0, queues,
authentication and authorization), `deps/rabbit_common` (frame parsing),
`deps/amqp10_common` (AMQP 1.0 binary parser), `deps/rabbitmq_mqtt`,
`deps/rabbitmq_stomp`, `deps/rabbitmq_stream`, `deps/rabbitmq_management`, and the
authentication backends. The other plugins under `deps/` and the management UI
JavaScript in `deps/rabbitmq_management/priv/www` are also in scope.

RabbitMQ-specific risks:

- Permission checks that one protocol or API path performs and another omits
- Virtual host isolation, including messages converted between protocols and links
  created by Shovel and Federation
- Message `user_id` validation in AMQP 0-9-1, AMQP 1.0, STOMP and the HTTP API
  publish endpoint
- Client-supplied filter expressions: AMQP 1.0 SQL and property filters, JMS topic
  exchange selectors and stream filters
- Atoms created from client input, and memory or CPU use that a client can trigger
  before authentication or at the expense of other clients
- Stored client data that makes nodes fail on Ra or Khepri log replay, restart or
  recovery, or that is rendered in the management UI
- Credentials in logs, crash reports or process state that other users can see
- Values passed to the HTTP and LDAP authentication backends, and OAuth 2.0 token
  and key validation
- Responses from remote brokers that Shovel and Federation connect to

Out of scope: test code and test helpers (`rabbitmq_ct_helpers`,
`rabbitmq_ct_client_helpers`), `selenium/`, `packaging/`, `release-notes/`, the
`*_examples` plugins, and community plugins that are not in this repository.
Dependencies fetched into `deps/` (`ra`, `khepri`, `osiris`, `cowboy`, `ranch` and
others) are in scope only through RabbitMQ's use of them.

## Trust boundaries

User tags (`management`, `policymaker`, `monitoring`, `administrator`,
`impersonator`) grant the capabilities documented at
<https://www.rabbitmq.com/docs/management#permissions>, not messaging permissions.

The following are trusted by design:

- Parties holding the Erlang cookie and authenticated cluster peers, which have full
  control of the node, including through the CLI tools
- Operators and the files they control: `rabbitmq.conf`, `advanced.config`,
  `enabled_plugins`, definition files, TLS certificates and keys
- Users with the `administrator` tag, for broker administration. The tag does not
  grant code execution on the node, file system access or the Erlang cookie, so a
  way to reach those is in scope
- The LDAP server, OAuth 2.0 identity provider and HTTP authentication service the
  operator configures. How their responses are validated is still in scope

The default `guest` user is limited to loopback connections (`loopback_users`).
`ANONYMOUS` is enabled by default and logs in as `guest` unless
`anonymous_login_user` is changed, so by default the same restriction applies to it.

## What not to report

These are intended behavior. A finding that goes beyond what each item describes is
still in scope.

- A `policymaker` configuring Shovel or Federation connections to any host from a
  virtual host they have access to. The outbound connection itself is not SSRF
- `management` users seeing topology and statistics in the virtual hosts they have
  access to, and `monitoring` and `administrator` users seeing global statistics,
  node information and other users' connections and channels
- `impersonator` users publishing with another user's `user_id`, and Federation
  upstreams with `trust-user-id` enabled keeping the upstream `user_id`
- Code execution by Erlang cookie holders or through operator-controlled files
- `ANONYMOUS` login, which uses the configured identity and its restrictions
- The Prometheus endpoint being unauthenticated unless
  `prometheus.authentication.enabled` is set. Secrets exposed through it are still
  in scope
- Topic permissions applying only to topic exchanges, and the internal backend
  allowing topic access when the user has no topic permissions for the exchange
- Existing connections keeping cached authorization decisions as documented
- Malformed input that closes only the offending connection
- Resource use within enforced limits (`max_message_size`, `frame_max`, virtual host
  and user limits, memory and disk alarms) that is not disproportionate and does not
  affect other clients
- Flaws in a dependency that are not caused by how RabbitMQ uses it
- Documented `administrator` capabilities, such as managing users, permissions and
  virtual hosts, or importing definitions
- Issues that the repository history shows as fixed, unless still reachable at the
  scanned commit

## How to exercise it

The checkout is at `/src` and has been built with `make`. Test dependencies have
been fetched and built for every component, so no network access is needed.

Start a node in the background with the plugins under test, then stop it:

```sh
cd /src
make start-background-broker ENABLED_PLUGINS="rabbitmq_management rabbitmq_mqtt rabbitmq_stomp rabbitmq_stream"
sbin/rabbitmqctl -n rabbit@$(hostname -s) status
make stop-node
```

The `guest` user (password `guest`) can connect from within the container. It has
the `administrator` tag and full permissions, so test access control with a separate
user created with `rabbitmqctl add_user` and `set_permissions`.

Run a Common Test suite, a group, or a single test case from a component directory,
for example:

```sh
make -C deps/rabbit ct-unit_access_control
make -C deps/rabbit ct-amqp_client t=cluster_size_1:durable_field_classic_queue
make -C deps/rabbitmq_mqtt ct-auth
```

`make` exits with a non-zero code when a test fails. The summary is the
`TEST COMPLETE` line in `/src/logs/ct_run.*/*/run.*/suite.log`.

Writing a reproducer as a test case in an existing suite is preferred. Erlang
clients are available for AMQP 0-9-1 (`amqp_client`), AMQP 1.0 (`amqp10_client`,
`rabbitmq_amqp_client`), and MQTT (`emqtt`).

These suites need tools or services that are not in the image and cannot run:
the Java-based suites (`rabbitmq_mqtt` `java_SUITE`, `rabbitmq_stream`
`rabbit_stream_SUITE`, `rabbitmq_stream_management` `http_SUITE`), `rabbit`
`amqp_dotnet_SUITE`, `rabbitmq_stomp` `python_SUITE`, `amqp10_client`
`system_SUITE` (ActiveMQ), `rabbitmq_auth_backend_ldap` (`slapd`), the peer
discovery integration suites, and mixed-version cluster tests.

## Severity and reporting

Rate severity with CVSS 4.0 and include the score and vector string. When a finding
is in a plugin or a feature that is not enabled by default, set Attack Requirements
to Present (`AT:P`).

Typical triage priorities, not fixed assignments:

- Critical: remote code execution, or authentication bypass reachable by an
  unauthenticated client
- High: access to virtual hosts, resources or messages outside the user's
  permissions, escalation to the `administrator` tag, or unbounded resource use
  that an unauthenticated client can trigger
- Medium: denial of service that requires an authenticated user, stored XSS in the
  management UI by a non-administrator, and disclosure of credentials or other
  secrets

Persistent or cluster-wide denial of service can rank above Medium even when
authentication is required.

Each report should name the scanned commit, the entry point, the required plugins
and configuration, the attacker's privileges, and the root cause with source
locations. Include a reproducer, preferably a Common Test case, and a proposed
patch, and say whether the reproducer was run. Group reports that share a root
cause.
