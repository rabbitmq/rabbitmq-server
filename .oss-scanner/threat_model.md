# RabbitMQ threat model

RabbitMQ is a multi-protocol messaging and streaming broker written in Erlang, with
CLI tools written in Elixir. Clients connect over the network, authenticate, and then
publish and consume messages within the virtual hosts and resources they are
permitted to use.

## Where untrusted input enters

Treat everything a network client sends as untrusted:

- AMQP 0-9-1 and AMQP 1.0 (port `5672`, `5671` with TLS), including the protocol
  header, SASL exchange, frames, and message sections, properties, and headers
- MQTT 3.1, 3.1.1 and 5.0 (`rabbitmq_mqtt`, port `1883`, `8883` with TLS)
- STOMP 1.0 to 1.2 (`rabbitmq_stomp`, port `61613`, `61614` with TLS)
- The RabbitMQ Stream Protocol (`rabbitmq_stream`, port `5552`, `5551` with TLS)
- MQTT and STOMP over WebSockets (`rabbitmq_web_mqtt`, `rabbitmq_web_stomp`)
- The HTTP API and management UI (`rabbitmq_management`, port `15672`) and the
  Prometheus endpoint (`rabbitmq_prometheus`, port `15692`)
- Credentials and tokens checked by the authentication backends: internal, LDAP,
  OAuth 2.0 (JWT validation), HTTP, cache, and X.509 certificate login
  (`rabbitmq_auth_mechanism_ssl`)
- Client-supplied filter expressions: AMQP 1.0 SQL and property filters, and JMS
  topic exchange selectors
- Responses from remote brokers that Shovel and Federation links connect to

Data from a client stays untrusted when it is stored, replicated to other nodes, or
rendered in the management UI.

## Trust boundaries

Authenticated users are limited by their virtual host access, configure, write and
read permissions, topic permissions, and user tags (`management`, `policymaker`,
`monitoring`, `administrator`, `impersonator`). A user must not reach a virtual host
or resource outside their permissions, gain a tag they do not have, or set a message
`user_id` other than their own without the `impersonator` tag. Access to shared
resources depends on permissions, not on who created them.

The following are trusted:

- Erlang distribution (port `25672`) and `epmd` (port `4369`). Any party holding
  the Erlang cookie has full control of the node by design. This includes the CLI
  tools, which connect over Erlang distribution
- Operators and the files they control: `rabbitmq.conf`, `advanced.config`,
  `enabled_plugins`, definition files, TLS certificates and keys
- Users with the `administrator` tag, for broker administration. The tag does not
  grant code execution on the node, file system access or the Erlang cookie, so a
  way to reach those is in scope
- The LDAP server, OAuth 2.0 identity provider, and HTTP authentication service the
  operator configures. Still check how their responses are validated

The default `guest` user is limited to loopback connections by design
(`loopback_users`). `ANONYMOUS` is enabled by default and logs in as `guest`, so the
same restriction applies to it.

## Components that matter most and least

Most important: `deps/rabbit` (core broker, AMQP 0-9-1 and AMQP 1.0, queues,
authentication and authorization), `deps/rabbit_common` (frame parsing),
`deps/amqp10_common` (AMQP 1.0 binary parser), `deps/rabbitmq_mqtt`,
`deps/rabbitmq_stomp`, `deps/rabbitmq_stream`, `deps/rabbitmq_management`, and the
authentication backends.

Also in scope: the other plugins under `deps/`, such as shovel, federation, the
WebSocket plugins, and the management UI JavaScript in
`deps/rabbitmq_management/priv/www`, where values set by a non-administrator user
are rendered to an administrator.

Out of scope: test code and test helpers (`rabbitmq_ct_helpers`,
`rabbitmq_ct_client_helpers`), `selenium/`, `packaging/`, `release-notes/`, the
`*_examples` plugins, and community plugins that are not in this repository.
Dependencies fetched into `deps/` during the build (`ra`, `khepri`, `osiris`, `cowboy`,
`ranch` and others) are in scope only when a vulnerability is reachable through
RabbitMQ.

## What to look for

Common classes of past issues:

- Atoms created from client input (`binary_to_atom/2`, `list_to_atom/1`)
- Unbounded memory or CPU use reachable before authentication, or from one client
  at the expense of others
- Credentials in logs, crash reports or process state
- Permission checks missing on one protocol or API path but present on others

These are not vulnerabilities by themselves:

- A failure limited to the offending connection
- `monitoring` and `administrator` users seeing other users' connections and
  resources
- The Prometheus endpoint being unauthenticated unless
  `prometheus.authentication.enabled` is set
- Topic permissions applying only to topic exchanges

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

## How we rate severity

Rate severity with CVSS 4.0 and include the vector string. When a finding is in a
plugin or a feature that is not enabled by default, set Attack Requirements to
Present (`AT:P`).

- Critical: remote code execution, or authentication bypass reachable by an
  unauthenticated client
- High: authorization bypass that gives a user access to virtual hosts, resources or
  messages outside their permissions, privilege escalation to the `administrator`
  tag, or unbounded resource use of the node that an unauthenticated
  client can trigger
- Medium: denial of service that requires an authenticated user, stored XSS in the
  management UI by a non-administrator user, and disclosure of credentials or other
  secrets in logs or API responses
- Low or not a vulnerability: issues that require the `administrator` tag, the
  Erlang cookie or access to configuration files, and resource use that stays within
  the limits an operator can configure (`max_message_size`, `frame_max`, virtual host
  and user limits, memory and disk alarms)

## Reports

Each report should name the scanned commit, the protocol or API entry point, the
required plugins and configuration, and the attacker's privileges. Include a
reproducer, preferably a Common Test case, and a proposed patch, and say whether the
reproducer was run. Group reports that share a root cause. Check
<https://github.com/rabbitmq/rabbitmq-server/security/advisories> for findings that
have already been reported and fixed.
