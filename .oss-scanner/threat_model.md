# RabbitMQ threat model

RabbitMQ is a multi-protocol messaging and streaming broker. Clients authenticate and
then use the virtual hosts and resources they are permitted to use. The
[Access Control](https://www.rabbitmq.com/docs/access-control),
[Virtual Hosts](https://www.rabbitmq.com/docs/vhosts) and
[Production Deployment Guidelines](https://www.rabbitmq.com/docs/production-checklist)
guides describe the intended behavior.

## Trust boundaries

Everything a network client sends is untrusted: protocol listeners, the HTTP API, the
Prometheus endpoint, and the credentials and tokens checked by authentication backends.

A user must not reach a virtual host or resource outside their permissions, gain a
user tag they do not have, or read messages and metadata that belong to other users.

The following are trusted:

- Anyone holding the Erlang cookie, which includes the CLI tools
- Operators and the files they control: `rabbitmq.conf`, `advanced.config`,
  definition files, TLS certificates and keys
- Users with the `administrator` tag. Most users have neither this tag nor `policymaker`
- The LDAP server, OAuth 2.0 identity provider and HTTP authentication service the
  operator configures. Still check how their responses are validated

Production deployments delete the default `guest` user. `guest`, and `ANONYMOUS` login
as `guest`, are limited to loopback connections by default (`loopback_users`).

## Multi-tenancy

Virtual hosts provide soft multi-tenancy: tenants are assumed not to be malicious, and
isolation protects them from each other's mistakes, not from attacks. Tenants that do
not trust each other use separate clusters.

Resource use by an authenticated user is a vulnerability only when one of these holds:

- A faulty client application or client library could plausibly cause it
- It bypasses a configured limit or a memory or disk alarm
- It persists after a node restart, for example a queue that fails on Ra log replay

Otherwise, exhausting memory, CPU or the metadata store with input that an
authenticated user crafts for that purpose is not a vulnerability. Examples are hash
collisions, regular expressions with catastrophic backtracking, million-digit integers
and input that triggers quadratic behavior.

## What not to report

These are intended behavior. A finding that goes beyond what each item describes is
still in scope.

- A `policymaker` configuring Shovel or Federation connections to any host from a
  virtual host they have access to. The outbound connection itself is not SSRF
- `management` users seeing topology and statistics in the virtual hosts they have
  access to, and `monitoring` and `administrator` users seeing global statistics,
  node information and other users' connections and channels
- Users with the `impersonator` tag publishing with another user's `user_id`, and
  Federation upstreams with `trust-user-id` enabled keeping the upstream `user_id`
- The Prometheus endpoint being unauthenticated unless
  `prometheus.authentication.enabled` is set
- Topic permissions applying only to topic exchanges, and the internal backend
  allowing topic access when the user has no topic permissions for the exchange
- Existing connections keeping cached authorization decisions as documented
- Malformed input that closes only the offending connection
- Documented `administrator` capabilities, such as managing users, permissions and
  virtual hosts, or importing definitions

Out of scope: test code and test helpers, `selenium/`, `packaging/`, `release-notes/`,
the `*_examples` plugins, and community plugins. Dependencies fetched into `deps/`
are in scope only when a vulnerability is reachable through RabbitMQ.

## How to exercise it

The checkout at `/src` is built, and test dependencies are fetched and built. There is
no network access. `CONTRIBUTING.md` describes how to run a suite, a group or a case:

```sh
cd /src
make start-background-broker ENABLED_PLUGINS="rabbitmq_management rabbitmq_mqtt rabbitmq_stomp rabbitmq_stream"
make stop-node
make -C deps/rabbit ct-amqp_client t=cluster_size_1:durable_field_classic_queue
```

The summary is the `TEST COMPLETE` line in `/src/logs/ct_run.*/*/run.*/suite.log`.

`guest` (password `guest`) is an administrator, so reproduce with a user that has only
the permissions and tags the attack needs. Prefer a test case in an existing suite,
using `amqp10_client` or `rabbitmq_amqp_client` unless the issue is specific to another
protocol.

These suites need tools or services that are not in the image: the Java-based suites,
`amqp_dotnet_SUITE`, the `rabbitmq_stomp` `python_SUITE`, the `amqp10_client`
`system_SUITE`, `rabbitmq_auth_backend_ldap`, the peer discovery integration suites,
and mixed-version cluster tests.

## Reports

Name the scanned commit, the protocol or API entry point, the required plugins and
configuration, and the attacker's privileges. For resource use by an authenticated
user, state which condition under [Multi-tenancy](#multi-tenancy) applies. Include a
reproducer and a proposed patch. Group reports that share a root cause.

Rate severity with CVSS 4.0 and include the vector string. For a plugin or a feature
that is not enabled by default, set Attack Requirements to Present (`AT:P`).
