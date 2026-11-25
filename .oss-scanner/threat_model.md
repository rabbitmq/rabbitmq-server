# RabbitMQ threat model

This document describes the security scope, intended behavior, test guidance and
reporting preferences for RabbitMQ scans by the Anthropic OSS Scanner. Verify
version-dependent statements against the scanned commit, its configuration defaults
and its tests.

## Project overview

RabbitMQ is a multi-protocol messaging and streaming broker written in Erlang, with
CLI tools written in Elixir. Clients connect over the network, authenticate, and then
publish and consume messages within the virtual hosts and resources they are
permitted to use. A deployment may contain multiple nodes, tenants, protocol
listeners, authentication backends and optional plugins.

The goal is to protect the confidentiality and integrity of messages, credentials,
tokens, tenant resources, policies and cluster state, to keep the broker available,
and to keep client access, broker administration and node-level control separate.

## Where untrusted input enters

Treat network input as untrusted both before and after authentication. The ports
below are configurable defaults for listeners that are enabled; they are not
security boundaries by themselves.

- AMQP 0-9-1 and AMQP 1.0 (port `5672`, `5671` with TLS): protocol negotiation,
  the SASL exchange, frames, and message sections, properties, headers and arguments
- MQTT 3.1, 3.1.1 and 5.0 (`rabbitmq_mqtt`, port `1883`, `8883` with TLS)
- STOMP 1.0 to 1.2 (`rabbitmq_stomp`, port `61613`, `61614` with TLS)
- The RabbitMQ Stream Protocol (`rabbitmq_stream`, port `5552`, `5551` with TLS)
- MQTT and STOMP over WebSockets (`rabbitmq_web_mqtt`, `rabbitmq_web_stomp`),
  including the HTTP upgrade request
- The HTTP API and management UI (`rabbitmq_management`, port `15672`) and the
  Prometheus endpoint (`rabbitmq_prometheus`, port `15692`)
- Credentials and tokens checked by the authentication backends: internal, LDAP,
  OAuth 2.0 (JWT validation), HTTP, cache, and X.509 certificate login
  (`rabbitmq_auth_mechanism_ssl`)
- Client-supplied filter expressions: AMQP 1.0 SQL filters (`rabbit_amqp_sql_lexer`,
  `rabbit_amqp_sql_parser`, `rabbit_amqp_filter_sql`), AMQP 1.0 property filters
  (`rabbit_amqp_filter_prop`), JMS topic exchange selectors and stream filtering.
  Grammar validation, nesting depth and evaluation cost all matter
- Responses to outbound connections: Shovel and Federation links (through
  `amqp_client` and `amqp10_client`), HTTP authentication backend requests, LDAP
  queries, and OAuth 2.0 discovery and key retrieval. Permission to connect to a
  remote broker does not mean its protocol responses are trusted

Indirect inputs are untrusted too: stored message properties, resource names,
routing keys, bindings, client-supplied queue and exchange arguments, runtime
parameters, messages converted between protocols, and messages received through
Federation or Shovel. Data that came from a restricted user stays untrusted when it
is persisted, replicated, forwarded between nodes, or rendered to an administrator
in the management UI.

## Trust boundaries and attacker model

Distinguish unauthenticated remote clients, authenticated clients with limited
permissions, and users with management tags. A client may have permissions in one
virtual host and none in another. Several clients may legitimately share a resource.

Authorization depends on virtual host access, configure, write and read
permissions, topic permissions, and protocol-specific semantics. User tags grant
management capabilities; they are not a substitute for resource permissions. The
table summarizes the intended behavior:

| Tag | Intended capabilities |
|-----|-----------------------|
| (none) | No management access. Messaging permissions still apply |
| `management` | Queues, exchanges, bindings and their statistics in the virtual hosts the user can access, including resources other users created. Only the user's own connections and channels |
| `policymaker` | `management`, plus policies and runtime parameters (including dynamic Shovels and Federation upstreams) in the virtual hosts the user can access |
| `monitoring` | `management`, plus all virtual hosts, all connections and channels, and node, cluster and global statistics. No policy management |
| `administrator` | `policymaker` and `monitoring`, plus users, permissions, virtual hosts, user and virtual host limits, global parameters, definitions import and export, feature flags, and closing any connection. Messaging operations still need resource permissions |
| `impersonator` | Publishing with a message `user_id` that differs from the authenticated user. Grants no management access and does not change the connection's identity |

The HTTP API enforces this table through `rabbit_mgmt_util`: `is_authorized/2`,
`is_authorized_vhost/2`, `is_authorized_policies/2`, `is_authorized_monitor/2`,
`is_authorized_global_parameters/2` and `is_authorized_admin/2`. Check the function
an endpoint actually uses rather than relying on the table.

The `impersonator` tag is not implied by `administrator`. Message `user_id`
validation goes through `rabbit_access_control:check_user_id/2`, called from
`check_user_id_header/2` in `rabbit_channel` for AMQP 0-9-1 and from
`check_user_id/2` in `rabbit_amqp_session` for AMQP 1.0. A message without a `user_id` is not a spoofed identity. Federation's
`trust-user-id` upstream setting is intentional when configured.

The following are trusted:

- Anyone holding the Erlang cookie, which gives full control of the node by design,
  including through the CLI tools. Authenticated cluster peers are trusted at the
  same level. Erlang distribution (port `25672`) and `epmd` (port `4369`) are meant
  for restricted networks. Data that originated from a client is still untrusted
  when it is relayed between nodes, even though the peer node is trusted
- Operators and the files they control: `rabbitmq.conf`, `advanced.config`,
  `enabled_plugins`, operator-managed definition files, TLS certificates and keys
- The LDAP server, OAuth 2.0 identity provider and HTTP authentication service the
  operator configures. How their responses are validated, how identities are mapped
  and how failures are handled are still in scope

The `administrator` tag allows the documented operations in the table, as well as
creating Shovels and Federation upstreams that point at any URI. The tag does not
give host access or the Erlang cookie. Reaching those from the HTTP API or a
messaging protocol is a reportable crossing of this boundary, for example code
execution on the node (`GHSA-3526-xvv4-q9mr`), a file write outside the intended
directory (`GHSA-h7cq-qrr8-7vgc`), or atom table exhaustion
(`GHSA-34c7-r8w9-5wv8`).

The default `guest` user is restricted to loopback connections by `loopback_users`.
In this repository, `deps/rabbit/Makefile` enables `PLAIN`, `AMQPLAIN` and
`ANONYMOUS` by default, and `anonymous_login_user` and `anonymous_login_pass` default
to `guest`, so default `ANONYMOUS` login is subject to the same loopback
restriction. An advertised `ANONYMOUS` mechanism is not proof of remote anonymous
access, and anonymous access that an operator enables is not an authentication
bypass. A way around the configured identity or the loopback restriction is in
scope (`GHSA-4r6f-9cpw-f6g6`).

## Security invariants

- Operations that require authentication are not available to unauthenticated
  clients, except through mechanisms the operator enabled
- Clients cannot acquire identities, tags, permissions, virtual hosts, resources,
  messages or metadata beyond their effective authorization
- Authorization stays consistent with the documented semantics of each protocol and
  API, including when messages are converted or forwarded between protocols or nodes
- Validated message `user_id` properties match the authenticated user unless the
  `impersonator` tag or Federation `trust-user-id` applies
- Authentication and authorization caches keep identities separate and follow the
  documented permission change, credential refresh and revocation behavior
- Malformed or adversarial input does not disrupt unrelated clients, virtual hosts
  or nodes beyond the broker's intended resource sharing
- Credentials, tokens and other secrets are not exposed to unauthorized parties
  through APIs, metrics, logs, crash reports or error responses
- Restricted clients cannot reach operator-controlled configuration, the host
  environment or node-level credentials

## Components and scope

Highest priority:

- `deps/rabbit`: the core broker, AMQP 0-9-1 and AMQP 1.0, access control, routing,
  queues and cluster interactions
- `deps/rabbit_common`: frame parsing and common protocol handling
- `deps/amqp10_common`: the AMQP 1.0 binary parser
- `deps/rabbitmq_mqtt`, `deps/rabbitmq_stomp`, `deps/rabbitmq_stream`
- `deps/rabbitmq_management` and the authentication and authorization backends

Also in scope: the other production plugins under `deps/`, such as Shovel,
Federation, the WebSocket plugins and Prometheus; CLI code where it crosses a
security boundary; and the management UI JavaScript in
`deps/rabbitmq_management/priv/www`, where values set by non-administrators are
rendered.

Out of scope: test code (`deps/*/test/`) and test helpers (`rabbitmq_ct_helpers`,
`rabbitmq_ct_client_helpers`, `trust_store_http`), example code (`deps/*/examples/`
and the `*_examples` plugins), `rabbitmq_codegen`, `selenium/`, `packaging/`,
`release-notes/`, and community plugins that are not in this repository. Tests are
still useful as evidence and as a place for regression tests.

Dependencies fetched into `deps/` during the build (`ra`, `khepri`, `osiris`,
`cowboy`, `ranch` and others) matter only through RabbitMQ's use of them. Do not
report a flaw in a dependency just because RabbitMQ uses it. When a RabbitMQ fix
depends on an upstream change, say so in the report.

## Additional security-sensitive surfaces

These are review priorities, not known vulnerabilities. Confirm that the feature
exists at the scanned commit, what the attacker controls, and which permissions
apply.

- Shovel and Federation: validation and authorization of runtime parameters and
  URIs, TLS peer verification, credential handling, and responses from a malicious
  remote broker. Choosing a remote endpoint is the purpose of these features, so
  outbound connectivity alone is not an SSRF finding. It becomes one when it
  violates a configured restriction or discloses credentials. Creating these
  parameters requires the `policymaker` tag; access to a virtual host alone is not
  enough
- HTTP authentication backend: encoding of usernames, virtual host names and other
  client-derived values in outbound requests, response validation and failure
  handling
- LDAP: escaping in filter and DN templates, identity mapping and response
  validation
- OAuth 2.0: JWKS retrieval and TLS verification, issuer discovery, algorithm and
  key validation, issuer and audience checks, scope-prefix mapping, token expiry and
  credential refresh
- `rabbitmq_trust_store`: HTTP provider responses and certificate updates.
  `rabbitmq_tracing`: trace names used as file paths and access to trace data.
  `rabbitmq_event_exchange`: which events are published and who can consume them
- Persistence and replication: untrusted data in metadata, Ra and Khepri logs,
  quorum queue commands and definitions. Check that validation and limits still hold
  during replication, log replay, restart and failover

## Erlang and OTP review priorities

- Atom creation from untrusted input (`binary_to_atom/2`, `list_to_atom/1`) in a
  bounded atom table, unless the input is validated or mapped to existing atoms
- External term decoding with `binary_to_term/1,2`. The `[safe]` option alone does
  not make decoded terms or their size safe
- Dynamic dispatch (`erlang:apply/3`), command execution (`os:cmd/1`) and file
  operations (`file` module) on client-derived values
- Regular expressions (`re`) built from client input, and client-supplied
  expressions in general: who may supply them, and their complexity and limits
- Unbounded growth of process mailboxes, ETS tables, maps, retained binaries or
  process counts, and missing backpressure that affects unrelated clients
- Secret comparison where timing differences are observable, and credentials or
  tokens in `logger` output, exceptions, crash reports or process state (for
  example `#amqp_params_network{}` in a `gen_server` state)

A sensitive function name, a missing option or a process crash is not enough on its
own. A finding needs an untrusted input path, a violated invariant and real impact.

## Expected behavior and false-positive exclusions

- Access to shared messages and metadata depends on permissions, not on who
  published them. Visibility granted by the tags in the table above is intentional
- Existing connections may keep cached authorization decisions as documented. Do not
  assume immediate revocation in every protocol
- Topic permissions apply only to topic exchanges
  (`check_topic_authorisation/5` in `rabbit_channel` and `rabbit_amqp_session`)
- The Prometheus endpoint is unauthenticated unless
  `prometheus.authentication.enabled` is set
- X.509 certificate login maps a certificate field (`ssl_cert_login_from`) to a
  username without a password. Trust comes from TLS peer verification, which the
  operator configures
- Allowed CORS and WebSocket origins are operator configuration, but mishandling
  that configuration is in scope (`GHSA-p3hp-v9wh-ghm7`, `GHSA-9c4f-rxxm-88q3`)
- A new MQTT connection with a client ID already in use in the same virtual host
  disconnects the existing connection, whichever user owns it, as the MQTT
  specification requires (`register_client_id/4` in `rabbit_mqtt_processor`).
  Report it only if the new connection gains access to the previous session's
  messages or subscriptions without the permissions they require
- A malformed request that the broker rejects, or a failure limited to the
  offending connection, is not a vulnerability by itself
- Normal resource use by authorized clients, backpressure, and memory and disk
  alarms are not vulnerabilities by themselves. Consider whether configured limits
  work, whether the cost to the attacker is disproportionately small, and whether
  other clients or nodes are affected
- Plaintext listeners, broad permissions and anonymous access that an operator
  enabled are not vulnerabilities because a safer configuration exists. A bypass of
  a configured control is in scope
- Capabilities that follow from holding the Erlang cookie or controlling trusted
  configuration files are not vulnerabilities

## Published advisories

Published advisories are at
<https://github.com/rabbitmq/rabbitmq-server/security/advisories>. A finding covered
by a published fix is a duplicate unless the fix is incomplete, as in
`GHSA-q46v-hrvq-hp24`. These show the kind of report that was accepted and how it
was rated:

- `GHSA-42pc-678q-v8qj` (medium): Federation upstreams skip virtual host
  authorization, giving message access across virtual hosts
- `GHSA-6588-rqcr-59pw` (medium): AMQP 1.0 properties placed after the body bypass
  `user_id` validation
- `GHSA-cfqc-c682-93mm` (high): compressed WebSocket messages sent before
  authentication exhaust broker memory
- `GHSA-x96j-244m-mrr2` (high): atom table exhaustion through reply-to queue name
  decoding
- `GHSA-chxf-3hfg-j8f7` (medium): a JMS topic selector `LIKE` pattern consumes CPU
- `GHSA-6gmw-wxch-cvvc` (medium): Shovel URI credentials disclosed to `monitoring`
  users
- `GHSA-xm9p-57xv-vxwc` (high): stored XSS through a TLS peer certificate DN in the
  stream management UI
- `GHSA-3wxh-g236-vm46` (high): failed LDAP authentication lets unauthenticated
  clients amplify log volume

## How to exercise it

The checkout is at `/src` and has been built with `make`. Test dependencies are
expected to be fetched and built for every component, so no network access should
be needed. Confirm the commit and the environment before relying on this.

Start a node in the background with the plugins under test, then stop it:

```sh
cd /src
make start-background-broker ENABLED_PLUGINS="rabbitmq_management rabbitmq_mqtt rabbitmq_stomp rabbitmq_stream"
sbin/rabbitmqctl -n rabbit@$(hostname -s) status
make stop-node
```

`ENABLED_PLUGINS` only applies to a fresh node. To change it, run
`make virgin-test-tmpdir` first. To test a non-default configuration, add
`RABBITMQ_CONFIG_FILE=/path/to/rabbitmq.conf` to the `start-background-broker`
command.

The `guest` user (password `guest`) can connect from within the container. It has
the `administrator` tag and full permissions, which hides access-control errors. Use
a separate user with only the permissions the test needs:

```sh
ctl="sbin/rabbitmqctl -n rabbit@$(hostname -s)"
$ctl add_vhost other
$ctl add_user limited 'synthetic-password'
$ctl set_permissions -p / limited '^limited\.' '^limited\.' '^limited\.'
$ctl set_topic_permissions -p / limited amq.topic '^limited\.' '^limited\.'
$ctl set_user_tags limited management
```

Use synthetic messages and credentials, bounded resource use and timeouts.

Run a Common Test suite, a group, or a single test case, for example:

```sh
make -C deps/rabbit ct-unit_access_control
make -C deps/rabbit ct-amqp_client t=cluster_size_1:durable_field_classic_queue
make -C deps/rabbitmq_mqtt ct-auth
```

Suites start their own nodes on separate ports, so a running background node does
not interfere. A non-zero exit code means the run failed, not that a defect exists.
The summary is the `TEST COMPLETE` line in
`/src/logs/ct_run.*/*/run.*/suite.log`; broker logs are next to it.

Write a reproducer as a deterministic test case in an existing suite, with an
authorized control case and an unauthorized case where relevant. Erlang clients are
available for AMQP 0-9-1 (`amqp_client`), AMQP 1.0 (`amqp10_client`,
`rabbitmq_amqp_client`), MQTT (`emqtt`), STOMP (`rabbit_stomp_client` in
`deps/rabbitmq_stomp/test/src`) and the Stream Protocol (`stream_test_utils` in
`rabbitmq_ct_helpers`).

These suites need tools or services that are not in the image and cannot run: the
Java-based suites (`rabbitmq_mqtt` `java_SUITE`, `rabbitmq_stream`
`rabbit_stream_SUITE`, `rabbitmq_stream_management` `http_SUITE`), `rabbit`
`amqp_dotnet_SUITE`, `rabbitmq_stomp` `python_SUITE`, `amqp10_client` `system_SUITE`
(ActiveMQ), the `rabbitmq_auth_backend_ldap` suites (`slapd`), the peer discovery
integration suites, and mixed-version cluster tests. This limits dynamic
validation, not scope. Do not claim a reproduction or a validated patch unless it
was run and observed.

## How we rate severity

Rate severity with CVSS 4.0, with the score and the full vector string, and justify
any metric that is not obvious. When a finding is in a plugin or a feature that is
not enabled by default, set Attack Requirements to Present (`AT:P`).

Use `VA` for availability impact on the RabbitMQ cluster itself, and `SA` only for
demonstrated impact on a separate system. Several affected nodes or replicas do not
justify `SA:H` by themselves.

These are triage preferences, not fixed scores or caps:

- Critical: unauthenticated remote code execution, or an authentication bypass that
  gives extensive privileged access
- High: significant authorization bypass, including access across virtual hosts,
  escalation to the `administrator` tag, substantial unauthenticated denial of
  service, or exposure of sensitive credentials
- Medium: meaningful denial of service by an authenticated user, limited
  authorization bypass, stored XSS by a non-administrator, or disclosure of
  sensitive information with limited impact
- Low: a demonstrated defect with limited impact or restrictive prerequisites
- Not a vulnerability: intended operations, normal resource use with no further
  security impact, and capabilities of operators and Erlang cookie holders

For denial of service, consider the attacker's cost, the configured limits
(`max_message_size`, `frame_max`, user and virtual host limits, memory and disk
alarms), which clients and nodes are affected, and what recovery takes. Persistent
or cluster-wide impact, such as stored data that crashes nodes on every log replay,
recovery or replica start, ranks above a failure of one connection. Rate a secret
by the access it gives. Explain any rating that departs from these preferences.

## Reports, patches and deduplication

Each report should contain:

- The scanned commit, and affected release lines where known (`main`, `v4.4.x`,
  `v4.3.x`)
- The component and the protocol or API entry point
- The required plugins and configuration
- The minimum attacker privileges: tags, and the virtual host and resource
  permissions
- The violated invariant from this document
- The root cause, with source paths and function references including arity
- Expected and observed behavior, and the concrete confidentiality, integrity or
  availability impact. For persistent or distributed impact, the affected nodes and
  what recovery requires
- The CVSS 4.0 score and vector
- A regression test, preferably a Common Test case
- A minimal root-cause patch and its compatibility considerations
- Whether reproduction and patch validation were run, with their results and
  limitations. A hypothesis from reading the source is not an observed result

Group findings that share a root cause and fix, and list every affected protocol and
component. Keep defects that can be fixed independently in separate reports, even
when they share a CWE or impact. Leave out findings that cannot be tied to a
specific function and input path.
