# Security Policy

Team RabbitMQ will investigate all responsibly disclosed vulnerabilities that affect
a recent version in one of the [supported release series](https://www.rabbitmq.com/versions.html).

## Reporting a Vulnerability

To responsibly disclose a vulnerability, please use [GitHub Security Advisories](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability).

If you are unable to use GitHub Security Advisories, you can email `tnz-rabbitmq-core.pdl@broadcom.com`.

In case you'd prefer to encrypt your report, use the [RabbitMQ release signing public key](https://github.com/rabbitmq/signing-keys/releases).

When reporting a vulnerability, please include the following information:

 * Supported RabbitMQ version used
 * Any relevant environment information (e.g. operating system and Erlang version used)
 * A set of steps to reproduce the problem
 * Why do you think this behavior is a security vulnerability

A received vulnerability report will be acknowledged by a RabbitMQ core team member.

A [new CVE ID will be requested](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/publishing-a-repository-security-advisory#requesting-a-cve-identification-number-optional) as part of the advisory and be published after a new patch release has shipped.
The associated discussion will be removed when the advisory is published.
The advisory will credit the reporters.

### When Should I Report a Vulnerability?

 * You think you discovered a potential security vulnerability in the RabbitMQ server
 * For vulnerabilities in RabbitMQ client libraries and dependencies, please try to report the vulnerability directly in their respective repositories. (For example for the RabbitMQ AMQP 0.9.1 Java client, report a vulnerability [here](https://github.com/rabbitmq/rabbitmq-java-client/security)).
 * For projects with their own vulnerability reporting and disclosure process (e.g. Erlang/OTP), please report it directly there

### When Should I NOT Report a Vulnerability?

 * Not enough information is available to triage (try to reliably reproduce) the issue
 * You need help tuning RabbitMQ for security.

### On Security Scan Dumps

A warning from a security scanner does not necessarily indicate a vulnerability in RabbitMQ.
Many of such warnings are specific to a certain environment. RabbitMQ core team does not have
access to most commercial security scanners or enough information about the deployment,
so security scan results alone will not be considered sufficient evidence of a vulnerability.

### Irresponsible Disclosure

Publicly disclosed vulnerabilities (e.g. publicly filed issues with reproduction steps or scripts)
will be removed or otherwise taken private as irresponsibly disclosed.

## Public Disclosure Timing

A public disclosure date is negotiated by the RabbitMQ core team and the vulnerability reporter.
In most cases disclosure happens at the exact same time as, or shortly after, a patch release of RabbitMQ is made available.

## VMware Tanzu RabbitMQ

[VMware Tanzu RabbitMQ](https://www.vmware.com/products/app-platform/tanzu-data-intelligence/rabbitmq) is covered by the [Broadcom Security Response Policy](https://www.broadcom.com/support/vmware-services/security-response).
