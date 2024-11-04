# Security Policy

Team RabbitMQ will investigate all responsibly disclosed vulnerabilities that affect
a recent version in one of the [supported release series](https://www.rabbitmq.com/versions.html).
We ask all reporters to provide a reasonable amount of information that can be used to reproduce
the observed behavior.

## Reporting a Vulnerability

RabbitMQ Core team really appreciates responsible vulnerability reports
from security researchers and our user community.

To responsibly disclose a vulnerability, please use [GitHub Security Advisories](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability) or email `rabbitmq-core@groups.vmware.com` or
[sign up for RabbitMQ Discord server]([https://rabbitmq-slack.herokuapp.com](https://rabbitmq.com/discord)) and
send a DM to @michaelklishin. For reports received via Discord, a separate private
channel will be set up so that multiple RabbitMQ maintainers can access the disclosed
information.

In case you'd prefer to encrypt your report, use the [RabbitMQ release signing public key](https://github.com/rabbitmq/signing-keys/releases).

When reporting a vulnerability, please including the following information:

 * Supported RabbitMQ version used
 * Any relevant environment information (e.g. operating system and Erlang version used)
 * A set of steps to reproduce the problem
 * Why do you think this behavior is a security vulnerability

A received vulnerability report will be acknowledged by a RabbitMQ core team or VMware R&D staff member.
For reports that will be considered legitimate and serious enough, a [GitHub Security Advisory](https://github.com/rabbitmq/rabbitmq-server/security/advisories)
will be drafted. An advisory is a private way for reporters and collaborators to work on a solution.

After a new patch release is shipped, a [new CVE ID will be requested](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/publishing-a-repository-security-advisory#requesting-a-cve-identification-number-optional) as
part of the advisory and eventually published. The advisory will credit the reporters.
The associated discussion will be removed when the advisory is published.


### When Should I Report a Vulnerability?

 * You think you discovered a potential security vulnerability in RabbitMQ
 * You think you discovered a potential security vulnerability in one of RabbitMQ client libraries or dependencies
   * For projects with their own vulnerability reporting and disclosure process (e.g. Erlang/OTP), please report it directly there

### When Should I NOT Report a Vulnerability?

 * Not enough information is available to triage (try to reliably reproduce) the issue
 * You need help tuning RabbitMQ for security. See [Commercial Services](https://www.rabbitmq.com/services.html)
 * You need help applying security related updates. See [Upgrades](https://www.rabbitmq.com/upgrade.html)

### On Security Scan Dumps

A warning from a security scanner does not necessarily indicate a vulnerability in RabbitMQ.
Many of such warnings are specific to a certain environment. RabbitMQ core team does not have
access to most commercial security scanners or enough information about the deployment,
so **security scan results alone will not be considered sufficient evidence** of a vulnerability.


### Irresponsible Disclosure

Publicly disclosed vulnerabilities (e.g. publicly filed issues with repoduction steps or scripts)
will be removed or otherwise taken private as irresponsibly disclosed.


## Public Disclosure Timing

A public disclosure date is negotiated by the RabbitMQ core team and the vulnerability reporter.
In most cases disclosure happens within two weeks after a patch release of RabbitMQ is made available.
When VMware products that depend on RabbitMQ are also affected, the disclosure period can be extended
further to allow those projects to ship patch releases.


## VMware RabbitMQ

[VMware RabbitMQ](https://www.vmware.com/products/rabbitmq.html) is covered by the [VMware Security Response Policy](https://www.vmware.com/support/policies/security_response.html).

Vulnerabilities found in VMware RabbitMQ can be reported to the RabbitMQ core team or
via the [VMware Security Response Center](https://www.vmware.com/security/vsrc.html).
