
# RabbitMQ Community Support Eligibility 

This document explains who is eligible for community support for open source RabbitMQ.

### What is Community Support?

Community support is defined as all questions, root cause analysis requests, issue reports, and other interactions the RabbitMQ core team has with open source RabbitMQ users on GitHub
and our community forums. 

### What is Broadcom's Obligation to Reply to Messages or Issues Reported?

The RabbitMQ Core team at Broadcom has no obligation to reply to any message or issue posted by the community of open source RabbitMQ users. 

### Who is Eligible for community support

The following groups of users are eligible for community support:

 * Users who regularly contribute to RabbitMQ development (a definition of "contribution" is provided at the end of this document)
 * Users who use [the most recent release series](https://www.rabbitmq.com/release-information) and provide detailed and well researched issue reports, including responsibly disclosed security vulnerabilities

All other users are not eligible for community support from the RabbitMQ Core Team.

Users with a [commercial support license](https://tanzu.vmware.com/rabbitmq/oss) or a [commercial edition license](https://tanzu.vmware.com/rabbitmq) should
use commercial support channels.

### Exceptions: Reports that Will Always Be Investigated

The RabbitMQ core team will always investigate the following issues, even if they are reported by an ineligible user:

 * Responsibly disclosed security vulnerabilities
 * Detailed issues with a proof that data safety may be at risk
 * Detailed issues with a proof that a node may fail to start, join the cluster, or rejoin the cluster

### Exceptions: Question that Will Be Ignored

Unless overwhelming evidence of a systemic problem in RabbitMQ is demonstrated, the following issues will get minimum or no attention at all from the core team:

* Questions related to [OAuth2 configuration](https://www.rabbitmq.com/docs/oauth2), [OAuth 2 configuration examples](https://www.rabbitmq.com/docs/oauth2-examples) and [troubleshooting of OAuth 2](https://www.rabbitmq.com/docs/troubleshooting-oauth2)
* Questions related to [TLS configuration](https://www.rabbitmq.com/docs/ssl) and [troubleshooting of TLS connections](https://www.rabbitmq.com/docs/troubleshooting-ssl)
* Questions related to [troubleshooting of network connectivity](https://www.rabbitmq.com/docs/troubleshooting-networking) 
* Questions related to [LDAP configuration](https://www.rabbitmq.com/docs/ldap) and [troubleshooting](https://www.rabbitmq.com/docs/ldap#troubleshooting)

These topics represent some of the most time consuming questions to investigate and respond to thoroughly. Guidance and investigations related to these features will only be available to customers with VMware Tanzu RabbitMQ commercial licenses.

## Definition of "contribution"

For the purpose of this policy, the RabbitMQ team defines a "contribution" as any of the following:

* A pull request that fixes any bug, introduces a new feature, clarifies example documentation, or introduces any other behavior change that may not be easy to categorize but the team is willing to accept
* An issue report that includes RabbitMQ and Erlang versions used, a reasonably detailed problem definition, a detailed set of specific steps that can be followed in order to quickly reproduce the behavior, and all the necessary evidence: log snippets from all nodes with relevant information, metrics dashboards over a relevant period of time,
  code snippets that demonstrate application behavior, and any other information necessary to quickly and efficiently reproduce the reported behavior at least some of the time
* Executable benchmarks (for example, using PerfTest) that demonstrate regressions
* Donated infrastructure or services (this can be IaaS provider credits, credits for services, and anything else that the RabbitMQ core team can use to build and distribute open source RabbitMQ packages, tools, libraries)
* Meaningful contributions to RabbitMQ documentation, not including typo fixes, grammar corrections, re-wording. Contributions must include new original content, produced by a human, that makes it easier to install, operate, upgrade, and communicate with RabbitMQ from applications
* A detailed, RFC-style feature request where the status quo, the end goal, the pros and the cons of the proposed feature are well defined
* Meaningful build system updates previously pre-approved by the RabbitMQ core team

The above rules equally apply to contributions to RabbitMQ, officially supported RabbitMQ client libraries, key RabbitMQ dependencies (Erlang/OTP, Ra, Osiris, Khepri, Cuttlefish, Horus), and the Kubernetes cluster Operators maintained by the RabbitMQ core team.

## Release Series Eligible for Community Support

Only releases in the latest minor series of the latest major version are eligible for community support. Currently this is RabbitMQ 3.13.x in the 3.x major series.

All patches (bug fixes and improvements alike) will only be available for the latest minor series in the latest major series. This applies to all changes contributed by the community.

For example, if the latest supported series (minor) is 3.13.x, all core and community contributions will ship in a 3.13.x release until a newer minor or major comes out (say, 3.14.x or 4.0.x). 

The RabbitMQ team will not backport patches to older release series (such as 3.12.x) of open source RabbitMQ, including cases where a patch was contributed by the community.
Patch releases for older release series are exclusively available to users with VMware Tanzu RabbitMQ commercial licenses.
