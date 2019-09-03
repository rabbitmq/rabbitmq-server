Thank you for using RabbitMQ and for taking the time to contribute to the project.
This document has two main parts:

 * when and how to file GitHub issues for RabbitMQ projects
 * how to submit pull requests

They intend to save you and RabbitMQ maintainers some time, so please
take a moment to read through them.

## Overview

### GitHub issues

The RabbitMQ team uses GitHub issues for _specific actionable items_ that
engineers can work on. This assumes the following:

* GitHub issues are not used for questions, investigations, root cause
  analysis, discussions of potential issues, etc (as defined by this team)
* Enough information is provided by the reporter for maintainers to work with

The team receives many questions through various venues every single
day. Frequently, these questions do not include the necessary details
the team needs to begin useful work. GitHub issues can very quickly
turn into a something impossible to navigate and make sense
of. Because of this, questions, investigations, root cause analysis,
and discussions of potential features are all considered to be
[mailing list][rmq-users] material. If you are unsure where to begin,
the [RabbitMQ users mailing list][rmq-users] is the right place.

Getting all the details necessary to reproduce an issue, make a
conclusion or even form a hypothesis about what's happening can take a
fair amount of time. Please help others help you by providing a way to
reproduce the behavior you're observing, or at least sharing as much
relevant information as possible on the [RabbitMQ users mailing
list][rmq-users].

Please provide versions of the software used:

 * RabbitMQ server
 * Erlang
 * Operating system version (and distribution, if applicable)
 * All client libraries used
 * RabbitMQ plugins (if applicable)

The following information greatly helps in investigating and reproducing issues:

 * RabbitMQ server logs
 * A code example or terminal transcript that can be used to reproduce
 * Full exception stack traces (a single line message is not enough!)
 * `rabbitmqctl report` and `rabbitmqctl environment` output
 * Other relevant details about the environment and workload, e.g. a traffic capture
 * Feel free to edit out hostnames and other potentially sensitive information.

To make collecting much of this and other environment information, use
the [`rabbitmq-collect-env`][rmq-collect-env] script. It will produce an archive with
server logs, operating system logs, output of certain diagnostics commands and so on.
Please note that **no effort is made to scrub any information that may be sensitive**.

### Pull Requests

RabbitMQ projects use pull requests to discuss, collaborate on and accept code contributions.
Pull requests is the primary place of discussing code changes.

Here's the recommended workflow:

 * [Fork the repository][github-fork] or repositories you plan on contributing to. If multiple
   repositories are involved in addressing the same issue, please use the same branch name
   in each repository
 * Create a branch with a descriptive name in the relevant repositories
 * Make your changes, run tests (usually with `make tests`), commit with a
   [descriptive message][git-commit-msgs], push to your fork
 * Submit pull requests with an explanation what has been changed and **why**
 * Submit a filled out and signed [Contributor Agreement][ca-agreement] if needed (see below)
 * Be patient. We will get to your pull request eventually

If what you are going to work on is a substantial change, please first
ask the core team for their opinion on the [RabbitMQ users mailing list][rmq-users].

## Running Tests

    make tests

will run all suites.

## Code of Conduct

See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

## Contributor Agreement

If you want to contribute a non-trivial change, please submit a signed
copy of our [Contributor Agreement][ca-agreement] around the time you
submit your pull request. This will make it much easier (in some
cases, possible) for the RabbitMQ team at Pivotal to merge your
contribution.

## Where to Ask Questions

If something isn't clear, feel free to ask on our [mailing list][rmq-users].

[rmq-collect-env]: https://github.com/rabbitmq/support-tools/blob/master/scripts/rabbitmq-collect-env
[git-commit-msgs]: https://chris.beams.io/posts/git-commit/
[rmq-users]: https://groups.google.com/forum/#!forum/rabbitmq-users
[ca-agreement]: https://cla.pivotal.io/sign/rabbitmq
[github-fork]: https://help.github.com/articles/fork-a-repo/
