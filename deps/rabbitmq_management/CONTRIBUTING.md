Thank you for using RabbitMQ and for taking the time to contribute to the project.

## Overview

### GitHub issues

Team RabbitMQ uses GitHub issues for _specific actionable items_ engineers can work on. This assumes the following:

* GitHub issues are not used for questions, investigations, root cause analysis, discussions of potential issues, etc (as defined by this team)
* We have a certain amount of information to work with

The team receives many questions through various venues every single day. Frequently, these questions do not include the necessary details the team needs to begin useful work. GitHub issues can very quickly turn into a something impossible to navigate and make sense of. Because of this, questions, investigations, root cause analysis, and discussions of potential features are all considered to be [mailing list][rmq-users] material. If you are unsure where to begin, the [RabbitMQ users mailing list][rmq-users] is the right place.

Getting all the details necessary to reproduce an issue, make a conclusion or even form a hypothesis about what's happening can take a fair amount of time. Please help others help you by providing a way to reproduce the behavior you're observing, or at least sharing as much relevant information as possible on the [RabbitMQ users mailing list][rmq-users]:

* Versions of the following:
    * Operating system (distribution as well)
    * RabbitMQ server
    * All client libraries used
    * RabbitMQ plugins (if applicable)
* Server logs
* A code example or terminal transcript that can be used to reproduce
* Full exception stack traces (not a single line message)
* `rabbitmqctl status` (and, if possible, `rabbitmqctl environment` output)
* Other relevant details about the environment and workload, e.g. a traffic capture
* Feel free to edit out hostnames and other potentially sensitive information.
* As an alternative, the [`rabbitmq-collect-env`][rmq-collect-env] script will collect all of this information (and more). Please note that no effort is made to scrub any information that may be sensitive

### Pull Requests

RabbitMQ projects use pull requests to discuss, collaborate on and accept code contributions. Pull requests is the primary place of discussing code changes.

 * Fork the repository or repositories you plan on contributing to
 * Clone the [RabbitMQ umbrella repository][rmq-umbrella-repo]
 * Run `make co` in the cloned umbrella repository to fetch dependencies into `deps/`
 * Create a branch with a descriptive name in the relevant repositories in `deps/`
 * Make your changes, run tests, commit with a [descriptive message][git-commit-msgs], push to your fork
 * Submit pull requests with an explanation what has been changed and **why**
 * Submit a filled out and signed [Contributor Agreement][ca-agreement] if needed (see below)
 * Be patient. We will get to your pull request eventually

If what you are going to work on is a substantial change, please first ask the core team for their opinion on the [RabbitMQ users mailing list][rmq-users].

## Code of Conduct

See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

## Contributor Agreement

If you want to contribute a non-trivial change, please submit a signed copy of our [Contributor Agreement][ca-agreement] around the time you submit your pull request. This will make it much easier (in some cases, possible) for the RabbitMQ team at Pivotal to merge your contribution.

## Where to Ask Questions

If something isn't clear, feel free to ask on our [mailing list][rmq-users].

[rmq-collect-env]: https://github.com/rabbitmq/support-tools/blob/master/scripts/rabbitmq-collect-env
[rmq-umbrella-repo]: https://github.com/rabbitmq/rabbitmq-public-umbrella
[git-commit-msgs]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
[rmq-users]: https://groups.google.com/forum/#!forum/rabbitmq-users
[ca-agreement]: https://github.com/rabbitmq/ca#how-to-submit
