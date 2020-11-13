## Overview

RabbitMQ projects use pull requests to discuss, collaborate on and accept code contributions.
Pull requests is the primary place of discussing code changes.

## How to Contribute

The process is fairly standard:

 * Fork the repository or repositories you plan on contributing to
 * Clone [RabbitMQ umbrella repository](https://github.com/rabbitmq/rabbitmq-public-umbrella)
 * `cd umbrella`, `make co`
 * Create a branch with a descriptive name in the relevant repositories
 * Make your changes, run tests, commit with a [descriptive message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html), push to your fork
 * Submit pull requests with an explanation what has been changed and **why**
 * Submit a filled out and signed [Contributor Agreement](https://github.com/rabbitmq/ca#how-to-submit) if needed (see below)
 * Be patient. We will get to your pull request eventually

If what you are going to work on is a substantial change, please first ask the core team
of their opinion on [RabbitMQ mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).


## Code of Conduct

See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).


## Contributor Agreement

If you want to contribute a non-trivial change, please submit a signed copy of our
[Contributor Agreement](https://github.com/rabbitmq/ca#how-to-submit) around the time
you submit your pull request. This will make it much easier (in some cases, possible)
for the RabbitMQ team at Pivotal to merge your contribution.


## Running Tests

Assuming you have:

* Installed [Elixir](http://elixir-lang.org/install.html)
* Have a local running RabbitMQ node with the `rabbitmq-federation` plugin enabled (for parameter management testing), e.g.  `make run-broker PLUGINS='rabbitmq_federation rabbitmq_stomp'` from a server release repository clone

...you can simply run `make tests` within this project's root directory.

### Running a Single Test Case

To run a single test case, use `make test` like so:

```
make TEST_FILE=test/help_command_test.exs test
```

And if you want to run in verbose mode, set the `V` make variable:

```
make TEST_FILE=test/help_command_test.exs V=1 test
```

NOTE: You may see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using setup context
functions in Mix to start a new distributed node and connect it to the RabbitMQ
server. It complains because we don't actually use the context dictionary, but
it's fine otherwise.


## Where to Ask Questions

If something isn't clear, feel free to ask on our [mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).
