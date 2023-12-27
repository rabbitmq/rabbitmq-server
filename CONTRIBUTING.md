## Overview

RabbitMQ projects use pull requests to discuss, collaborate on and accept code contributions.
Pull requests is the primary place of discussing code changes.

## How to Contribute

The process is fairly standard:

 * Fork the repository or repositories you plan on contributing to
 * Run `bazel sync` if you plan to [use Bazel](https://github.com/rabbitmq/contribute/wiki/Bazel-and-BuildBuddy), or `make`
 * Create a branch with a descriptive name in the relevant repositories
 * Make your changes, run tests, ensure correct code formatting, commit with a [descriptive message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html), push to your fork
 * Submit pull requests with an explanation what has been changed and **why**
 * Submit a filled out and signed [Contributor Agreement](https://cla.pivotal.io/) if needed (see below)
 * Be patient. We will get to your pull request eventually

If what you are going to work on is a substantial change, please first ask the core team
of their opinion on [RabbitMQ mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).

## Running Tests

See [this guide on how to use Bazel and BuildBuddy for RabbitMQ core development](https://github.com/rabbitmq/contribute/wiki/Bazel-and-BuildBuddy).


## Working on Management UI with BrowserSync

When working on management UI code, besides starting the node with

``` shell
bazel run broker RABBITMQ_ENABLED_PLUGINS=rabbitmq_management
```

(or any other set of plugins), it is highly recommended to use [BrowserSync](https://browsersync.io/#install)
to shorten the edit/feedback cycle for JS files, CSS, and so on.

First, install BrowserSync using NPM:

``` shell
npm install -g browser-sync
```

Assuming a node running locally with HTTP API on port `15672`, start
a BrowserSync proxy like so:

``` shell
cd deps/rabbitmq_management/priv/www

browser-sync start --proxy localhost:15672 --serverStatic . --files .
```

BrowserSync will automatically open a browser window for you to use. The window
will automatically refresh when one of the static (templates, JS, CSS) files change.

All HTTP requests that BrowserSync does not know how to handle will be proxied to
the HTTP API at `localhost:15672`.


## Formatting the RabbitMQ CLI

The RabbitMQ CLI uses the standard [Elixir code formatter](https://hexdocs.pm/mix/main/Mix.Tasks.Format.html). To ensure correct code formatting of the CLI:

```
cd deps/rabbitmq_cli
mix format
```

Running `make` will validate the CLI formatting and issue any necessary warnings. Alternatively, run the format checker in the `deps/rabbitmq_cli` directory:

```
mix format --check-formatted
```

## Code of Conduct

See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

## Contributor Agreement

If you want to contribute a non-trivial change, please submit a signed copy of our
[Contributor Agreement](https://cla.pivotal.io/) around the time
you submit your pull request. This will make it much easier (in some cases, possible)
for the RabbitMQ team at Pivotal to merge your contribution.

## Where to Ask Questions

If something isn't clear, feel free to ask on our [mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users)
and [community Slack](https://rabbitmq-slack.herokuapp.com/).
