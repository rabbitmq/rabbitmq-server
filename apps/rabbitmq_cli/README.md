# RabbitMQ CLI Tools

[![Build Status](https://travis-ci.org/rabbitmq/rabbitmq-cli.svg?branch=master)](https://travis-ci.org/rabbitmq/rabbitmq-cli)

This repository contains [RabbitMQ CLI tools](https://rabbitmq.com/cli.html) ([rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) and
others).

This generation of CLI tools first shipped with RabbitMQ `3.7.0`.


## Goals

Team RabbitMQ wanted a set of tools that

 * Was extensible from/with plugins
 * Supported pluggable output formats (in particular machine-friendly ones)
 * Had good test coverage
 * Wasn't as coupled to the server repository
 * Could be used as a low risk vehicle for [Elixir](https://elixir-lang.org) evaluation

## Supported RabbitMQ Versions

Long lived branches in this repository track the same branch in RabbitMQ core and related
repositories. So `master` tracks `master` in rabbitmq-server, `v3.7.x` tracks branch `v3.7.x` in
rabbitmq-server and so on.

Please use the version of CLI tools that come with the RabbitMQ distribution version installed.


## Building

### Requirements

Building this project requires

 * Erlang/OTP 21.3 (or later)
 * [Elixir](https://elixir-lang.org/) 1.10.0 (or later).

Command line tools depend on [rabbitmq-common](https://github.com/rabbitmq/rabbitmq-common).
Dependencies are being resolved by `erlang.mk`

### Building Standalone Executables

This repo produces a `rabbitmqctl` executable which can be used as different tools
(`rabbitmq-plugins`, `rabbitmq-diagnostics`, `rabbitmq-queues`, `rabbitmq-streams`, `rabbitmq-upgrade`) by copying or symlinking it with different names.
Depending on the name, a different set of commands will be loaded and available, including
for `--help`.

To generate the executable, run

```
make
```

## Usage

### `rabbitmqctl`

See `rabbitmqctl help` and [rabbitmqctl man page](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) for details.

### `rabbitmq-plugins`

See `rabbitmq-plugins help` and [rabbitmq-plugins man page](https://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html) for details.

### `rabbitmq-diagnostics`

See `rabbitmq-diagnostics help` and [rabbitmq-diagnostics man page](https://www.rabbitmq.com/rabbitmq-diagnostics.8.html).




## Testing

See [CONTRIBUTING.md](CONTRIBUTING.md).


## Developing

### Adding a New Command

#### Conventions

RabbitMQ CLI tools use module name conventions to match the command-line
actions (commands) to modules. The convention is outlined in the `CommandBehaviour` module.

#### Command Module Interface

Each command module must implement the `RabbitMQ.CLI.CommandBehaviour` behaviour,
which includes the following functions:

  * `validate(args, opts)`, which returns either `:ok` or a tuple of `{:validation_failure, failure_detail}` where failure detail is typically one of: `:too_many_args`, `:not_enough_args` or `{:bad_argument, String.t}`.

  * `merge_defaults(args, opts)`, which is used to return updated arguments and/or options.

  * `run(args, opts)`, where the actual command is implemented. Here, `args` is a list of command-specific parameters and `opts` is a Map containing option flags.

  * `usage`, which returns a string describing the command, its arguments and its optional flags.
  * `banner(args, opts)`, which returns a string to be printed before the command output.

There are also a number of optional callbacks:

 * `switches`, which returns command specific switches.
 * `aliases`, which returns a list of command aliases (if any).
 * `formatter`: what output formatter should be used by default.
 * `usage_additional`: extra values appended to the `usage` output
   to provide additional command-specific documentation.
 * `scopes`: what scopes this command appears in. Scopes associate
   tools (e.g. `rabbitmqctl`, `rabbitmq-diagnostics`, `rabbitmq-queues`, `rabbitmq-streams`) with commands.
 * `distribution`: control erlang distribution.
   Can be `:cli` (default), `:none` or `{:fun, fun}`

### Tutorial

We have [a tutorial](./COMMAND_TUTORIAL.md) that demonstrates how to add a CLI
command that deletes a queue.

### Examples

See `lib/rabbitmq/cli/ctl/commands/status_command.ex` and `test/status_command_test.exs` for minimalistic
but not entirely trivial examples.


## Copyright and License

The project is [licensed under the MPL](LICENSE-MPL-RabbitMQ), the same license
as RabbitMQ.

(c) 2007-2020 VMware, Inc. or its affiliates.

