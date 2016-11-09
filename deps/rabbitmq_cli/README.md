# RabbitMQ CLI Tools

This is the [next generation](https://groups.google.com/forum/#!topic/rabbitmq-users/x0XugmBt-IE) implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) and
other RabbitMQ CLI tools.

This project is a work in progress and targets RabbitMQ `3.7.0` (currently the `master` branch).
For production use, go with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.


## Goals

Team RabbitMQ wanted a set of tools that

 * Was extensible from/with plugins
 * Supported pluggable output formats (in particular machine-friendly ones)
 * Had good test coverage
 * Wasn't as coupled to the server repository
 * Could be used as a low risk vehicle for [Elixir](elixir-lang.org) evaluation

## Supported RabbitMQ Versions

This version of CLI tools targets RabbitMQ master (future `3.7.0`). Some operations (for example, the `list_*` ones)
will not work with earlier server releases.



## Building

### Requirements

Building this project requires [Elixir](http://elixir-lang.org/) 1.3.1 or greater.

Command line tools depend on [rabbitmq-common](https://github.com/rabbitmq/rabbitmq-common).
Dependencies are being resolved by `erlang.mk`

### Building Standalone Executables

This repo produce a `rabbitmqctl` executable which can be used as different tools
by copying or symlinking it with different names.

Currently `rabbitmq-plugins` and `rabbitmq-diagnostics` tools are supported.

To generate the executable, run

```
make
```

## Using

### `rabbitmqctl`

`rabbitmqctl [-n node] [-t timeout] [-q] {command} [command options...]`

See the [rabbitmqctl man page](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) for a full list of options.


## Testing

Assuming you have:

 * installed [Elixir](http://elixir-lang.org/install.html)
 * have a local running RabbitMQ node with the `rabbitmq-federation` plugin enabled (for parameter management testing), e.g. `make run-broker PLUGINS='rabbitmq_federation rabbitmq_stomp'` from a [server release repository](https://github.com/rabbitmq/rabbitmq-server-release) clone

you can simply run `make tests` within the project root directory.

NOTE: You may see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using `setup context` functions in Mix to start a new distributed node and connect it to the RabbitMQ server. It complains because we don't actually use the context dictionary, but it's fine otherwise.


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

* `flags`, which returns command-specific option flags as a list of atoms.

* `banner(args, opts)`, which returns a string to be printed before the command output.

* `switches`, which returns command specific switches.

<br>

For example, to add a new command `rabbitmqctl egg_salad`:

1. Create a new test file `test/egg_salad_command_test.exs`.

2. In your new test file, define a module `RabbitMQ.CLI.Ctl.Commands.EggSaladCommandTest` that
   runs tests against command behaviour functions, e.g. `EggSaladCommand.run`, `EggSaladCommand.validate` etc.

3. Create a new source file `test/egg_salad_command.exs`.

4. Implement the all the `RabbitMQ.CLI.CommandBehaviour` functions in the new module.

See `lib/rabbitmq/cli/ctl/commands/status_command.ex` and `test/status_command_test.exs` for simple
examples.


## Copyright and License

The project is [licensed under the MPL](LICENSE-MPL-RabbitMQ), the same license
as RabbitMQ.

(c) Pivotal Software, Inc, 2016-Current.
