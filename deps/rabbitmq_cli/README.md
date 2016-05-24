# RabbitMQ CLI Tools

This is a next generation implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) and 
other RabbitMQ CLI tools.

This is still very much a work in progress right now. For production use, go 
with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.


## Building

### Requirements

Building this project requires Elixir 1.2.2 or greater.

Command line tools depend on [rabbitmq-common](https://github.com/rabbitmq/rabbitmq-common). This library is included as a dependency in the `mix.exs` file, though, so the `mix deps.*` commands in the build process below will pull it in.

### Building Standalone Executables

`rabbitmqctl` is the only executable provided at the moment. To generate a runnable version,
use the following commands:

```
mix deps.get
mix compile
mix escript.build
```

## Using

### `rabbitmqctl`

`rabbitmqctl [-n node] [-t timeout] [-q] {command} [command options...]`

See the [rabbitmqctl man page](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) for a full list of options.


## Testing

Assuming you have:

 * installed [Elixir](http://elixir-lang.org/install.html)
 * have a local running RabbitMQ node with the `rabbitmq-federation` plugin enabled (for parameter management testing), e.g. `make run-broker PLUGINS='rabbitmq_federation rabbitmq_management'` from a server repository clone

you can simply run `mix test` within the project root directory.

NOTE: You may see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using `setup context` functions in Mix to start a new distributed node and connect it to the RabbitMQ server. It complains because we don't actually use the context dictionary, but it's fine otherwise.


## Developing
### Adding a New Command

RabbitMQCtl uses Elixir's `Code.eval_string/2` method to match the command-line
argument to the right module. This keeps the main module a reasonable size,
but it does mean that commands have to follow a certain convention. This convention is outlined in the `CommandBehaviour` module.


Each command module requires the following methods:

* `run(args, opts)`, where the actual command is implemented. Here, `args` is a list of command-specific parameters and `opts` is a Map containing option flags.

* `usage`, which returns a string describing the command, its arguments and its optional flags.

* `flags`, which returns command-specific option flags as a list of atoms.

<br>

For example, to add a new command `rabbitmqctl egg_salad`:

1. Create a new test file `test/egg_salad_command_test.exs`.

2. In your new test file, define a module `EggSaladCommandTest` that runs tests against a function
  `EggSaladCommand.run`.

3. Create a new source file `test/egg_salad_command.exs`.

4. In your new source file, define a module `EggSaladCommand` that implements the `run/2` function, the `flags/0` function and the `usage/0` function.

See `src/status_command.ex` and `test/status_command_test.exs` for simple
examples of this format.


## License

The project is [licensed under the MPL](LICENSE-MPL-RabbitMQ), the same license
as RabbitMQ.
