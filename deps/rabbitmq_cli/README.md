# RabbitMQ CLI Tools

This is a next generation implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) and 
other RabbitMQ CLI tools.

This is still very much a work in progress right now. For production use, go 
with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.


## Building

### Requirements

Building RabbitMQCtl requires Elixir 1.2.2 or greater. As Elixir runs on Erlang, you must also have Erlang installed (which you would need for RabbitMQ anyway).

RabbitMQCtl requires the [rabbitmq-common](https://github.com/rabbitmq/rabbitmq-common) repo. This library is included as a dependency in the `mix.exs` file, though, so the `mix deps.*` commands in the build process below will pull it in.

### Building a Standalone Executable

To generate an executable `rabbitmqctl`, run the following commands:

```
mix deps.get
mix deps.compile
mix escript.build
```

## Using

`rabbitmqctl [-n node] [-t timeout] [-q] {command} [command options...]`

See the [man page](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) for a ful list of options.


## Testing

Assuming you have:

 * installed [Elixir](http://elixir-lang.org/install.html)
 * set up an active instance of RabbitMQ with NO users or vhosts beyond the defaults
 * Are running the `rabbitmq-federation` plugin (for parameter management testing)

you can simply run `mix test` within the project root directory.

NOTE: You may see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using `setup context` functions in Mix to start a new distributed node and connect it to the RabbitMQ server. It complains because we don't actually use the context dictionary, but it's fine otherwise.


## Developing
### Adding a New Command (the easy way)

RabbitMQCtl uses Elixir's `eval_string/2` method to match the command-line
argument to the right module. This keeps the main module a reasonable size,
but it does mean that commands have to follow a certain convention.

If you want to add a new command, make sure that the new command name is
`snake_case`, and that the command exists within a module of the same name.
Do not implement more than one command per module.

Commands should follow the form `command_name(args, opts)`, where:

* `args` is a list of command-specific parameters.
* `opts` is a Map containing standard `rabbitmqctl` flags.

You will also need to implement a `usage/0` method that returns a usage 
string if you want your command to show up in the usage output (i.e., what 
comes out of `rabbitmqctl help`).

For example, to add a new command `rabbitmqctl egg_salad`:

1. Create a new test file `test/egg_salad_command_test.exs`.

2. In your new test file, define a module `EggSaladCommandTest` that runs tests against a function
  `EggSaladCommand.egg_salad`.

3. Create a new source file `test/egg_salad_command.exs`.

4. In your new source file, define a module `EggSaladCommand` that implements the `egg_salad/2`
  function and the `usage/0` function.

See `src/status_command.ex` and `test/status_command_test.exs` for simple
examples of this format.


## License

The project is [licensed under the MPL](LICENSE-MPL-RabbitMQ), the same license
as RabbitMQ.
