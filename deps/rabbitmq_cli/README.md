# RabbitMQCtl

An Elixir-based implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) CLI.

This is still very much a work in progress right now. For production use, go 
with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.


# Testing

Assuming you have:
 * installed [Elixir](http://elixir-lang.org/install.html)
 * set up an active instance of RabbitMQ

you can simply run `mix test` within the project root directory.

NOTE: You will see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using `setup context`
function in Mix to start a new distributed node and connect it to the 
RabbitMQ server. It complains because we don't actually use the context 
dictionary, but it's fine otherwise.


# Adding a New Command (the easy way)

RabbitMQCtl uses Elixir's `eval_string/2` method to match the command-line
argument to the right module. This keeps the main module a reasonable size,
but it does mean that commands have to follow a certain convention.

If you want to add a new command, make sure that the new command name is
`snake_case`, and that the command exists within a module of the same name.
Do not implement more than one command per module.

For example, to add a new command `rabbitmqctl egg_salad`:

1. Create a new test file `test/egg_salad_command_test.exs`.

2. Define a module `EggSaladCommandTest` that runs tests against a function
  `EggSaladCommand.egg_salad`.

3. Create a new source file `test/egg_salad_command.exs`.

4. Define a module `EggSaladCommand` that implements the `egg_salad/0`
  function.

See `src/status_command.ex` and `test/status_command_test.exs` for simple
examples of this format.


## Installation


## License

RabbitMQCtl is [licensed under the MPL](LICENSE-MPL-RabbitMQ).
