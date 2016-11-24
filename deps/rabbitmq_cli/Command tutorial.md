# Implementing your own rabbitmqctl command

RabbitMQ [CLI project](https://github.com/rabbitmq/rabbitmq-cli) allows plugin developers
to implement their own commands.

The CLI is written in Elixir language and commands can be implemented in Elixir,
Erlang or any other Erlang-based language.
This tutorial mostly targets Elixir implementation, but also provides an Erlang example.
Basic principles are the same.

This tutorial doesn't cover plugin development process.
To develop a new plugin you should check existing tutorials:

- [erlang](https://www.rabbitmq.com/plugin-development.html)
- [elixir](https://www.rabbitmq.com/blog/2013/06/03/using-elixir-to-write-rabbitmq-plugins/)

A CLI command is an elixir/erlang module.
It should follow some requirements to be discovered and used in CLI:

- Follow a naming convention (module name should match `RabbitMQ.CLI.(.*).Commands.(.*)Command`)
- Be included in a plugin application `modules`
- Implement `RabbitMQ.CLI.CommandBehaviour`

When implementing a command in Erlang, you should add `Elixir` as a prefix to
the module name and behaviour, because CLI is written in elixir.
It should match `Elixir.RabbitMQ.CLI.(.*).Commands.(.*)Command`
And implement `Elixir.RabbitMQ.CLI.CommandBehaviour`

Let's write a command, that does something simple, e.g. deleting a queue.
We will use Elixir language for that.

First, we need to declare a module and behaviour

```
defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
end
```

Good. But now we see compilation errors:

```
warning: undefined behaviour function usage/0 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1

warning: undefined behaviour function banner/2 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1

warning: undefined behaviour function merge_defaults/2 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1

warning: undefined behaviour function validate/2 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1

warning: undefined behaviour function run/2 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1

warning: undefined behaviour function output/2 (for behaviour RabbitMQ.CLI.CommandBehaviour)
  lib/delete_queue_command.ex:1
```

Lets implement the missing functions.
We start with the `usage/0` function, to describe how the command should be called.

```
  def usage(), do: "delete_queue queue_name [--if-empty|-e] [--if-unused|-u] [--vhost|-p vhost]"
```

We want our command to accept a `queue_name` unnamed argument,
and two flag arguments: `if_empty` and `if_unused`,
and `vhost` argument with a value.

We also want to specify our named arguments in a shorter format.

Then we implement `switches/0` and `aliases/0` functions to let CLI know how it
should parse command line arguments.

```
  def switches(), do: [if_empty: :boolean, if_unused: :boolean]
  def aliases(), do: [e: :if_empty, u: :is_unused]
```

Switches specify long arguments names and types, aliases specify shorter names.
You can see there is no `vhost` switch there. It's because `vhost` is a global
switch and will be available for any command in the CLI. (see [Global arguments])

`switches/0` and `aliases/0` callbacks are optional.
If your command doesn't have shorter argument names, you can omit `aliases/0`.
If the command doesn't have specific named arguments at all, you can omit both functions.

We've described how the CLI should parse commands, now let's start describing what
the command should do.

We start with the `banner/2` function, that tells a user what the command is going to do.
If you call the command with `--dry-run` argument, it will only print the banner,
without running the command.

```
  def banner([qname], %{vhost: vhost,
                        if_empty: if_empty,
                        if_unused: if_unused}) do
    if_empty_str = case if_empty do
      true  -> "if queue is empty"
      false -> ""
    end
    if_unused_str = case if_unused do
      true  -> "if queue is unused"
      false -> ""
    end
    "Deleting queue #{qname} on vhost #{vhost} " <>
      Enum.join([if_empty_str, if_unused_str], " and ")
  end

```

The function can access arguments and options to tell exactly what the command
is going to do.

As you can see, the `banner/2` function accepts exactly one argument and expects
`vhost`, `if_empty` and `if_unused` options.
To make sure the command have all correct arguments, you can use
`merge_defaults/2` and `validate/2` functions.

```
  def merge_defaults(args, options) do
    {
      args,
      Map.merge(%{if_empty: false, if_unused: false, vhost: "/"}, options)
    }
  end

  def validate([], _options) do
    {:validation_failure, :not_enough_args}
  end
  def validate([_,_|_], _options) do
    {:validation_failure, :too_many_args}
  end
  def validate([""], _options) do
    {
      :validation_failure,
      {:bad_argument, "queue name cannot be empty string."}
    }
  end
  def validate([_], _options) do
    :ok
  end
```

The `merge_defaults/2` function accepts arguments and options and returns a tuple
with effective arguments and options, that will be passed to `validate/2`,
`banner/2` and `run/2`.

The `validate/2` function can return either the `:ok` atom or the
`{:validate, failure}` tuple.
This function checks that we have exactly one argument and that it is not empty.

At least one `validate/2` clause should return `:ok`.


To do the actual thing, the `run/2` command is used:

```
  def run([qname], %{node: node, vhost: vhost,
                     if_empty: if_empty, if_unused: if_unused}) do
    ## Generate the queue resource name from queue name and vhost
    queue_resource = :rabbit_misc.r(vhost, :queue, qname)
    ## Lookup the queue on broker node using resource name
    case :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :lookup,
                                     [queue_resource]) do
      {:ok, queue} ->
        ## Delete the queue
        :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :delete,
                                    [queue, if_empty, if_unused]);
      {:error, _} = error -> error
    end
  end
```

As you can see, we use `:rabbit_misc` in the function. You can use any functions
from [rabbit_common](https://github.com/rabbitmq/rabbitmq-common) directly, but to
do something on a broker node, you should use RPC calls.
You can use regular erlang `rpc:call`.

Rabbit node name is specified with the `node` option, which is a global option and will
be available for all commands.


Finally, we would like to print the command execution result.
We use `output/2` to format `run/2` return value to standard formattable output.

```
  def output({:error, :not_found}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue not found"}
  end
  def output({:error, :not_empty}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is not empty"}
  end
  def output({:error, :in_use}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is in use"}
  end
  def output({:ok, queue_length}, _options) do
    {:ok, "Queue was successfully deleted with #{queue_length} messages"}
  end
  ## Use default output for all non-special case outputs
  use RabbitMQ.CLI.DefaultOutput
```

We have function clauses for all possible outputs of `rabbit_amqqueue:delete`.
For successfull result the `output/2` function should return `{:ok, result}`,
for errors it should return `{:error, exit_code, message}`,
where `exit_code` is an integer, and `message` is a string or a list of strings.

Program will exit with `exit_code` in case of error, or `0` in case of successfull result.

`RabbitMQ.CLI.DefaultOutput` is a module which handles different error cases
(e.g. `badrpc`). This `use` statement will import function clauses for `output/2`
from the `DefaultOutput` module. For some commands the
`use` statement can be sufficient to handle any output.


That's it. Now you can add this command to your plugin, enable it and run

`rabbitmqctl delete_queue my_queue --vhost my_vhost`

to delete the queue.


Full module definition in Elixir:

```
defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [if_empty: :boolean, if_unused: :boolean]
  def aliases(), do: [e: :if_empty, u: :is_unused]

  def usage(), do: "delete_queue queue_name [--if_empty|-e] [--if_unused|-u]"

  def banner([qname], %{vhost: vhost,
                        if_empty: if_empty,
                        if_unused: if_unused}) do
    if_empty_str = case if_empty do
      true  -> "if queue is empty"
      false -> ""
    end
    if_unused_str = case if_unused do
      true  -> "if queue is unused"
      false -> ""
    end
    "Deleting queue #{qname} on vhost #{vhost} " <>
      Enum.join([if_empty_str, if_unused_str], " and ")
  end

  def merge_defaults(args, options) do
    {
      args,
      Map.merge(%{if_empty: false, if_unused: false, vhost: "/"}, options)
    }
  end

  def validate([], _options) do
    {:validation_failure, :not_enough_args}
  end
  def validate([_,_|_], _options) do
    {:validation_failure, :too_many_args}
  end
  def validate([""], _options) do
    {
      :validation_failure,
      {:bad_argument, "queue name cannot be empty string."}
    }
  end

  def run([qname], %{node: node, vhost: vhost,
                     if_empty: if_empty, if_unused: if_unused}) do
    ## Generate queue resource name from queue name and vhost
    queue_resource = :rabbit_misc.r(vhost, :queue, qname)
    ## Lookup a queue on broker node using resource name
    case :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :lookup,
                                     [queue_resource]) do
      {:ok, queue} ->
        ## Delete queue
        :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :delete,
                                    [queue, if_empty, if_unused]);
      {:error, _} = error -> error
    end
  end

  def output({:error, :not_found}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue not found"}
  end
  def output({:error, :not_empty}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is not empty"}
  end
  def output({:error, :in_use}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is in use"}
  end
  def output({:ok, qlen}, _options) do
    {:ok, "Queue was successfully deleted with #{qlen} messages"}
  end
  ## Use default output for all non-special case outputs
  use RabbitMQ.CLI.DefaultOutput
end
```

The same module on Erlang.

```
-module('Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([switches/0, aliases/0, usage/0,
         banner/2, merge_defaults/2, validate/2, run/2, output/2]).

switches() -> [{if_empty, boolean}, {if_unused, boolean}].
aliases() -> [{e, if_empty}, {u, is_unused}].

usage() -> <<"delete_queue queue_name [--if_empty|-e] [--if_unused|-u] [--vhost|-p vhost]">>.

banner([Qname], #{vhost := Vhost,
                  if_empty := IfEmpty,
                  if_unused := IfUnused}) ->
    IfEmptyStr = case IfEmpty of
        true  -> ["if queue is empty"];
        false -> []
    end,
    IfUnusedStr = case IfUnused of
        true  -> ["if queue is unused"];
        false -> []
    end,
    iolist_to_binary(
        io_lib:format("Deleting queue ~s on vhost ~s ~s",
                      [Qname, Vhost,
                       string:join(IfEmptyStr ++ IfUnusedStr, " and ")])).

merge_defaults(Args, Options) ->
    {
      Args,
      maps:merge(#{if_empty => false, if_unused => false, vhost => <<"/">>},
                 Options)
    }.

validate([], _Options) ->
    {validation_failure, not_enough_args};
validate([_,_|_], _Options) ->
    {validation_failure, too_many_args};
validate([<<"">>], _Options) ->
    {
        validation_failure,
        {bad_argument, <<"queue name cannot be empty string.">>}
    };
validate([_], _Options) -> ok.

run([Qname], #{node := Node, vhost := Vhost,
               if_empty := IfEmpty, if_unused := IfUnused}) ->
    %% Generate queue resource name from queue name and vhost
    QueueResource = rabbit_misc:r(Vhost, queue, Qname),
    %% Lookup a queue on broker node using resource name
    case rabbit_misc:rpc_call(Node, rabbit_amqqueue, lookup, [QueueResource]) of
        {ok, Queue} ->
        %% Delete queue
            rabbit_misc:rpc_call(Node, rabbit_amqqueue, delete,
                                       [Queue, IfEmpty, IfUnused]);
        {error, _} = Error -> Error
    end.

output({error, not_found}, _Options) ->
    {
        error,
        'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_usage(),
        <<"Queue not found">>
    };
output({error, not_empty}, _Options) ->
    {
        error,
        'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_usage(),
        <<"Queue is not empty">>
    };
output({error, in_use}, _Options) ->
    {
        error,
        'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_usage(),
        <<"Queue is in use">>
    };
output({ok, qlen}, _Options) ->
    {ok, <<"Queue was successfully deleted with #{qlen} messages">>};
output(Other, Options) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Other, Options, ?MODULE).
```