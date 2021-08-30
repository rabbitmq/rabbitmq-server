# Implementing Your Own rabbitmqctl Command

## Introduction

As of `3.7.0`, RabbitMQ [CLI
tools](https://github.com/rabbitmq/rabbitmq-cli) (e.g. `rabbitmqctl`)
allow plugin developers to extend them their own commands.

The CLI is written in the [Elixir programming
language](https://elixir-lang.org/) and commands can be implemented in
Elixir, Erlang or any other Erlang-based language.  This tutorial will
use Elixir but also provides an Erlang example.  The fundamentals are
the same.

This tutorial doesn't cover RabbitMQ plugin development process.
To develop a new plugin you should check existing tutorials:

 * [RabbitMQ Plugin Development](https://www.rabbitmq.com/plugin-development.html) (in Erlang)
 * [Using Elixir to Write RabbitMQ Plugins](https://www.rabbitmq.com/blog/2013/06/03/using-elixir-to-write-rabbitmq-plugins/)


## Anatomy of a RabbitMQ CLI Command

A RabbitMQ CLI command is an Elixir/Erlang module that implements a
particular [behavior](https://elixir-lang.org/getting-started/typespecs-and-behaviours.html).
It should fulfill certain requirements in order to be discovered and load by CLI tools:

 * Follow a naming convention (module name should match `RabbitMQ.CLI.(.*).Commands.(.*)Command`)
 * Be included in a plugin application's module list (`modules` in the `.app` file)
 * Implement `RabbitMQ.CLI.CommandBehaviour`

## Implementing `RabbitMQ.CLI.CommandBehaviour` in Erlang

When implementing a command in Erlang, you should add `Elixir` as a prefix to
the module name and behaviour, because CLI is written in Elixir.
It should match `Elixir.RabbitMQ.CLI.(.*).Commands.(.*)Command`
And implement `Elixir.RabbitMQ.CLI.CommandBehaviour`


## The Actual Tutorial

Let's write a command, that does something simple, e.g. deleting a queue.
We will use Elixir for that.

First we need to declare a module with a behaviour, for example:

```
defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
end
```

So far so good. But if we try to compile it, we'd see compilation errors:

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

So some functions are missing. Let's implement them.


### Usage: Help Section

We'll start with
the `usage/0` function, to provide command name in the help section:

```
  def usage(), do: "delete_queue queue_name [--if-empty|-e] [--if-unused|-u] [--vhost|-p vhost]"
```

### CLI Argument Parsing: Switches, Positional Arguments, Aliases

We want our command to accept a `queue_name` positional argument,
and two named arguments (flags): `if_empty` and `if_unused`,
and a `vhost` argument with a value.

We also want to specify shortcuts to our named arguments so that the user can use
`-e` instead of `--if-empty`.

We'll next implement the `switches/0` and `aliases/0` functions to let CLI know how it
should parse command line arguments for this command:

```
  def switches(), do: [if_empty: :boolean, if_unused: :boolean]
  def aliases(), do: [e: :if_empty, u: :is_unused]
```

Switches specify long arguments names and types, aliases specify shorter names.

You might have noticed there is no `vhost` switch there. It's because `vhost` is a global
switch and will be available to all commands in the CLI: after all, many things
in RabbitMQ are scoped per vhost.

Both `switches/0` and `aliases/0` callbacks are optional.
If your command doesn't have shorter argument names, you can omit `aliases/0`.
If the command doesn't have any named arguments at all, you can omit both functions.

We've described how the CLI should parse commands, now let's start describing what
the command should do.

### Command Banner

We start with the `banner/2` function, that tells a user what the command is going to do.
If you call the command with with `--dry-run` argument, it would only print the banner,
without executing the actual command:

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

The function above can access arguments and command flags (named arguments)
to decide what exactly it should do.

### Default Argument Values and Argument Validation

As you can see, the `banner/2` function accepts exactly one argument and expects
the `vhost`, `if_empty` and `if_unused` options.
To make sure the command have all the correct arguments, you can use
the `merge_defaults/2` and `validate/2` functions:

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

The `merge_defaults/2` function accepts positional and options and returns a tuple
with effective arguments and options that will be passed on to `validate/2`,
`banner/2` and `run/2`.

The `validate/2` function can return either `:ok` (just the atom) or a
tuple in the form of `{:validation_failure, error}`. The function above checks
that we have exactly one position argument and that it is not empty.

While this is not enforced, for a command to be practical
at least one `validate/2` head must return `:ok`.


### Command Execution

`validate/2` is useful for command line argument validation but there can be
other things that require validation before a command can be executed. For example,
a command may require a RabbitMQ node to be running (or stopped), a file to exist
and be readable, an environment variable to be exported and so on.

There's another validation function, `validate_execution_environment/2`, for
such cases. That function accepts the same arguments and must return either `:ok`
or `{:validation_failure, error}`. What's the difference, you may ask?
`validate_execution_environment/2` is optional.

To perform the actual command operation, the `run/2` command needs to be defined:

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

In the example above we delegate to a `:rabbit_misc` function in `run/2`. You can use any functions
from [rabbit_common](https://github.com/rabbitmq/rabbitmq-common) directly but to
do something on a broker (remote) node, you need to use RPC calls.
It can be the standard Erlang `rpc:call` set of functions or `rabbit_misc:rpc_call/4`.
The latter is used by all standard commands and is generally recommended.

Target RabbitMQ node name is passed in as the `node` option, which is
a global option and is available to all commands.


### Command Output

Finally we would like to present the user with a command execution result.
To do that, we'll define `output/2` to format the `run/2` return value:

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
  ## Use default output for all other cases
  use RabbitMQ.CLI.DefaultOutput
```

We have function clauses for every possible output of `rabbit_amqqueue:delete/3` used
in the `run/2` function.

For a run to be successful, the `output/2` function should return a pair of `{:ok, result}`,
and to indicate an error it should return a `{:error, exit_code, message}` tuple.
`exit_code` must be an integer and `message` is a string or a list of strings.

CLI program will exit with an `exit_code` in case of an error, or `0` in case of a success.

`RabbitMQ.CLI.DefaultOutput` is a module which can handle common error cases
(e.g. `badrpc` when the target RabbitMQ node cannot be contacted or authenticated with using the Erlang cookie).

In the example above, we use Elixir's `use` statement to import
function clauses for `output/2` from the `DefaultOutput` module. For
some commands such delegation will be sufficient.

### Testing the Command

That's it. Now you can add this command to your plugin, compile it, enable the plugin and run

`rabbitmqctl delete_queue my_queue --vhost my_vhost`

to delete a queue.


## Full Module Example in Elixir

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
  def validate([_], _options) do
    :ok
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
                                    [queue, if_unused, if_empty, "cli_user"]);
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

## Full Module Example in Erlang

The same module implemented in Erlang. Note the fairly
unusual Elixir module and behaviour names: since they contain
dots, they must be escaped with single quotes to be valid Erlang atoms:

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
                                       [Queue, IfUnused, IfEmpty, <<"cli_user">>]);
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

## Wrapping Up

Phew. That's it! Implementing a new CLI command wasn't too difficult.
That's because extensibility was one of the goals of this new CLI tool suite.


## Feedback and Getting Help

If you have any feedback about CLI tools extensibility,
don't hesitate to reach out on the [RabbitMQ mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).

