# New (3.7.0+) RabbitMQ CLI Tools

## Summary

RabbitMQ version 3.7 comes with brave new CLI tool to replace `rabbitmqctl`.

Some of the issues in the older tool suite we wanted to address:

 * Built-in into RabbitMQ server code
 * Home-grown argument parser
 * Started with `erl` with a lot of installation-dependent parameters
 * Too many commands in a single tool
 * All commands in resided the same module (function clauses)

All this made it hard to maintain and extend the tools.

The new CLI is different in a number of ways that address
the above issues:

 * Standalone [repository on GitHub](https://github.com/rabbitmq/rabbitmq-cli).
 * Implemented in Elixir (using Elixir's standard CLI parser)
 * Each command is its own module
 * Extensible
 * Commands can support more output formats (e.g. JSON and CSV)
 * A single executable file that bundles all dependencies but the Erlang runtime
 * Command scopes associate commands with tools (e.g. `rabbitmq-diagnostics` reuses relevant commands from `rabbitmqctl`)

## Architecture

Each command is defined in its own module and implements an Elixir (Erlang) behaviour.
(See [Command behaviour](#command-behaviour))

Output is processed by a formatter and printer which formats command output
and render the output (the default being the standard I/O output).
(see [Output formatting](#output-formatting))

CLI core consists of several modules implementing command execution process:

 * `RabbitMQCtl`: entry point. Generic execution logic.
 * `Parser`: responsible for command line argument parsing (drives Elixir's `OptionParser`)
 * `CommandModules`: responsible for command module discovery and loading
 * `Config`: responsible for config unification: merges environment variable and command argument values
 * `Output`: responsible for output formatting
 * `Helpers`: self-explanatory

### Command Execution Process

#### Arguments parsing

Command line arguments are parsed with [OptionParser](https://elixir-lang.org/docs/stable/elixir/OptionParser.html)
Parser returns a list of unnamed arguments and a map of options (named arguments)
First unnamed argument is a command name.
Named arguments can be global or command specific.
Command specific argument names and types are specified in the `switches/0` callback.
Global argument names are described in [Global arguments]

#### Command discovery

If arguments list is not empty, its first element is considered a command name.
Command name is converted to CamelCase and a module with
`RabbitMQ.CLI.*.Commands.<CommandName>Command` name is selected as a command module.

List of available command modules depend on current tool scope
(see [Command scopes](#command-scopes))

#### Defaults and validation

After the command module is found, effective command arguments are calculated by
 merging global defaults and command specific defaults for both unnamed and named arguments.

A command specifies defaults using the `merge_defaults/2` callback
 (see [Command behaviour](#command-behaviour))

Arguments are then validated using the `validate/2` callback. `validate_execution_environment/2` is
another (optional) validation function with the same signature as `validate/2`. It is meant to
validate everything that is not CLI arguments, for example, whether a file exists or RabbitMQ is running
or stopped on the target node.

##### Command Aliases

It is possible to define aliases for commands using an aliases file. The file name can be
specified using `RABBITMQ_CLI_ALIASES_FILE` environment variable or the `--aliases-file`
command lineargument.

Aliases can be specified using `alias = command [options]` format.
For example:

```
lq = list_queues
lq_vhost1 = list_queues -p vhost1
lq_off = list_queues --offline
```

with such aliases config file running

```
RABBITMQ_CLI_ALIASES_FILE=/path/to/aliases.conf rabbitmqctl lq
```

will be equivalent to executing

```
RABBITMQ_CLI_ALIASES_FILE=/path/to/aliases.conf rabbitmqctl list_queues
```

while

```
RABBITMQ_CLI_ALIASES_FILE=/path/to/aliases.conf rabbitmqctl lq_off
```

is the same as running

```
RABBITMQ_CLI_ALIASES_FILE=/path/to/aliases.conf rabbitmqctl list_queues --offline
```

Builtin or plugin-provided commands are looked up first, if that yields no result,
aliases are inspected. Therefore it's not possible to override a command by
configuring an alias.

Just like command lookups, alias expansion happens in the `RabbitMQ.CLI.Core.Parser`
module.


##### Command Aliases with Variables

Aliases can also contain arguments. Command name must be the first word after the `=`.
Arguments specified in an alias will precede those passed from the command line.

For example, if you specify the alias `passwd_user1 = change_password user1`,
you can call it with `rabbitmqctl passwd_user1 new_password`.

Combined with the `eval` command, aliases can be a powerful tool for users who cannot or don't want
to develop, distribute and deploy their own commands via plugins.

For instance, the following alias deletes all queues in a vhost:

```
delete_vhost_queues = eval '[rabbit_amqqueue:delete(Q, false, false, <<"rabbit-cli">>) || Q <- rabbit_amqqueue:list(_1)]'
```

This command will require a single positional argument for the vhost:

```
rabbitmqctl delete_vhost_queues vhost1
```

It is also possible to use a named vhost argument by specifying an underscore
variable that's not an integer:

```
delete_vhost_queues = eval '[rabbit_amqqueue:delete(Q, false, false, <<"rabbit-cli">>) || Q <- rabbit_amqqueue:list(_vhost)]'
```

Then the alias can be called like this:

```
rabbitmqctl delete_vhost_queues -p vhost1
# or
rabbitmqctl delete_vhost_queues ---vhost <vhost>1
```

Keep in mind that `eval` command can accept only [global arguments](#global-arguments) as named,
and it's advised to use positional arguments instead.

Numbered arguments will be passed to the eval'd code as Elixir strings (or Erlang binaries).
The code that relies on them should perform type conversion as necessary, e.g. by
using functions from the [`rabbit_data_coercion` module](https://github.com/rabbitmq/rabbitmq-common/blob/stable/src/rabbit_data_coercion.erl).

#### Command execution

 Command is executed with the `run/2` callback, which contains main command logic.
 This callback can return any value.

 Output from the `run/2` callback is being processed with the `output/2` callback,
 which should format the output to a specific type.
 (see [Output formatting](#output-formatting))

 Output callback can return an error, with a specific exit code.

#### Printing and formatting

 The `output/2` callback return value is being processed in `output.ex` module by
 by the appropriate formatter and printer.

 A formatter translates the output value to a sequence of strings or error value.
 Example formatters are `json`, `csv` and `erlang`

 A printer renders formatted strings to an output device (e.g. stdout, file)

#### Return

 Errors during execution (e.g. validation failures, command errors) are being printed
 to `stderr`.

 If command has failed to execute, a non-zero code is returned.


## Usage

CLI tool is an Elixir Mix application. It is compiled into an escript
executable file.

This file embeds Elixir, rabbit_common, and rabbitmqcli applications and can be executed anywhere
where `escript` is installed (it comes as a part of `erlang`).

Although, some commands require rabbitmq broker code and data directory to be known.
For example, commands in `rabbitmq-plugins` tool and those controlling clustering
(e.g. `forget_cluster_node`, `rename_cluster_node` ...)

Those directories can be defined using environment options or rabbitmq environment variables.

In the broker distribution the escript file is called from a shell/cmd script, which loads
broker environment and exports it into the script.

Environment variables also specify the locations of the enabled plugins file
and the plugins directory.

All enabled plugins will be searched for commands. Commands from enabled plugins will
be shown in usage and available for execution.

### Global Arguments

#### Commonly Used Arguments

 * node (n): atom, broker node name, defaults to `rabbit@<current host>`
 * quiet (q): boolean, if set to `true`, command banner is not shown, defaults to `false`
 * timeout (t): integer, timeout value in **seconds** (used in some commands), defaults to `infinity`
 * vhost (p): string, vhost to talk to, defaults to `/`
 * formatter: string, formatter to use to format command output. (see [Output formatting](#output-formatting))
 * printer: string, printer to render output (see [Output formatting](#output-formatting))
 * dry-run: boolean, if specified the command will not run, but print banner only

#### Environment Arguments

 * script-name: atom, configurable tool name (`rabbitmq-plugins`, `rabbitmqctl`) to select command scope (see [Command scopes](#command-scopes))
 * rabbitmq-home: string, broker install directory
 * mnesia-dir: string, broker mnesia data directory
 * plugins-dir: string, broker plugins directory
 * enabled-plugins-file: string, broker enabled plugins file
 * longnames (l): boolean, use longnames to communicate with broker erlang node. Should be set to `true` only if broker is started with longnames.
 * aliases-file: string, a file name to load aliases from
 * erlang-cookie: atom, an [erlang distribution cookie](http://erlang.org/doc/reference_manual/distributed.html)

Environment argument defaults are loaded from rabbitmq environment variables (see [Environment configuration](#environment-configuration)).

Some named arguments have single-letter aliases (in parenthesis).

Boolean options without a value are parsed as `true`

For example, parsing command

    rabbitmqctl list_queues --vhost my_vhost -t 10 --formatter=json name pid --quiet

Will result with unnamed arguments list `["list_queues", "name", "pid"]`
and named options map `%{vhost: "my_vhost", timeout: 10, quiet: true}`


### Usage (Listing Commands in `help`)

Usage is shown when the CLI is called without any arguments, or if there are some
problems parsing arguments.

In that cases exit code is `64`.

If you want to show usage and return `0` exit code run `help` command

    rabbitmqctl help

Each tool (`rabbitmqctl`, `rabbitmq-plugins`) shows its scope of commands (see [Command scopes](#command-scopes))

## Command Behaviour

Each command is implemented as an Elixir (or Erlang) module. Command module should
implement `RabbitMQ.CLI.CommandBehaviour` behaviour.

Behaviour summary:

Following functions MUST be implemented in a command module:

    usage() :: String.t | [String.t]

Command usage string, or several strings (one per line) to print in command listing in usage.
Typically looks like `command_name [arg] --option=opt_value`

    banner(arguments :: List.t, options :: Map.t) :: String.t

Banner to print before the command execution.
Ignored if argument `--quiet` is specified.
If `--dry-run` argument is specified, th CLI will only print the banner.

    merge_defaults(arguments :: List.t, options :: Map.t) :: {List.t, Map.t}

Merge default values for arguments and options (named arguments).
Returns a tuple with effective arguments and options, that will be passed to `validate/2` and `run/2`

    validate(arguments :: List.t, options :: Map.t) :: :ok | {:validation_failure, Atom.t | {Atom.t, String.t}}

Validate effective arguments and options.

If function returns `{:validation_failure, err}`
CLI will print usage to `stderr` and exit with non-zero exit code (typically 64).

    run(arguments :: List.t, options :: Map.t) :: run_result :: any

Run command. This function usually calls RPC on broker.

    output(run_result :: any, options :: Map.t) :: :ok | {:ok, output :: any} | {:stream, Enum.t} | {:error, ExitCodes.exit_code, [String.t]}

Cast the return value of `run/2` command to a formattable value and an exit code.

- `:ok` - return `0` exit code and won't print anything

- `{:ok, output}` - return `exit` code and print output with `format_output/2` callback in formatter

- `{:stream, stream}` - format with `format_stream/2` callback in formatter, iterating over enumerable elements.
Can return non-zero code, if error occurs during stream processing
(stream element for error should be `{:error, Message}`).

- `{:error, exit_code, strings}` - print error messages to `stderr` and return `exit_code` code

There is a default implementation for this callback in `DefaultOutput` module

Most of the standard commands use the default implementation via `use RabbitMQ.CLI.DefaultOutput`

Following functions are optional:

    switches() :: Keyword.t

Keyword list of switches (argument names and types) for the command.

For example: `def switches(), do: [offline: :boolean, time: :integer]` will
parse `--offline --time=100` arguments to `%{offline: true, time: 100}` options.

This switches are added to global switches (see [Arguments parsing](#arguments-parsing))

    aliases() :: Keyword.t

Keyword list of argument names one-letter aliases.
For example: `[o: :offline, t: :timeout]`
(see [Arguments parsing](#arguments-parsing))

    usage_additional() :: String.t | [String.t]

Additional usage strings to print after all commands basic usage.
Used to explain additional arguments and not interfere with command listing.

    formatter() :: Atom.t

Default formatter for the command.
Should be a module name of a module implementing `RabbitMQ.CLI.FormatterBehaviour` (see [Output formatting](#output-formatting))

    scopes() :: [Atom.t]

List of scopes to include command in. (see [Command scopes](#command-scopes))

More information about command development can be found in [the command tutorial](COMMAND_TUTORIAL.md)

## Command Scopes

Commands can be organized in scopes to be used in different tools
like `rabbitmq-diagnostics` or `rabbitmq-plugins`.

One command can belong to multiple scopes. Scopes for a command can be
defined in the `scopes/0` callback in the command module.
Each scope is defined as an atom value.

By default a command scope is selected using naming convention.
If command module is called `RabbitMQ.CLI.MyScope.Commands.DoSomethingCommand`, it will belong to
`my_scope` scope. A scope should be defined in snake_case. Namespace for the scope will be translated to CamelCase.

When CLI is run, a scope is selected by a script name, which is the escript file name
or the `--script-name` argument passed.

A script name is associated with a single scope in the application environment:

Script names for scopes:

 * `rabbitmqctl` - `:ctl`
 * `rabbitmq-plugins` - `:plugins`
 * `rabbitmq-diagnostics` - `:diagnostics`

This environment is extended by plugins `:scopes` environment variables,
but cannot be overridden. Plugins scopes can override each other,
so should be used with caution.

So all the commands in the `RabbitMQ.CLI.Ctl.Commands` namespace will be available
for `rabbitmqctl` script.
All the commands in the `RabbitMQ.CLI.Plugins.Commands` namespace will be available
for `rabbitmq-plugins` script.

To add a command to `rabbitmqctl`, one should either name it with
the `RabbitMQ.CLI.Ctl.Commands` prefix or add the `scopes()` callback,
returning a list with `:ctl` element

## Output Formatting

The CLI supports extensible output formatting. Formatting consists of two stages:

 * formatting - translating command output to a sequence of lines
 * printing - outputting the lines to some device (e.g. stdout or filesystem)

A formatter module performs formatting.
Formatter is a module, implementing the `RabbitMQ.CLI.FormatterBehaviour` behaviour:

    format_output(output :: any, options :: Map.t) :: String.t | [String.t]

Format a single value, returned. It accepts output from command and named arguments (options)
and returns a list of strings, that should be printed.

    format_stream(output_stream :: Enumerable.t, options :: Map.t) :: Enumerable.t

Format a stream of return values. This function uses elixir
Stream [https://elixir-lang.org/docs/stable/elixir/Stream.html] abstraction
to define processing of continuous data, so the CLI can output data in realtime.

Used in `list_*` commands, that emit data asynchronously.

`DefaultOutput` will return all enumerable data as stream,
so it will be formatted with this function.

A printer module performs printing. Printer module should implement
the `RabbitMQ.CLI.PrinterBehaviour` behaviour:

    init(options :: Map.t) :: {:ok, printer_state :: any} | {:error, error :: any}

Init the internal printer state (e.g. open a file handler).

    finish(printer_state :: any) :: :ok

Finalize the internal printer state.

    print_output(output :: String.t | [String.t], printer_state :: any) :: :ok

Print the output lines in the printer state context.
Is called for `{:ok, val}` command output after formatting `val` using formatter,
and for each enumerable element of `{:stream, enum}` enumerable

    print_ok(printer_state :: any) :: :ok

Print an output without any values. Is called for `:ok` command return value.

Output formatting logic is defined in `output.ex` module.

Following rules apply for a value, returned from `output/2` callback:

 * `:ok` - initializes a printer, calls `print_ok` and finishes the printer. Exit code is `0`.
 * `{:ok, value}` - calls `format_output/2` from formatter, then passes the value to printer. Exit code is `0`.
 * `{:stream, enum}` - calls `format_stream/2` to augment the stream with formatting logic,
initializes a printer, then calls `print_output/2` for each successfully formatted stream
element (which is not `{:error, msg}`). Exit code is `0` if there were no errors.
In case of an error element, stream processing stops, error is printed to `stderr`
and the CLI exits with nonzero exit code.
 * `{:error, exit_code, msg}` - prints `msg` to `stderr` and exits with `exit_code`


## Environment Configuration

Some commands require information about the server environment to run correctly.
Such information is:

 * rabbitmq code directory
 * rabbitmq mnesia directory
 * enabled plugins file
 * plugins directory

Enabled plugins file and plugins directory are also used to locate plugins commands.

This information can be provided using command line arguments (see [Environment arguments](#environment-arguments))

By default it will be loaded from environment variables, same as used in rabbitmq broker:

| Argument name        | Environment variable          |
|----------------------|-------------------------------|
| rabbitmq-home        | RABBITMQ_HOME                 |
| mnesia-dir           | RABBITMQ_MNESIA_DIR           |
| plugins-dir          | RABBITMQ_PLUGINS_DIR          |
| enabled-plugins-file | RABBITMQ_ENABLED_PLUGINS_FILE |
| longnames            | RABBITMQ_USE_LONGNAME         |
| node                 | RABBITMQ_NODENAME             |
| aliases-file         | RABBITMQ_CLI_ALIASES_FILE     |
| erlang-cookie        | RABBITMQ_ERLANG_COOKIE        |
