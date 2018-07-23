## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.HelpCommand do

  alias RabbitMQ.CLI.Core.{CommandModules, Config, ExitCodes}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def validate(_, _), do: :ok

  def distribution(_), do: :none

  def merge_defaults(args, opts), do: {args, opts}

  def scopes(), do: [:ctl, :diagnostics, :plugins]

  def switches(), do: [list_commands: :boolean]

  def run([command_name|_], opts) do
    CommandModules.load(opts)
    case CommandModules.module_map[command_name] do
      nil ->
        all_usage(opts);
      command ->
        Enum.join([base_usage(command, opts)] ++
                  options_usage() ++
                  additional_usage(command), "\n\n")
    end
  end
  def run(_, opts) do
    CommandModules.load(opts)
    case opts[:list_commands] do
      true  -> commands();
      _     -> all_usage(opts)
    end
  end

  def output(result, _) do
    {:error, ExitCodes.exit_ok, result}
  end

  def program_name(opts) do
    Config.get_option(:script_name, opts)
  end

  def all_usage(opts) do
    Enum.join(tool_usage(program_name(opts)) ++
              options_usage() ++
              [Enum.join(["Commands:"] ++ commands(), "\n")] ++
              additional_usage(), "\n\n")
  end

  def usage(), do: "help <command>"

  defp tool_usage(tool_name) do
    ["\nUsage:\n" <>
     "#{tool_name} [-n <node>] [-l] [-q] <command> [<command options>]"]
  end

  def base_usage(command, opts) do
    tool_name = program_name(opts)
    maybe_timeout = case command_supports_timeout(command) do
      true  -> " [-t <timeout>]"
      false -> ""
    end
    Enum.join(["\nUsage:\n",
               "#{tool_name} [-n <node>] [-l] [-q] " <>
               flatten_string(command.usage(), maybe_timeout)])
  end

  defp flatten_string(list, additional) when is_list(list) do
    list
    |> Enum.map(fn(line) -> line <> additional end)
    |> Enum.join("\n")
  end
  defp flatten_string(str, additional) when is_binary(str) do
    str <> additional
  end

  defp options_usage() do
    ["General options:
    -n node
    -q quiet
    -l longnames

Default node is \"rabbit@server\", where `server` is the local hostname. On a host
named \"server.example.com\", the node name of the RabbitMQ Erlang node will
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some
non-default value at broker startup time). The output of hostname -s is usually
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are
suppressed when quiet mode is in effect.

If RabbitMQ broker uses long node names for erlang distribution, \"longnames\"
option should be specified.

Some commands accept an optional virtual host parameter for which
to display results. The default value is \"/\"."]
  end

  def commands() do
    # Enum.map obtains the usage string for each command module.
    # Enum.each prints them all.
    CommandModules.module_map
    |>  Map.values
    |>  Enum.sort
    |>  Enum.map( fn(cmd) ->
                    maybe_timeout = case command_supports_timeout(cmd) do
                      true  -> " [-t <timeout>]"
                      false -> ""
                    end
                    case cmd.usage() do
                      bin when is_binary(bin) ->
                        bin <> maybe_timeout;
                      list when is_list(list) ->
                        Enum.map(list, fn(line) -> line <> maybe_timeout end)
                    end
                  end)
    |>  List.flatten
    |>  Enum.sort
    |>  Enum.map(fn(cmd_usage) -> "    #{cmd_usage}" end)
  end

  defp additional_usage(command) do
    if :erlang.function_exported(command, :usage_additional, 0) do
      case command.usage_additional() do
        list when is_list(list) -> ["<timeout> - operation timeout in seconds. Default is \"infinity\"." | list];
        bin when is_binary(bin) -> ["<timeout> - operation timeout in seconds. Default is \"infinity\".", bin]
      end
    else
      case command_supports_timeout(command) do
        true ->
          ["<timeout> - operation timeout in seconds. Default is \"infinity\"."];
        false ->
          []
      end
    end
  end

  defp additional_usage() do
    ["<timeout> - operation timeout in seconds. Default is \"infinity\".",
     CommandModules.module_map
     |> Map.values
     |> Enum.filter(&:erlang.function_exported(&1, :usage_additional, 0))
     |> Enum.map(&(&1.usage_additional))
     |> Enum.join("\n\n")]
  end

  defp command_supports_timeout(command) do
    case :erlang.function_exported(command, :switches, 0) do
      true  -> nil != command.switches[:timeout];
      false -> false
    end
  end

  def banner(_,_), do: nil
end
