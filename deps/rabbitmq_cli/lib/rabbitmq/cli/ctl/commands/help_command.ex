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

  alias RabbitMQ.CLI.Core.CommandModules, as: CommandModules
  alias RabbitMQ.CLI.Core.ExitCodes,      as: ExitCodes
  alias RabbitMQ.CLI.Core.Config,         as: Config

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def validate(_, _), do: :ok

  def merge_defaults(args, opts), do: {args, opts}

  def scopes(), do: [:ctl, :diagnostics, :plugins]

  def switches(), do: [list_commands: :boolean]
  def aliases(), do: [l: :list_commands]

  def run([command_name|_], opts) do
    CommandModules.load(opts)
    case CommandModules.module_map[command_name] do
      nil ->
        all_usage(opts);
      command ->
        Enum.join([base_usage(command, opts)] ++
                  options_usage() ++
                  input_types(command), "\n")
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
              ["Commands:"] ++
              commands() ++
              input_types(), "\n")
  end

  def usage(), do: "help <command>"

  defp tool_usage(tool_name = :'rabbitmqctl') do
    ["Usage:",
     "#{tool_name} [-n <node>] [-t <timeout>] [-l] [-q] <command> [<command options>]"]

  end

  defp tool_usage(tool_name = :'rabbitmq-plugins') do
    ["Usage:",
     "#{tool_name} [-n <node>] [-q] <command> [<command options>]"]
  end

  defp tool_usage(tool_name = :'rabbitmq_plugins') do
    ["Usage:",
     "#{tool_name} [-n <node>] [-q] <command> [<command options>]"]
  end

  defp tool_usage(tool_name) do
    ["Usage:",
     "#{tool_name} [-n <node>] [-q] <command> [<command options>]"]
  end

  def base_usage(command, opts) do
    tool_name = program_name(opts)
    Enum.join(["Usage:",
               "#{tool_name} [-n <node>] [-t <timeout>] [-q] " <>
               flatten_string(command.usage())], "\n")
  end

  defp flatten_string(list) when is_list(list) do
    Enum.join(list, "\n")
  end
  defp flatten_string(str) when is_binary(str) do
    str
  end

  defp options_usage() do
    ["
General options:
    -n node
    -q quiet
    -t timeout
    -l longnames

Default node is \"rabbit@server\", where `server` is the local hostname. On a host
named \"server.example.com\", the node name of the RabbitMQ Erlang node will
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some
non-default value at broker startup time). The output of hostname -s is usually
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are
suppressed when quiet mode is in effect.

Operation timeout in seconds. Only applicable to \"list\" commands. Default is
\"infinity\".

If RabbitMQ broker uses long node names for erlang distribution, \"longnames\"
option should be specified.

Some commands accept an optional virtual host parameter for which
to display results. The default value is \"/\".\n"]
  end

  def commands() do
    # Enum.map obtains the usage string for each command module.
    # Enum.each prints them all.
    CommandModules.module_map
    |>  Map.values
    |>  Enum.sort
    |>  Enum.map(&(&1.usage))
    |>  List.flatten
    |>  Enum.sort
    |>  Enum.map(fn(cmd_usage) -> "    #{cmd_usage}" end)
  end

  defp input_types(command) do
    if :erlang.function_exported(command, :usage_additional, 0) do
      [command.usage_additional()]
    else
      []
    end
  end

  defp input_types() do
    [CommandModules.module_map
     |> Map.values
     |> Enum.filter(&:erlang.function_exported(&1, :usage_additional, 0))
     |> Enum.map(&(&1.usage_additional))
     |> Enum.join("\n\n")]
  end

  def banner(_,_), do: nil
end
