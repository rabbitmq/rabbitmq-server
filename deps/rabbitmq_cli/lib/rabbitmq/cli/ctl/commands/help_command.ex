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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HelpCommand do
  alias RabbitMQ.CLI.Core.{CommandModules, Config, ExitCodes}
  alias RabbitMQ.CLI.Core.CommandModules


  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics, :plugins]

  def switches(), do: [list_commands: :boolean]

  def distribution(_), do: :none

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate(_, _), do: :ok

  def run([command_name | _], opts) do
    CommandModules.load(opts)

    case CommandModules.module_map()[command_name] do
      nil ->
        all_usage(opts)

      command ->
        all_usage(command, opts)
    end
  end

  def run(_, opts) do
    CommandModules.load(opts)

    case opts[:list_commands] do
      true -> commands_description()
      _ -> all_usage(opts)
    end
  end

  def output(result, _) do
    {:error, ExitCodes.exit_ok(), result}
  end

  def program_name(opts) do
    Config.get_option(:script_name, opts)
  end

  def all_usage(opts) do
    tool_name = program_name(opts)
    Enum.join(
      tool_usage(tool_name) ++
        options_usage() ++
        [Enum.join(["Commands:"] ++ commands_description(), "\n")] ++
        help_additional(tool_name),
      "\n\n"
    )
  end

  def all_usage(command, opts) do
    Enum.join([base_usage(command, opts)] ++
              options_usage() ++
              additional_usage(command),
              "\n\n")
  end

  def usage(), do: "help (<command> | [--list-commands])"

  def help_section(), do: :help

  defp tool_usage(tool_name) do
    [
      "\nUsage:\n" <>
        "#{tool_name} [-n <node>] [-t <timeout>] [-l|--longnames] [-q|--quiet] <command> [<command options>]"
    ]
  end

  def base_usage(command, opts) do
    tool_name = program_name(opts)

    maybe_timeout =
      case command_supports_timeout(command) do
        true -> " [-t <timeout>]"
        false -> ""
      end

    Enum.join([
      "\nUsage:\n",
      "#{tool_name} [-n <node>] [-l] [-q] " <>
        flatten_string(command.usage(), maybe_timeout)
    ])
  end

  defp flatten_string(list, additional) when is_list(list) do
    list
    |> Enum.map(fn line -> line <> additional end)
    |> Enum.join("\n")
  end

  defp flatten_string(str, additional) when is_binary(str) do
    str <> additional
  end

  defp options_usage() do
    [
    #   "General options:
    # short            | long          | description
    # -----------------|---------------|--------------------------------
    # -?               | --help        | displays command usage information
    # -n <node>        | --node <node> | connect to node <node>
    # -l               | --longnames   | use long host names
    # -q               | --quiet       | suppress informational messages
    # -s               | --silent      | suppress informational messages
                                     # | and table header row"
# ,
# "
# Default node is \"rabbit@server\", where `server` is the local hostname. On a host
# named \"server.example.com\", the node name of the RabbitMQ Erlang node will
# usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some
# non-default value at broker startup time). The output of hostname -s is usually
# the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for
# details of configuring the RabbitMQ broker.

# Most options have a corresponding \"long option\" i.e. \"-q\" or \"--quiet\".
# Long options for boolean values may be negated with the \"--no-\" prefix,
# i.e. \"--no-quiet\" or \"--no-table-headers\"

# Quiet output mode is selected with the \"-q\" flag. Informational messages are
# suppressed when quiet mode is in effect.

# If target RabbitMQ node is configured to use long node names, the \"--longnames\"
# option must be specified.

# Some commands accept an optional virtual host parameter for which
# to display results. The default value is \"/\"."
]
  end

  def commands_description() do
    module_map = CommandModules.module_map()

    pad_commands_to = Enum.reduce(module_map, 0,
      fn({name, _}, longest) ->
        name_length = String.length(name)
        case name_length > longest do
          true  -> name_length
          false -> longest
        end
      end)

    module_map
    |> Enum.map(
      fn({name, cmd}) ->
        description = case function_exported?(cmd, :description, 0) do
          true  -> cmd.description()
          false -> ""
        end
        help_section = case function_exported?(cmd, :help_section, 0) do
          true  -> cmd.help_section()
          false -> :other
        end
        {name, {description, help_section}}
      end)
    |> Enum.group_by(fn({name, {description, help_section}}) -> help_section end)
    |> Enum.sort_by(
      fn({help_section, _}) ->
        ## TODO: sort help sections
        case help_section do
          :other -> 100
          :help -> 1
          _ -> 2
        end
      end)
    |> Enum.map(
      fn({help_section, section_helps}) ->
        [
          section_head(help_section) <> ":" |
          Enum.sort(section_helps)
          |> Enum.map(
            fn({name, {description, _}}) ->
              "    #{String.pad_trailing(name, pad_commands_to)}  #{description}"
            end)
        ]

      end)
    |> Enum.concat()
  end

  def section_head(help_section) do
    case help_section do
      :help ->
        "Print this help and commad specific help"
      :user_management ->
        "User management"
      :cluster_management ->
        "Cluster management"
      :node_management ->
        "Node start/stop"
      :queues ->
        "Queue management"
      :list ->
        "List internal objects"
      :vhost ->
        "Vhost management"
      :parameters ->
        "Runtime parameters and policies"
      :report ->
        "Node status"
      :trace ->
        "Enable/disable tracing"
      :encode ->
        "Encoding/decoding"
      :settings ->
        "Node settings"
      :feature_flags ->
        "Feature flag management"
      :other ->
        "Other"
    end
  end

  def commands() do
    # Enum.map obtains the usage string for each command module.
    # Enum.each prints them all.
    CommandModules.module_map()
    |> Map.values()
    |> Enum.sort()
    |> Enum.map(fn cmd ->
      maybe_timeout =
        case command_supports_timeout(cmd) do
          true -> " [-t <timeout>]"
          false -> ""
        end

      case cmd.usage() do
        bin when is_binary(bin) ->
          bin <> maybe_timeout

        list when is_list(list) ->
          Enum.map(list, fn line -> line <> maybe_timeout end)
      end
    end)
    |> List.flatten()
    |> Enum.sort()
    |> Enum.map(fn cmd_usage -> "    #{cmd_usage}" end)
  end

  defp additional_usage(command) do
    if :erlang.function_exported(command, :usage_additional, 0) do
      case command.usage_additional() do
        list when is_list(list) ->
          ["<timeout> - operation timeout in seconds. Default is \"infinity\"." | list]

        bin when is_binary(bin) ->
          ["<timeout> - operation timeout in seconds. Default is \"infinity\".", bin]
      end
    else
      case command_supports_timeout(command) do
        true ->
          ["<timeout> - operation timeout in seconds. Default is \"infinity\"."]

        false ->
          []
      end
    end
  end

  defp help_additional(tool_name) do
    ["Use '#{tool_name} help <command>' to get more info about a specific command"]
  end

  defp additional_usage() do
    [
      # "<timeout> - operation timeout in seconds. Default is \"infinity\".",
      # CommandModules.module_map()
      # |> Map.values()
      # |> Enum.filter(&:erlang.function_exported(&1, :usage_additional, 0))
      # |> Enum.map(& &1.usage_additional)
      # |> Enum.join("\n\n")
    ]
  end

  defp command_supports_timeout(command) do
    case :erlang.function_exported(command, :switches, 0) do
      true -> nil != command.switches[:timeout]
      false -> false
    end
  end

  def banner(_, _), do: nil
end
