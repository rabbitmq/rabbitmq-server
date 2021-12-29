## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

alias RabbitMQ.CLI.CommandBehaviour

defmodule RabbitMQ.CLI.Ctl.Commands.HelpCommand do
  alias RabbitMQ.CLI.Core.{CommandModules, Config, ExitCodes}
  alias RabbitMQ.CLI.Core.CommandModules

  import RabbitMQ.CLI.Core.ANSI

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics, :plugins, :queues, :tanzu, :upgrade]
  def switches(), do: [list_commands: :boolean]

  def distribution(_), do: :none
  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _), do: :ok
  def validate([_command], _), do: :ok
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def run([command_name | _], opts) do
    CommandModules.load(opts)

    module_map = CommandModules.module_map()
    case module_map[command_name] do
      nil ->
        # command not found
        # {:error, all_usage(opts)}
        case RabbitMQ.CLI.AutoComplete.suggest_command(command_name, module_map) do
          {:suggest, suggested} ->
            suggest_message = "\nCommand '#{command_name}' not found. \n" <>
              "Did you mean '#{suggested}'? \n"
            {:error, ExitCodes.exit_usage(), suggest_message}
          nil ->
            {:error, ExitCodes.exit_usage(), "\nCommand '#{command_name}' not found."}
        end

      command ->
        {:ok, command_usage(command, opts)}
    end
  end

  def run([], opts) do
    CommandModules.load(opts)

    case opts[:list_commands] do
      true ->
        {:ok, commands_description()}
      _ ->
        {:ok, all_usage(opts)}
    end
  end

  def output({:ok, result}, _) do
    {:ok, result}
  end
  def output({:error, result}, _) do
    {:error, ExitCodes.exit_usage(), result}
  end
  use RabbitMQ.CLI.DefaultOutput

  def banner(_, _), do: nil

  def help_section(), do: :help

  def description(), do: "Displays usage information for a command"

  def usage(), do: "help (<command> | [--list-commands])"

  def usage_additional() do
    [
      ["--list-commands", "only output a list of discovered commands"]
    ]
  end


  #
  # Implementation
  #

  def all_usage(opts) do
    tool_name = program_name(opts)
    tool_usage(tool_name) ++
        ["\n\nAvailable commands:\n"] ++ commands_description() ++
        help_footer(tool_name)
  end

  def command_usage(command, opts) do
    Enum.join([base_usage(command, opts)] ++
              command_description(command) ++
              additional_usage(command) ++
              relevant_doc_guides(command) ++
              general_options_usage(),
              "\n\n") <> "\n"
  end

  defp tool_usage(tool_name) do
    [
      "\n#{bright("Usage")}\n\n" <>
        "#{tool_name} [--node <node>] [--timeout <timeout>] [--longnames] [--quiet] <command> [<command options>]"
    ]
  end

  def base_usage(command, opts) do
    tool_name = program_name(opts)

    maybe_timeout =
      case command_supports_timeout(command) do
        true -> " [--timeout <timeout>]"
        false -> ""
      end

    Enum.join([
      "\n#{bright("Usage")}\n\n",
      "#{tool_name} [--node <node>] [--longnames] [--quiet] " <>
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

  defp general_options_usage() do
    [
    "#{bright("General Options")}

The following options are accepted by most or all commands.

short            | long          | description
-----------------|---------------|--------------------------------
-?               | --help        | displays command help
-n <node>        | --node <node> | connect to node <node>
-l               | --longnames   | use long host names
-t               | --timeout <n> | for commands that support it, operation timeout in seconds
-q               | --quiet       | suppress informational messages
-s               | --silent      | suppress informational messages
                                 | and table header row
-p               | --vhost       | for commands that are scoped to a virtual host,
                 |               | virtual host to use
                 | --formatter   | alternative result formatter to use
                                 | if supported: json, pretty_table, table, csv, erlang
                                   not all commands support all (or any) alternative formatters."]
  end

  defp command_description(command) do
    case CommandBehaviour.description(command) do
      ""    -> []
      other -> [other <> ".\n"]
    end
  end

  defp list_item_formatter([option, description]) do
    "#{option}\n\t#{description}\n"
  end
  defp list_item_formatter({option, description}) do
    "#{option}\n\t#{description}\n"
  end
  defp list_item_formatter(line) do
    "#{line}\n"
  end

  defp additional_usage(command) do
    command_usage =
      case CommandBehaviour.usage_additional(command) do
        list when is_list(list) -> list |> Enum.map(&list_item_formatter/1)
        bin when is_binary(bin) -> ["#{bin}\n"]
      end
    case command_usage do
      []    -> []
      usage ->
        [flatten_string(["#{bright("Arguments and Options")}\n" | usage], "")]
    end
  end

  defp relevant_doc_guides(command) do
    guide_list =
      case CommandBehaviour.usage_doc_guides(command) do
        list when is_list(list) -> list |> Enum.map(fn ln -> " * #{ln}\n" end)
        bin when is_binary(bin) -> [" * #{bin}\n"]
      end
    case guide_list do
      []    -> []
      usage ->
        [flatten_string(["#{bright("Relevant Doc Guides")}\n" | usage], "")]
    end
  end

  defp help_footer(tool_name) do
    ["Use '#{tool_name} help <command>' to learn more about a specific command"]
  end

  defp command_supports_timeout(command) do
    nil != CommandBehaviour.switches(command)[:timeout]
  end

  defp commands_description() do
    module_map = CommandModules.module_map()

    pad_commands_to = Enum.reduce(module_map, 0,
      fn({name, _}, longest) ->
        name_length = String.length(name)
        case name_length > longest do
          true  -> name_length
          false -> longest
        end
      end)

    lines = module_map
    |> Enum.map(
      fn({name, cmd}) ->
        description = CommandBehaviour.description(cmd)
        help_section = CommandBehaviour.help_section(cmd)
        {name, {description, help_section}}
      end)
    |> Enum.group_by(fn({_, {_, help_section}}) -> help_section end)
    |> Enum.sort_by(
      fn({help_section, _}) ->
        case help_section do
          :deprecated -> 999
          :other -> 100
          {:plugin, _} -> 99
          :help -> 1
          :node_management -> 2
          :cluster_management -> 3
          :replication -> 3
          :user_management -> 4
          :access_control -> 5
          :observability_and_health_checks -> 6
          :parameters -> 7
          :policies -> 8
          :virtual_hosts -> 9
          _ -> 98
        end
      end)
    |> Enum.map(
      fn({help_section, section_helps}) ->
        [
          "\n" <> bright(section_head(help_section)) <> ":\n\n" |
          Enum.sort(section_helps)
          |> Enum.map(
            fn({name, {description, _}}) ->
              "   #{String.pad_trailing(name, pad_commands_to)}  #{description}\n"
            end)
        ]

      end)
    |> Enum.concat()

    lines ++ ["\n"]
  end

  defp section_head(help_section) do
    case help_section do
      :help ->
        "Help"
      :user_management ->
        "Users"
      :cluster_management ->
        "Cluster"
      :replication ->
        "Replication"
      :node_management ->
        "Nodes"
      :queues ->
        "Queues"
      :observability_and_health_checks ->
        "Monitoring, observability and health checks"
      :virtual_hosts ->
          "Virtual hosts"
      :access_control ->
        "Access Control"
      :parameters ->
        "Parameters"
      :policies ->
        "Policies"
      :configuration ->
        "Configuration and Environment"
      :feature_flags ->
        "Feature flags"
      :other ->
        "Other"
      {:plugin, plugin} ->
        plugin_section(plugin) <> " plugin"
      custom ->
        snake_case_to_capitalized_string(custom)
    end
  end

  defp strip_rabbitmq_prefix(value, regex) do
    Regex.replace(regex, value, "")
  end

  defp format_known_plugin_name_fragments(value) do
    case value do
      ["amqp1.0"]    -> "AMQP 1.0"
      ["amqp1", "0"] -> "AMQP 1.0"
      ["management"]  -> "Management"
      ["management", "agent"]  -> "Management"
      ["mqtt"]       -> "MQTT"
      ["stomp"]      -> "STOMP"
      ["web", "mqtt"]  -> "Web MQTT"
      ["web", "stomp"] -> "Web STOMP"
      [other]        -> snake_case_to_capitalized_string(other)
      fragments      -> snake_case_to_capitalized_string(Enum.join(fragments, "_"))
    end
  end

  defp plugin_section(plugin) do
    regex = Regex.recompile!(~r/^rabbitmq_/)

    to_string(plugin)
    # drop rabbitmq_
    |> strip_rabbitmq_prefix(regex)
    |> String.split("_")
    |> format_known_plugin_name_fragments()
  end

  defp snake_case_to_capitalized_string(value) do
    to_string(value)
    |> String.split("_")
    |> Enum.map(&String.capitalize/1)
    |> Enum.join(" ")
  end

  defp program_name(opts) do
    Config.get_option(:script_name, opts)
  end
end
