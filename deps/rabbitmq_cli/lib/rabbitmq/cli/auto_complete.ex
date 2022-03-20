## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.AutoComplete do
  alias RabbitMQ.CLI.Core.{CommandModules, Parser}

  # Use the same jaro distance limit as in Elixir's "did you mean?"
  @jaro_distance_limit 0.77

  @spec complete(String.t(), [String.t()]) :: [String.t()]
  def complete(_, []) do
    []
  end

  def complete(script_name, args) do
    case Parser.parse_global(args) do
      {_, %{script_name: _args_script_name}, _} ->
        complete(args)

      _ ->
        complete(["--script-name", script_name | args])
    end
  end

  def suggest_command(_cmd_name, empty) when empty == %{} do
    nil
  end
  def suggest_command(typed, module_map) do
    suggestion =
      module_map
      |> Map.keys()
      |> Enum.map(fn existing ->
        {existing, String.jaro_distance(existing, typed)}
      end)
      |> Enum.max_by(fn {_, distance} -> distance end)

    case suggestion do
      {cmd, distance} when distance >= @jaro_distance_limit ->
        {:suggest, cmd}
      _ ->
        nil
    end
  end

  defp complete(tokens) do
    {command, command_name, _, _, _} = Parser.parse(tokens)
    last_token = List.last(tokens)

    case {command, command_name} do
      ## No command provided
      {_, ""} ->
        complete_default_opts(last_token)

      ## Command is not found/incomplete
      {:no_command, command_name} ->
        complete_command_name(command_name)

      {{:suggest, _}, command_name} ->
        complete_command_name(command_name)

      ## Command is found
      {command, _} ->
        complete_command_opts(command, last_token)
    end
    |> Enum.sort()
  end

  defp complete_default_opts(opt) do
    Parser.default_switches()
    |> Keyword.keys()
    |> Enum.map(fn sw -> "--" <> to_string(sw) end)
    |> select_starts_with(opt)
    |> format_options
  end

  defp complete_command_name(command_name) do
    module_map = CommandModules.module_map()

    case module_map[command_name] do
      nil ->
        module_map
        |> Map.keys()
        |> select_starts_with(command_name)

      _ ->
        command_name
    end
  end

  defp complete_command_opts(command, <<"-", _::binary>> = opt) do
    switches =
      command.switches
      |> Keyword.keys()
      |> Enum.map(fn sw -> "--" <> to_string(sw) end)

    # aliases = command.aliases
    #           |> Keyword.keys
    #           |> Enum.map(fn(al) -> "-" <> to_string(al) end)
    select_starts_with(switches, opt)
    |> format_options
  end

  defp complete_command_opts(_, _) do
    []
  end

  defp select_starts_with(list, prefix) do
    Enum.filter(list, fn el -> String.starts_with?(el, prefix) end)
  end

  defp format_options(options) do
    options
    |> Enum.map(fn opt -> String.replace(opt, "_", "-") end)
  end
end
