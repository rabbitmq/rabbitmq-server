## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
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

defmodule Rabbitmq.CLI.AutoComplete do
  alias RabbitMQ.CLI.Core.{CommandModules, Parser}

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
