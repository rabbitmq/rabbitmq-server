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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.CommandModules do
  @commands_ns ~r/RabbitMQ.CLI.(.*).Commands/

  def module_map do
    case Application.get_env(:rabbitmqctl, :commands) do
      nil -> load;
      val -> val
    end
  end

  def load do
    scope = script_scope
    commands = load_commands(scope)
    Application.put_env(:rabbitmqctl, :commands, commands)
    commands
  end

  def script_scope do
    scopes = Application.get_env(:rabbitmqctl, :scopes, [])
    scopes[script_name] || :none
  end

  def script_name do
    Path.basename(:escript.script_name())
    |> Path.rootname
    |> String.to_atom
  end

  defp load_commands(scope) do
    ctl_and_plugin_modules
    |> Enum.filter(fn(mod) ->
                     to_string(mod) =~ @commands_ns
                     and
                     implements_command_behaviour?(mod)
                     and
                     command_in_scope(mod, scope)
                   end)
    |> Enum.map(&command_tuple/1)
    |> Map.new
  end

  defp ctl_and_plugin_modules do
    # No plugins so far
    applications = [:rabbitmqctl]
    applications
    |> Enum.flat_map(fn(app) -> Application.spec(app, :modules) end)
  end


  defp implements_command_behaviour?(nil) do
    false
  end
  defp implements_command_behaviour?(module) do
    Enum.member?(module.module_info(:attributes)[:behaviour] || [],
                 RabbitMQ.CLI.CommandBehaviour)
  end

  defp command_tuple(cmd) do
    {
      cmd
      |> to_string
      |> strip_namespace
      |> to_snake_case
      |> String.replace_suffix("_command", ""),
      cmd
    }
  end

  def strip_namespace(str) do
    str
    |> String.split(".")
    |> List.last
  end

  def to_snake_case(<<c, str :: binary>>) do
    tail = for <<c <- str>>, into: "", do: snake(c)
    <<to_lower_char(c), tail :: binary >>
  end

  defp snake(c) do
    if (c >= ?A) and (c <= ?Z) do
      <<"_", c+32>>
    else
      <<c>>
    end
  end

  defp to_lower_char(c) do
    if (c >= ?A) and (c <= ?Z) do
      c+32
    else
      c
    end
  end

  defp command_in_scope(_cmd, :none) do
    false
  end
  defp command_in_scope(cmd, scope) do
    Enum.member?(command_scopes(cmd), scope)
  end

  defp command_scopes(cmd) do
    case :erlang.function_exported(cmd, :scopes, 0) do
      true  ->
        cmd.scopes()
      false ->
        @commands_ns
        |> Regex.run(to_string(cmd), capture: :all_but_first)
        |> List.first
        |> to_snake_case
        |> String.to_atom
        |> List.wrap
    end
  end
end
