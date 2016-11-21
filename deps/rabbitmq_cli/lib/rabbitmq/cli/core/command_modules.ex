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

alias RabbitMQ.CLI.Core.Config, as: Config
alias RabbitMQ.CLI.Plugins.Helpers, as: PluginsHelpers
alias RabbitMQ.CLI.Core.Helpers, as: Helpers

defmodule RabbitMQ.CLI.Core.CommandModules do
  @commands_ns ~r/RabbitMQ.CLI.(.*).Commands/

  def module_map do
    Application.get_env(:rabbitmqctl, :commands) || load(%{})
  end

  def is_command?(str), do: module_map[str] != nil

  def load(opts) do
    scope = script_scope(opts)
    commands = load_commands(scope, opts)
    Application.put_env(:rabbitmqctl, :commands, commands)
    commands
  end

  def script_scope(opts) do
    scopes = Application.get_env(:rabbitmqctl, :scopes, [])
    scopes[Config.get_option(:script_name, opts)] || :none
  end

  def load_commands(scope, opts) do
    ctl_and_plugin_modules(opts)
    |> Enum.filter(fn(mod) ->
                     to_string(mod) =~ @commands_ns
                     and
                     module_exists?(mod)
                     and
                     implements_command_behaviour?(mod)
                     and
                     command_in_scope(mod, scope)
                   end)
    |> Enum.map(&command_tuple/1)
    |> Map.new
  end

  def ctl_and_plugin_modules(opts) do
    Helpers.require_rabbit(opts)
    enabled_plugins = PluginsHelpers.read_enabled(opts)
    [:rabbitmqctl | enabled_plugins]
    |> Enum.flat_map(fn(app) -> Application.spec(app, :modules) || [] end)
  end

  defp module_exists?(nil) do
    false
  end
  defp module_exists?(mod) do
    Code.ensure_loaded?(mod)
  end

  defp implements_command_behaviour?(nil) do
    false
  end
  defp implements_command_behaviour?(module) do
    Enum.member?(module.module_info(:attributes)[:behaviour] || [],
                 RabbitMQ.CLI.CommandBehaviour)
  end

  def module_to_command(mod) do
    mod
    |> to_string
    |> strip_namespace
    |> to_snake_case
    |> String.replace_suffix("_command", "")
  end

  defp command_tuple(cmd) do
    {module_to_command(cmd), cmd}
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
      <<"_", c + 32>>
    else
      <<c>>
    end
  end

  defp to_lower_char(c) do
    if (c >= ?A) and (c <= ?Z) do
      c + 32
    else
      c
    end
  end

  defp command_in_scope(_cmd, :none) do
    case Mix.env do
      :test -> true;
      _     -> false
    end
  end
  defp command_in_scope(_cmd, :all) do
    true
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
