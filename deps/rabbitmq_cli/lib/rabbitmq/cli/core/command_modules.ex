## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.CommandModules do
  alias RabbitMQ.CLI.Core.{Config, DataCoercion}
  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginsHelpers
  alias RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.CodePath

  @commands_ns ~r/RabbitMQ.CLI.(.*).Commands/

  def module_map(opts \\ %{}) do
    Application.get_env(:rabbitmqctl, :commands) || load(opts)
  end

  def module_map_core(opts \\ %{}) do
    Application.get_env(:rabbitmqctl, :commands_core) || load_core(opts)
  end

  def load_core(opts) do
    scope = script_scope(opts)
    commands = load_commands_core(scope)
    Application.put_env(:rabbitmqctl, :commands_core, commands)
    commands
  end

  def load(opts) do
    scope = script_scope(opts)
    commands = load_commands(scope, opts)
    Application.put_env(:rabbitmqctl, :commands, commands)
    commands
  end

  def script_scope(opts) do
    scopes = Application.get_env(:rabbitmqctl, :scopes, [])
    scopes[DataCoercion.to_atom(Config.get_option(:script_name, opts))] || :none
  end

  def load_commands_core(scope) do
    make_module_map(ctl_modules(), scope)
  end

  def load_commands(scope, opts) do
    make_module_map(plugin_modules(opts) ++ ctl_modules(), scope)
  end

  def ctl_modules() do
    Application.spec(:rabbitmqctl, :modules)
  end

  def plugin_modules(opts) do
    require_rabbit(opts)

    enabled_plugins =
      try do
        PluginsHelpers.read_enabled(opts)
      catch
        err ->
          {:ok, enabled_plugins_file} = PluginsHelpers.enabled_plugins_file(opts)
          require Logger

          Logger.warn(
            "Unable to read the enabled plugins file.\n" <>
              "  Reason: #{inspect(err)}\n" <>
              "  Commands provided by plugins will not be available.\n" <>
              "  Please make sure your user has sufficient permissions to read to\n" <>
              "    #{enabled_plugins_file}"
          )

          []
      end

    partitioned =
      Enum.group_by(enabled_plugins, fn app ->
        case Application.load(app) do
          :ok -> :loaded
          {:error, {:already_loaded, ^app}} -> :loaded
          _ -> :not_found
        end
      end)

    loaded = partitioned[:loaded] || []
    missing = partitioned[:not_found] || []
    ## If plugins are not in ERL_LIBS, they should be loaded from plugins_dir
    case missing do
      [] ->
        :ok

      _ ->
        add_plugins_to_load_path(opts)
        Enum.each(missing, fn app -> Application.load(app) end)
    end

    Enum.flat_map(loaded ++ missing, fn app ->
      Application.spec(app, :modules) || []
    end)
  end

  defp make_module_map(modules, scope) when modules != nil do
    commands_ns = Regex.recompile!(@commands_ns)

    modules
    |> Enum.filter(fn mod ->
      to_string(mod) =~ commands_ns and
        module_exists?(mod) and
        implements_command_behaviour?(mod) and
        command_in_scope(mod, scope)
    end)
    |> Enum.map(&command_tuple/1)
    |> Map.new()
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
    Enum.member?(
      module.module_info(:attributes)[:behaviour] || [],
      RabbitMQ.CLI.CommandBehaviour
    )
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
    |> List.last()
  end

  def to_snake_case(<<c, str::binary>>) do
    tail = for <<c <- str>>, into: "", do: snake(c)
    <<to_lower_char(c), tail::binary>>
  end

  defp snake(c) do
    if c >= ?A and c <= ?Z do
      <<"_", c + 32>>
    else
      <<c>>
    end
  end

  defp to_lower_char(c) do
    if c >= ?A and c <= ?Z do
      c + 32
    else
      c
    end
  end

  defp command_in_scope(_cmd, :all) do
    true
  end

  defp command_in_scope(cmd, scope) do
    Enum.member?(command_scopes(cmd), scope)
  end

  defp command_scopes(cmd) do
    case CommandBehaviour.scopes(cmd) do
      nil ->
        Regex.recompile!(@commands_ns)
        |> Regex.run(to_string(cmd), capture: :all_but_first)
        |> List.first()
        |> to_snake_case
        |> String.to_atom()
        |> List.wrap()
      scopes ->
        scopes
    end
  end
end
