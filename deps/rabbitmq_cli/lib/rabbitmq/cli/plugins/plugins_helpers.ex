## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Plugins.Helpers do
  import RabbitMQ.CLI.Core.DataCoercion
  import RabbitCommon.Records
  import RabbitMQ.CLI.Core.Platform, only: [path_separator: 0]
  import RabbitMQ.CLI.Core.{CodePath, Paths}
  alias RabbitMQ.CLI.Core.{Config, Validators}

  def mode(opts) do
    %{online: online, offline: offline} = opts

    case {online, offline} do
      {true, false} -> :online
      {false, true} -> :offline
      {false, false} -> :best_effort
    end
  end

  def can_set_plugins_with_mode(args, opts) do
    case mode(opts) do
      # can always set offline plugins list
      :offline ->
        :ok

      # assume online mode, fall back to offline mode in case of errors
      :best_effort ->
        :ok

      # a running node is required
      :online ->
        Validators.chain(
          [&Validators.node_is_running/2, &Validators.rabbit_is_running/2],
          [args, opts]
        )
    end
  end

  def list(opts) do
    {:ok, dir} = plugins_dir(opts)
    add_all_to_path(dir)
    :lists.usort(:rabbit_plugins.list(to_charlist(dir)))
  end

  def list_names(opts) do
    list(opts) |> plugin_names
  end

  def read_enabled(opts) do
    case enabled_plugins_file(opts) do
      {:ok, enabled} ->
        :rabbit_plugins.read_enabled(to_charlist(enabled))

      # Existence of enabled_plugins_file should be validated separately
      {:error, :no_plugins_file} ->
        []
    end
  end

  def enabled_plugins_file(opts) do
    case Config.get_option(:enabled_plugins_file, opts) do
      nil -> {:error, :no_plugins_file}
      file -> {:ok, file}
    end
  end

  def enabled_plugins_file(_, opts) do
    enabled_plugins_file(opts)
  end

  def set_enabled_plugins(plugins, opts) do
    plugin_atoms = :lists.usort(for plugin <- plugins, do: to_atom(plugin))
    require_rabbit_and_plugins(opts)
    {:ok, plugins_file} = enabled_plugins_file(opts)
    write_enabled_plugins(plugin_atoms, plugins_file, opts)
  end

  @spec update_enabled_plugins(
          [atom()],
          :online | :offline | :best_effort,
          node(),
          map()
        ) :: map() | {:error, any()}
  def update_enabled_plugins(enabled_plugins, mode, node_name, opts) do
    {:ok, plugins_file} = enabled_plugins_file(opts)

    case mode do
      :online ->
        case update_enabled_plugins(node_name, plugins_file) do
          {:ok, started, stopped} ->
            %{
              mode: :online,
              started: Enum.sort(started),
              stopped: Enum.sort(stopped),
              set: Enum.sort(enabled_plugins)
            }

          {:error, _} = err ->
            err
        end

      :best_effort ->
        case update_enabled_plugins(node_name, plugins_file) do
          {:ok, started, stopped} ->
            %{
              mode: :online,
              started: Enum.sort(started),
              stopped: Enum.sort(stopped),
              set: Enum.sort(enabled_plugins)
            }

          {:error, :offline} ->
            %{mode: :offline, set: Enum.sort(enabled_plugins)}

          {:error, {:enabled_plugins_mismatch, _, _}} = err ->
            err
        end

      :offline ->
        %{mode: :offline, set: Enum.sort(enabled_plugins)}
    end
  end

  def validate_plugins(requested_plugins, opts) do
    ## Maybe check all plugins
    plugins =
      case opts do
        %{all: true} -> plugin_names(list(opts))
        _ -> requested_plugins
      end

    all = list(opts)
    deps = :rabbit_plugins.dependencies(false, plugins, all)

    deps_plugins =
      Enum.filter(all, fn plugin ->
        name = plugin_name(plugin)
        Enum.member?(deps, name)
      end)

    case :rabbit_plugins.validate_plugins(deps_plugins) do
      {_, []} -> :ok
      {_, invalid} -> {:error, :rabbit_plugins.format_invalid_plugins(invalid)}
    end
  end

  def plugin_name(plugin) when is_binary(plugin) do
    plugin
  end

  def plugin_name(plugin) when is_atom(plugin) do
    Atom.to_string(plugin)
  end

  def plugin_name(plugin) do
    plugin(name: name) = plugin
    name
  end

  def plugin_names(plugins) do
    for plugin <- plugins, do: plugin_name(plugin)
  end

  def comma_separated_names(plugins) do
    Enum.join(plugin_names(plugins), ", ")
  end

  #
  # Implementation
  #

  defp to_list(str) when is_binary(str) do
    :erlang.binary_to_list(str)
  end

  defp to_list(lst) when is_list(lst) do
    lst
  end

  defp to_list(atm) when is_atom(atm) do
    to_list(Atom.to_string(atm))
  end

  defp write_enabled_plugins(plugins, plugins_file, opts) do
    all = list(opts)
    all_plugin_names = Enum.map(all, &plugin_name/1)
    missing = MapSet.difference(MapSet.new(plugins), MapSet.new(all_plugin_names))

    case Enum.empty?(missing) do
      true ->
        case :rabbit_file.write_term_file(to_charlist(plugins_file), [plugins]) do
          :ok ->
            all_enabled = :rabbit_plugins.dependencies(false, plugins, all)
            {:ok, Enum.sort(all_enabled)}

          {:error, reason} ->
            {:error, {:cannot_write_enabled_plugins_file, plugins_file, reason}}
        end

      false ->
        {:error, {:plugins_not_found, Enum.to_list(missing)}}
    end
  end

  defp update_enabled_plugins(node_name, plugins_file) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :ensure, [to_list(plugins_file)]) do
      {:badrpc, :nodedown} -> {:error, :offline}
      {:error, :rabbit_not_running} -> {:error, :offline}
      {:ok, start, stop} -> {:ok, start, stop}
      {:error, _} = err -> err
    end
  end

  defp add_all_to_path(plugins_directories) do
    directories = String.split(to_string(plugins_directories), path_separator())

    Enum.map(directories, fn directory ->
      with {:ok, subdirs} <- File.ls(directory) do
        for subdir <- subdirs do
          Path.join([directory, subdir, "ebin"])
          |> Code.append_path()
        end
      end
    end)

    :ok
  end
end
