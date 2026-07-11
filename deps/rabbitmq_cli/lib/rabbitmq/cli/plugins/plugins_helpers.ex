## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Plugins.Helpers do
  import RabbitMQ.CLI.Core.DataCoercion
  import RabbitCommon.Records
  import RabbitMQ.CLI.Core.Platform, only: [path_separator: 0]
  import RabbitMQ.CLI.Core.{CodePath, Paths}
  alias RabbitMQ.CLI.Core.{Config, Validators}

  def mode(opts) do
    online = Map.get(opts, :online, false)
    offline = Map.get(opts, :offline, false)

    case {online, offline} do
      {true, false} -> :online
      {false, true} -> :offline
      {false, false} -> :best_effort
    end
  end

  def validate_node_and_mode(args, opts) do
    case mode(opts) do
      :offline ->
        require_offline_node_is_local(args, opts)

      # For the local node, assume online and fall back to offline if the node is not running.
      # For a remote node, a running node is required: offline changes cannot reach a remote node's plugins file.
      :best_effort ->
        if node_is_local?(opts) do
          :ok
        else
          require_node_running(args, opts)
        end

      :online ->
        require_node_running(args, opts)
    end
  end

  def node_is_local?(%{node: node_name}) do
    case node_locality(node_name) do
      :local -> true
      {:remote, _} -> false
    end
  end

  def node_is_local?(_opts), do: true

  defp require_offline_node_is_local(_args, %{offline: true} = opts) do
    case node_locality(opts.node) do
      :local ->
        :ok

      {:remote, reason} ->
        {:validation_failure,
         {:bad_argument,
          "--offline mode operates on the local node's environment and cannot be used with a " <>
            "remote node. #{reason}. Use --online to target the remote node."}}
    end
  end

  defp require_offline_node_is_local(_args, _opts), do: :ok

  defp require_node_running(args, opts) do
    Validators.chain(
      [&Validators.node_is_running/2, &Validators.rabbit_is_running/2],
      [args, opts]
    )
  end

  defp node_locality(node_name) do
    local_node_str = :rabbit_env.get_context()[:nodename] |> to_string()
    node_str = to_string(node_name)

    if node_str == local_node_str do
      :local
    else
      case String.split(local_node_str, "@", parts: 2) do
        [local_name, local_host] ->
          case String.split(node_str, "@", parts: 2) do
            [name, host] ->
              name_ok = name == local_name
              host_ok = hosts_match?(host, local_host)

              if name_ok and host_ok do
                :local
              else
                reason =
                  cond do
                    not name_ok and not host_ok ->
                      "node #{node_name} has a different name and host than the local node #{local_node_str}"

                    not name_ok ->
                      "node #{node_name} has a different name than the local node #{local_node_str}"

                    true ->
                      "node #{node_name} is on a different host than the local node"
                  end

                {:remote, reason}
              end

            [bare] ->
              if bare == local_name do
                :local
              else
                {:remote,
                 "node #{node_name} has a different name than the local node #{local_node_str}"}
              end
          end

        _ ->
          {:remote, "could not determine the local node name"}
      end
    end
  end

  # Treats a short hostname and its fully-qualified form as the same host (e.g. "host" matches "host.example.com").
  defp hosts_match?(a, b) do
    a == b or String.starts_with?(a, b <> ".") or String.starts_with?(b, a <> ".")
  end

  def list(opts) do
    list(opts, local_fallback: false)
  end

  # With `local_fallback: true`, a failed RPC falls back to the local
  # environment instead of returning an empty list. Command discovery uses
  # this: it runs before distribution is started, so the RPC can fail even
  # when the node is running, and an empty list would drop all
  # plugin-provided commands.
  def list(opts, local_fallback: local_fallback) do
    if mode(opts) == :offline or node_is_local?(opts) do
      list_local(opts)
    else
      case list_remote(opts.node) do
        {:ok, plugins} ->
          plugins

        {:error, _} ->
          case {mode(opts), local_fallback} do
            {:best_effort, true} -> list_local(opts)
            _ -> []
          end
      end
    end
  end

  defp list_local(opts) do
    case plugins_dir(opts) do
      {:ok, dir} ->
        add_all_to_path(dir)
        :lists.usort(:rabbit_plugins.list(to_charlist(dir)))

      {:error, _} ->
        []
    end
  end

  defp list_remote(node_name) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :list, []) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      plugins when is_list(plugins) -> {:ok, plugins}
      other -> {:error, {:unexpected_response, other}}
    end
  end

  def read_enabled(opts) do
    read_enabled(opts, local_fallback: false)
  end

  # Same fallback as in `list/2` above, for the same reason.
  def read_enabled(opts, local_fallback: local_fallback) do
    if mode(opts) == :offline or node_is_local?(opts) do
      read_enabled_local(opts)
    else
      case read_enabled_remote(opts) do
        {:ok, plugins} ->
          plugins

        {:error, _} ->
          case {mode(opts), local_fallback} do
            {:best_effort, true} -> read_enabled_local(opts)
            _ -> []
          end
      end
    end
  end

  defp read_enabled_local(opts) do
    case enabled_plugins_file(opts) do
      {:ok, file} ->
        :rabbit_plugins.read_enabled(to_charlist(file))

      # Existence of enabled_plugins_file should be validated separately
      {:error, :no_plugins_file} ->
        []
    end
  end

  defp read_enabled_remote(%{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :enabled_plugins, []) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      plugins when is_list(plugins) -> {:ok, plugins}
      other -> {:error, {:unexpected_response, other}}
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

  defp rpc_enabled_plugins_file(node_name) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :enabled_plugins_file, []) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      file -> {:ok, file}
    end
  end

  def set_enabled_plugins(plugins, opts) do
    plugin_atoms = :lists.usort(for plugin <- plugins, do: to_atom(plugin))
    _ = require_rabbit_and_plugins(opts)

    if mode(opts) == :offline or node_is_local?(opts) do
      {:ok, plugins_file} = enabled_plugins_file(opts)
      write_enabled_plugins(plugin_atoms, plugins_file, opts)
    else
      write_enabled_plugins_on_remote(opts.node, plugin_atoms, opts)
    end
  end

  @spec update_enabled_plugins(
          [atom()],
          :online | :offline | :best_effort,
          node(),
          map()
        ) :: map() | {:error, any()}
  def update_enabled_plugins(enabled_plugins, mode, node_name, opts) do
    case mode do
      :online ->
        update_enabled_plugins(node_name, enabled_plugins, opts)

      :best_effort ->
        case update_enabled_plugins(node_name, enabled_plugins, opts) do
          %{} = result ->
            result

          {:error, :offline} ->
            %{mode: :offline, set: Enum.sort(enabled_plugins)}

          {:error, {:enabled_plugins_mismatch, _, _}} = err ->
            err

          {:error, _} = err ->
            err
        end

      :offline ->
        %{mode: :offline, set: Enum.sort(enabled_plugins)}
    end
  end

  def validate_plugins(requested_plugins, opts) do
    ## Maybe check all plugins
    all = list(opts)

    plugins =
      case opts do
        %{all: true} -> plugin_names(all)
        _ -> requested_plugins
      end

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

  def missing_plugins(plugins, all) do
    all_plugin_names = Enum.map(all, &plugin_name/1)
    MapSet.difference(MapSet.new(plugins), MapSet.new(all_plugin_names))
  end

  def warn_about_missing_plugins(missing, opts) do
    case Enum.empty?(missing) or Config.output_less?(opts) do
      true ->
        :ok

      false ->
        names = Enum.join(Enum.to_list(missing), ", ")
        IO.puts("WARNING - plugins currently enabled but missing: #{names}\n")
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
    all = list_local(opts)
    missing = missing_plugins(plugins, all)

    case check_missing_plugins(missing, opts) do
      :ok ->
        warn_about_missing_plugins(missing, opts)

        case :rabbit_file.write_term_file(to_charlist(plugins_file), [plugins]) do
          :ok ->
            all_enabled = :rabbit_plugins.dependencies(false, plugins, all)
            {:ok, Enum.sort(all_enabled)}

          {:error, reason} ->
            {:error, {:cannot_write_enabled_plugins_file, plugins_file, reason}}
        end

      {:error, _} = err ->
        err
    end
  end

  defp write_enabled_plugins_on_remote(node_name, plugins, opts) do
    with {:ok, all} <- list_remote(node_name),
         {:ok, remote_file} <- rpc_enabled_plugins_file(node_name) do
      missing = missing_plugins(plugins, all)

      case check_missing_plugins(missing, opts) do
        :ok ->
          warn_about_missing_plugins(missing, opts)

          case :rabbit_misc.rpc_call(node_name, :rabbit_file, :write_term_file, [
                 remote_file,
                 [plugins]
               ]) do
            :ok ->
              all_enabled = :rabbit_plugins.dependencies(false, plugins, all)
              {:ok, Enum.sort(all_enabled)}

            {:badrpc, reason} ->
              {:error, {:badrpc, reason}}

            {:error, reason} ->
              {:error, {:cannot_write_enabled_plugins_file, remote_file, reason}}
          end

        {:error, _} = err ->
          err
      end
    end
  end

  # `enable` and `disable` commands set `keep_missing_plugins` to tolerate an enabled but
  # no longer installed plugin instead of failing; `set` still rejects it.
  defp check_missing_plugins(missing, opts) do
    if Enum.empty?(missing) or Map.get(opts, :keep_missing_plugins, false) do
      :ok
    else
      {:error, {:plugins_not_found, Enum.to_list(missing)}}
    end
  end

  # File was already written to local disk by set_enabled_plugins;
  defp update_enabled_plugins(node_name, enabled_plugins, opts) do
    if node_is_local?(opts) do
      # Tell the running node to re-read it.
      {:ok, plugins_file} = enabled_plugins_file(opts)
      rpc_ensure(node_name, plugins_file, enabled_plugins)
    else
      # Tell the remote node to re-read and apply its own enabled plugins file.
      case rpc_enabled_plugins_file(node_name) do
        {:error, _} ->
          # Node went down after the file write; changes will take effect at next restart.
          {:error, :offline}

        {:ok, remote_file} ->
          rpc_ensure(node_name, remote_file, enabled_plugins)
      end
    end
  end

  defp rpc_ensure(node_name, plugins_file, enabled_plugins) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :ensure, [to_list(plugins_file)]) do
      {:badrpc, _} ->
        {:error, :offline}

      {:error, :rabbit_not_running} ->
        {:error, :offline}

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
  end

  defp add_all_to_path(plugins_directories) do
    directories = String.split(to_string(plugins_directories), path_separator())

    Enum.each(directories, fn directory ->
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
