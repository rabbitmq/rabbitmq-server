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


defmodule RabbitMQ.CLI.Plugins.Helpers do
  def list(opts) do
    {:ok, dir} = plugins_dir(opts)
    add_all_to_path(dir)
    :rabbit_plugins.list(to_char_list(dir))
  end

  def read_enabled(opts) do
    {:ok, enabled} = enabled_plugins_file(opts)
    :rabbit_plugins.read_enabled(to_char_list(enabled))
  end

  def enabled_plugins_file(opts) do
    case opts[:enabled_plugins_file] || System.get_env("RABBITMQ_ENABLED_PLUGINS_FILE") do
      nil  -> {:error, :no_plugins_file};
      file ->
        case File.exists?(file) do
          true  -> {:ok, file};
          false -> {:error, :plugins_file_not_exists}
        end
    end
  end

  def plugins_dir(opts) do
    case opts[:plugins_dir] || System.get_env("RABBITMQ_PLUGINS_DIR") do
      nil -> {:error, :no_plugins_dir};
      dir ->
        case File.dir?(dir) do
          true  -> {:ok, dir};
          false -> {:error, :plugins_dir_not_exists}
        end
    end
  end

  def require_rabbit(opts) do
    home = opts[:rabbitmq_home] || System.get_env("RABBITMQ_HOME")
    case home do
      nil ->
        {:error, {:unable_to_load_rabbit, :no_rabbit_home}};
      _   ->
        path = Path.join(home, "ebin")
        Code.append_path(path)
        case Application.load(:rabbit) do
          :ok -> :ok;
          {:error, {:already_loaded, :rabbit}} -> :ok;
          {:error, err} -> {:error, {:unable_to_load_rabbit, err}}
        end
    end
  end

  def set_enabled_plugins(plugins, mode, node_name, opts) do
    {:ok, plugins_file} = enabled_plugins_file(opts)
    case write_enabled_plugins(plugins, plugins_file, opts) do
      {:ok, enabled_plugins} ->
        case mode do
          :online  ->
            case update_enabled_plugins(node_name, plugins_file) do
              {:ok, started, stopped} ->
                %{mode: :online, started: started, stopped: stopped, enabled: enabled_plugins};
              {:error, :offline} ->
                %{mode: :offline, enabled: enabled_plugins};
              {:error, {:enabled_plugins_mismatch, _, _}} = err ->
                err
            end;
          :offline ->
            %{mode: :offline, enabled: enabled_plugins}
        end;
      {:error, _} = err -> err
    end
  end

  defp write_enabled_plugins(plugins, plugins_file, opts) do
    all = list(opts)
    case plugins -- all do
      [] ->
        case :rabbit_file.write_term_file(to_char_list(plugins_file), [plugins]) do
          :ok ->
            {:ok, :rabbit_plugins.dependencies(false, plugins, all)};
          {:error, reason} ->
            {:error, {:cannot_write_enabled_plugins_file, plugins_file, reason}}
        end;
      missing  ->
        {:error, {:plugins_not_found, missing}}
    end
  end

  defp update_enabled_plugins(node_name, plugins_file) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins,
                                          :ensure, [plugins_file]) do
      {:badrpc, :nodedown} -> {:error, :offline};
      {:ok, start, stop}   -> {:ok, start, stop};
      {:error, _} = err    -> err
    end
  end

  defp add_all_to_path(dir) do
    {:ok, subdirs} = File.ls(dir)
    for subdir <- subdirs do
      Path.join([dir, subdir, "ebin"])
      |> Code.append_path
    end
  end
end
