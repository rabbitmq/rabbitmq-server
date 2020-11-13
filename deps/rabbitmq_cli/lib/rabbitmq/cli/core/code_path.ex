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

defmodule RabbitMQ.CLI.Core.CodePath do
  alias RabbitMQ.CLI.Core.{Config, Paths, Platform}

  def add_plugins_to_load_path(opts) do
    with {:ok, plugins_dir} <- Paths.plugins_dir(opts) do
      String.split(to_string(plugins_dir), Platform.path_separator())
      |> Enum.map(&add_directory_plugins_to_load_path/1)

      :ok
    end
  end

  def add_directory_plugins_to_load_path(directory_with_plugins_inside_it) do
    with {:ok, files} <- File.ls(directory_with_plugins_inside_it) do
      Enum.map(
        files,
        fn filename ->
          cond do
            String.ends_with?(filename, [".ez"]) ->
              Path.join([directory_with_plugins_inside_it, filename])
              |> String.to_charlist()
              |> add_archive_code_path()

            File.dir?(filename) ->
              Path.join([directory_with_plugins_inside_it, filename])
              |> add_dir_code_path()

            true ->
              {:error, {:not_a_plugin, filename}}
          end
        end
      )
    end
  end

  defp add_archive_code_path(ez_dir) do
    case :erl_prim_loader.list_dir(ez_dir) do
      {:ok, [app_dir]} ->
        app_in_ez = :filename.join(ez_dir, app_dir)
        add_dir_code_path(app_in_ez)

      _ ->
        {:error, :no_app_dir}
    end
  end

  defp add_dir_code_path(app_dir_0) do
    app_dir = to_charlist(app_dir_0)

    case :erl_prim_loader.list_dir(app_dir) do
      {:ok, list} ->
        case Enum.member?(list, 'ebin') do
          true ->
            ebin_dir = :filename.join(app_dir, 'ebin')
            Code.append_path(ebin_dir)

          false ->
            {:error, :no_ebin}
        end

      _ ->
        {:error, :app_dir_empty}
    end
  end

  def require_rabbit_and_plugins(_, opts) do
    require_rabbit_and_plugins(opts)
  end

  def require_rabbit_and_plugins(opts) do
    with :ok <- require_rabbit(opts),
         :ok <- add_plugins_to_load_path(opts),
         do: :ok
  end

  def require_rabbit(_, opts) do
    require_rabbit(opts)
  end

  def require_rabbit(opts) do
    home = Config.get_option(:rabbitmq_home, opts)

    case home do
      nil ->
        {:error, {:unable_to_load_rabbit, :rabbitmq_home_is_undefined}}

      _ ->
        path = Path.join(home, "ebin")
        Code.append_path(path)

        case Application.load(:rabbit) do
          :ok ->
            Code.ensure_loaded(:rabbit_plugins)
            :ok

          {:error, {:already_loaded, :rabbit}} ->
            Code.ensure_loaded(:rabbit_plugins)
            :ok

          {:error, err} ->
            {:error, {:unable_to_load_rabbit, err}}
        end
    end
  end
end
