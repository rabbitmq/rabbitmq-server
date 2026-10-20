## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.CodePath do
  alias RabbitMQ.CLI.Core.{Config, Paths, Platform}

  def add_plugins_to_load_path(opts) do
    case Paths.plugins_dir(opts) do
      {:ok, plugins_dir} ->
        String.split(to_string(plugins_dir), Platform.path_separator())
        |> Enum.each(&add_directory_plugins_to_load_path/1)

      {:error, _} ->
        :ok
    end

    :ok
  end

  def add_directory_plugins_to_load_path(directory_with_plugins_inside_it) do
    with {:ok, files} <- File.ls(directory_with_plugins_inside_it) do
      Enum.each(files, fn filename ->
        ebin = Path.join([to_string(directory_with_plugins_inside_it), filename, "ebin"])

        if File.dir?(ebin) do
          Code.append_path(ebin)
        end
      end)
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
        case Application.load(:rabbit) do
          :ok ->
            _ = Code.ensure_loaded(:rabbit_plugins)
            :ok

          {:error, {:already_loaded, :rabbit}} ->
            _ = Code.ensure_loaded(:rabbit_plugins)
            :ok

          {:error, err} ->
            {:error, {:unable_to_load_rabbit, err}}
        end
    end
  end
end
