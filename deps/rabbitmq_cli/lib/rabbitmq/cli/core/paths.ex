## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Paths do
  alias RabbitMQ.CLI.Core.Config
  import RabbitMQ.CLI.Core.Platform

  def plugins_dir(_, opts) do
    plugins_dir(opts)
  end

  def plugins_dir(opts) do
    case Config.get_option(:plugins_dir, opts) do
      nil ->
        {:error, :no_plugins_dir}

      dir ->
        paths = String.split(to_string(dir), path_separator())

        case Enum.any?(paths, &File.dir?/1) do
          true -> {:ok, dir}
          false -> {:error, :plugins_dir_does_not_exist}
        end
    end
  end

  def require_data_dir(opts) do
    case Application.get_env(:rabbit, :data_dir) do
      nil ->
        case Application.get_env(:mnesia, :dir) do
          nil ->
            case Config.get_option(:data_dir, opts) do
              nil ->
                {:error, :data_dir_not_found}

              val ->
                Application.put_env(:rabbit, :data_dir, to_charlist(val))
                Application.put_env(:mnesia, :dir, to_charlist(val))
            end

          val ->
            Application.put_env(:rabbit_, :data_dir, to_charlist(val))
            :ok
        end

      _ ->
        :ok
    end
  end

  def require_feature_flags_file(opts) do
    case Application.get_env(:rabbit, :feature_flags_file) do
      nil ->
        case Config.get_option(:feature_flags_file, opts) do
          nil -> {:error, :feature_flags_file_not_found}
          val -> Application.put_env(:rabbit, :feature_flags_file, to_charlist(val))
        end

      _ ->
        :ok
    end
  end
end
