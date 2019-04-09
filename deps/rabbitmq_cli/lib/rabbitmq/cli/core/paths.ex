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

  def require_mnesia_dir(opts) do
    case Application.get_env(:mnesia, :dir) do
      nil ->
        case Config.get_option(:mnesia_dir, opts) do
          nil -> {:error, :mnesia_dir_not_found}
          val -> Application.put_env(:mnesia, :dir, to_charlist(val))
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
