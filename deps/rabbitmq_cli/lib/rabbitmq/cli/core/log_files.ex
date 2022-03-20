## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.LogFiles do
  @spec get_log_locations(atom, integer | :infinity) :: [String.t] | {:badrpc, term}
  def get_log_locations(node_name, timeout) do
    case :rabbit_misc.rpc_call(node_name,
                               :rabbit, :log_locations, [],
                               timeout) do
      {:badrpc, _} = error -> error;
      list -> Enum.map(list, &to_string/1)
    end
  end

  @spec get_default_log_location(atom, integer | :infinity) ::
    {:ok, String.t} | {:badrpc, term} | {:error, term}
  def get_default_log_location(node_name, timeout) do
    case get_log_locations(node_name, timeout) do
      {:badrpc, _} = error -> error;
      [] -> {:error, "No log files configured on the node"};
      [first_log | _] = log_locations ->
        case get_log_config_file_location(node_name, timeout) do
          {:badrpc, _} = error -> error;
          nil      -> {:ok, first_log};
          location ->
            case Enum.member?(log_locations, location) do
              true  -> {:ok, to_string(location)};
              false -> {:ok, first_log}
            end
        end
    end
  end

  defp get_log_config_file_location(node_name, timeout) do
    case :rabbit_misc.rpc_call(node_name,
                               :application, :get_env, [:rabbit, :log, :none],
                               timeout) do
      {:badrpc, _} = error -> error;
      :none      -> nil;
      log_config ->
        case log_config[:file] do
          nil -> nil;
          file_config ->
            file_config[:file]
        end
    end
  end
end
