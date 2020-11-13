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
## Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.LogFiles do
  @spec get_log_locations(atom, integer | :infinity) :: [String.t] | {:badrpc, term}
  def get_log_locations(node_name, timeout) do
    case :rabbit_misc.rpc_call(node_name,
                               :rabbit_lager, :log_locations, [],
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
              ## Configured location was not propagated to lager?
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