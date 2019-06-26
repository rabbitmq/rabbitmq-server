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

defmodule RabbitMQ.CLI.Diagnostics.Commands.LogLocationCommand do
  @moduledoc """
  Displays standard log file location on the target node
  """
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches, do: [all: :boolean]
  def aliases, do: [a: :all]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{all: false}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout, all: all}) do
    case :rabbit_misc.rpc_call(node_name,
                               :rabbit_lager, :log_locations, [],
                               timeout) do
      {:badrpc, _} = error ->
        error;
      ## No log files?
      [] -> {:error, "No log files configured on the node"};
      [first_log | _] = log_locations ->
        case all do
          true  -> log_locations;
          false ->
            ## Select the "main" file
            case get_log_config_file_location(node_name, timeout) do
              {:badrpc, _} = error -> error;
              nil      -> to_string(first_log);
              location ->
                case Enum.member?(log_locations, location) do
                  true  -> to_string(location);
                  ## Configured location was not propagated to lager?
                  false -> to_string(first_log)
                end
            end
        end
    end
  end

  def get_log_config_file_location(node_name, timeout) do
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

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :configuration

  def description(), do: "Shows the log file location(s) on the target node"

  def usage, do: "log_location"

  def banner([], %{node: node_name}) do
    "Log file location on node #{node_name} ..."
  end
end
