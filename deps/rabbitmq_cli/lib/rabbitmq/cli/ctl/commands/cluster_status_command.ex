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

defmodule RabbitMQ.CLI.Ctl.Commands.ClusterStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :status, []) do
      {:badrpc, _} = err ->
        err

      status ->
        case :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :cluster_nodes, [:running]) do
          {:badrpc, _} = err ->
            err

          {:error, {:corrupt_or_missing_cluster_files, _, _}} ->
            {:error, "Could not read mnesia files containing cluster status"}

          nodes ->
            alarms_by_node = Enum.map(nodes, &alarms_by_node/1)
            status ++ [{:alarms, alarms_by_node}]
        end
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "cluster_status"

  def usage_doc_guides() do
    [
      DocGuide.clustering(),
      DocGuide.cluster_formation(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Displays all the nodes in the cluster grouped by node type, together with the currently running nodes"

  def banner(_, %{node: node_name}), do: "Cluster status of node #{node_name} ..."

  #
  # Implementation
  #

  defp alarms_by_node(node) do
    alarms = :rabbit_misc.rpc_call(node, :rabbit, :alarms, [])
    {node, alarms}
  end
end
