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


defmodule ClusterStatusCommand do
  @behaviour CommandBehaviour
  @flags []

  def run([_|_] = args, _) when length(args) != 0, do: {:too_many_args, args}
  def run([], %{node: node_name} = opts) do
    info(opts)
    target_node =
      node_name
      |> Helpers.parse_node
    status = :rabbit_misc.rpc_call(target_node, :rabbit_mnesia, :status, [])
    case :rabbit_misc.rpc_call(target_node, :rabbit_mnesia, :cluster_nodes, [:running]) do
      {:badrpc, _} = err ->
        err
      nodes ->
        alarms_by_node = Enum.map(nodes, &alarms_by_node/1)
        status ++ [{:alarms, alarms_by_node}]
    end
  end

  def usage, do: "cluster_status"

  def flags, do: @flags

  defp alarms_by_node(node) do
    status = :rabbit_misc.rpc_call(node, :rabbit, :status, [])
    {node, List.keyfind(status, :alarms, 1, [])}
  end

  defp info(%{quiet: true}), do: nil
  defp info(%{node: node_name}), do: IO.puts "Cluster status of node #{node_name} ..."
end
