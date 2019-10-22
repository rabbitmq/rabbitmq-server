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
  import RabbitMQ.CLI.Core.{Alarms, ANSI, Listeners, Platform, FeatureFlags}
  import RabbitMQ.CLI.Core.Distribution, only: [per_node_timeout: 2]
  import Rabbitmq.Atom.Coerce

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 60_000

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(%{timeout: timeout}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
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
            count = length(nodes)
            alarms_by_node    = Enum.map(nodes, fn n -> alarms_by_node(n, per_node_timeout(timeout, count)) end)
            listeners_by_node = Enum.map(nodes, fn n -> listeners_of(n, per_node_timeout(timeout, count)) end)
            versions_by_node  = Enum.map(nodes, fn n -> versions_by_node(n, per_node_timeout(timeout, count)) end)

            feature_flags = case :rabbit_misc.rpc_call(node_name, :rabbit_ff_extra, :cli_info, [], timeout) do
                              {:badrpc, {:EXIT, {:undef, _}}} -> []
                              {:badrpc, _} = err    -> err
                              val                   -> val
                            end

            status ++ [{:alarms, alarms_by_node}] ++ [{:listeners, listeners_by_node}] ++ [{:versions, versions_by_node}] ++ [{:feature_flags, feature_flags}]
        end
    end
  end

  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: timed out while waiting for a response from #{node_name}."}
  end

  def output(result, %{formatter: "erlang"}) do
    {:ok, result}
  end

  def output(result, %{formatter: "json"}) when is_list(result) do
    # format more data structures as map for sensible JSON output
    m = result_map(result)
          |> Map.update(:alarms, [], fn xs -> alarm_maps(xs) end)
          |> Map.update(:listeners, %{}, fn m ->
                                           Enum.map(m, fn {n, xs} -> {n, listener_maps(xs)} end) |> Enum.into(%{})
                                         end)

    {:ok, m}
  end

  def output(result, %{node: node_name}) when is_list(result) do
    m = result_map(result)

    cluster_name_section = [
      "#{bright("Basics")}\n",
      "Cluster name: #{m[:cluster_name]}"
    ]

    disk_nodes_section = [
      "\n#{bright("Disk Nodes")}\n",
    ] ++ node_lines(m[:disk_nodes])

    ram_nodes_section = case m[:ram_nodes] do
      [] -> []
      xs -> [
        "\n#{bright("RAM Nodes")}\n",
      ] ++ node_lines(xs)
    end

    running_nodes_section = [
      "\n#{bright("Running Nodes")}\n",
    ] ++ node_lines(m[:running_nodes])

    versions_section = [
      "\n#{bright("Versions")}\n",
    ] ++ version_lines(m[:versions])

    alarms_section = [
      "\n#{bright("Alarms")}\n",
    ] ++ case m[:alarms] do
           [] -> ["(none)"]
           xs -> alarm_lines(xs, node_name)
         end

    partitions_section = [
      "\n#{bright("Network Partitions")}\n"
    ] ++ case map_size(m[:partitions]) do
           0 -> ["(none)"]
           _ -> partition_lines(m[:partitions])
         end

    listeners_section = [
      "\n#{bright("Listeners")}\n"
    ] ++ case map_size(m[:listeners]) do
           0 -> ["(none)"]
           _ -> Enum.reduce(m[:listeners], [], fn {node, listeners}, acc ->
                                                 acc ++ listener_lines(listeners, node)
                                               end)
         end

    feature_flags_section = [
      "\n#{bright("Feature flags")}\n"
    ] ++ case Enum.count(m[:feature_flags]) do
           0 -> ["(none)"]
           _ -> feature_flag_lines(m[:feature_flags])
         end

    lines = cluster_name_section ++ disk_nodes_section ++ ram_nodes_section ++ running_nodes_section ++
            versions_section ++ alarms_section ++ partitions_section ++ listeners_section ++ feature_flags_section

    {:ok, Enum.join(lines, line_separator())}
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.String

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

  defp result_map(result) do
    # [{nodes,[{disc,[hare@warp10,rabbit@warp10]},{ram,[flopsy@warp10]}]},
    #  {running_nodes,[flopsy@warp10,hare@warp10,rabbit@warp10]},
    #  {cluster_name,<<"rabbit@localhost">>},
    #  {partitions,[{flopsy@warp10,[rabbit@vagrant]},
    #               {hare@warp10,[rabbit@vagrant]}]},
    #  {alarms,[{flopsy@warp10,[]},
    #           {hare@warp10,[]},
    #           {rabbit@warp10,[{resource_limit,memory,rabbit@warp10}]}]}]
    %{
      cluster_name: Keyword.get(result, :cluster_name),
      disk_nodes: result |> Keyword.get(:nodes, []) |> Keyword.get(:disc, []),
      ram_nodes: result |> Keyword.get(:nodes, []) |> Keyword.get(:ram, []),
      running_nodes: result |> Keyword.get(:running_nodes, []) |> Enum.map(&to_string/1),
      alarms: Keyword.get(result, :alarms) |> Keyword.values |> Enum.concat |> Enum.uniq,
      partitions: Keyword.get(result, :partitions, []) |> Enum.into(%{}),
      listeners: Keyword.get(result, :listeners, []) |> Enum.into(%{}),
      versions: Keyword.get(result, :versions, []) |> Enum.into(%{}),
      feature_flags: Keyword.get(result, :feature_flags, []) |> Enum.map(fn ff -> Enum.into(ff, %{}) end)
    }
  end

  defp alarms_by_node(node, timeout) do
    alarms = case :rabbit_misc.rpc_call(to_atom(node), :rabbit, :alarms, [], timeout) do
      {:badrpc, _} -> []
      xs           -> xs
    end

    {node, alarms}
  end

  defp listeners_of(node, timeout) do
    # This may seem inefficient since this call returns all known listeners
    # in the cluster, so why do we run it on every node? See the badrpc clause,
    # some nodes may be inavailable or partitioned from other nodes. This way we
    # gather as complete a picture as possible. MK.
    listeners = case :rabbit_misc.rpc_call(to_atom(node), :rabbit_networking, :active_listeners, [], timeout) do
      {:badrpc, _} -> []
      xs           -> xs
    end

    {node, listeners_on(listeners, node)}
  end

  defp versions_by_node(node, timeout) do
    {rmq_vsn, otp_vsn} = case :rabbit_misc.rpc_call(
                          to_atom(node), :rabbit_misc, :rabbitmq_and_erlang_versions, [], timeout) do
      {:badrpc, _} -> {nil, nil}
      pair         -> pair
    end

    {node, %{rabbitmq_version: to_string(rmq_vsn), erlang_version: to_string(otp_vsn)}}
  end

  defp node_lines(nodes) do
    Enum.map(nodes, &to_string/1) |> Enum.sort
  end

  defp version_lines(mapping) do
    Enum.map(mapping, fn {node, %{rabbitmq_version: rmq_vsn, erlang_version: otp_vsn}} ->
                        "#{node}: RabbitMQ #{rmq_vsn} on Erlang #{otp_vsn}"
                      end)
  end

  defp partition_lines(mapping) do
    Enum.map(mapping, fn {node, unreachable_peers} -> "Node #{node} cannot communicate with #{Enum.join(unreachable_peers, ", ")}" end)
  end
end
