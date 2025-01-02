## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClusterStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.{Alarms, ANSI, Listeners, Platform, FeatureFlags}
  import RabbitMQ.CLI.Core.Distribution, only: [per_node_timeout: 2]
  import RabbitMQ.CLI.Core.DataCoercion

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

  def run([], %{node: node_name, timeout: timeout} = opts) do
    status0 =
      case :rabbit_misc.rpc_call(node_name, :rabbit_db_cluster, :cli_cluster_status, []) do
        {:badrpc, {:EXIT, {:undef, _}}} ->
          :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :status, [])

        {:badrpc, _} = err ->
          err

        status ->
          status
      end

    case status0 do
      {:badrpc, _} = err ->
        err

      status0 ->
        tags = cluster_tags(node_name, timeout)
        status = status0 ++ [{:cluster_tags, tags}]
        case :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :list_running, []) do
          {:badrpc, _} = err ->
            err

          {:error, {:corrupt_or_missing_cluster_files, _, _}} ->
            {:error, "Could not read mnesia files containing cluster status"}

          nodes ->
            count = length(nodes)

            alarms_by_node =
              Enum.map(nodes, fn n -> alarms_by_node(n, per_node_timeout(timeout, count)) end)

            listeners_by_node =
              Enum.map(nodes, fn n -> listeners_of(n, per_node_timeout(timeout, count)) end)

            versions_by_node =
              Enum.map(nodes, fn n -> versions_by_node(n, per_node_timeout(timeout, count)) end)

            maintenance_status_by_node =
              Enum.map(
                nodes,
                fn n -> maintenance_status_by_node(n, per_node_timeout(timeout, count), opts) end
              )

            cpu_cores_by_node =
              Enum.map(
                nodes,
                fn n -> cpu_cores_by_node(n, per_node_timeout(timeout, count)) end
              )

            feature_flags =
              case :rabbit_misc.rpc_call(node_name, :rabbit_ff_extra, :cli_info, [], timeout) do
                {:badrpc, {:EXIT, {:undef, _}}} -> []
                {:badrpc, _} = err -> err
                val -> val
              end

            status ++
              [{:alarms, alarms_by_node}] ++
              [{:listeners, listeners_by_node}] ++
              [{:versions, versions_by_node}] ++
              [{:cpu_cores, cpu_cores_by_node}] ++
              [{:maintenance_status, maintenance_status_by_node}] ++
              [{:feature_flags, feature_flags}]
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
    m =
      result_map(result)
      |> Map.update(:alarms, [], fn xs -> alarm_maps(xs) end)
      |> Map.update(:listeners, %{}, fn m ->
        Enum.map(m, fn {n, xs} -> {n, listener_maps(xs)} end) |> Enum.into(%{})
      end)

    {:ok, m}
  end

  def output(result, %{node: node_name}) when is_list(result) do
    m = result_map(result)
    total_cores = Enum.reduce(m[:cpu_cores], 0, fn {_, val}, acc -> acc + val end)

    cluster_name_section = [
      "#{bright("Basics")}\n",
      "Cluster name: #{m[:cluster_name]}",
      "Total CPU cores available cluster-wide: #{total_cores}"
    ]

    cluster_tag_section =
      [
        "\n#{bright("Cluster Tags")}\n"
      ] ++
      case m[:cluster_tags] do
        [] -> ["(none)"]
        tags -> cluster_tag_lines(tags)
      end

    disk_nodes_section =
      [
        "\n#{bright("Disk Nodes")}\n"
      ] ++ node_lines(m[:disk_nodes])

    ram_nodes_section =
      case m[:ram_nodes] do
        [] ->
          []

        xs ->
          [
            "\n#{bright("RAM Nodes")}\n"
          ] ++ node_lines(xs)
      end

    running_nodes_section =
      [
        "\n#{bright("Running Nodes")}\n"
      ] ++ node_lines(m[:running_nodes])

    versions_section =
      [
        "\n#{bright("Versions")}\n"
      ] ++ version_lines(m[:versions])

    alarms_section =
      [
        "\n#{bright("Alarms")}\n"
      ] ++
        case m[:alarms] do
          [] -> ["(none)"]
          xs -> alarm_lines(xs, node_name)
        end

    partitions_section =
      [
        "\n#{bright("Network Partitions")}\n"
      ] ++
        case map_size(m[:partitions]) do
          0 -> ["(none)"]
          _ -> partition_lines(m[:partitions])
        end

    listeners_section =
      [
        "\n#{bright("Listeners")}\n"
      ] ++
        case map_size(m[:listeners]) do
          0 ->
            ["(none)"]

          _ ->
            Enum.reduce(m[:listeners], [], fn {node, listeners}, acc ->
              acc ++ listener_lines(listeners, node)
            end)
        end

    cpu_cores_section =
      [
        "\n#{bright("CPU Cores")}\n"
      ] ++ cpu_core_lines(m[:cpu_cores])

    maintenance_section =
      [
        "\n#{bright("Maintenance status")}\n"
      ] ++ maintenance_lines(m[:maintenance_status])

    feature_flags_section =
      [
        "\n#{bright("Feature flags")}\n"
      ] ++
        case Enum.count(m[:feature_flags]) do
          0 -> ["(none)"]
          _ -> feature_flag_lines(m[:feature_flags])
        end

    lines =
      cluster_name_section ++
        cluster_tag_section ++
        disk_nodes_section ++
        ram_nodes_section ++
        running_nodes_section ++
        versions_section ++
        cpu_cores_section ++
        maintenance_section ++
        alarms_section ++
        partitions_section ++
        listeners_section ++ feature_flags_section

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

  def description(),
    do:
      "Displays all the nodes in the cluster grouped by node type, together with the currently running nodes"

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
      cluster_tags: result |> Keyword.get(:cluster_tags, []),
      disk_nodes: result |> Keyword.get(:nodes, []) |> Keyword.get(:disc, []),
      ram_nodes: result |> Keyword.get(:nodes, []) |> Keyword.get(:ram, []),
      running_nodes: result |> Keyword.get(:running_nodes, []) |> Enum.map(&to_string/1),
      alarms: Keyword.get(result, :alarms) |> Keyword.values() |> Enum.concat() |> Enum.uniq(),
      maintenance_status: Keyword.get(result, :maintenance_status, []) |> Enum.into(%{}),
      partitions: Keyword.get(result, :partitions, []) |> Enum.into(%{}),
      listeners: Keyword.get(result, :listeners, []) |> Enum.into(%{}),
      versions: Keyword.get(result, :versions, []) |> Enum.into(%{}),
      cpu_cores: Keyword.get(result, :cpu_cores, []) |> Enum.into(%{}),
      feature_flags:
        Keyword.get(result, :feature_flags, []) |> Enum.map(fn ff -> Enum.into(ff, %{}) end)
    }
  end

  defp alarms_by_node(node, timeout) do
    alarms =
      case :rabbit_misc.rpc_call(to_atom(node), :rabbit, :alarms, [], timeout) do
        {:badrpc, _} -> []
        xs -> xs
      end

    {node, alarms}
  end

  defp listeners_of(node, timeout) do
    node = to_atom(node)

    listeners =
      case :rabbit_misc.rpc_call(
             node,
             :rabbit_networking,
             :node_listeners,
             [node],
             timeout
           ) do
        {:badrpc, _} -> []
        xs -> xs
      end

    {node, listeners}
  end

  defp versions_by_node(node, timeout) do
    {rmq_name, rmq_vsn, otp_vsn} =
      case :rabbit_misc.rpc_call(
             to_atom(node),
             :rabbit,
             :product_info,
             [],
             timeout
           ) do
        {:badrpc, _} ->
          {nil, nil, nil}

        map ->
          %{:otp_release => otp} = map

          name =
            case map do
              %{:product_name => v} -> v
              %{:product_base_name => v} -> v
            end

          vsn =
            case map do
              %{:product_version => v} -> v
              %{:product_base_version => v} -> v
            end

          {name, vsn, otp}
      end

    {node,
     %{
       rabbitmq_name: to_string(rmq_name),
       rabbitmq_version: to_string(rmq_vsn),
       erlang_version: to_string(otp_vsn)
     }}
  end

  defp maintenance_status_by_node(node, timeout, opts) do
    target = to_atom(node)
    formatter = Map.get(opts, :formatter)

    rpc_result =
      :rabbit_misc.rpc_call(target, :rabbit_maintenance, :status_local_read, [target], timeout)

    result =
      case {rpc_result, formatter} do
        {{:badrpc, _}, _} -> "unknown"
        {:regular, _} -> "not under maintenance"
        {:draining, "json"} -> "marked for maintenance"
        {:draining, _} -> magenta("marked for maintenance")
        # forward compatibility: should we figure out a way to know when
        # draining completes (it involves inherently asynchronous cluster
        # operations such as quorum queue leader re-election), we'd introduce
        # a new state
        {:drained, "json"} -> "marked for maintenance"
        {:drained, _} -> magenta("marked for maintenance")
        {value, _} -> to_string(value)
      end

    {node, result}
  end

  defp cpu_cores_by_node(node, timeout) do
    target = to_atom(node)

    result =
      case :rabbit_misc.rpc_call(
             target,
             :rabbit_runtime,
             :guess_number_of_cpu_cores,
             [],
             timeout
           ) do
        {:badrpc, _} -> 0
        value -> value
      end

    {node, result}
  end

  defp cluster_tags(node, timeout) do
    case :rabbit_misc.rpc_call(
          node,
          :rabbit_runtime_parameters,
          :value_global,
          [:cluster_tags],
          timeout) do
      :not_found -> []
      tags -> tags
    end
  end

  defp node_lines(nodes) do
    Enum.map(nodes, &to_string/1) |> Enum.sort()
  end

  defp version_lines(mapping) do
    Enum.map(mapping, fn {node,
                          %{
                            rabbitmq_name: rmq_name,
                            rabbitmq_version: rmq_vsn,
                            erlang_version: otp_vsn
                          }} ->
      "#{node}: #{rmq_name} #{rmq_vsn} on Erlang #{otp_vsn}"
    end)
  end

  defp partition_lines(mapping) do
    Enum.map(mapping, fn {node, unreachable_peers} ->
      "Node #{node} cannot communicate with #{Enum.join(unreachable_peers, ", ")}"
    end)
  end

  defp cpu_core_lines(mapping) do
    Enum.map(mapping, fn {node, core_count} ->
      "Node: #{node}, available CPU cores: #{core_count}"
    end)
  end

  defp maintenance_lines(mapping) do
    Enum.map(mapping, fn {node, status} -> "Node: #{node}, status: #{status}" end)
  end

  defp cluster_tag_lines(mapping) do
    Enum.map(mapping, fn {key, value} ->
      "#{key}: #{value}"
    end)
  end
end
