## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Alarms do
  def alarm_lines(alarms, node_name) do
    Enum.reduce(alarms, [], fn
      :file_descriptor_limit, acc ->
        ["File descriptor limit alarm on node #{node_name}" | acc]

      {{:resource_limit, :memory, alarmed_node_name}, _}, acc ->
        ["Memory alarm on node #{alarmed_node_name}" | acc]

      {:resource_limit, :memory, alarmed_node_name}, acc ->
        ["Memory alarm on node #{alarmed_node_name}" | acc]

      {{:resource_limit, :disk, alarmed_node_name}, _}, acc ->
        ["Free disk space alarm on node #{alarmed_node_name}" | acc]

      {:resource_limit, :disk, alarmed_node_name}, acc ->
        ["Free disk space alarm on node #{alarmed_node_name}" | acc]
    end)
    |> Enum.reverse()
  end

  def local_alarms(alarms, node_name) do
    Enum.filter(
      alarms,
      fn
        # local by definition
        :file_descriptor_limit ->
          true

        {{:resource_limit, _, a_node}, _} ->
          node_name == a_node
      end
    )
  end

  def clusterwide_alarms(alarms, node_name) do
    alarms
    |> Enum.reject(fn x -> x == :file_descriptor_limit end)
    |> Enum.filter(fn {{:resource_limit, _, a_node}, _} ->
      a_node != node_name
    end)
  end

  def alarm_types(xs) do
    Enum.map(xs, &alarm_type/1)
  end

  def alarm_type(val) when is_atom(val) do
    val
  end
  def alarm_type({:resource_limit, val, _node}) do
    val
  end
  def alarm_type({{:resource_limit, val, _node}, []}) do
    val
  end

  def alarm_maps(xs) do
    Enum.map(xs, &alarm_map/1)
  end
  def alarm_map(:file_descriptor_limit) do
    %{
      type: :resource_limit,
      resource: :file_descriptors,
      node: node()
    }
  end
  def alarm_map({{:resource_limit, resource, node}, _}) do
    %{
      type: :resource_limit,
      resource: resource,
      node: node
    }
  end
  def alarm_map({:resource_limit, resource, node}) do
    %{
      type: :resource_limit,
      resource: resource,
      node: node
    }
  end
end
