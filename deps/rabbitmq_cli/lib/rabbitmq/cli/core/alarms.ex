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
end
