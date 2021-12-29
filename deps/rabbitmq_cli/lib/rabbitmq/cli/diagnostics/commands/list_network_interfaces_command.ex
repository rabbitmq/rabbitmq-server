## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ListNetworkInterfacesCommand do
  @moduledoc """
  Displays all network interfaces (NICs) reported by the target node.
  """
  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]
  import RabbitMQ.CLI.Core.ANSI

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [timeout: :integer, offline: :boolean]
  def aliases(), do: [t: :timeout]

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{offline: true}) do
    :rabbit_net.getifaddrs()
  end
  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_net, :getifaddrs, [], timeout)
  end

  def output(nic_map, %{node: node_name, formatter: "json"}) when map_size(nic_map) == 0 do
    {:ok, %{"result" => "ok", "node" => node_name, "interfaces" => %{}}}
  end
  def output(nic_map, %{node: node_name}) when map_size(nic_map) == 0 do
    {:ok, "Node #{node_name} reported no network interfaces"}
  end
  def output(nic_map0, %{node: node_name, formatter: "json"}) do
    nic_map = Enum.map(nic_map0, fn ({k, v}) -> {to_string(k), v} end)
    {:ok,
     %{
       "result" => "ok",
       "interfaces" => Enum.into(nic_map, %{}),
       "message" => "Node #{node_name} reported network interfaces"
     }}
  end
  def output(nic_map, _) when is_map(nic_map) do
    lines = nic_lines(nic_map)

    {:ok, Enum.join(lines, line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists network interfaces (NICs) on the target node"

  def usage, do: "list_network_interfaces"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report its network interfaces ..."
  end

  #
  # Implementation
  #

  defp nic_lines(nic_map) do
    Enum.reduce(nic_map, [],
      fn({iface, props}, acc) ->
        iface_lines = Enum.reduce(props, [],
          fn({prop, val}, inner_acc) ->
            ["#{prop}: #{val}" | inner_acc]
          end)

        header = "#{bright("Interface #{iface}")}\n"
        acc ++ [header | iface_lines] ++ ["\n"]
      end)
  end
end
