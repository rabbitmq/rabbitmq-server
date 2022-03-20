## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckProtocolListenerCommand do
  @moduledoc """
  Exits with a non-zero code if there is no active listener
  for the given protocol on the target node.

  This command is meant to be used in health checks.
  """

  import RabbitMQ.CLI.Core.Listeners,
    only: [listeners_on: 2, listener_maps: 1, normalize_protocol: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([proto], %{node: node_name, timeout: timeout}) do
    proto = normalize_protocol(proto)

    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err ->
        err

      {:error, _, _} = err ->
        err

      xs when is_list(xs) ->
        locals = listeners_on(xs, node_name) |> listener_maps

        found =
          Enum.any?(locals, fn %{protocol: p} ->
            to_string(proto) == to_string(p)
          end)

        case found do
          true -> {true, proto}
          false -> {false, proto, locals}
        end

      other ->
        other
    end
  end

  def output({true, proto}, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "protocol" => proto}}
  end

  def output({true, proto}, %{node: node_name}) do
    {:ok, "A listener for protocol #{proto} is running on node #{node_name}."}
  end

  def output({false, proto, listeners}, %{formatter: "json"}) do
    protocols = Enum.map(listeners, fn %{protocol: p} -> p end)

    {:error,
     %{
       "result" => "error",
       "missing" => proto,
       "protocols" => protocols,
       "listeners" => listeners
     }}
  end

  def output({false, proto, listeners}, %{node: node_name}) do
    protocols = Enum.map(listeners, fn %{protocol: p} -> p end) |> Enum.sort() |> Enum.join(", ")

    {:error,
     "No listener for protocol #{proto} is active on node #{node_name}. " <>
       "Found listeners for the following protocols: #{protocols}"}
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if target node does not have an active listener for given protocol"

  def usage, do: "check_protocol_listener <protocol>"

  def banner([proto], %{node: node_name}) do
    "Asking node #{node_name} if there's an active listener for protocol #{proto} ..."
  end
end
