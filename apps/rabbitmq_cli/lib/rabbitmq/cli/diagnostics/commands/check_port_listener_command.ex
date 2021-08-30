## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckPortListenerCommand do
  @moduledoc """
  Exits with a non-zero code if there is no active listener
  for the given port on the target node.

  This command is meant to be used in health checks.
  """

  import RabbitMQ.CLI.Core.Listeners, only: [listeners_on: 2, listener_maps: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositiveIntegerArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([port], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err ->
        err

      {:error, _, _} = err ->
        err

      xs when is_list(xs) ->
        locals = listeners_on(xs, node_name) |> listener_maps

        found =
          Enum.any?(locals, fn %{port: p} ->
            to_string(port) == to_string(p)
          end)

        case found do
          true -> {true, port}
          false -> {false, port, locals}
        end

      other ->
        other
    end
  end

  def output({true, port}, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "port" => port}}
  end

  def output({true, port}, %{node: node_name}) do
    {:ok, "A listener for port #{port} is running on node #{node_name}."}
  end

  def output({false, port, listeners}, %{formatter: "json"}) do
    ports = Enum.map(listeners, fn %{port: p} -> p end)

    {:error, :check_failed,
     %{"result" => "error", "missing" => port, "ports" => ports, "listeners" => listeners}}
  end

  def output({false, port, listeners}, %{node: node_name}) do
    ports = Enum.map(listeners, fn %{port: p} -> p end) |> Enum.sort() |> Enum.join(", ")

    {:error,  :check_failed,
     "No listener for port #{port} is active on node #{node_name}. " <>
       "Found listeners that use the following ports: #{ports}"}
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if target node does not have an active listener for given port"

  def usage, do: "check_port_listener <port>"

  def banner([port], %{node: node_name}) do
    "Asking node #{node_name} if there's an active listener on port #{port} ..."
  end
end
