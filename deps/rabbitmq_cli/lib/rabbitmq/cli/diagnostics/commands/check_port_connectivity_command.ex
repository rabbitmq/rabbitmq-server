## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckPortConnectivityCommand do
  @moduledoc """
  Checks all listeners on the target node by opening a TCP connection to each
  and immediately closing it.

  Returns a code of 0 unless there were connectivity and authentication
  errors. This command is meant to be used in health checks.
  """

  import RabbitMQ.CLI.Diagnostics.Helpers,
    only: [check_listener_connectivity: 3]
  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]
  import RabbitMQ.CLI.Core.Listeners

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 30_000

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(opts, %{timeout: timeout})}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err ->
        err

      {:error, _, _} = err ->
        err

      xs when is_list(xs) ->
        locals = listeners_on(xs, node_name)

        case locals do
          [] -> {true, locals}
          _ -> check_connectivity_of(locals, node_name, timeout)
        end

      other ->
        other
    end
  end

  def output({true, listeners}, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "listeners" => listener_maps(listeners)}}
  end

  def output({true, listeners}, %{node: node_name}) do
    ports =
      listeners
      |> listener_maps
      |> Enum.map(fn %{port: p} -> p end)
      |> Enum.sort()
      |> Enum.join(", ")

    {:ok, "Successfully connected to ports #{ports} on node #{node_name}."}
  end

  def output({false, failures}, %{formatter: "json", node: node_name}) do
    {:error, %{"result" => "error", "node" => node_name, "failures" => listener_maps(failures)}}
  end

  def output({false, failures}, %{node: node_name}) do
    lines = [
      "Connection to ports of the following listeners on node #{node_name} failed: "
      | listener_lines(failures)
    ]

    {:error, Enum.join(lines, line_separator())}
  end

  def description(), do: "Basic TCP connectivity health check for each listener's port on the target node"

  def help_section(), do: :observability_and_health_checks

  def usage, do: "check_port_connectivity"

  def banner([], %{node: node_name}) do
    "Testing TCP connections to all active listeners on node #{node_name} ..."
  end

  #
  # Implementation
  #

  defp check_connectivity_of(listeners, node_name, timeout) do
    # per listener timeout
    t = Kernel.trunc(timeout / (length(listeners) + 1))

    failures =
      Enum.reject(
        listeners,
        fn l -> check_listener_connectivity(listener_map(l), node_name, t) end
      )

    case failures do
      [] -> {true, listeners}
      fs -> {false, fs}
    end
  end
end
