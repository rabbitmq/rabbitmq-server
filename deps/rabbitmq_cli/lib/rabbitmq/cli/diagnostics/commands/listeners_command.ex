## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ListenersCommand do
  @moduledoc """
  Displays all listeners on a node.

  Returns a code of 0 unless there were connectivity and authentication
  errors. This command is not meant to be used in health checks.
  """

  import RabbitMQ.CLI.Core.Listeners,
    only: [listeners_on: 2, listener_lines: 1, listener_maps: 1, listener_rows: 1]

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    # Example listener list:
    #
    # [{listener,rabbit@warp10,clustering,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           25672,[]},
    # {listener,rabbit@warp10,amqp,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           5672,
    #           [{backlog,128},
    #            {nodelay,true},
    #            {linger,{true,0}},
    #            {exit_on_close,false}]},
    # {listener,rabbit@warp10,stomp,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           61613,
    #           [{backlog,128},{nodelay,true}]}]
    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      xs when is_list(xs) -> listeners_on(xs, node_name)
      other -> other
    end
  end

  def output([], %{formatter: fmt}) when fmt == "csv" or fmt == "erlang" do
    {:ok, []}
  end

  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "listeners" => []}}
  end

  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no enabled listeners."}
  end

  def output(listeners, %{formatter: "erlang"}) do
    {:ok, listener_rows(listeners)}
  end

  def output(listeners, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "listeners" => listener_maps(listeners)}}
  end

  def output(listeners, %{formatter: "csv"}) do
    {:stream, [listener_rows(listeners)]}
  end

  def output(listeners, _opts) do
    lines = listener_lines(listeners)

    {:ok, Enum.join(lines, line_separator())}
  end

  def help_section(), do: :observability_and_health_checks

  def description(),
    do: "Lists active connection listeners (bound interface, port, protocol) on the target node"

  def usage, do: "listeners"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report its protocol listeners ..."
  end
end
