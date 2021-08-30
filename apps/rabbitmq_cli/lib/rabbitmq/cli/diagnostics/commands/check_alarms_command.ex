## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckAlarmsCommand do
  @moduledoc """
  Exits with a non-zero code if the target node reports any alarms,
  local or clusterwide.

  This command is meant to be used in health checks.
  """

  import RabbitMQ.CLI.Core.Alarms
  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    # Example response when there are alarms:
    #
    # [
    #  file_descriptor_limit,
    #  {{resource_limit,disk,hare@warp10},[]},
    #  {{resource_limit,memory,hare@warp10},[]},
    #  {{resource_limit,disk,rabbit@warp10},[]},
    #  {{resource_limit,memory,rabbit@warp10},[]}
    # ]
    #
    # The topmost file_descriptor_limit alarm is node-local.
    :rabbit_misc.rpc_call(node_name, :rabbit_alarm, :get_alarms, [], timeout)
  end

  def output([], %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output([], %{silent: true}) do
    {:ok, :check_passed}
  end

  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no alarms, local or clusterwide"}
  end

  def output(alarms, %{node: node_name, formatter: "json"}) do
    local = local_alarms(alarms, node_name)
    global = clusterwide_alarms(alarms, node_name)

    {:error, :check_failed,
     %{
       "result" => "error",
       "local" => alarm_lines(local, node_name),
       "global" => alarm_lines(global, node_name),
       "message" => "Node #{node_name} reported alarms"
     }}
  end

  def output(alarms, %{silent: true} = _opts) when is_list(alarms) do
    {:error, :check_failed}
  end

  def output(alarms, %{node: node_name}) when is_list(alarms) do
    lines = alarm_lines(alarms, node_name)

    {:error, :check_failed, Enum.join(lines, line_separator())}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if the target node reports any alarms, local or cluster-wide."

  def usage, do: "check_alarms"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report any local resource alarms ..."
  end
end
