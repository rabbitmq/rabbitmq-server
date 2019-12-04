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

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckLocalAlarmsCommand do
  @moduledoc """
  Exits with a non-zero code if the target node reports any local alarms.

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
    case :rabbit_misc.rpc_call(node_name, :rabbit_alarm, :get_alarms, [], timeout) do
      [] -> []
      xs when is_list(xs) -> local_alarms(xs, node_name)
      other -> other
    end
  end

  def output([], %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output([], %{silent: true}) do
    {:ok, :check_passed}
  end

  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no local alarms"}
  end

  def output(alarms, %{node: node_name, formatter: "json"}) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "local" => alarm_lines(alarms, node_name),
       "message" => "Node #{node_name} reported local alarms"
     }}
  end

  def output(_alarms, %{silent: true}) do
    {:error, :check_failed}
  end

  def output(alarms, %{node: node_name}) do
    lines = alarm_lines(alarms, node_name)

    {:error, Enum.join(lines, line_separator())}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if the target node reports any local alarms"

  def usage, do: "check_local_alarms"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report any local resource alarms ..."
  end
end
