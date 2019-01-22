## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
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

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckAlarmsCommand do
  @moduledoc """
  Exits with a non-zero code if the target node reports any alarms,
  local or clusterwide.

  This command meant to be used in health checks.
  """

  alias RabbitMQ.CLI.Core.Helpers
  import RabbitMQ.CLI.Diagnostics.Helpers, only: [alarm_lines: 2,
                                                  local_alarms: 2,
                                                  clusterwide_alarms: 2]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok
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
    {:ok, %{"result"  => "ok"}}
  end
  def output([], %{silent: true}) do
    {:ok, :check_passed}
  end
  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no alarms, local or clusterwide"}
  end
  def output(alarms, %{node: node_name, formatter: "json"}) do
    local  = local_alarms(alarms, node_name)
    global = clusterwide_alarms(alarms, node_name)

    {:ok, %{"result"  => "ok",
            "local"   => alarm_lines(local, node_name),
            "global"  => alarm_lines(global, node_name),
            "message" => "Node #{node_name} reported alarms"}}
  end
  def output(_alarms, %{silent: true}) do
    {:error, :check_failed}
  end
  def output(alarms, %{node: node_name}) do
    lines = alarm_lines(alarms, node_name)

    {:error, Enum.join(lines, Helpers.line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "check_local_alarms"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report any local resource alarms ..."
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.String
end
