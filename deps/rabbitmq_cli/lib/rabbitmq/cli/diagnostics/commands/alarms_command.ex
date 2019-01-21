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

defmodule RabbitMQ.CLI.Diagnostics.Commands.AlarmsCommand do
  alias RabbitMQ.CLI.Core.Helpers

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


  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result"  => "ok",
            "alarms"  => []}}
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
  def output(alarms, %{node: node_name}) do
    lines = alarm_lines(alarms, node_name)

    {:ok, Enum.join(lines, Helpers.line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "alarms"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report any known resource alarms ..."
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.String


  defp alarm_lines(alarms, node_name) do
    Enum.reduce(alarms, [],
      fn
        (:file_descriptor_limit, acc) ->
          ["File descriptor limit alarm on node #{node_name}" | acc]
        ({{resource_limit, :memory, alarmed_node_name}, _}, acc) ->
          ["Memory alarm on node #{alarmed_node_name}" | acc]
        ({{resource_limit, :disk,   alarmed_node_name}, _}, acc) ->
          ["Free disk space alarm on node #{alarmed_node_name}" | acc]
      end) |> Enum.reverse
  end

  defp local_alarms(alarms, node_name) do
    Enum.filter(alarms,
      fn
        # local by definition
        (:file_descriptor_limit) ->
          true
        ({{:resource_limit, _, a_node}, _}) ->
          node_name == a_node
      end)
  end

  defp clusterwide_alarms(alarms, node_name) do
    alarms
    |> Enum.reject(fn x -> x == :file_descriptor_limit end)
    |> Enum.filter(fn ({{:resource_limit, _, a_node}, _}) ->
      a_node != node_name
    end)
  end
end
