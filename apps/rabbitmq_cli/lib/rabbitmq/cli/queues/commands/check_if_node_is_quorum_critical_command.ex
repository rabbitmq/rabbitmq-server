## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CheckIfNodeIsQuorumCriticalCommand do
  @moduledoc """
  Exits with a non-zero code if there are quorum queues that would lose their quorum
  if the target node is shut down.

  This command is meant to be used as a pre-upgrade (pre-shutdown) check.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def scopes(), do: [:diagnostics, :queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :is_single_node_cluster, [], timeout) do
      # if target node is the only one in the cluster, the check makes little sense
      # and false positives can be misleading
      true  -> {:ok, :single_node_cluster}
      false ->
        case :rabbit_misc.rpc_call(node_name, :rabbit_maintenance, :is_being_drained_local_read, [node_name]) do
          # if target node is under maintenance, it has already transferred all of its quorum queue
          # replicas. Don't consider it to be quorum critical. See rabbitmq/rabbitmq-server#2469
          true  -> {:ok, :under_maintenance}
          false ->
            case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :list_with_minimum_quorum_for_cli, [], timeout) do
              [] -> {:ok, []}
              qs when is_list(qs) -> {:ok, qs}
              other -> other
            end
        end
      other -> other
    end
  end

  def output({:ok, :single_node_cluster}, %{formatter: "json"}) do
    {:ok, %{
      "result"  => "ok",
      "message" => "Target node seems to be the only one in a single node cluster, the check does not apply"
    }}
  end
  def output({:ok, :under_maintenance}, %{formatter: "json"}) do
    {:ok, %{
      "result"  => "ok",
      "message" => "Target node seems to be in maintenance mode, the check does not apply"
    }}
  end
  def output({:ok, []}, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end
  def output({:ok, :single_node_cluster}, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output({:ok, :under_maintenance}, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output({:ok, []}, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output({:ok, :single_node_cluster}, %{node: node_name}) do
    {:ok, "Node #{node_name} seems to be the only one in a single node cluster, the check does not apply"}
  end
  def output({:ok, :under_maintenance}, %{node: node_name}) do
    {:ok, "Node #{node_name} seems to be in maintenance mode, the check does not apply"}
  end
  def output({:ok, []}, %{node: node_name}) do
    {:ok, "Node #{node_name} reported no quorum queues with minimum quorum"}
  end
  def output({:ok, qs}, %{node: node_name, formatter: "json"}) when is_list(qs) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "queues" => qs,
       "message" => "Node #{node_name} reported local queues with minimum online quorum"
     }}
  end
  def output({:ok, qs}, %{silent: true}) when is_list(qs) do
    {:error, :check_failed}
  end
  def output({:ok, qs}, %{node: node_name}) when is_list(qs) do
    lines = queue_lines(qs, node_name)

    {:error, :check_failed, Enum.join(lines, line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description() do
    "Health check that exits with a non-zero code if there are queues " <>
    "with minimum online quorum (queues that would lose their quorum if the target node is shut down)"
  end

  def usage, do: "check_if_node_is_quorum_critical"

  def banner([], %{node: node_name}) do
    "Checking if node #{node_name} is critical for quorum of any quorum queues ..."
  end

  #
  # Implementation
  #

  def queue_lines(qs, node_name) do
    for q <- qs, do: "#{q["readable_name"]} would lose quorum if node #{node_name} is stopped"
  end
end
