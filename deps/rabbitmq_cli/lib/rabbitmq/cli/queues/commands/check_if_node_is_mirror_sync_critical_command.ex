## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CheckIfNodeIsMirrorSyncCriticalCommand do
  @moduledoc """
  Exits with a non-zero code if there are classic mirrored queues that don't
  have any in sync mirrors online and would potentially lose data
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
    case :rabbit_misc.rpc_call(node_name,
           :rabbit_nodes, :is_single_node_cluster, [], timeout) do
      # if target node is the only one in the cluster, the check makes little sense
      # and false positives can be misleading
      true  -> {:ok, :single_node_cluster}
      false ->
        case :rabbit_misc.rpc_call(node_name,
                                    :rabbit_amqqueue, :list_local_mirrored_classic_without_synchronised_mirrors_for_cli, [], timeout) do
          [] -> {:ok, []}
          qs when is_list(qs) -> {:ok, qs}
          other -> other
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
  def output({:ok, []}, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end
  def output({:ok, :single_node_cluster}, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output({:ok, []}, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output({:ok, :single_node_cluster}, %{node: node_name}) do
    {:ok, "Node #{node_name} seems to be the only one in a single node cluster, the check does not apply"}
  end
  def output({:ok, []}, %{node: node_name}) do
    {:ok, "Node #{node_name} reported no classic mirrored queues without online synchronised mirrors"}
  end
  def output({:ok, qs}, %{node: node_name, formatter: "json"}) when is_list(qs) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "queues" => qs,
       "message" => "Node #{node_name} reported local classic mirrored queues without online synchronised mirrors"
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
    "Health check that exits with a non-zero code if there are classic mirrored queues " <>
    "without online synchronised mirrors (queues that would potentially lose data if the target node is shut down)"
  end

  def usage, do: "check_if_node_is_mirror_sync_critical"

  def banner([], %{node: node_name}) do
    "Checking if node #{node_name} is critical for data safety of any classic mirrored queues ..."
  end

  #
  # Implementation
  #

  def queue_lines(qs, node_name) do
    for q <- qs do
      "#{q["readable_name"]} would lose its only synchronised replica (master) if node #{node_name} is stopped"
    end
  end
end
