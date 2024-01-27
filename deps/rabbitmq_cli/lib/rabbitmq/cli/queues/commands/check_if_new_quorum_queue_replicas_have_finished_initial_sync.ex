## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CheckIfNewQuorumQueueReplicasHaveFinishedInitialSyncCommand do
  @moduledoc """
  Exits with a non-zero code if there are quorum queues
  that run "non-voter" (not yet done with their initial sync, promotable to voters)
  replicas on the current node.

  This command is used to verify if a new cluster node hosts only
  fully synchronized.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def scopes(), do: [:diagnostics, :queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(
           node_name,
           :rabbit_quorum_queue,
           :list_with_local_promotable_for_cli,
           [],
           timeout
         ) do
      [] -> {:ok, []}
      qs when is_list(qs) -> {:ok, qs}
      other -> other
    end
  end

  def output({:ok, []}, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output({:ok, []}, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output({:ok, []}, %{node: node_name}) do
    {:ok, "Node #{node_name} reported no queues with promotable replicas"}
  end

  def output({:ok, qs}, %{node: node_name, formatter: "json"}) when is_list(qs) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "queues" => qs,
       "message" => "Node #{node_name} reported local queues promotable replicas"
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
      "that run promotable replicas on the current node."
  end

  def usage, do: "check_if_new_quorum_queue_replicas_have_finished_initial_sync"

  def banner([], %{node: node_name}) do
    "Checking if node #{node_name} runs promotable replicas of any queues ..."
  end

  #
  # Implementation
  #

  def queue_lines(qs, node_name) do
    for q <- qs, do: "#{q["readable_name"]} hasn't finished synchronization with #{node_name}."
  end
end
