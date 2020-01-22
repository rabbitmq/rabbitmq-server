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
## Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.

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
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :list_with_minimum_quorum_for_cli, [], timeout) do
      [] -> []
      qs when is_list(qs) -> qs
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
    {:ok, "Node #{node_name} reported no quorum queues with minimum quorum"}
  end

  def output(qs, %{node: node_name, formatter: "json"}) when is_list(qs) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "queues" => qs,
       "message" => "Node #{node_name} reported local queues with minimum online quorum"
     }}
  end

  def output(_qs, %{silent: true}) do
    {:error, :check_failed}
  end

  def output(qs, %{node: node_name}) when is_list(qs) do
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
