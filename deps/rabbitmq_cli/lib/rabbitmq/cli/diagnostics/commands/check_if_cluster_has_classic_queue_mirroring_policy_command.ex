## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckIfClusterHasClassicQueueMirroringPolicyCommand do
  @moduledoc """
  Exits with a non-zero code if there are policies enabling classic queue mirroring.

  This command is meant to be used as a pre-upgrade (pre-shutdown) check before classic queue
  mirroring is removed.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def scopes(), do: [:diagnostics, :queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    policies =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_mirror_queue_misc,
        :list_policies_with_classic_queue_mirroring_for_cli,
        [],
        timeout
      )

    op_policies =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_mirror_queue_misc,
        :list_operator_policies_with_classic_queue_mirroring_for_cli,
        [],
        timeout
      )

    case {policies, op_policies} do
      {[], []} ->
        true

      {_, _} when is_list(policies) and is_list(op_policies) ->
        {false, policies, op_policies}

      {{:badrpc, _} = left, _} ->
        left

      {_, {:badrpc, _} = right} ->
        right

      other ->
        other
    end
  end

  def output(true, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output(true, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output(true, %{}) do
    {:ok, "Cluster reported no policies that enable classic queue mirroring"}
  end

  def output({false, ps, op_ps}, %{formatter: "json"})
      when is_list(ps) and is_list(op_ps) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "policies" => ps,
       "operator_policies" => op_ps,
       "message" => "Cluster reported policies enabling classic queue mirroring"
     }}
  end

  def output({false, ps, op_ps}, %{silent: true}) when is_list(ps) and is_list(op_ps) do
    {:error, :check_failed}
  end

  def output({false, ps, op_ps}, _) when is_list(ps) and is_list(op_ps) do
    lines = policy_lines(ps)
    op_lines = op_policy_lines(op_ps)

    {:error, :check_failed, Enum.join(Enum.concat(lines, op_lines), line_separator())}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description() do
    "Health check that exits with a non-zero code if there are policies that enable classic queue mirroring"
  end

  def usage, do: "check_if_cluster_has_classic_queue_mirroring_policy"

  def banner([], _) do
    "Checking if cluster has any classic queue mirroring policy ..."
  end

  #
  # Implementation
  #

  def policy_lines(ps) do
    for p <- ps do
      "Policy #{p[:name]} enables classic queue mirroring"
    end
  end

  def op_policy_lines(ps) do
    for p <- ps do
      "Operator policy #{p[:name]} enables classic queue mirroring"
    end
  end
end
