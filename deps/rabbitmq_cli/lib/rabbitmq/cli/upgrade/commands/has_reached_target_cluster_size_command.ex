## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Upgrade.Commands.HasReachedTargetClusterSizeCommand do
  @moduledoc """
  Exits with a non-zero code if the node has not yet reached
  its target cluster size.

  The default target cluster size is 1. Users of this health check
  are expected to override it to the desired value using
  `cluster_formation.target_cluster_size_hint` in `rabbitmq.conf`.

  This command is meant to be used by automation tooling during
  rolling upgrades but can also double as a health check.
  """

  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics, :upgrade]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :reached_target_cluster_size, [], timeout) do
      true ->
        true

      false ->
        online = :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :running_count, [], timeout)
        target = :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :target_cluster_size_hint, [], timeout)

        case {online, target} do
          {o, t} when is_integer(o) and is_integer(t) ->
            {false, o, t}

          _ ->
            false
        end

      {:badrpc, _} = err ->
        err
    end
  end

  def output(true, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output(true, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output(true, %{node: node_name}) do
    {:ok, "Target cluster size has been reached on node #{node_name}"}
  end

  def output({false, online, target}, %{silent: true})
      when is_integer(online) and is_integer(target) do
    {:error, :check_failed}
  end

  def output({false, online, target}, %{node: node_name, formatter: "json"})
      when is_integer(online) and is_integer(target) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "message" =>
         "Target cluster size on node #{node_name} has not been reached: #{online} of #{target} nodes are online",
       "online" => online,
       "target" => target
     }}
  end

  def output({false, online, target}, %{node: node_name})
      when is_integer(online) and is_integer(target) do
    {:error,
     "Target cluster size on node #{node_name} has not been reached: #{online} of #{target} nodes are online"}
  end

  def output(false, %{silent: true}) do
    {:error, :check_failed}
  end

  def output(false, %{node: node_name}) do
    {:error,
     "Target cluster size on node #{node_name} has not been reached"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "has_reached_target_cluster_size"

  def usage_doc_guides() do
    [
      DocGuide.upgrade(),
      DocGuide.cluster_formation()
    ]
  end

  def help_section(), do: :upgrade

  def description(),
    do:
      "Health check that exits with a non-zero code if the cluster has not yet reached its target size"

  def banner([], %{node: node_name}) do
    "Checking if target cluster size has been reached on node #{node_name} ..."
  end
end
