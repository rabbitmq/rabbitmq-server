## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Upgrade.Commands.ReviveCommand do
  @moduledoc """
  Puts the node out of maintenance and into regular operating mode.
  Such nodes will again serve client traffic and be considered for
  primary queue replica placement.

  A node will automatically go into regular operational mode
  after a restart.

  This command is meant to be used when automating upgrades.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.DocGuide

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_maintenance, :revive, [], timeout) do
      # Server does not support maintenance mode
      {:badrpc, {:EXIT, {:undef, _}}} -> {:error, :unsupported}
      {:badrpc, _} = err    -> err
      other                 -> other
    end
  end

  def output({:error, :unsupported}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Maintenance mode is not supported by node #{node_name}"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "revive"

  def usage_doc_guides() do
    [
      DocGuide.upgrade()
    ]
  end

  def help_section(), do: :upgrade

  def description(), do: "Puts the node out of maintenance and into regular operating mode. Such nodes will again serve client traffic and host primary queue replicas"

  def banner(_, %{node: node_name}) do
    "Will put node #{node_name} back into regular operating mode. "
    <> "The node will again serve client traffic and host primary queue replicas."
  end
end
