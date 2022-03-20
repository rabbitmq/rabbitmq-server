## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Upgrade.Commands.DrainCommand do
  @moduledoc """
  Puts the node in maintenance mode. Such node would not accept any
  new client connections, closes the connections it previously had,
  transfers leadership of locally hosted queues, and will not be considered
  for primary queue replica placement.

  This command is meant to be used when automating upgrades.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.DocGuide

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_maintenance, :drain, [], timeout) do
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

  def usage, do: "drain"

  def usage_doc_guides() do
    [
      DocGuide.upgrade()
    ]
  end

  def help_section(), do: :upgrade

  def description(), do: "Puts the node in maintenance mode. Such nodes will not serve any client traffic or host any primary queue replicas"

  def banner(_, %{node: node_name}) do
    "Will put node #{node_name} into maintenance mode. "
    <> "The node will no longer serve any client traffic!"
  end
end
