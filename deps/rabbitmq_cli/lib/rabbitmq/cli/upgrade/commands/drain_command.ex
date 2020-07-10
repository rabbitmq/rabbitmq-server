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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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
