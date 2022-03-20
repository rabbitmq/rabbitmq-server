## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AwaitStartupCommand do
  @moduledoc """
  Waits until target node is fully booted. If the node is already running,
  returns immediately.

  This command is meant to be used when automating deployments.
  See also `AwaitOnlineNodesCommand`.
  """

  import RabbitMQ.CLI.Core.Config, only: [output_less?: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 300_000

  def merge_defaults(args, opts) do
    {args, Map.merge(%{timeout: @default_timeout}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout} = opts) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :await_startup, [
      node_name,
      not output_less?(opts),
      timeout
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "await_startup"

  def help_section(), do: :node_management

  def description(), do: "Waits for the RabbitMQ application to start on the target node"

  def banner(_, _), do: nil
end
