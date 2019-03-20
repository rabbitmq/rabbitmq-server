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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AwaitStartupCommand do
  @moduledoc """
  Waits until target node is fully booted. If the node is already running,
  returns immediately.

  This command is meant to be used when automating deployments.
  See also `AwaitOnlineNodes`.
  """

  import RabbitMQ.CLI.Core.Config, only: [output_less?: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 300

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
