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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2018 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AwaitOnlineNodesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 300_000

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(opts, %{timeout: timeout})}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsOnePositiveIntegerArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([count], %{node: node_name, timeout: timeout}) do
    {n, _} = Integer.parse(count)
    :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :await_running_count, [n, timeout])
  end

  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: timed out while waiting. Not enough nodes joined #{node_name}'s cluster."}
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner([count], %{node: node_name, timeout: timeout}) when is_number(timeout) do
    "Will wait for at least #{count} nodes to join the cluster of #{node_name}. Timeout: #{
      trunc(timeout / 1000)
    } seconds."
  end

  def banner([count], %{node: node_name, timeout: _timeout}) do
    "Will wait for at least #{count} nodes to join the cluster of #{node_name}."
  end

  def usage() do
    "await_online_nodes <count>"
  end

  def usage_additional() do
    [
      ["<count>", "how many cluster members must be up in order for this command to exit. When <count> is 1, always exits immediately."]
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Waits for <count> nodes to join the cluster"
end
