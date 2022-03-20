## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
