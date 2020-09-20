## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.PeekCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:queues]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, pos] = _args, %{node: node_name, vhost: vhost}) do
    {pos, _} = Integer.parse(pos)
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :peek, [vhost, name, pos]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot peek into a classic queue"}
      {:ok, msg} ->
        res = Enum.map(msg, fn {k,v} -> [{"keys", k}, {"values", v}] end)
        {:ok, res}
      err ->
        err
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "peek [--vhost <vhost>] <queue> <position>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue",
       "<position>", "Position in the queue, starts at 1"]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def description(), do: "Peeks at the given position of a quorum queue"

  def banner([name, pos], %{node: node_name}),
    do: "Peeking at quorum queue #{name} at position #{pos} on node #{node_name} ..."
end
