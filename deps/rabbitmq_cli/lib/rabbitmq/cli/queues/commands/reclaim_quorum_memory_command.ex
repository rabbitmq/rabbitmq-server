## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ReclaimQuorumMemoryCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :queues]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :reclaim_memory, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot reclaim memory of a classic queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "reclaim_quorum_memory [--vhost <vhost>] <queue>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues(),
      DocGuide.memory_use()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Makes all Erlang processes used by a quorum queue perform a full sweep garbage collection and flush of the WAL"

  def banner([name], %{}),
    do: "Reclaim memory used by quorum queue #{name} ..."
end
