## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.DeleteReplicaCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, node] = _args, %{vhost: vhost, node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :delete_replica, [
           vhost,
           name,
           to_atom(node)
         ]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot delete replicas from a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot delete replicas from a quorum queue"}

      {:error, :last_stream_member} ->
        {:error, "Cannot delete the last member of a stream"}
      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "delete_replica [--vhost <vhost>] <queue> <node>"

  def usage_additional do
    [
      ["<queue>", "stream queue name"],
      ["<node>", "node to remove a replica from"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.stream_queues()
    ]
  end

  def help_section, do: :replication

  def description, do: "Removes a stream queue replica on the given node."

  def banner([name, node], _) do
    "Removing a replica of queue #{name} on node #{node}..."
  end
end
