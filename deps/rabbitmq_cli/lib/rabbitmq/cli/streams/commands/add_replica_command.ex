## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.AddReplicaCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, node] = _args, %{vhost: vhost, node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :add_replica, [
           vhost,
           name,
           to_atom(node)
         ]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot add replicas to classic queues"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot add replicas to quorum queues"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_replica [--vhost <vhost>] <stream> <node>"

  def usage_additional do
    [
      ["<queue>", "stream name"],
      ["<node>", "node to add a new replica on"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section, do: :replication

  def description, do: "Adds a stream replica on the given node"

  def banner([name, node], _) do
    [
      "Adding a replica for stream #{name} on node #{node}..."
    ]
  end
end
