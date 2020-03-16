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
## Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.AddReplicaCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import Rabbitmq.Atom.Coerce

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
        {:error, "Cannot add replicas to a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot add replicas to a quorum queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_replica [--vhost <vhost>] <queue> <node>"

  def usage_additional do
    [
      ["<queue>", "stream queue name"],
      ["<node>", "node to add a new replica on"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.stream_queues()
    ]
  end

  def help_section, do: :replication

  def description, do: "Adds a stream queue replica on the given node."

  def banner([name, node], _) do
    [
      "Adding a replica for queue #{name} on node #{node}..."
    ]
  end
end
