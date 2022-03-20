## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.DeleteMemberCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, node] = _args, %{vhost: vhost, node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :delete_member, [
           vhost,
           name,
           to_atom(node)
         ]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot add members to a classic queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "delete_member [--vhost <vhost>] <queue> <node>"

  def usage_additional do
    [
      ["<queue>", "quorum queue name"],
      ["<node>", "node to remove a new replica on"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :replication

  def description, do: "Removes a quorum queue member (replica) on the given node."

  def banner([name, node], _) do
    "Removing a replica of queue #{name} on node #{node}..."
  end
end
