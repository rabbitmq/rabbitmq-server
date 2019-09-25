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

defmodule RabbitMQ.CLI.Queues.Commands.DeleteMemberCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import Rabbitmq.Atom.Coerce

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
