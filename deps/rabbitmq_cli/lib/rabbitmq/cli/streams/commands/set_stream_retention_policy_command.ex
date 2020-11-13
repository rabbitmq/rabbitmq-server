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
## Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.SetStreamRetentionPolicyCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, retention_policy], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :set_retention_policy, [
          name,
          vhost,
          retention_policy
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner([name, retention_policy], _) do
    "Setting retention policy of stream queue #{name} to #{retention_policy} ..."
  end

  def usage, do: "set_stream_retention_policy [--vhost <vhost>] <name> <policy>"

  def usage_additional() do
    [
      ["<name>", "stream queue name"],
      ["<policy>", "retention policy"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.stream_queues()
    ]
  end

  def help_section(), do: :policies

  def description(), do: "Sets the retention policy of a stream queue"
end
