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
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SyncQueueCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([queue], %{vhost: vhost, node: node_name}) do
    :rpc.call(
      node_name,
      :rabbit_mirror_queue_misc,
      :sync_queue,
      [:rabbit_misc.r(vhost, :queue, queue)],
      :infinity
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage do
    "sync_queue [--vhost <vhost>] <queue>"
  end

  def usage_additional() do
    [
      ["<queue>", "Name of the queue to synchronise"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.mirroring()
    ]
  end

  def help_section(), do: :replication

  def description(), do: "Instructs a mirrored queue with unsynchronised mirrors (follower replicas) to synchronise them"

  def banner([queue], %{vhost: vhost, node: _node}) do
    "Synchronising queue '#{queue}' in vhost '#{vhost}' ..."
  end
end
