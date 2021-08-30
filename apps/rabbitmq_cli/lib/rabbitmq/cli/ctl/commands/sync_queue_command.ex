## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
