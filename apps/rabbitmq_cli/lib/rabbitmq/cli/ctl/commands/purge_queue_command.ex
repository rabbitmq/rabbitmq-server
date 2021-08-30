## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.PurgeQueueCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([queue], %{node: node_name, vhost: vhost, timeout: timeout}) do
    res =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_amqqueue,
        :lookup,
        [:rabbit_misc.r(vhost, :queue, queue)],
        timeout
      )

    case res do
      {:ok, q} -> purge(node_name, q, timeout)
      _ -> res
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "purge_queue <queue>"

  def usage_additional() do
    [
      ["<queue>", "Name of the queue to purge"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.queues()
    ]
  end

  def help_section(), do: :queues

  def description(), do: "Purges a queue (removes all messages in it)"

  def banner([queue], %{vhost: vhost}) do
    "Purging queue '#{queue}' in vhost '#{vhost}' ..."
  end

  #
  # Implementation
  #

  defp purge(node_name, q, timeout) do
    res = :rabbit_misc.rpc_call(node_name, :rabbit_amqqueue, :purge, [q], timeout)

    case res do
      {:ok, _message_count} -> :ok
      _ -> res
    end
  end
end
