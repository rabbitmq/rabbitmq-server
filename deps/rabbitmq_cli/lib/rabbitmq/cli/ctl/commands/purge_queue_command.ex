## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.PurgeQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([queue], %{node: node_name, vhost: vhost, timeout: timeout}) do
    res = :rabbit_misc.rpc_call(node_name,
      :rabbit_amqqueue, :lookup, [:rabbit_misc.r(vhost, :queue, queue)], timeout)

    case res do
      {:ok, q} -> purge(node_name, q, timeout)
      _        -> res
    end
  end

  defp purge(node_name, q, timeout) do
    res = :rabbit_misc.rpc_call(node_name, :rabbit_amqqueue, :purge, [q], timeout)
    case res do
      {:ok, _message_count} -> :ok
      _                     -> res
    end
  end

  def usage, do: "purge_queue <queue>"

  def banner([queue], %{vhost: vhost}) do
    "Purging queue '#{queue}' in vhost '#{vhost}' ..."
  end
end
