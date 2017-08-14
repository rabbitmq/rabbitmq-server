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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SyncQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  defp default_opts, do: %{vhost: "/"}

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok
  def validate(_, _),   do: {:validation_failure, :too_many_args}

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([queue], %{vhost: vhost, node: node_name}) do
    :rpc.call(node_name,
      :rabbit_mirror_queue_misc,
      :sync_queue,
      [:rabbit_misc.r(vhost, :queue, queue)],
      :infinity
    )
  end

  def usage, do: "sync_queue [-p <vhost>] queue"

  def banner([queue], %{vhost: vhost, node: _node}) do
    "Synchronising queue '#{queue}' in vhost '#{vhost}' ..."
  end
end
