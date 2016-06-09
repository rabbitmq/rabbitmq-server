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
## Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.

defmodule CancelSyncQueueCommand do
  alias RabbitMQ.CLI.Ctl.Helpers, as: Helpers

  @behaviour CommandBehaviour

  def merge_defaults([_|_] = args, opts) do
    {args, Map.merge(default_opts, opts)}
  end

  def flags, do: [:vhost]

  def switches, do: []

  def usage, do: "cancel_sync_queue [-p <vhost>] queue"

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok
  def validate(_, _),   do: {:validation_failure, :too_many_args}

  def run([queue], %{vhost: vhost, node: node_name}) do
    node_name
    |> Helpers.parse_node
    |> :rpc.call(
      :rabbit_mirror_queue_misc,
      :cancel_sync_queue,
      [:rabbit_misc.r(vhost, :queue, queue)],
      :infinity
    )
  end
  
  def banner([queue], %{vhost: vhost, node: _node}) do
    "Stopping synchronising queue '#{queue}' in vhost '#{vhost}' ..."
  end

  defp default_opts, do: %{vhost: "/"}
end
