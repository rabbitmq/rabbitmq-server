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


defmodule RabbitMQ.CLI.Ctl.Commands.CloseAllConnectionsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  def merge_defaults(args, opts) do
    {args, Map.merge(%{global: false, vhost: "/", per_connection_delay: 0, limit: 0}, opts)}
  end

  def validate(args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([explanation], %{node: node_name, vhost: vhost, global: global_opt,
                           per_connection_delay: delay, limit: limit}) do
    conns = case global_opt do
              false ->
                per_vhost = :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking, :list, [vhost])
                apply_limit(per_vhost, limit)
              true ->
                :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking,
                  :list_on_node, [node_name])
            end
    case conns do
      {:badrpc, _} = err ->
        err
      _ ->
        :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking_handler,
          :close_connections, [conns, explanation, delay])
        {:ok, "Closed #{length(conns)} connections"}
    end
  end

  defp apply_limit(conns, 0) do
    conns
  end
  defp apply_limit(conns, number) do
    :lists.sublist(conns, number)
  end

  def output({:stream, stream}, _opts) do
    {:stream, Stream.filter(stream, fn(x) -> x != :ok end)}
  end
  use RabbitMQ.CLI.DefaultOutput

  def switches(), do: [global: :boolean, per_connection_delay: :integer, limit: :integer]

  def usage, do: "close_all_connections [-p <vhost> --limit <limit>] [-n <node> --global] [--per-connection-delay <delay>] <explanation>"

  def banner([explanation], %{node: node_name, global: true}) do
    "Closing all connections to node #{node_name} (across all vhosts), reason: #{explanation}..."
  end
  def banner([explanation], %{vhost: vhost, limit: 0}) do
    "Closing all connections in vhost #{vhost}, reason: #{explanation}..."
  end
  def banner([explanation], %{vhost: vhost, limit: limit}) do
    "Closing #{limit} connections in vhost #{vhost}, reason: #{explanation}..."
  end
end
