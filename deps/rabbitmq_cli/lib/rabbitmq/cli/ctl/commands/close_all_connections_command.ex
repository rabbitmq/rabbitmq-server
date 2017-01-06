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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.CloseAllConnectionsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  @flags []

  def merge_defaults(args, opts) do
    {args, Map.merge(%{global: false, vhost: "/", per_connection_delay: 0}, opts)}
  end
  
  def validate(args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok

  def run([explanation], %{node: node_name, vhost: vhost, global: global_opt,
                           per_connection_delay: delay}) do
    conns = case global_opt do
              false ->
                :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking, :list, [vhost])
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

  def output({:stream, stream}, _opts) do
    {:stream, Stream.filter(stream, fn(x) -> x != :ok end)}
  end
  use RabbitMQ.CLI.DefaultOutput

  def switches(), do: [global: :boolean, per_connection_delay: :integer]
  
  def usage, do: "close_all_connections [-p <vhost>] [-n <node> --global] [--per-connection-delay <delay>] <explanation>"

  def banner([explanation], %{node: node_name, global: true}), do: "Closing all connections to node #{node_name}, reason: #{explanation}..."
  def banner([explanation], %{vhost: vhost}), do: "Closing all connections to vhost #{vhost}, reason: #{explanation}..."

end
