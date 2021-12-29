## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.CloseAllConnectionsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [global: :boolean, per_connection_delay: :integer, limit: :integer]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{global: false, vhost: "/", per_connection_delay: 0, limit: 0}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([explanation], %{
        node: node_name,
        vhost: vhost,
        global: global_opt,
        per_connection_delay: delay,
        limit: limit
      }) do
    conns =
      case global_opt do
        false ->
          per_vhost =
            :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking, :list, [vhost])

          apply_limit(per_vhost, limit)

        true ->
          :rabbit_misc.rpc_call(node_name, :rabbit_connection_tracking, :list_on_node, [node_name])
      end

    case conns do
      {:badrpc, _} = err ->
        err

      _ ->
        :rabbit_misc.rpc_call(
          node_name,
          # As of 3.7.15, this function was moved to the rabbit_connection_tracking module.
          # rabbit_connection_tracking_handler still contains a delegating function with the same name.
          # Continue using this one for now for maximum CLI/server version compatibility. MK.
          :rabbit_connection_tracking_handler,
          :close_connections,
          [conns, explanation, delay]
        )

        {:ok, "Closed #{length(conns)} connections"}
    end
  end

  def run(args, %{
        node: node_name,
        global: global_opt,
        per_connection_delay: delay,
        limit: limit
      }) do
        run(args, %{
              node: node_name,
              vhost: nil,
              global: global_opt,
              per_connection_delay: delay,
              limit: limit
            })
  end

  def output({:stream, stream}, _opts) do
    {:stream, Stream.filter(stream, fn x -> x != :ok end)}
  end
  use RabbitMQ.CLI.DefaultOutput

  def banner([explanation], %{node: node_name, global: true}) do
    "Closing all connections to node #{node_name} (across all vhosts), reason: #{explanation}..."
  end

  def banner([explanation], %{vhost: vhost, limit: 0}) do
    "Closing all connections in vhost #{vhost}, reason: #{explanation}..."
  end

  def banner([explanation], %{vhost: vhost, limit: limit}) do
    "Closing #{limit} connections in vhost #{vhost}, reason: #{explanation}..."
  end

  def usage do
    "close_all_connections [--vhost <vhost> --limit <limit>] [-n <node> --global] [--per-connection-delay <delay>] <explanation>"
  end

  def usage_additional do
    [
      ["--global", "consider connections across all virtual hosts"],
      ["--limit <number>", "close up to this many connections"],
      ["--per-connection-delay <milliseconds>", "inject a delay between closures"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.connections()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Instructs the broker to close all connections for the specified vhost or entire RabbitMQ node"

  #
  # Implementation
  #

  defp apply_limit(conns, 0) do
    conns
  end

  defp apply_limit(conns, number) do
    :lists.sublist(conns, number)
  end
end
