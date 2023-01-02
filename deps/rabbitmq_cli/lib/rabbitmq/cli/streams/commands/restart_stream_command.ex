## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.RestartStreamCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def switches() do
    [preferred_leader_node: :string]
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name], %{vhost: vhost, node: node_name} = switches) do
    preferred = Map.get(switches, :preferred_leader_node, :undefined)

    options = %{preferred_leader_node: to_atom(preferred)}

    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :restart_stream, [
           vhost,
           name,
           options
         ]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot restart a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot restart a quorum queue"}

      {:ok, LeaderNode} ->
        LeaderNode

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "restart_stream [--vhost <vhost>] <stream> [--preferred-leader-node <node>]"

  def usage_additional do
    [
      ["<stream>", "stream name"],
      ["--preferred-leader-node", "preferred leader node"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section, do: :replication

  def description, do: "Restarts a stream."

  def banner([name], _) do
    [
      "Restarting stream #{name}..."
    ]
  end
end
