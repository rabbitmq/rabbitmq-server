## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.

defmodule RabbitMQ.CLI.Streams.Commands.TransferLeadershipCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, node] = _args, %{vhost: vhost, node: node_name}) do
    args = [vhost, name, to_atom(node)]

    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :transfer_leadership, args) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot transfer leadership of a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot transfer leadership of a quorum queue"}

      {:error, {:not_supported_for_type, _type}} ->
        {:error, "Cannot transfer leadership of a non-stream queue"}

      {:error, :not_found} ->
        {:error, {:not_found, :queue, vhost, name}}

      {:ok, new_leader} ->
        {:ok, new_leader}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "transfer_leadership [--vhost <vhost>] <stream> <node>"

  def usage_additional do
    [
      ["<stream>", "stream name"],
      ["<node>", "node to transfer the leadership to"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section, do: :replication

  def description, do: "Transfers the leadership of a stream to the given node."

  def banner([name, node], _) do
    [
      "Transferring leadership of stream #{name} to node #{node}..."
    ]
  end
end
