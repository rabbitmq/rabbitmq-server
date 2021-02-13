## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ReclaimQuorumMemoryCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :queues]

  use RabbitMQ.CLI.Core.MergesDefaultVirtualHost
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :reclaim_memory, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "This operation is not applicable to classic queues"}

      other ->
        other
    end
  end

  def output({:error, :not_found}, %{vhost: vhost, formatter: "json"}) do
    {:error,
     %{
       "result" => "error",
       "message" => "Target queue was not found in virtual host '#{vhost}'"
     }}
  end
  def output({:error, error}, %{formatter: "json"}) do
    {:error,
     %{
       "result" => "error",
       "message" => "Failed to perform the operation: #{error}"
     }}
  end
  def output({:error, :not_found}, %{vhost: vhost}) do
    {:error, "Target queue was not found in virtual host '#{vhost}'"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "reclaim_quorum_memory  [--vhost <vhost>] <quorum queue>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the quorum queue"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues(),
      DocGuide.memory_use()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Flushes quorum queue processes WAL, performs a full sweep GC on all of its local Erlang processes"

  def banner([name], %{}),
    do: "Will flush Raft WAL of quorum queue #{name} ..."
end
