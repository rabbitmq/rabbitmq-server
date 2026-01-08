## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.QuorumStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :queues]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost}) do
    args = [vhost, name]
    case :rabbit_misc.rpc_call(node_name, :rabbit_queue_type, :status, args) do
      {:error, :not_found} ->
        {:error, {:not_found, :queue, vhost, name}}

      {:badrpc, {:EXIT, {:undef, _}}} ->
        :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :status, args)

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "quorum_status [--vhost <vhost>] <queue>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays the quorum status of a queue"

  def banner([name], %{node: node_name}),
    do: "Status of queue #{name} on node #{node_name} ..."
end
