## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.StreamStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :queues, :streams]

  def merge_defaults(args, opts), do: {args, Map.merge(%{tracking: false, vhost: "/"}, opts)}

  def switches(), do: [tracking: :boolean]
  
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost, tracking: :false}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :status, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot get stream status of a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot get stream status of a quorum queue"}
        
      other ->
        other
    end
  end
  def run([name] = _args, %{node: node_name, vhost: vhost, tracking: :true}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :tracking_status, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot get stream status of a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot get stream status of a quorum queue"}
        
      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "stream_status [--vhost <vhost>] [--tracking] <queue>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.stream_queues()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays the status of a stream queue"

  def banner([name], %{node: node_name}),
    do: "Status of stream queue #{name} on node #{node_name} ..."
end
