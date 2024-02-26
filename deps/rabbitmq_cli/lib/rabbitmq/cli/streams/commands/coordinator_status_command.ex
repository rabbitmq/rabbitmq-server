## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CoordinatorStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :streams]

  def merge_defaults(args, opts), do: {args, opts}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([] = _args, %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_coordinator, :status, []) do
      {:error, :coordinator_not_started_or_available} ->
        {:error, "Cannot get coordinator status as coordinator not started or unavailable"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "coordinator_status"
  end

  def usage_additional do
    []
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays raft status of the stream coordinator"

  def banner([], %{node: _node_name}),
    do: "Status of stream coordinator ..."
end
