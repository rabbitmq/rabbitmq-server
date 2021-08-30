## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.LogTailCommand do
  @moduledoc """
  Displays standard log file location on the target node
  """
  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.LogFiles

  def switches, do: [number: :integer, timeout: :integer]
  def aliases, do: ['N': :number, t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{number: 50}, opts)}
  end
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout, number: n}) do
    case LogFiles.get_default_log_location(node_name, timeout) do
      {:ok, file} ->
        :rabbit_misc.rpc_call(node_name,
                              :rabbit_log_tail, :tail_n_lines, [file, n],
                              timeout)
      error -> error
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Prints the last N lines of the log on the node"

  def usage, do: "log_tail [--number|-N <number>]"

  def usage_additional do
    [
      ["<number>", "number of lines to print. Defaults to 50"]
    ]
  end

  def banner([], %{node: node_name, number: n}) do
    "Last #{n} log lines on node #{node_name} ..."
  end
end
