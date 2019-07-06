## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.

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
