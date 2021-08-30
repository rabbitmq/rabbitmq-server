## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.RuntimeThreadStatsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [sample_interval: :integer]
  def aliases(), do: [i: :sample_interval]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{sample_interval: 5}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout, sample_interval: interval}) do
    case :rabbit_misc.rpc_call(
           node_name,
           :rabbit_runtime,
           :msacc_stats,
           [interval * 1000],
           timeout
         ) do
      {:ok, stats} -> stats
      other -> other
    end
  end

  def output(result, %{formatter: "json"}) when is_list(result) do
    {:error, "JSON formatter is not supported by this command"}
  end

  def output(result, %{formatter: "csv"}) when is_list(result) do
    {:error, "CSV formatter is not supported by this command"}
  end

  def output(result, _options) when is_list(result) do
    {:ok, result}
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Provides a breakdown of runtime thread activity stats on the target node"

  def usage, do: "runtime_thread_stats [--sample-interval <interval>]"

  def usage_additional() do
    [
      ["--sample-interval <seconds>", "sampling interval to use in seconds"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.runtime_tuning()
    ]
  end

  def banner([], %{node: node_name, sample_interval: interval}) do
    "Will collect runtime thread stats on #{node_name} for #{interval} seconds..."
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Msacc
end
