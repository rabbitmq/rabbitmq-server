## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ConsumeEventStreamCommand do
  @moduledoc """
  Displays standard log file location on the target node
  """
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [duration: :integer, pattern: :string, timeout: :integer]
  def aliases(), do: [d: :duration, t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{duration: :infinity, pattern: ".*", quiet: true}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout, duration: duration, pattern: pattern}) do
    pid = self()
    ref = make_ref()
    subscribed = :rabbit_misc.rpc_call(
      node_name,
      :rabbit_event_consumer, :register,
      [pid, ref, duration, pattern],
      timeout)
    case subscribed do
      {:ok, ^ref} ->
        Stream.unfold(:confinue,
          fn(:finished) -> nil
            (:confinue) ->
              receive do
              {^ref, data, :finished} ->
                {data, :finished};
              {^ref, data, :confinue} ->
                {data, :confinue}
            end
          end)
      error -> error
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.JsonStream

  def printer(), do: RabbitMQ.CLI.Printers.StdIORaw

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Streams internal events from a running node. Output is jq-compatible."

  def usage, do: "consume_event_stream [--duration|-d <seconds>] [--pattern <pattern>]"

  def usage_additional() do
    [
      ["<duration_in_seconds>", "duration in seconds to stream log. Defaults to infinity"],
      ["<pattern>", "regular expression to pick events"]
    ]
  end

  def banner([], %{node: node_name, duration: :infinity}) do
    "Streaming logs from node #{node_name} ..."
  end
  def banner([], %{node: node_name, duration: duration}) do
    "Streaming logs from node #{node_name} for #{duration} seconds ..."
  end
end
