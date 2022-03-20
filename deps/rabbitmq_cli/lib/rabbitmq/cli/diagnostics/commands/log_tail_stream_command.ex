## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.LogTailStreamCommand do
  @moduledoc """
  Displays standard log file location on the target node
  """
  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.LogFiles


  def switches(), do: [duration: :integer, timeout: :integer]
  def aliases(), do: [d: :duration, t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{duration: :infinity}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def printer(), do: RabbitMQ.CLI.Printers.StdIORaw

  def run([], %{node: node_name, timeout: timeout, duration: duration}) do
    case LogFiles.get_default_log_location(node_name, timeout) do
      {:ok, file} ->
        pid = self()
        ref = make_ref()
        subscribed = :rabbit_misc.rpc_call(
                          node_name,
                          :rabbit_log_tail, :init_tail_stream,
                          [file, pid, ref, duration],
                          timeout)
        case subscribed do
          {:ok, ^ref} ->
            Stream.unfold(:confinue,
              fn(:finished) -> nil
                (:confinue) ->
                  receive do
                    {^ref, data, :finished} -> {data, :finished};
                    {^ref, data, :confinue} -> {data, :confinue}
                  end
              end)
          error -> error
        end
      error -> error
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Streams logs from a running node for a period of time"

  def usage, do: "log_tail_stream [--duration|-d <seconds>]"

  def usage_additional() do
    [
      ["<duration_in_seconds>", "duration in seconds to stream log. Defaults to infinity"]
    ]
  end

  def banner([], %{node: node_name, duration: :infinity}) do
    "Streaming logs from node #{node_name} ..."
  end
  def banner([], %{node: node_name, duration: duration}) do
    "Streaming logs from node #{node_name} for #{duration} seconds ..."
  end
end
