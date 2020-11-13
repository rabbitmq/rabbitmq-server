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
