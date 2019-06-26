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


  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.MergesNoDefaults

  def printer(), do: RabbitMQ.CLI.Printers.StdIORaw

  def run([], %{node: node_name, timeout: timeout}) do
    case LogFiles.get_default_log_location(node_name, timeout) do
      {:ok, file} ->
        pid = self()
        ref = make_ref()
        subscribed = :rabbit_misc.rpc_call(
                          node_name,
                          :rabbit_log_tail, :init_tail_stream, [file, pid, ref],
                          timeout)
        case subscribed do
          {:ok, ^ref} ->
            Stream.unfold(:started,
              fn(_) ->
                receive do
                  {^ref, data, :finished} -> {data, nil};
                  {^ref, data, :confinue} -> {data, :confinue}
                end
              end)
          error -> error
        end
      error -> error
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :configuration

  def description(), do: "Streams logs from the node"

  def usage, do: "log_tail_stream"

  def banner([], %{node: node_name}) do
    "Streaming logs from node #{node_name} ..."
  end
end
