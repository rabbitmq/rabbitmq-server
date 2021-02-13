## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ForceGcCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_runtime, :gc_all_processes, [], timeout)
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "force_gc"

  def usage_doc_guides() do
    [
      DocGuide.memory_use()
    ]
  end

  def help_section(), do: :operations

  def description, do: "Makes all Erlang processes on the target node perform/schedule a full sweep garbage collection"

  def banner([], %{node: node_name}), do: "Will ask all processes on node #{node_name} to schedule a full sweep GC"
end
