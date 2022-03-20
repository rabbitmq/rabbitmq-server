## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.TraceOnCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(_, opts) do
    {[], Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_trace, :start, [vhost]) do
      :ok -> {:ok, "Trace enabled for vhost #{vhost}"}
      other -> other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage do
    "trace_on [--vhost <vhost>]"
  end

  def usage_doc_guides() do
    [
      DocGuide.firehose(),
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def banner(_, %{vhost: vhost}), do: "Starting tracing for vhost \"#{vhost}\" ..."
end
