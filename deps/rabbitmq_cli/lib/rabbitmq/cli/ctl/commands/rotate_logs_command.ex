## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.RotateLogsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :rotate_logs, [])
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "rotate_logs"

  def usage_doc_guides() do
    [
      DocGuide.logging()
    ]
  end

  def help_section(), do: :node_management

  def description(), do: "Instructs the RabbitMQ node to perform internal log rotation"

  def banner(_, %{node: node_name}), do: "Rotating logs for node #{node_name} ..."
end
