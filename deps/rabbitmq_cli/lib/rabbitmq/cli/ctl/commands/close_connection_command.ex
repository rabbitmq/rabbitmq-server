## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.CloseConnectionCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([pid, explanation], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_networking, :close_connection, [
      :rabbit_misc.string_to_pid(pid),
      explanation
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "close_connection <connection pid> <explanation>"

  def usage_additional do
    [
      ["<connection pid>", "connection identifier (Erlang PID), see list_connections"],
      ["<explanation>", "reason for connection closure"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.connections()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Instructs the broker to close the connection associated with the Erlang process id"

  def banner([pid, explanation], _), do: "Closing connection #{pid}, reason: #{explanation}..."
end
