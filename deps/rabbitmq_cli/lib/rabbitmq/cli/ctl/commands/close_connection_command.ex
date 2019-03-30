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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

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
