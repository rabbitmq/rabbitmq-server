## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.CloseAllUserConnectionsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username, explanation], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_networking,
      :close_all_user_connections,
      [username, explanation]
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "close_all_user_connections <username> <explanation>"

  def usage_additional do
    [
      ["<username>", "Self-explanatory"],
      ["<explanation>", "reason for connection closure, will be logged and provided to clients"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.connections()
    ]
  end

  def help_section(), do: :operations

  def description(),
    do: "Instructs the broker to close all connections of the specified user"

  def banner([username, explanation], _),
    do: "Closing connections of user #{username}, reason: #{explanation}..."
end
