## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetUserLimitsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username, definition], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :set_user_limits, [
      username,
      definition,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_user_limits <username> <definition>"

  def usage_additional() do
    [
      ["<username>", "Self-explanatory"],
      ["<definition>", "Limit definitions as a JSON document"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Sets user limits"

  def banner([username, definition], %{}) do
    "Setting user limits to \"#{definition}\" for user \"#{username}\" ..."
  end
end
