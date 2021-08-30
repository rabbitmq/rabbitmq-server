## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearUserLimitsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments

  def run([username, limit_type], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :clear_user_limits, [
      username,
      limit_type,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def usage, do: "clear_user_limits username <limit_type> | all"

  def usage_additional() do
    [
      ["<limit_type>", "Limit type, must be max-connections or max-channels"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Clears user connection/channel limits"

  def banner([username, "all"], %{}) do
    "Clearing all limits for user \"#{username}\" ..."
  end
  def banner([username, limit_type], %{}) do
    "Clearing \"#{limit_type}\" limit for user \"#{username}\" ..."
  end
end
