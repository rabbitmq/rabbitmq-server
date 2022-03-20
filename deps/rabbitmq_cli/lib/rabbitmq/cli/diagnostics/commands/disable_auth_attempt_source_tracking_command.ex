## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.DisableAuthAttemptSourceTrackingCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :application, :set_env,
      [:rabbit, :track_auth_attempt_source, :false])
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "disable_track_auth_attempt_source"

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Disables the tracking of peer IP address and username of authentication attempts"

  def banner([], _), do: "Disabling authentication attempt source tracking ..."
end
