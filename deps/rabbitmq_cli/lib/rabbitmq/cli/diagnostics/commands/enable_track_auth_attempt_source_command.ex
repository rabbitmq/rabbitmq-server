## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.EnableTrackAuthAttemptSourceCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :application, :set_env,
      [:rabbit, :track_auth_attempt_source, :true])
  end

  def usage, do: "enable_track_auth_attempt_source"

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Disables the tracking of remote address and username of authentication attempts"

  def banner([], _), do: "Disabling the tracking of the source of authentication attempts ..."

  use RabbitMQ.CLI.DefaultOutput
end
