## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ResetNodeAuthAttemptMetricsCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_core_metrics, :reset_auth_attempt_metrics, [])
  end

  def usage, do: "reset_node_auth_attempt_metrics"

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Resets auth attempt metrics on the target node"

  def banner([], %{node: node_name}) do
    "Reset auth attempt metrics on node #{node_name} ..."
  end

  use RabbitMQ.CLI.DefaultOutput
end
