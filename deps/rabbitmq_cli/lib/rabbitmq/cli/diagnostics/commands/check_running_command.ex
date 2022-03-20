## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckRunningCommand do
  @moduledoc """
  Exits with a non-zero code if the RabbitMQ app on the target node is not running.

  This command is meant to be used in health checks.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    # Note: we use is_booted/1 over is_running/1 to avoid
    # returning a positive result when the node is still booting
    :rabbit_misc.rpc_call(node_name, :rabbit, :is_booted, [node_name], timeout)
  end

  def output(true, %{node: node_name} = _options) do
    {:ok, "RabbitMQ on node #{node_name} is fully booted and running"}
  end

  def output(false, %{node: node_name} = _options) do
    {:error,
     "RabbitMQ on node #{node_name} is not running or has not fully booted yet (check with is_booting)"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if the RabbitMQ app on the target node is not running"

  def usage, do: "check_running"

  def banner([], %{node: node_name}) do
    "Checking if RabbitMQ is running on node #{node_name} ..."
  end
end
