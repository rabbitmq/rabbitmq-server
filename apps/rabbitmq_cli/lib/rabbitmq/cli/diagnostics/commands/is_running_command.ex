## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.IsRunningCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    # Note: we use is_booted/1 over is_running/1 to avoid
    # returning a positive result when the node is still booting
    :rabbit_misc.rpc_call(node_name, :rabbit, :is_booted, [node_name], timeout)
  end

  def output(true, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => true, "message" => "RabbitMQ on node #{node_name} is fully booted and running"}}
  end
  def output(false, %{node: node_name, formatter: "json"}) do
    {:ok,
     %{"result" => false, "message" => "RabbitMQ on node #{node_name} is not running or has not fully booted yet (check with is_booting)"}}
  end
  def output(true, %{node: node_name}) do
    {:ok, "RabbitMQ on node #{node_name} is fully booted and running"}
  end
  def output(false, %{node: node_name}) do
    {:ok,
     "RabbitMQ on node #{node_name} is not running or has not fully booted yet (check with is_booting)"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Checks if RabbitMQ is fully booted and running on the target node"

  def usage, do: "is_running"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} for its status ..."
  end
end
