## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.IsBootingCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :is_booting, [node_name], timeout)
  end

  def output(true, %{node: node_name, formatter: "json"}) do
    m = %{
      "result" => true,
      "message" => "RabbitMQ on node #{node_name} is booting"
    }
    {:ok, m}
  end

  def output(false, %{node: node_name, formatter: "json"}) do
    m = %{
      "result" => false,
      "message" => "RabbitMQ on node #{node_name} is fully booted (check with is_running), stopped or has not started booting yet"
    }
    {:ok, m}
  end
  def output(true, %{node: node_name}) do
    {:ok, "RabbitMQ on node #{node_name} is booting"}
  end

  def output(false, %{node: node_name}) do
    {:ok,
     "RabbitMQ on node #{node_name} is fully booted (check with is_running), stopped or has not started booting yet"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Checks if RabbitMQ is still booting on the target node"

  def usage, do: "is_booting"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} for its boot status ..."
  end
end
