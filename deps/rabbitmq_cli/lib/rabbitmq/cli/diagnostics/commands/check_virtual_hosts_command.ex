## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckVirtualHostsCommand do
  @moduledoc """
  Exits with a non-zero code if the target node reports any vhost down.

  This command is meant to be used in health checks.
  """

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost_sup_sup, :check, [], timeout)
  end

  def output([], %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output([], %{silent: true}) do
    {:ok, :check_passed}
  end

  def output([], %{formatter: "erlang"}) do
    {:ok, :check_passed}
  end

  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported all vhosts as running"}
  end

  def output(vhosts, %{formatter: "erlang"} = _opts) when is_list(vhosts) do
    {:error, :check_failed, {:down_vhosts, vhosts}}
  end

  def output(vhosts, %{formatter: "json"} = _opts) when is_list(vhosts) do
    {:error, :check_failed, %{"result" => "error", "down_vhosts" => vhosts}}
  end

  def output(vhosts, %{silent: true} = _opts) when is_list(vhosts) do
    {:error, :check_failed}
  end

  def output(vhosts, %{node: node_name}) when is_list(vhosts) do
    lines = Enum.join(vhosts, line_separator())
    {:error, "Some virtual hosts on node #{node_name} are down:\n#{lines}"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def description(), do: "Health check that checks if all vhosts are running in the target node"

  def help_section(), do: :observability_and_health_checks

  def usage, do: "check_virtual_hosts"

  def banner([], %{node: node_name}) do
    "Checking if all vhosts are running on node #{node_name} ..."
  end
end
