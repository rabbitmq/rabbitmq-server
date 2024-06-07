## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

alias RabbitMQ.CLI.Core.ExitCodes

defmodule RabbitMQ.CLI.Ctl.Commands.ReconcileVhostsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts), do: {args, opts}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhosts, :reconcile_once, [], timeout)
  end

  def output({:ok, _pid}, _opts) do
    {:ok, "Will reconcile all virtual hosts in the cluster. This operation is asynchronous."}
  end

  def output({:error, err}, _output) do
    {:error, ExitCodes.exit_software(),
     ["Failed to start virtual host reconciliation", "Reason: #{inspect(err)}"]}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "reconcile_vhosts"

  def usage_additional() do
    []
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(),
    do: "Makes sure all virtual hosts were initialized on all reachable cluster nodes"

  def banner(_, _) do
    "Will try to initiate virtual host reconciliation on all reachable cluster nodes..."
  end
end
