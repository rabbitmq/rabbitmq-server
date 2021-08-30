## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

alias RabbitMQ.CLI.Core.ExitCodes

defmodule RabbitMQ.CLI.Ctl.Commands.RestartVhostCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, vhost: vhost, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost_sup_sup, :start_vhost, [vhost], timeout)
  end

  def output({:ok, _pid}, %{vhost: vhost, node: node_name}) do
    {:ok, "Successfully restarted vhost '#{vhost}' on node '#{node_name}'"}
  end

  def output({:error, {:already_started, _pid}}, %{vhost: vhost, node: node_name}) do
    {:ok, "Vhost '#{vhost}' is already running on node '#{node_name}'"}
  end

  def output({:error, err}, %{vhost: vhost, node: node_name}) do
    {:error, ExitCodes.exit_software(),
     ["Failed to start vhost '#{vhost}' on node '#{node_name}'", "Reason: #{inspect(err)}"]}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "restart_vhost [--vhost <vhost>]"

  def usage_additional() do
    [
      ["--vhost", "Virtual host name"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Restarts a failed vhost data stores and queues"

  def banner(_, %{node: node_name, vhost: vhost}) do
    "Trying to restart vhost '#{vhost}' on node '#{node_name}' ..."
  end
end
