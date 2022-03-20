## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetClusterNameCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([cluster_name], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_nodes, :set_cluster_name, [
      cluster_name,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner([cluster_name], _) do
    "Setting cluster name to #{cluster_name} ..."
  end

  def usage, do: "set_cluster_name <name>"

  def usage_additional() do
    [
      ["<name>", "New cluster name"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts(),
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Sets the cluster name"
end
