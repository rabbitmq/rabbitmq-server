## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.UpdateClusterNodesCommand do
  alias RabbitMQ.CLI.Core.{Config, DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([seed_node], options=%{node: node_name}) do
    long_or_short_names = Config.get_option(:longnames, options)
    seed_node_normalised = Helpers.normalise_node(seed_node, long_or_short_names)
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_mnesia,
      :update_cluster_nodes,
      [seed_node_normalised]
    )
  end

  def usage() do
    "update_cluster_nodes <seed_node>"
  end

  def usage_additional() do
    [
      ["<seed_node>", "Cluster node to seed known cluster members from"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Instructs a cluster member node to sync the list of known cluster members from <seed_node>"

  def banner([seed_node], %{node: node_name}) do
    "Will seed #{node_name} from #{seed_node} on next start"
  end

  def output({:error, :mnesia_unexpectedly_running}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     RabbitMQ.CLI.DefaultOutput.mnesia_running_error(node_name)}
  end

  def output({:error, :cannot_cluster_node_with_itself}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: cannot cluster node with itself: #{node_name}"}
  end

  use RabbitMQ.CLI.DefaultOutput
end
