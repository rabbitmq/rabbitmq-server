## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.JoinClusterCommand do
  alias RabbitMQ.CLI.Core.{Config, DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches() do
    [
      disc: :boolean,
      ram: :boolean
    ]
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{disc: false, ram: false}, opts)}
  end

  def validate(_, %{disc: true, ram: true}) do
    {:validation_failure, {:bad_argument, "The node type must be either disc or ram."}}
  end

  def validate([], _), do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok
  def validate(_, _), do: {:validation_failure, :too_many_args}

  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([target_node], %{node: node_name, ram: ram, disc: disc} = opts) do
    node_type =
      case {ram, disc} do
        {true, false} -> :ram
        {false, true} -> :disc
        ## disc is default
        {false, false} -> :disc
      end

    long_or_short_names = Config.get_option(:longnames, opts)
    target_node_normalised = Helpers.normalise_node(target_node, long_or_short_names)

    ret =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_db_cluster,
        :join,
        [target_node_normalised, node_type]
      )

    case ret do
      {:badrpc, {:EXIT, {:undef, _}}} ->
        :rabbit_misc.rpc_call(
          node_name,
          :rabbit_mnesia,
          :join_cluster,
          [target_node_normalised, node_type]
        )

      _ ->
        ret
    end
  end

  def output({:ok, :already_member}, _) do
    {:ok, "The node is already a member of this cluster"}
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

  def banner([target_node], %{node: node_name}) do
    "Clustering node #{node_name} with #{target_node}"
  end

  def usage() do
    "join_cluster [--disc|--ram] <existing_cluster_member>"
  end

  def usage_additional() do
    [
      ["<existing_cluster_member>", "Existing cluster member (node) to join"],
      [
        "--disc",
        "new node should be a disk one (stores its schema on disk). Highly recommended, used by default."
      ],
      [
        "--ram",
        "new node should be a RAM one (stores schema in RAM). Not recommended. Consult clustering doc guides first."
      ]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering(),
      DocGuide.cluster_formation()
    ]
  end

  def help_section(), do: :cluster_management

  def description(),
    do: "Instructs the node to become a member of the cluster that the specified node is in"
end
