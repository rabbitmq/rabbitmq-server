## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ChangeClusterNodeTypeCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, opts}
  end

  def validate([], _), do: {:validation_failure, :not_enough_args}

  # node type
  def validate(["disc"], _), do: :ok
  def validate(["disk"], _), do: :ok
  def validate(["ram"], _), do: :ok

  def validate([_], _),
    do: {:validation_failure, {:bad_argument, "The node type must be either disc or ram."}}

  def validate(_, _), do: {:validation_failure, :too_many_args}

  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([node_type_arg], %{node: node_name}) do
    normalized_type = normalize_type(String.to_atom(node_type_arg))
    current_type = :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :node_type, [])

    case current_type do
      ^normalized_type ->
        {:ok, "Node type is already #{normalized_type}"}

      _ ->
        :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :change_cluster_node_type, [
          normalized_type
        ])
    end
  end

  def usage() do
    "change_cluster_node_type <disc | ram>"
  end

  def usage_additional() do
    [
      ["<disc | ram>", "New node type"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Changes the type of the cluster node"

  def banner([node_type], %{node: node_name}) do
    "Turning #{node_name} into a #{node_type} node"
  end

  def output({:error, :mnesia_unexpectedly_running}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     RabbitMQ.CLI.DefaultOutput.mnesia_running_error(node_name)}
  end

  use RabbitMQ.CLI.DefaultOutput

  defp normalize_type(:ram) do
    :ram
  end

  defp normalize_type(:disc) do
    :disc
  end

  defp normalize_type(:disk) do
    :disc
  end
end
