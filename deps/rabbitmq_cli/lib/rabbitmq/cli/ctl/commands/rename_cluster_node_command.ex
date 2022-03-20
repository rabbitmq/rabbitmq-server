## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.RenameClusterNodeCommand do
  require Integer
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(_, _) do
    :ok
  end

  def validate_execution_environment(args, opts) do
    Validators.chain(
      [
        &validate_args_count_even/2,
        &Validators.node_is_not_running/2,
        &Validators.mnesia_dir_is_set/2,
        &Validators.feature_flags_file_is_set/2,
        &Validators.rabbit_is_loaded/2
      ],
      [args, opts]
    )
  end

  def run(nodes, %{node: node_name}) do
    node_pairs = make_node_pairs(nodes)

    try do
      :rabbit_mnesia_rename.rename(node_name, node_pairs)
    catch
      _, reason ->
        {:rename_failed, reason}
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "rename_cluster_node <oldnode1> <newnode1> [oldnode2] [newnode2] ..."
  end

  def usage_additional() do
    [
      ["<oldnode>", "Original node name"],
      ["<newnode>", "New node name"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Renames cluster nodes in the local database"

  def banner(args, _) do
    [
      "Renaming cluster nodes: \n ",
      for {node_from, node_to} <- make_node_pairs(args) do
        "#{node_from} -> #{node_to} \n"
      end
    ]
    |> List.flatten()
    |> Enum.join()
  end

  #
  # Implementation
  #

  defp validate_args_count_even(args, _) do
    case agrs_count_even?(args) do
      true ->
        :ok

      false ->
        {:validation_failure,
         {:bad_argument, "Argument list should contain even number of nodes"}}
    end
  end

  defp agrs_count_even?(args) do
    Integer.is_even(length(args))
  end

  defp make_node_pairs([]) do
    []
  end

  defp make_node_pairs([from, to | rest]) do
    [{to_atom(from), to_atom(to)} | make_node_pairs(rest)]
  end
end
