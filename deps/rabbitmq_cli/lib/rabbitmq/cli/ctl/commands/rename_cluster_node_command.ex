## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.

require Integer

alias RabbitMQ.CLI.Ctl.Helpers, as: Helpers

defmodule RabbitMQ.CLI.Ctl.Commands.RenameClusterNodeCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def flags, do: [:mnesia_dir, :rabbitmq_home]
  def switches(), do: [mnesia_dir: :string, rabbitmq_home: :string]

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: {:validation_failure, :not_enough_args}
  def validate(args, opts) do
    validate([&validate_args_count_even/2,
              &validate_node_is_not_running/2,
              &validate_mnesia_dir_is_set/2,
              &validate_rabbit_is_loaded/2],
             args,
             opts)
  end


  defp validate([validator | rest], args, opts) do
    case validator.(args, opts) do
      :ok -> validate(rest, args, opts);
      err -> err
    end
  end
  defp validate([], _, _) do
    :ok
  end

  defp validate_args_count_even(args, _) do
    case agrs_count_even?(args) do
      true  -> :ok;
      false ->
        {:validation_failure,
          {:bad_argument, "Argument list should contain even number of nodes"}}
    end
  end

  defp validate_node_is_not_running(_, %{node: node_name}) do
    case node_running?(node_name) do
      true  -> {:validation_failure, :node_running};
      false -> :ok
    end
  end

  defp validate_mnesia_dir_is_set(_, opts) do
    case Helpers.require_mnesia_dir(opts) do
      :ok           -> :ok;
      {:error, err} -> {:validation_failure, err}
    end
  end

  defp validate_rabbit_is_loaded(_, opts) do
    case Helpers.require_rabbit(opts) do
      :ok           -> :ok;
      {:error, err} -> {:validation_failure, err}
    end
  end

  defp agrs_count_even?(args) do
    Integer.is_even(length(args))
  end

  defp node_running?(node) do
    :net_adm.ping(node) == :pong
  end

  def run(nodes, %{node: node_name}) do
    update_mnesia_dir()
    node_pairs = make_node_pairs(nodes)
    try do
      :rabbit_mnesia_rename.rename(node_name, node_pairs)
    catch _,reason ->
      {:error, reason}
    end
  end

  defp update_mnesia_dir() do
    case System.get_env("RABBITMQ_MNESIA_DIR") do
      nil -> :ok;
      val -> Application.put_env(:mnesia, :dir, to_char_list(val))
    end
  end

  defp make_node_pairs([]) do
    []
  end
  defp make_node_pairs([from, to | rest]) do
    [{String.to_atom(from), String.to_atom(to)} | make_node_pairs(rest)]
  end

  def usage() do
    "rename_cluster_node <oldnode1> <newnode1> [oldnode2] [newnode2 ...]"
  end

  def banner(args, _) do
    ["Renaming cluster nodes: \n ",
     for {node_from, node_to} <- make_node_pairs(args) do "#{node_from} -> #{node_to} \n" end]
    |> List.flatten
    |> Enum.join
  end
end
