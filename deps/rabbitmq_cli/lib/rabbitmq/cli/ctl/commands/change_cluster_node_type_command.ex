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


defmodule RabbitMQ.CLI.Ctl.Commands.ChangeClusterNodeTypeCommand do
  alias RabbitMQ.CLI.Ctl.Helpers, as: Helpers

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def flags, do: []
  def switches(), do: []

  def merge_defaults(args, opts) do
    {args, opts}
  end

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([node_type], _) when node_type == "disc" or node_type == "ram", do: :ok
  def validate([_], _), do: {:validation_failure, {:bad_argument, "The node type must be either disc or ram."}}
  def validate(_, _),   do: {:validation_failure, :too_many_args}

  def run([node_type_arg], %{node: node_name}) do
    node_type = String.to_atom(node_type_arg)
    ret = :rabbit_misc.rpc_call(node_name,
        :rabbit_mnesia,
        :change_cluster_node_type,
        [node_type]
      )
    case ret do
      {:error, reason} ->
        {:change_node_type_failed, {reason, node_name}}
      result ->
        result
    end
  end

  def usage() do
    "change_cluster_node_type disk|ram"
  end

  def banner([node_type], %{node: node_name}) do
    "Turning #{node_name} into a #{node_type} node"
  end
end
