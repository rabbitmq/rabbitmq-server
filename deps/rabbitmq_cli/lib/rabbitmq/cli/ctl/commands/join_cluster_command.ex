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
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.JoinClusterCommand do
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers

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
    {:validation_failure,
     {:bad_argument, "The node type must be either disc or ram."}}
  end
  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_], _), do: :ok
  def validate(_, _),   do: {:validation_failure, :too_many_args}

  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([target_node], %{node: node_name, ram: ram, disc: disc}) do
    node_type = case {ram, disc} do
      {true, false}  -> :ram
      {false, true}  -> :disc
      ## disc is default
      {false, false} -> :disc
    end
    :rabbit_misc.rpc_call(node_name,
        :rabbit_mnesia,
        :join_cluster,
        [Helpers.parse_node(target_node), node_type]
      )
  end

  def usage() do
    "join_cluster [--disc|--ram] <existing_cluster_member_node>"
  end

  def banner([target_node], %{node: node_name}) do
    "Clustering node #{node_name} with #{target_node}"
  end

  def output({:ok, :already_member}, _) do
    {:ok, "The node is already a member of this cluster"}
  end
  def output({:error, :mnesia_unexpectedly_running}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software,
     RabbitMQ.CLI.DefaultOutput.mnesia_running_error(node_name)}
  end
  def output({:error, :cannot_cluster_node_with_itself}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software,
     "Error: cannot cluster node with itself: #{node_name}"}
  end
  use RabbitMQ.CLI.DefaultOutput
end
