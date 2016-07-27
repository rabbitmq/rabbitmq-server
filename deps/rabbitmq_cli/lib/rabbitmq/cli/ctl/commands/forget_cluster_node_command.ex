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

alias RabbitMQ.CLI.Ctl.Validators, as: Validators


defmodule RabbitMQ.CLI.Ctl.Commands.ForgetClusterNodeCommand do

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def flags, do: [:offline]
  def switches(), do: [offline: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{offline: false}, opts)}
  end

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_,_|_], _),   do: {:validation_failure, :too_many_args}
  def validate([_] = args, %{offline: true} = opts) do
    Validators.chain([&Validators.node_is_not_running/2,
                      &Validators.mnesia_dir_is_set/2,
                      &Validators.rabbit_is_loaded/2],
                     [args, opts])
  end
  def validate([_], %{offline: false}) do
    :ok
  end

  def run([target_node], %{node: node_name, offline: true}) do
    :rabbit_control_main.become(node_name)
    :rabbit_mnesia.forget_cluster_node(target_node, true)
  end

  def run([target_node], %{node: node_name, offline: false}) do
    :rabbit_misc.rpc_call(node_name,
                          :rabbit_mnesia, :forget_cluster_node,
                          [target_node, false])
  end

  def usage() do
    "forget_cluster_node [--offline] <existing_cluster_member_node>"
  end

  def banner([target_node], _) do
    "Removing node #{target_node} from cluster"
  end
end
