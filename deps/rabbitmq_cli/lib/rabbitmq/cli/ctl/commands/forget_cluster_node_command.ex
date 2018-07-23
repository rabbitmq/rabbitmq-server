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

defmodule RabbitMQ.CLI.Ctl.Commands.ForgetClusterNodeCommand do
  alias RabbitMQ.CLI.Core.{Distribution, Validators}
  import Rabbitmq.Atom.Coerce

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def switches(), do: [offline: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{offline: false}, opts)}
  end

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_,_|_], _), do: {:validation_failure, :too_many_args}
  def validate([_], _), do: :ok


  def validate_execution_environment([_node_to_remove] = args, %{offline: true} = opts) do
    Validators.chain([&Validators.node_is_not_running/2,
                      &Validators.mnesia_dir_is_set/2,
                      &Validators.rabbit_is_loaded/2],
                     [args, opts])
  end
  def validate_execution_environment([_], %{offline: false}) do
    :ok
  end

  def run([node_to_remove], %{node: node_name, offline: true} = opts) do
    Stream.concat([
      become(node_name, opts),
      RabbitMQ.CLI.Core.Helpers.defer(fn() ->
        :rabbit_event.start_link()
        :rabbit_mnesia.forget_cluster_node(to_atom(node_to_remove), true)
      end)])
  end

  def run([node_to_remove], %{node: node_name, offline: false}) do
    :rabbit_misc.rpc_call(node_name,
                          :rabbit_mnesia, :forget_cluster_node,
                          [to_atom(node_to_remove), false])
  end

  def usage() do
    "forget_cluster_node [--offline] <existing_cluster_member_node>"
  end

  def banner([node_to_remove], _) do
    "Removing node #{node_to_remove} from the cluster"
  end


  defp become(node_name, opts) do
    :error_logger.tty(false)
    case :net_adm.ping(node_name) do
        :pong -> exit({:node_running, node_name});
        :pang -> :ok = :net_kernel.stop()
                 Stream.concat([
                   ["  * Impersonating node: #{node_name}..."],
                   RabbitMQ.CLI.Core.Helpers.defer(fn() ->
                     {:ok, _} = Distribution.start_as(node_name, opts)
                     " done"
                   end),
                   RabbitMQ.CLI.Core.Helpers.defer(fn() ->
                     dir = :mnesia.system_info(:directory)
                     "  * Mnesia directory: #{dir}..."
                   end)])
    end
  end
end
