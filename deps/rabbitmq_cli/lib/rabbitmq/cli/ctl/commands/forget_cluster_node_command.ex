## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ForgetClusterNodeCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Distribution, Validators}
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [offline: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{offline: false}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument

  def validate_execution_environment([_node_to_remove] = args, %{offline: true} = opts) do
    Validators.chain(
      [
        &Validators.node_is_not_running/2,
        &Validators.mnesia_dir_is_set/2,
        &Validators.feature_flags_file_is_set/2,
        &Validators.rabbit_is_loaded/2
      ],
      [args, opts]
    )
  end

  def validate_execution_environment([_], %{offline: false}) do
    :ok
  end

  def run([node_to_remove], %{node: node_name, offline: true} = opts) do
    Stream.concat([
      become(node_name, opts),
      RabbitMQ.CLI.Core.Helpers.defer(fn ->
        :rabbit_event.start_link()
        :rabbit_mnesia.forget_cluster_node(to_atom(node_to_remove), true)
      end)
    ])
  end

  def run([node_to_remove], %{node: node_name, offline: false}) do
    atom_name = to_atom(node_to_remove)
    args      = [atom_name, false]
    case :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :forget_cluster_node, args) do
      {:error, {:failed_to_remove_node, ^atom_name, {:active, _, _}}} ->
        {:error, "RabbitMQ on node #{node_to_remove} must be stopped with 'rabbitmqctl -n #{node_to_remove} stop_app' before it can be removed"};
      {:error, _}  = error -> error;
      {:badrpc, _} = error -> error;
      :ok ->
        case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :shrink_all, [atom_name]) do
          {:error, _} ->
            {:error, "RabbitMQ failed to shrink some of the quorum queues on node #{node_to_remove}"};
          _ -> :ok
        end
      other                -> other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "forget_cluster_node [--offline] <existing_cluster_member_node>"
  end

  def usage_additional() do
    [
      ["--offline", "try to update cluster membership state directly. Use when target node is stopped. Only works for local nodes."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering(),
      DocGuide.cluster_formation()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Removes a node from the cluster"

  def banner([node_to_remove], %{offline: true}) do
    "Removing node #{node_to_remove} from the cluster. Warning: quorum queues cannot be shrunk in offline mode"
  end
  def banner([node_to_remove], _) do
    "Removing node #{node_to_remove} from the cluster"
  end

  #
  # Implementation
  #

  defp become(node_name, opts) do
    :error_logger.tty(false)

    case :net_adm.ping(node_name) do
      :pong ->
        exit({:node_running, node_name})

      :pang ->
        :ok = :net_kernel.stop()

        Stream.concat([
          ["  * Impersonating node: #{node_name}..."],
          RabbitMQ.CLI.Core.Helpers.defer(fn ->
            {:ok, _} = Distribution.start_as(node_name, opts)
            " done"
          end),
          RabbitMQ.CLI.Core.Helpers.defer(fn ->
            dir = :mnesia.system_info(:directory)
            "  * Mnesia directory: #{dir}..."
          end)
        ])
    end
  end
end
