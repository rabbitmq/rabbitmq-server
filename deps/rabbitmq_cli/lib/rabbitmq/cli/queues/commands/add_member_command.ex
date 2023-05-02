## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.AddMemberCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 5_000

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(%{vhost: "/", timeout: timeout}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments

  def validate_execution_environment(args, opts) do
    Validators.chain(
      [
        &Validators.rabbit_is_running/2,
        fn args, opts ->
          extractor = fn [_, val] -> val end
          Validators.existing_cluster_member(args, opts, extractor)
        end
      ],
      [args, opts]
    )
  end

  def run([name, node] = _args, %{vhost: vhost, node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :add_member, [
           vhost,
           name,
           to_atom(node),
           timeout
         ]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot add members to a classic queue"}

      {:error, :not_found} ->
        {:error, {:not_found, :queue, vhost, name}}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_member [--vhost <vhost>] <queue> <node>"

  def usage_additional do
    [
      ["<queue>", "quorum queue name"],
      ["<node>", "node to add a new replica on"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :replication

  def description, do: "Adds a quorum queue member (replica) on the given node."

  def banner([name, node], _) do
    [
      "Adding a replica for queue #{name} on node #{node}..."
    ]
  end
end
