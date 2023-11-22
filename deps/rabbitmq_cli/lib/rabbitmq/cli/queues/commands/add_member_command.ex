## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.AddMemberCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  defp default_opts, do: %{vhost: "/", membership: "promotable", timeout: 5_000}

  def merge_defaults(args, opts) do
    default = default_opts()

    opts =
      Map.update(
        opts,
        :timeout,
        :infinity,
        &case &1 do
          :infinity -> default.timeout
          other -> other
        end
      )

    {args, Map.merge(default, opts)}
  end

  def switches(),
    do: [
      timeout: :integer,
      membership: :string
    ]

  def aliases(), do: [t: :timeout]

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, %{membership: m})
      when not (m == "promotable" or
                  m == "non_voter" or
                  m == "voter") do
    {:validation_failure, "voter status '#{m}' is not recognised."}
  end

  def validate(_, _) do
    :ok
  end

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

  def run(
        [name, node] = _args,
        %{vhost: vhost, node: node_name, timeout: timeout, membership: membership}
      ) do
    args = [vhost, name, to_atom(node)]

    args =
      case to_atom(membership) do
        :promotable -> args ++ [timeout]
        other -> args ++ [other, timeout]
      end

    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :add_member, args) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot add members to a classic queue"}

      {:error, :not_found} ->
        {:error, {:not_found, :queue, vhost, name}}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_member [--vhost <vhost>] <queue> <node> [--membership <promotable|voter>]"

  def usage_additional do
    [
      ["<queue>", "quorum queue name"],
      ["<node>", "node to add a new replica on"],
      ["--membership <promotable|voter>", "add a promotable non-voter (default) or full voter"]
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
