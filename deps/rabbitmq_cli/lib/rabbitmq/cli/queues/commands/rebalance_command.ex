## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.RebalanceCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  @known_types [
    "all",
    "classic",
    "quorum"
  ]

  defp default_opts, do: %{vhost_pattern: ".*",
                           queue_pattern: ".*"}

  def switches(),
    do: [
    vhost_pattern: :string,
    queue_pattern: :string
  ]

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([type], _) do
    case Enum.member?(@known_types, type) do
      true ->
        :ok

      false ->
        {:error, "type #{type} is not supported. Try one of all, classic, quorum."}
    end
  end

  def run([type], %{node: node_name,
                vhost_pattern: vhost_pat,
                queue_pattern: queue_pat}) do
    arg = String.to_atom(type)
    :rabbit_misc.rpc_call(node_name, :rabbit_amqqueue, :rebalance, [arg, vhost_pat, queue_pat])
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage, do: "rebalance < all | classic | quorum > [--vhost-pattern <pattern>] [--queue-pattern <pattern>]"

  def usage_additional do
    [
      ["<type>", "queue type, must be one of: all, classic, quorum"],
      ["--queue-pattern <pattern>", "regular expression to match queue names"],
      ["--vhost-pattern <pattern>", "regular expression to match virtual host names"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :cluster_management

  def description, do: "Re-balances leaders of replicated queues across up-and-running cluster nodes"

  def banner([:all], _) do
    "Re-balancing leaders of all replicated queues..."
  end
  def banner([:classic], _) do
    "Re-balancing leaders of replicated (mirrored, non-exclusive) classic queues..."
  end
  def banner([:quorum], _) do
    "Re-balancing leaders of quorum queues..."
  end
  def banner([type], _) do
    "Re-balancing leaders of #{type} queues..."
  end
end
