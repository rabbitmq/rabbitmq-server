## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ForceCheckpointCommand do
  alias RabbitMQ.CLI.Core.{DocGuide}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  defp default_opts,
    do: %{vhost_pattern: ".*", queue_pattern: ".*", errors_only: false}

  def switches(),
    do: [
      vhost_pattern: :string,
      queue_pattern: :string,
      errors_only: :boolean
    ]

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{
        node: node_name,
        vhost_pattern: vhost_pat,
        queue_pattern: queue_pat,
        errors_only: errors_only
      }) do
    args = [vhost_pat, queue_pat]

    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :force_checkpoint, args) do
      {:badrpc, _} = error ->
        error

      results when errors_only ->
        for {{:resource, vhost, _kind, name}, {:error, _, _} = res} <- results,
            do: [
              {:vhost, vhost},
              {:name, name},
              {:result, res}
            ]

      results ->
        for {{:resource, vhost, _kind, name}, res} <- results,
            do: [
              {:vhost, vhost},
              {:name, name},
              {:result, res}
            ]
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage,
    do: "force_checkpoint [--vhost-pattern <pattern>] [--queue-pattern <pattern>]"

  def usage_additional do
    [
      ["--queue-pattern <pattern>", "regular expression to match queue names"],
      ["--vhost-pattern <pattern>", "regular expression to match virtual host names"],
      ["--errors-only", "only list queues which reported an error"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :replication

  def description,
    do: "Forces checkpoints for all matching quorum queues"

  def banner([], _) do
    "Forcing checkpoint for all matching quorum queues..."
  end
end
