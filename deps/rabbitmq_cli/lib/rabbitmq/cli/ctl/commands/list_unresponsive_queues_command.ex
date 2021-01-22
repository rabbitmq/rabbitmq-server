## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListUnresponsiveQueuesCommand do
  require RabbitMQ.CLI.Ctl.InfoKeys
  require RabbitMQ.CLI.Ctl.RpcStream

  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}
  alias RabbitMQ.CLI.Core.Helpers

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @info_keys ~w(name durable auto_delete
            arguments pid recoverable_slaves)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  def switches() do
    [queue_timeout: :integer, local: :boolean, timeout: :integer]
  end

  def aliases(), do: [t: :timeout]

  defp default_opts() do
    %{vhost: "/", local: false, queue_timeout: 15, table_headers: true}
  end

  def merge_defaults([_ | _] = args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def merge_defaults([], opts) do
    merge_defaults(~w(name), opts)
  end

  def validate(args, _opts) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(args, %{
        node: node_name,
        vhost: vhost,
        timeout: timeout,
        queue_timeout: qtimeout,
        local: local_opt
      }) do
    info_keys = InfoKeys.prepare_info_keys(args)
    queue_timeout = qtimeout * 1000

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      local_mfa = {:rabbit_amqqueue, :emit_unresponsive_local, [vhost, info_keys, queue_timeout]}
      all_mfa = {:rabbit_amqqueue, :emit_unresponsive, [nodes, vhost, info_keys, queue_timeout]}

      {chunks, mfas} =
        case local_opt do
          true -> {1, [local_mfa]}
          false -> {Kernel.length(nodes), [all_mfa]}
        end

      RpcStream.receive_list_items(node_name, mfas, timeout, info_keys, chunks)
    end)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def banner(_, %{vhost: vhost}), do: "Listing unresponsive queues for vhost #{vhost} ..."

  def usage() do
    "list_unresponsive_queues [--local] [--queue-timeout <milliseconds>] [<column> ...] [--no-table-headers]"
  end

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")],
      ["--local", "only return queues hosted on the target node"],
      ["--queue-timeout <milliseconds>", "per-queue timeout to use when checking for responsiveness"]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Tests queues to respond within timeout. Lists those which did not respond"
end
