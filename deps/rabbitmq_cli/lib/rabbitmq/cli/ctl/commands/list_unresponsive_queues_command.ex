## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

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
