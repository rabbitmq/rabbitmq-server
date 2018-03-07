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
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.ListUnresponsiveQueuesCommand do
  require RabbitMQ.CLI.Ctl.InfoKeys
  require RabbitMQ.CLI.Ctl.RpcStream

  alias RabbitMQ.CLI.Ctl.InfoKeys, as: InfoKeys
  alias RabbitMQ.CLI.Ctl.RpcStream, as: RpcStream
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  @info_keys ~w(name durable auto_delete
            arguments pid recoverable_slaves)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  def switches(), do: [queue_timeout: :integer, local: :boolean, timeout: :integer]
  def aliases(), do: [t: :timeout]

  defp default_opts() do
    %{vhost: "/", local: false, queue_timeout: 15}
  end

  def merge_defaults([_|_] = args, opts) do
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

  def run(args, %{node: node_name, vhost: vhost, timeout: timeout,
                  queue_timeout: qtimeout, local: local_opt}) do
    info_keys = InfoKeys.prepare_info_keys(args)
    queue_timeout = qtimeout * 1000
    Helpers.with_nodes_in_cluster(node_name, fn(nodes) ->
      local_mfa  = {:rabbit_amqqueue, :emit_unresponsive_local, [vhost, info_keys, queue_timeout]}
      all_mfa  = {:rabbit_amqqueue, :emit_unresponsive, [nodes, vhost, info_keys, queue_timeout]}
      {chunks, mfas} = case local_opt do
          true  -> {1, [local_mfa]};
          false -> {Kernel.length(nodes), [all_mfa]}
        end
        RpcStream.receive_list_items(node_name, mfas, timeout, info_keys, chunks)
      end)
  end

  def usage() do
    "list_unresponsive_queues [--local] [--queue-timeout <queue-timeout>] [<unresponsiveq_ueueinfoitem> ...]"
  end

  def usage_additional() do
      "<unresponsive_queueinfoitem> must be a member of the list [" <>
      Enum.join(@info_keys, ", ") <> "]."
  end

  def banner(_,%{vhost: vhost}), do: "Listing unresponsive queues for vhost #{vhost} ..."
end
