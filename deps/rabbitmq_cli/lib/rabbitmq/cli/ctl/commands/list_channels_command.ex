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
##

defmodule RabbitMQ.CLI.Ctl.Commands.ListChannelsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  @info_keys ~w(pid connection name number user vhost transactional
                confirm consumer_count messages_unacknowledged
                messages_uncommitted acks_uncommitted messages_unconfirmed
                prefetch_count global_prefetch_count)a

  def info_keys(), do: @info_keys

  def merge_defaults([], opts) do
    merge_defaults(~w(pid user consumer_count messages_unacknowledged), opts)
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], opts) do
    run(~w(pid user consumer_count messages_unacknowledged), opts)
  end

  def run([_ | _] = args, %{node: node_name, timeout: timeout}) do
    info_keys = InfoKeys.prepare_info_keys(args)

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      RpcStream.receive_list_items(
        node_name,
        :rabbit_channel,
        :emit_info_all,
        [nodes, info_keys],
        timeout,
        info_keys,
        Kernel.length(nodes)
      )
    end)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def banner(_, _), do: "Listing channels ..."

  def usage() do
    "list_channels [--no-table-headers] [<column> ...]"
  end

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.channels()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists all channels in the node"
end
