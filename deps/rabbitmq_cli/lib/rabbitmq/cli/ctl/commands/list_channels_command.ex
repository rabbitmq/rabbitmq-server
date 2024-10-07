## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
##

defmodule RabbitMQ.CLI.Ctl.Commands.ListChannelsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  @info_keys ~w(pid connection name number user vhost transactional
                confirm consumer_count messages_unacknowledged
                messages_uncommitted acks_uncommitted messages_unconfirmed
                prefetch_count)a

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
    run(~w(pid user consumer_count messages_unacknowledged) |> Enum.map(&to_charlist/1), opts)
  end

  def run([_ | _] = args, %{node: node_name, timeout: timeout}) do
    info_keys = InfoKeys.prepare_info_keys(args)
    broker_keys = InfoKeys.broker_keys(info_keys)

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      RpcStream.receive_list_items(
        node_name,
        :rabbit_channel,
        :emit_info_all,
        [nodes, broker_keys],
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
