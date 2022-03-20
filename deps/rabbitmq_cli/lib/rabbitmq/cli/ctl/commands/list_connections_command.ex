## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListConnectionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  @info_keys ~w(pid name port host peer_port peer_host ssl ssl_protocol
                ssl_key_exchange ssl_cipher ssl_hash peer_cert_subject
                peer_cert_issuer peer_cert_validity state
                channels protocol auth_mechanism user vhost timeout frame_max
                channel_max client_properties recv_oct recv_cnt send_oct
                send_cnt send_pend connected_at)a

  def info_keys(), do: @info_keys

  def merge_defaults([], opts) do
    merge_defaults(~w(user peer_host peer_port state), opts)
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

  def run([_ | _] = args, %{node: node_name, timeout: timeout}) do
    info_keys = InfoKeys.prepare_info_keys(args)

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      RpcStream.receive_list_items(
        node_name,
        :rabbit_networking,
        :emit_connection_info_all,
        [nodes, info_keys],
        timeout,
        info_keys,
        Kernel.length(nodes)
      )
    end)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage() do
    "list_connections [--no-table-headers] [<column> ...]"
  end

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.connections()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists AMQP 0.9.1 connections for the node"

  def banner(_, _), do: "Listing connections ..."
end
