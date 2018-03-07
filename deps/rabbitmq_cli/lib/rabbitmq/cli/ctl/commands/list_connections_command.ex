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


defmodule RabbitMQ.CLI.Ctl.Commands.ListConnectionsCommand do
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  alias RabbitMQ.CLI.Ctl.InfoKeys, as: InfoKeys
  alias RabbitMQ.CLI.Ctl.RpcStream, as: RpcStream

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  @info_keys ~w(pid name port host peer_port peer_host ssl ssl_protocol
                ssl_key_exchange ssl_cipher ssl_hash peer_cert_subject
                peer_cert_issuer peer_cert_validity state
                channels protocol auth_mechanism user vhost timeout frame_max
                channel_max client_properties recv_oct recv_cnt send_oct
                send_cnt send_pend connected_at)a

  def info_keys(), do: @info_keys

  def merge_defaults([], opts) do
    {~w(user peer_host peer_port state), opts}
  end
  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) do
      case InfoKeys.validate_info_keys(args, @info_keys) do
        {:ok, _} -> :ok
        err -> err
      end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_|_] = args, %{node: node_name, timeout: timeout}) do
      info_keys = InfoKeys.prepare_info_keys(args)
      Helpers.with_nodes_in_cluster(node_name, fn(nodes) ->
        RpcStream.receive_list_items(node_name,
                                     :rabbit_networking,
                                     :emit_connection_info_all,
                                     [nodes, info_keys],
                                     timeout,
                                     info_keys,
                                     Kernel.length(nodes))
      end)
  end

  def usage() do
      "list_connections [<connectioninfoitem> ...]"
  end

  def usage_additional() do
      "<connectioninfoitem> must be a member of the list [" <>
      Enum.join(@info_keys, ", ") <> "]."
  end

  def banner(_, _), do: "Listing connections ..."
end
