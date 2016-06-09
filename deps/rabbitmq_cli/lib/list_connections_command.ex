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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule ListConnectionsCommand do
  alias RabbitMQ.CLI.RabbitMQCtl.Helpers, as: Helpers
  alias RabbitMQ.CLI.RabbitMQCtl.InfoKeys, as: InfoKeys
  alias RabbitMQ.CLI.RabbitMQCtl.RpcStream, as: RpcStream

  @behaviour CommandBehaviour

  @info_keys ~w(pid name port host peer_port peer_host ssl ssl_protocol
                ssl_key_exchange ssl_cipher ssl_hash peer_cert_subject
                peer_cert_issuer peer_cert_validity state
                channels protocol auth_mechanism user vhost timeout frame_max
                channel_max client_properties recv_oct recv_cnt send_oct
                send_cnt send_pend connected_at)a

  def validate(args, _) do
      case InfoKeys.validate_info_keys(args, @info_keys) do
        {:ok, _} -> :ok
        err -> err
      end
  end
  def merge_defaults([], opts) do
    {~w(user peer_host peer_port state), opts}
  end
  def merge_defaults(args, opts), do: {args, opts}

  def switches(), do: []

  def flags() do
      []
  end

  def usage() do
      "list_connections [<connectioninfoitem> ...]"
  end

  def usage_additional() do
      "<connectioninfoitem> must be a member of the list ["<>
      Enum.join(@info_keys, ", ") <>"]."
  end

  def run([_|_] = args, %{node: node_name, timeout: timeout}) do
      info_keys = Enum.map(args, &String.to_atom/1)
      node = Helpers.parse_node(node_name)
      nodes = Helpers.nodes_in_cluster(node_name)

      RpcStream.receive_list_items(node,
                                   :rabbit_networking,
                                   :emit_connection_info_all,
                                   [nodes, info_keys],
                                   timeout,
                                   info_keys,
                                   Kernel.length(nodes))
  end


  def banner(_, _), do: "Listing connections ..."
end
