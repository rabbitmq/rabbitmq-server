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


defmodule ListConsumersCommand do
  alias RabbitMQ.CLI.Ctl.Helpers, as: Helpers
  alias RabbitMQ.CLI.Ctl.InfoKeys, as: InfoKeys
  alias RabbitMQ.CLI.Ctl.RpcStream, as: RpcStream

  @behaviour CommandBehaviour

  @info_keys ~w(queue_name channel_pid consumer_tag
                ack_required prefetch_count arguments)a

  def validate(args, _) do
      case InfoKeys.validate_info_keys(args, @info_keys) do
        {:ok, _} -> :ok
        err -> err
      end
  end
  def merge_defaults([], opts) do
    {Enum.map(@info_keys, &Atom.to_string/1), opts}
  end
  def merge_defaults(args, opts), do: {args, opts}

  def switches(), do: []

  def flags() do
      [:vhost]
  end

  def usage() do
      "list_consumers [-p vhost] [<consumerinfoitem> ...]"
  end

  def usage_additional() do
      "<consumerinfoitem> must be a member of the list ["<>
      Enum.join(@info_keys, ", ") <>"]."
  end

  def run([_|_] = args, %{node: node_name, timeout: timeout, vhost: vhost}) do
      info_keys = Enum.map(args, &String.to_atom/1)
      node  = Helpers.parse_node(node_name)
      nodes = Helpers.nodes_in_cluster(node)
      RpcStream.receive_list_items(node, :rabbit_amqqueue, :emit_consumers_all,
                                   [nodes, vhost], timeout, info_keys)
  end
  def run(args, %{node: _node_name, timeout: _timeout} = opts) do
      run(args, Map.merge(default_opts, opts))
  end

  defp default_opts() do
      %{vhost: "/"}
  end

  def banner(_, %{vhost: vhost}), do: "Listing consumers on vhost #{vhost} ..."
end
