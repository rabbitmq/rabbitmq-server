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
##


defmodule ListChannelsCommand do
    @behaviour CommandBehaviour

    @info_keys ~w(pid connection name number user vhost transactional
                  confirm consumer_count messages_unacknowledged
                  messages_uncommitted acks_uncommitted messages_unconfirmed
                  prefetch_count global_prefetch_count)a

    def switches(), do: []

    def flags() do
        []
    end

    def usage() do
        "list_channels [<channelinfoitem> ...]"
    end

    def usage_additional() do
        "<channelinfoitem> must be a member of the list ["<>
        Enum.join(@info_keys, ", ") <>"]."
    end

    def run([], opts) do
        run(~w(pid user consumer_count messages_unacknowledged), opts)
    end
    def run([_|_] = args, %{node: node_name, timeout: timeout} = opts) do
        InfoKeys.with_valid_info_keys(args, @info_keys,
            fn(info_keys) ->
                info(opts)
                node  = Helpers.parse_node(node_name)
                nodes = Helpers.nodes_in_cluster(node)
                RpcStream.receive_list_items(node,
                                             :rabbit_channel, :emit_info_all,
                                             [nodes, info_keys],
                                             timeout,
                                             info_keys,
                                             Kernel.length(nodes))
            end)
    end

    defp info(%{quiet: true}), do: nil
    defp info(_), do: IO.puts "Listing channels ..."
end