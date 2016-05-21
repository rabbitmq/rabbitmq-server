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


defmodule ListExchangesCommand do
    @behaviour CommandBehaviour

    @info_keys ~w(name type durable auto_delete internal arguments policy)a

    def flags() do
        [:param]
    end

    def usage() do
        "list_exchanges [-p <vhost>] [<exchangeinfoitem> ...]"
    end

    def usage_additional() do
        "<exchangeinfoitem> must be a member of the list ["<>
        Enum.join(@info_keys, ", ") <>"]."
    end

    def run([], opts) do
        run(~w(name type), opts)
    end
    def run([_|_] = args, %{node: node_name, timeout: timeout, param: vhost} = opts) do
        info_keys = Enum.map(args, &String.to_atom/1)
        InfoKeys.with_valid_info_keys(args, @info_keys,
            fn(info_keys) ->
                info(opts)
                node_name
                |> Helpers.parse_node
                |> RpcStream.receive_list_items(:rabbit_exchange, :info_all,
                                                [vhost, info_keys],
                                                timeout,
                                                info_keys)
            end)
    end
    def run([_|_] = args, %{node: node_name, timeout: _timeout} = opts) do
        run(args, Map.merge(default_opts, opts))
    end

    defp default_opts() do
        %{param: "/"}
    end

    defp info(%{quiet: true}), do: nil
    defp info(_), do: IO.puts "Listing exchanges ..."

end