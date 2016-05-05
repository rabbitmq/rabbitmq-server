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
    @behaviour CommandBehaviour

    def flags() do
        []
    end

    def usage() do
        "list_consumers [-p vhost]"
    end

    def run(_args, %{node: node_name, timeout: timeout, param: vhost} = opts) do
        info(opts)
        node_name
        |> Helpers.parse_node
        |> RpcStream.receive_list_items(:rabbit_amqqueue, :consumers_all,
                                        [vhost], timeout, [])
    end
    def run(args, %{node: node_name, timeout: timeout} = opts) do
        run(args, Map.merge(default_opts, opts))
    end

    defp default_opts() do
        %{param: "/"}
    end

    defp info(%{quiet: true}), do: nil
    defp info(_), do: IO.puts "Listing channels ..."
end