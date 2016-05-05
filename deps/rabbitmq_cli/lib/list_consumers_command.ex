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