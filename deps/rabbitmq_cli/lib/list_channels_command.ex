defmodule ListChannelsCommand do
    @behaviour CommandBehaviour

    @info_keys ~w(pid connection name number user vhost transactional
                  confirm consumer_count messages_unacknowledged
                  messages_uncommitted acks_uncommitted messages_unconfirmed
                  prefetch_count global_prefetch_count)a

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
        info_keys = Enum.map(args, &String.to_atom/1)
        InfoKeys.with_valid_info_keys(args, @info_keys,
            fn(info_keys) ->
                info(opts)
                node_name
                |> Helpers.parse_node
                |> RpcStream.receive_list_items(:rabbit_channel, :info_all,
                                                [info_keys],
                                                timeout,
                                                info_keys)
            end)
    end

    defp default_opts() do
        %{}
    end

    defp info(%{quiet: true}), do: nil
    defp info(_), do: IO.puts "Listing channels ..."
end