defmodule ListConnectionsCommand do
	@behaviour CommandBehaviour

    @info_keys ~w(pid name port host peer_port peer_host ssl ssl_protocol
                  ssl_key_exchange ssl_cipher ssl_hash peer_cert_subject
                  peer_cert_issuer peer_cert_validity state
                  channels protocol auth_mechanism user vhost timeout frame_max
                  channel_max client_properties recv_oct recv_cnt send_oct
                  send_cnt send_pend connected_at)a

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

    def run([], opts) do
        run(~w(user peer_host peer_port state), opts)
    end
    def run([_|_] = args, %{node: node_name, timeout: timeout} = opts) do
        info_keys = Enum.map(args, &String.to_atom/1)
        InfoKeys.with_valid_info_keys(args, @info_keys,
            fn(info_keys) ->
                info(opts)
                node_name
                |> Helpers.parse_node
                |> RpcStream.receive_list_items(:rabbit_networking,
                                                :connection_info_all,
                                                [info_keys],
                                                timeout,
                                                info_keys)
            end)
    end

    defp default_opts() do
        %{}
    end

    defp info(%{quiet: true}), do: nil
    defp info(_), do: IO.puts "Listing connections ..."
end