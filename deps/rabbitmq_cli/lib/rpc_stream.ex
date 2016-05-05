defmodule RpcStream do
    def receive_list_items(_node, _mod, _fun, _args, 0, _info_keys) do
        #It will timeout anyway, so we don't waste broker resources
        [{:badrpc, {:timeout, 0.0}}]
    end
    def receive_list_items(node, mod, fun, args, timeout, info_keys) do
        pid = Kernel.self
        ref = Kernel.make_ref
        init_items_stream(node, mod, fun, args, timeout, pid, ref)
        Stream.unfold(:continue,
            fn
            :finished -> nil;
            :continue ->
                receive do
                    {^ref, :finished}         -> nil;
                    {^ref, {:timeout, t}}     -> {{:error, {:badrpc, {:timeout, (t / 1000)}}}, :finished};
                    {^ref, result, :continue} -> {result, :continue};
                    {:error, _} = error       -> {error,  :finished};
                    other                     -> Kernel.exit({:unexpected_message_in_items_stream, other})
                end
            end)
        |> display_list_items(info_keys)
    end

    defp display_list_items(items, info_keys) do
        Enum.map(items, fn({:error, error}) -> error;
                          (item)            -> 
                              InfoKeys.info_for_keys(item, info_keys)
                        end)
    end

    defp init_items_stream(node, mod, fun, args, timeout, pid, ref) do
        Kernel.spawn_link(
            fn() ->
                case :rabbit_misc.rpc_call(node, mod, fun, args, ref, pid, timeout) do
                    {:error, _} = error        -> send(pid, {:error, error});
                    {:bad_argument, _} = error -> send(pid, {:error, error});
                    {:badrpc, _} = error       -> send(pid, {:error, error}); 
                    _                          -> :ok
                end
            end)
         IO.puts("set Stream timeout")
        set_stream_timeout(pid, ref, timeout)
    end

    defp set_stream_timeout(_, _, :infinity) do
        :ok
    end
    defp set_stream_timeout(pid, ref, timeout) do
        Process.send_after(pid, {ref, {:timeout, timeout}}, timeout)
    end
end