defmodule RpcStream do
    def receive_list_items(node, mod, fun, args, timeout, info_keys) do
        pid = Kernel.self
        ref = Kernel.make_ref
        init_items_stream(node, mod, fun, args, timeout, pid, ref)
        Stream.unfold(:continue,
            fn
            :finished -> nil;
            :continue ->
                receive do
                    {ref, :finished}         -> nil;
                    {ref, {:timeout, t}}     -> Kernel.exit({:error, {:timeout, (t / 1000)}});
                    {ref, result, :continue} -> {result, :continue};
                    {:error, error}          -> {error,  :finished};
                    other                    -> Kernel.exit({:unexpected_message_in_items_stream, other})
                end
            end)
        |> info_for_keys(info_keys)
    end

    defp init_items_stream(node, mod, fun, args, timeout, pid, ref) do
        Kernel.spawn_link(
            fn() ->
                case :rabbit_misc.rpc_call(node, mod, fun, args, ref, pid, timeout) do
                    {:error, _} = error        -> send(pid, {:error, error});
                    {:bad_argument, _} = error -> send(pid, {:error, error});
                    _                          -> :ok
                end
            end)
        set_stream_timeout(pid, ref, timeout)
    end

    defp set_stream_timeout(_, _, :infinity) do
        :ok
    end
    defp set_stream_timeout(pid, ref, timeout) do
        Process.send_after(pid, {ref, {:timeout, timeout}}, timeout)
    end

    defp info_for_keys(items, []) do
        items
    end
    defp info_for_keys(items, info_keys) do
        Enum.map(items,
                 &Enum.filter(&1, fn({k,_}) -> Enum.member?(info_keys, k) end))
    end
end