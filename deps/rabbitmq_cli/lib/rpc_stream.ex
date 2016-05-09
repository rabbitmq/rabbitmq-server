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
              {^ref, :finished}         -> nil;
              {^ref, {:timeout, t}}     -> {{:error, {:badrpc, {:timeout, (t / 1000)}}}, :finished};
              {^ref, result, :continue} -> {result, :continue};
              {:error, _} = error       -> {error,  :finished}
            end
        end)
      |> display_list_items(info_keys)
  end


  defp display_list_items(items, info_keys) do
    items
    |> Stream.filter(fn([]) -> false; (_) -> true end)
    |> Enum.map(
        fn({:error, error}) -> error;
          # if item is list of keyword lists:
          ([[{_,_}|_]|_] = item) ->
            Enum.map(item, fn(i) -> InfoKeys.info_for_keys(i, info_keys) end);
          (item) ->
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
    set_stream_timeout(pid, ref, timeout)
  end

  defp set_stream_timeout(_, _, :infinity) do
    :ok
  end
  defp set_stream_timeout(pid, ref, timeout) do
    Process.send_after(pid, {ref, {:timeout, timeout}}, timeout)
  end
end