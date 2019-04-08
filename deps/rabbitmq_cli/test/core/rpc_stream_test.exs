defmodule RpcStreamTest do
  use ExUnit.Case, async: false

  require RabbitMQ.CLI.Ctl.RpcStream
  alias RabbitMQ.CLI.Ctl.RpcStream

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    :ok

  end

  test "emit empty list" do
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list, [[]], :infinity, []])

    assert [] == items
  end

  test "emit list without filters" do
    list  = [:one, :two, :three]
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list, [list], :infinity, []])

    assert list == items
  end


  test "emit list with filters" do
    list = [[one: 1, two: 2, three: 3], [one: 11, two: 12, three: 13]]
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list, [list], :infinity, [:one, :two]])

    assert [[one: 1, two: 2], [one: 11, two: 12]] == items
  end

  test "emit list of lists with filters" do
    list = [[[one: 1, two: 2, three: 3], [one: 11, two: 12, three: 13]],
            [[one: 21, two: 22, three: 23], [one: 31, two: 32, three: 33]]]
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list, [list], :infinity, [:one, :two]])

    assert [[[one: 1, two: 2], [one: 11, two: 12]], [[one: 21, two: 22], [one: 31, two: 32]]] == items
  end

  test "emission timeout 0 return badrpc" do
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list, [[]], 0, []])

    assert [{:badrpc, {:timeout, 0.0}}] == items
  end

  test "emission timeout return badrpc with timeout value in seconds" do
    timeout_fun = fn(x) -> :timer.sleep(1000); x end
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list_map, [[1,2,3], timeout_fun], 100, []])
    assert [{:badrpc, {:timeout, 0.1}}] == items
  end

  test "emission timeout in progress return badrpc with timeout value in seconds as last element" do
    timeout_fun = fn(x) -> :timer.sleep(100); x end
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list_map, [[1,2,3], timeout_fun], 150, []])
    assert [1, {:badrpc, {:timeout, 0.15}}] == items
  end

  test "parallel emission do not mix values" do
    {:ok, agent} = Agent.start_link(fn() -> :init end)
    list1  = [:one, :two, :three]
    list2  = [:dog, :cat, :pig]
    # Adding timeout to make sure emissions are executed in parallel
    timeout_fun = fn(x) -> :timer.sleep(10); x end
    Agent.update(agent,
                 fn(:init) ->
                   receive_list_items_to_list([Kernel.node, TestHelper, :emit_list_map, [list2, timeout_fun], :infinity, []])
                 end)
    items1 = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list_map, [list1, timeout_fun], :infinity, []])
    items2 = Agent.get(agent, fn(x) -> x end)

    assert items1 == list1
    assert items2 == list2
  end

  test "can receive from multiple emission sources in parallel" do
    list1  = [:one, :two, :three]
    list2  = [:dog, :cat, :pig]
    items = receive_list_items_to_list([Kernel.node, TestHelper, :emit_list_multiple_sources, [list1, list2], :infinity, []], 2)
    assert Kernel.length(list1 ++ list2) == Kernel.length(items)
    assert MapSet.new(list1 ++ list2) == MapSet.new(items)
  end

  def receive_list_items_to_list(args, chunks \\ 1) do
    res = Kernel.apply(RpcStream, :receive_list_items, args ++ [chunks])
    case Enumerable.impl_for(res) do
      nil -> res;
      _   -> Enum.to_list(res)
    end
  end
end
