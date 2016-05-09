defmodule RpcStreamTest do
  use ExUnit.Case, async: false

  setup_all do
    :rabbit_control_misc.start_distribution()
    :ok
  end

  test "emit empty list" do
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list, [[]], :infinity, [])

    assert [] == items
  end

  test "emit list without filters" do
    list  = [:one, :two, :three]
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list, [list], :infinity, [])

    assert list == items
  end


  test "emit list with filters" do
    list = [[one: 1, two: 2, three: 3], [one: 11, two: 12, three: 13]]
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list, [list], :infinity, [:one, :two])

    assert [[one: 1, two: 2], [one: 11, two: 12]] == items
  end

  test "emit list of lists with filters" do
    list = [[[one: 1, two: 2, three: 3], [one: 11, two: 12, three: 13]],
            [[one: 21, two: 22, three: 23], [one: 31, two: 32, three: 33]]]
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list, [list], :infinity, [:one, :two])

    assert [[[one: 1, two: 2], [one: 11, two: 12]], [[one: 21, two: 22], [one: 31, two: 32]]] == items
  end

  test "emission timeout 0 return badrpc" do
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list, [[]], 0, [])

    assert [{:badrpc, :timeout}] == items
  end

  test "emission timeout return badrpc with timeout value in seconds" do
    timeout_fun = fn(x) -> :timer.sleep(1000); x end
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list_map, [[1,2,3], timeout_fun], 100, [])
    assert [{:badrpc, {:timeout, 0.1}}] == items
  end

  test "emission timeout in progress return badrpc with timeout value in seconds as last element" do
    timeout_fun = fn(x) -> :timer.sleep(100); x end
    items = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list_map, [[1,2,3], timeout_fun], 200, [])
    assert [1, {:badrpc, {:timeout, 0.2}}] == items
  end


  test "parallel emission do not mix values" do
    {:ok, agent} = Agent.start_link(fn() -> :init end)
    list1  = [:one, :two, :three]
    list2  = [:dog, :cat, :pig]
    # Adding timeout to make sure emissions are executed in parallel
    timeout_fun = fn(x) -> :timer.sleep(10); x end
    Agent.update(agent, 
                 fn(:init) ->
                   RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list_map, [list2, timeout_fun], :infinity, [])
                 end)
    items1 = RpcStream.receive_list_items(Kernel.node, TestHelper, :emit_list_map, [list1, timeout_fun], :infinity, [])
    items2 = Agent.get(agent, fn(x) -> x end)

    assert items1 == list1
    assert items2 == list2
  end

end
