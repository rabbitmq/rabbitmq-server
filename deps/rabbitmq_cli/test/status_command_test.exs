defmodule StatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  setup_all do
    :net_kernel.start([:elixirmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    on_exit(context, fn -> :erlang.disconnect_node(context[:target]) end)
    :ok
  end

  @tag target: get_rabbit_hostname()
  test "status request on default RabbitMQ node",context do
    assert StatusCommand.status([])[:pid] != nil
  end

  @tag target: get_rabbit_hostname()
  test "status request on active RabbitMQ node", context do
    assert StatusCommand.status([node: context[:target]])[:pid] != nil
  end

  @tag target: :jake@thedog
  test "status request on nonexistent RabbitMQ node", context do
    assert StatusCommand.status([node: context[:target]]) == {:badrpc, :nodedown}
  end
end
