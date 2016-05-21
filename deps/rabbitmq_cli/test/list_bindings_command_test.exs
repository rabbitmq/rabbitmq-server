defmodule ListBindingsCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    add_vhost @vhost
    set_permissions @user, @vhost, [".*", ".*", ".*"]
    on_exit(fn ->
      delete_vhost @vhost
    end)
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname,
        timeout: context[:test_timeout] || @default_timeout,
        param: @vhost
      }
    }
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on a single bad arg", context do
    capture_io(fn ->
      assert ListBindingsCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  @tag test_timeout: :infinity
  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListBindingsCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListBindingsCommand.run(["quack", "source_name"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert ListBindingsCommand.run(["source_name", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert ListBindingsCommand.run(["source_kind", "oink", "source_name"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert ListBindingsCommand.run([], context[:opts]) ==
        [{:badrpc, {:timeout, 0.0}}]
    end)
  end

  test "no bindings for no queues", context do
    capture_io(fn ->
      [] = ListBindingsCommand.run([], context[:opts])
    end)
  end

  test "by default returns all info keys", context do
    default_keys = ~w(source_name source_kind destination_name destination_kind routing_key arguments)a
    capture_io(fn ->
      declare_queue("test_queue", @vhost)
      :timer.sleep(100)
      
      [binding] = ListBindingsCommand.run([], context[:opts])
      assert default_keys == Keyword.keys(binding)
    end)
  end

  test "can filter info keys", context do
    wanted_keys = ~w(source_name destination_name routing_key)
    capture_io(fn ->
      declare_queue("test_queue", @vhost)
      assert ListBindingsCommand.run(wanted_keys, context[:opts]) ==
              [[source_name: "", destination_name: "test_queue", routing_key: "test_queue"]]

    end)
  end

end