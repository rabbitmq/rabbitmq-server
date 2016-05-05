defmodule ListQueuesCommandTest do
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
      assert ListQueuesCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  @tag test_timeout: :infinity
  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListQueuesCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListQueuesCommand.run(["quack", "messages"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert ListQueuesCommand.run(["name", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert ListQueuesCommand.run(["name", "oink", "messages"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return a bad RPC", context do
    # capture_io(fn ->
      assert ListQueuesCommand.run([], context[:opts]) ==
        [{:badrpc, {:timeout, 0.0}}]
    # end)
  end

  @tag test_timeout: 1
  test "command timeout return badrpc", context do
    # We hope that broker will be unable to list 1000 queues in 1 millisecond.
    for i <- 1..1000 do
        declare_queue("test_queue_" <> Integer.to_string(i), @vhost)
    end
    # capture_io(fn ->
      assert ListQueuesCommand.run([], context[:opts]) ==
        [{:badrpc, {:timeout, 0.001}}]
    # end)
  end

  test "no info keys returns names and message count", context do
    queue_name = "test_queue"
    message_count = 3
    declare_queue(queue_name, @vhost)
    publish_messages(queue_name, 3)
    # capture_io(fn ->
        assert ListQueuesCommand.run([], context[:opts]) == 
            [[name: queue_name, messages: message_count]]
        # end)
  end

  test "return multiple queues", context do
    declare_queue("test_queue_1", @vhost)
    publish_messages("test_queue_1", 3)
    declare_queue("test_queue_2", @vhost)
    publish_messages("test_queue_2", 1)
    # capture_io(fn ->
        assert ListQueuesCommand.run([], context[:opts]) == 
            [[name: "test_queue_1", messages: 3],
             [name: "test_queue_2", messages: 1]]
        # end)
  end

  def publish_messages(name, count) do
    with_channel(@vhost, fn(channel) ->
        for i <- 1..count do 
            AMQP.Basic.publish(channel, "", name, 
                               "test_message" <> Integer.to_string(i))
        end
    end)
  end

end
