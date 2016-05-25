defmodule ListConsumersCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @vhost "test1"
  @user "guest"
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
        vhost: @vhost
      }
    }
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on a single bad arg", context do
    capture_io(fn ->
      assert run_command_to_list(ListConsumersCommand, [["quack"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  @tag test_timeout: :infinity
  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert run_command_to_list(ListConsumersCommand, [["quack", "oink"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert run_command_to_list(ListConsumersCommand, [["quack", "queue_name"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert run_command_to_list(ListConsumersCommand, [["queue_name", "oink"], context[:opts]]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert run_command_to_list(ListConsumersCommand, [["channel_pid", "oink", "queue_name"], context[:opts]]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert run_command_to_list(ListConsumersCommand, [[], context[:opts]]) ==
        [{:badrpc, {:timeout, 0.0}}]
    end)
  end

  test "no consumers for no open connections", context do
    close_all_connections(get_rabbit_hostname)
    capture_io(fn ->
      [] = run_command_to_list(ListConsumersCommand, [[], context[:opts]])
    end)
  end

  test "all info keys by default", context do
    queue_name = "test_queue1"
    consumer_tag = "i_am_consumer"
    info_keys = ~w(queue_name channel_pid consumer_tag ack_required prefetch_count arguments)a
    capture_io(fn ->
      declare_queue(queue_name, @vhost)
      with_channel(@vhost, fn(channel) ->
        {:ok, _} = AMQP.Basic.consume(channel, queue_name, nil, [consumer_tag: consumer_tag])
        :timer.sleep(100)
        [[consumer]] = run_command_to_list(ListConsumersCommand, [[], context[:opts]])
        assert info_keys == Keyword.keys(consumer)
        assert consumer[:consumer_tag] == consumer_tag
        assert consumer[:queue_name] == queue_name
        assert Keyword.delete(consumer, :channel_pid) ==
          [queue_name: queue_name, consumer_tag: consumer_tag,
           ack_required: true, prefetch_count: 0, arguments: []]

      end)
    end)
  end

  test "consumers are grouped by queues (multiple consumer per queue)", context do
    queue_name1 = "test_queue1"
    queue_name2 = "test_queue2"
    capture_io(fn ->
      declare_queue("test_queue1", @vhost)
      declare_queue("test_queue2", @vhost)
      with_channel(@vhost, fn(channel) ->
        {:ok, tag1} = AMQP.Basic.consume(channel, queue_name1)
        {:ok, tag2} = AMQP.Basic.consume(channel, queue_name2)
        {:ok, tag3} = AMQP.Basic.consume(channel, queue_name2)
        :timer.sleep(100)
        try do
          consumers = run_command_to_list(ListConsumersCommand, [["queue_name", "consumer_tag"], context[:opts]])
          {[[consumer1]], [consumers2]} = Enum.partition(consumers, fn([_]) -> true; ([_,_]) -> false end)
          assert [queue_name: queue_name1, consumer_tag: tag1] == consumer1
          assert Keyword.equal?([{tag2, queue_name2}, {tag3, queue_name2}],
            for([queue_name: q, consumer_tag: t] <- consumers2, do: {t, q}))
        after
          AMQP.Basic.cancel(channel, tag1)
          AMQP.Basic.cancel(channel, tag2)
          AMQP.Basic.cancel(channel, tag3)
        end
      end)
    end)
  end
end
