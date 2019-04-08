defmodule ListConsumersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListConsumersCommand

  @vhost "test1"
  @user "guest"
  @default_timeout :infinity
  @info_keys ~w(queue_name channel_pid consumer_tag ack_required prefetch_count arguments)
  @default_options %{vhost: "/", table_headers: true}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

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
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || @default_timeout,
        vhost: @vhost
      }
    }
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {@info_keys, @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {@info_keys, %{vhost: "non_default",
                                                                                  table_headers: true}}
  end

  test "validate: returns bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: returns multiple bad args return a list of bad info key values", context do
    assert @command.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink, :quack]}}
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "queue_name"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["queue_name", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["channel_pid", "oink", "queue_name"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: zero timeout causes command to return badrpc", context do
      assert run_command_to_list(@command, [["queue_name"], context[:opts]]) ==
        [{:badrpc, {:timeout, 0.0}}]
  end

  test "run: no consumers for no open connections", context do
    close_all_connections(get_rabbit_hostname())
    [] = run_command_to_list(@command, [["queue_name"], context[:opts]])
  end

  test "run: defaults test", context do
    queue_name = "test_queue1"
    consumer_tag = "i_am_consumer"
    info_keys_s = ~w(queue_name channel_pid consumer_tag ack_required prefetch_count arguments)
    info_keys_a = Enum.map(info_keys_s, &String.to_atom/1)
    declare_queue(queue_name, @vhost)
    with_channel(@vhost, fn(channel) ->
      {:ok, _} = AMQP.Basic.consume(channel, queue_name, nil, [consumer_tag: consumer_tag])
      :timer.sleep(100)
      [[consumer]] = run_command_to_list(@command, [info_keys_s, context[:opts]])
      assert info_keys_a == Keyword.keys(consumer)
      assert consumer[:consumer_tag] == consumer_tag
      assert consumer[:queue_name] == queue_name
      assert Keyword.delete(consumer, :channel_pid) ==
        [queue_name: queue_name, consumer_tag: consumer_tag,
         ack_required: true, prefetch_count: 0, arguments: []]

    end)
  end

  test "run: consumers are grouped by queues (multiple consumer per queue)", context do
    queue_name1 = "test_queue1"
    queue_name2 = "test_queue2"
    declare_queue("test_queue1", @vhost)
    declare_queue("test_queue2", @vhost)
    with_channel(@vhost, fn(channel) ->
      {:ok, tag1} = AMQP.Basic.consume(channel, queue_name1)
      {:ok, tag2} = AMQP.Basic.consume(channel, queue_name2)
      {:ok, tag3} = AMQP.Basic.consume(channel, queue_name2)
      :timer.sleep(100)
      try do
        consumers = run_command_to_list(@command, [["queue_name", "consumer_tag"], context[:opts]])
        {[[consumer1]], [consumers2]} = Enum.split_with(consumers, fn([_]) -> true; ([_,_]) -> false end)
        assert [queue_name: queue_name1, consumer_tag: tag1] == consumer1
        assert Keyword.equal?([{tag2, queue_name2}, {tag3, queue_name2}],
          for([queue_name: q, consumer_tag: t] <- consumers2, do: {t, q}))
      after
        AMQP.Basic.cancel(channel, tag1)
        AMQP.Basic.cancel(channel, tag2)
        AMQP.Basic.cancel(channel, tag3)
      end
    end)
  end
end
