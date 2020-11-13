defmodule ListQueuesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListQueuesCommand

  @vhost "test1"
  @user "guest"
  @default_timeout 15000

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    reset_vm_memory_high_watermark()
    delete_all_queues()
    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      delete_all_queues()
      close_all_connections(get_rabbit_hostname())
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
        quiet: true,
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || @default_timeout,
        vhost: @vhost,
        offline: false,
        online: false,
        local: false
      }
    }
  end

  test "merge_defaults: no info keys returns names and message count" do
    assert match?({["name", "messages"], _}, @command.merge_defaults([], %{}))
  end

  test "validate: returns bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: multiple bad args return a list of bad info key values", context do
    assert @command.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink, :quack]}}
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "messages"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["name", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["name", "oink", "messages"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return badrpc", context do
    assert run_command_to_list(@command, [["name"], context[:opts]]) ==
      [{:badrpc, {:timeout, 0.0, "Some queue(s) are unresponsive, use list_unresponsive_queues command."}}]
  end

  @tag test_timeout: 1
  test "run: command timeout (several thousands queues in 1ms) return badrpc with timeout value in seconds", context do
    # we assume it will take longer than 1 ms to list thousands of queues
    n = 5000
    for i <- 1..n do
        declare_queue("test_queue_" <> Integer.to_string(i), @vhost)
    end
    assert run_command_to_list(@command, [["name"], context[:opts]]) ==
      [{:badrpc, {:timeout, 0.001, "Some queue(s) are unresponsive, use list_unresponsive_queues command."}}]
    for i <- 1..n do
        delete_queue("test_queue_" <> Integer.to_string(i), @vhost)
    end
  end

  @tag test_timeout: 5000
  test "run: return multiple queues", context do
    declare_queue("test_queue_1", @vhost)
    publish_messages(@vhost, "test_queue_1", 3)
    declare_queue("test_queue_2", @vhost)
    publish_messages(@vhost, "test_queue_2", 1)
    assert Keyword.equal?(run_command_to_list(@command, [["name", "messages"], context[:opts]]),
      [[name: "test_queue_1", messages: 3],
       [name: "test_queue_2", messages: 1]])
  end

  @tag test_timeout: 5000
  test "run: info keys filter single key", context do
    declare_queue("test_queue_1", @vhost)
    declare_queue("test_queue_2", @vhost)
    assert Keyword.equal?(run_command_to_list(@command, [["name"], context[:opts]]),
      [[name: "test_queue_1"],
       [name: "test_queue_2"]])
  end

  @tag test_timeout: 5000
  test "run: info keys add additional keys", context do
    declare_queue("durable_queue", @vhost, true)
    publish_messages(@vhost, "durable_queue", 3)
    declare_queue("auto_delete_queue", @vhost, false, true)
    publish_messages(@vhost, "auto_delete_queue", 1)
    assert Keyword.equal?(
      run_command_to_list(@command, [["name", "messages", "durable", "auto_delete"], context[:opts]]),
      [[name: "durable_queue", messages: 3, durable: true, auto_delete: false],
       [name: "auto_delete_queue", messages: 1, durable: false, auto_delete: true]])
  end

  @tag test_timeout: 5000
  test "run: info keys order is preserved", context do
    declare_queue("durable_queue", @vhost, true)
    publish_messages(@vhost, "durable_queue", 3)
    declare_queue("auto_delete_queue", @vhost, false, true)
    publish_messages(@vhost, "auto_delete_queue", 1)
    assert Keyword.equal?(
      run_command_to_list(@command, [["messages", "durable", "name", "auto_delete"], context[:opts]]),
      [[messages: 3, durable: true, name: "durable_queue", auto_delete: false],
       [messages: 1, durable: false, name: "auto_delete_queue", auto_delete: true]])
  end

  @tag test_timeout: 5000
  test "run: specifying a vhost returns the targeted vhost queues", context do
    other_vhost = "other_vhost"
    add_vhost other_vhost
    on_exit(fn ->
      delete_vhost other_vhost
    end)
    declare_queue("test_queue_1", @vhost)
    declare_queue("test_queue_2", other_vhost)
    assert run_command_to_list(@command, [["name"], context[:opts]]) == [[name: "test_queue_1"]]
    assert run_command_to_list(@command, [["name"], %{context[:opts] | :vhost => other_vhost}]) == [[name: "test_queue_2"]]
  end
end
