defmodule ListExchangesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListExchangesCommand

  @vhost "test1"
  @user "guest"
  @default_timeout :infinity
  @default_exchanges [{"amq.direct", :direct},
                      {"amq.fanout", :fanout},
                      {"amq.match", :headers},
                      {"amq.rabbitmq.trace", :topic},
                      {"amq.headers", :headers},
                      {"amq.topic", :topic},
                      {"", :direct}]
  @default_options %{vhost: "/", table_headers: true}

  defp default_exchange_names() do
    {names, _types} = Enum.unzip(@default_exchanges)
    names
  end

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
        quiet: true,
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || @default_timeout,
        vhost: @vhost
      }
    }
  end

  test "merge_defaults: should include name and type when no arguments provided and add default vhost to opts" do
    assert @command.merge_defaults([], %{})
      == {["name", "type"], @default_options}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {["name", "type"], @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {["name", "type"], %{vhost: "non_default",
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
    assert @command.validate(["quack", "type"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["name", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["name", "oink", "type"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: zero timeout causes command to return badrpc", context do
    assert run_command_to_list(@command, [["name"], context[:opts]]) ==
      [{:badrpc, {:timeout, 0.0}}]
  end

  test "run: show default exchanges by default", context do
    assert MapSet.new(run_command_to_list(@command, [["name"], context[:opts]])) ==
           MapSet.new(for {ex_name, _ex_type} <- @default_exchanges, do: [name: ex_name])
  end

  test "run: default options test", context do
    exchange_name = "test_exchange"
    declare_exchange(exchange_name, @vhost)
      assert MapSet.new(run_command_to_list(@command, [["name", "type"], context[:opts]])) ==
        MapSet.new(
          for({ex_name, ex_type} <- @default_exchanges, do: [name: ex_name, type: ex_type]) ++
          [[name: exchange_name, type: :direct]])
  end

  test "run: list multiple exchanges", context do
    declare_exchange("test_exchange_1", @vhost, :direct)
    declare_exchange("test_exchange_2", @vhost, :fanout)
    non_default_exchanges = run_command_to_list(@command, [["name", "type"], context[:opts]])
                            |> without_default_exchanges
    assert_set_equal(
      non_default_exchanges,
      [[name: "test_exchange_1", type: :direct],
       [name: "test_exchange_2", type: :fanout]])
  end

  def assert_set_equal(one, two) do
    assert MapSet.new(one) == MapSet.new(two)
  end

  test "run: info keys filter single key", context do
    declare_exchange("test_exchange_1", @vhost)
    declare_exchange("test_exchange_2", @vhost)
    non_default_exchanges = run_command_to_list(@command, [["name"], context[:opts]])
                            |> without_default_exchanges
    assert_set_equal(
      non_default_exchanges,
      [[name: "test_exchange_1"],
       [name: "test_exchange_2"]])
  end


  test "run: info keys add additional keys", context do
    declare_exchange("durable_exchange", @vhost, :direct, true)
    declare_exchange("auto_delete_exchange", @vhost, :fanout, false, true)
    non_default_exchanges = run_command_to_list(@command, [["name", "type", "durable", "auto_delete"], context[:opts]])
                            |> without_default_exchanges
    assert_set_equal(
      non_default_exchanges,
      [[name: "auto_delete_exchange", type: :fanout, durable: false, auto_delete: true],
       [name: "durable_exchange", type: :direct, durable: true, auto_delete: false]])
  end

  test "run: specifying a vhost returns the targeted vhost exchanges", context do
    other_vhost = "other_vhost"
    add_vhost other_vhost
    on_exit(fn ->
      delete_vhost other_vhost
    end)
    declare_exchange("test_exchange_1", @vhost)
    declare_exchange("test_exchange_2", other_vhost)
    non_default_exchanges1 = run_command_to_list(@command, [["name"], context[:opts]])
                             |> without_default_exchanges

    non_default_exchanges2 = run_command_to_list(@command, [["name"], %{context[:opts] | :vhost => other_vhost}])
                             |> without_default_exchanges

    assert non_default_exchanges1 == [[name: "test_exchange_1"]]
    assert non_default_exchanges2 == [[name: "test_exchange_2"]]
  end

  defp without_default_exchanges(xs) do
    Enum.filter(xs,
                fn(x) ->
                  not Enum.member?(default_exchange_names(), x[:name])
                end)
  end

end
