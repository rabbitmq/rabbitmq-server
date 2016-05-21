defmodule ListExchangesCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity
  @default_exchanges [{"amq.direct", :direct},
                      {"amq.fanout", :fanout},
                      {"amq.match", :headers},
                      {"amq.rabbitmq.trace", :topic},
                      {"amq.headers", :headers},
                      {"amq.topic", :topic},
                      {"", :direct}]

  defp default_exchange_names() do
    {names, _types} = Enum.unzip(@default_exchanges)
    names
  end

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
        quiet: true,
        node: get_rabbit_hostname,
        timeout: context[:test_timeout] || @default_timeout,
        vhost: @vhost
      }
    }
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on a single bad arg", context do
    capture_io(fn ->
      assert ListExchangesCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  @tag test_timeout: :infinity
  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListExchangesCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListExchangesCommand.run(["quack", "type"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert ListExchangesCommand.run(["name", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert ListExchangesCommand.run(["name", "oink", "type"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert ListExchangesCommand.run([], context[:opts]) ==
        [{:badrpc, {:timeout, 0.0}}]
    end)
  end

  test "show default exchanges by default", context do
    capture_io(fn ->
      assert MapSet.new(ListExchangesCommand.run(["name"], context[:opts])) ==
             MapSet.new(for {ex_name, _ex_type} <- @default_exchanges, do: [name: ex_name])
      end)
  end

  test "no info keys returns name and type", context do
    exchange_name = "test_exchange"
    declare_exchange(exchange_name, @vhost)
    capture_io(fn ->
      assert MapSet.new(ListExchangesCommand.run([], context[:opts])) ==
        MapSet.new(
          for({ex_name, ex_type} <- @default_exchanges, do: [name: ex_name, type: ex_type]) ++
          [[name: exchange_name, type: :direct]])
      end)
  end

  test "list multiple excahnges", context do
    declare_exchange("test_exchange_1", @vhost, :direct)
    declare_exchange("test_exchange_2", @vhost, :fanout)
    capture_io(fn ->
      non_default_exchanges = ListExchangesCommand.run(["name", "type"], context[:opts])
                              |> without_default_exchanges
      assert_set_equal(
        non_default_exchanges,
        [[name: "test_exchange_1", type: :direct],
         [name: "test_exchange_2", type: :fanout]])
      end)
  end

  def assert_set_equal(one, two) do
    assert MapSet.new(one) == MapSet.new(two)
  end

  test "info keys filter single key", context do
    declare_exchange("test_exchange_1", @vhost)
    declare_exchange("test_exchange_2", @vhost)
    capture_io(fn ->
      non_default_exchanges = ListExchangesCommand.run(["name"], context[:opts])
                              |> without_default_exchanges
      assert_set_equal(
        non_default_exchanges,
        [[name: "test_exchange_1"],
         [name: "test_exchange_2"]])
      end)
  end


  test "info keys add additional keys", context do
    declare_exchange("durable_exchange", @vhost, :direct, true)
    declare_exchange("auto_delete_exchange", @vhost, :fanout, false, true)
    capture_io(fn ->
      non_default_exchanges = ListExchangesCommand.run(["name", "type", "durable", "auto_delete"], context[:opts])
                              |> without_default_exchanges
      assert_set_equal(
        non_default_exchanges,
        [[name: "auto_delete_exchange", type: :fanout, durable: false, auto_delete: true],
         [name: "durable_exchange", type: :direct, durable: true, auto_delete: false]])
      end)
  end

  test "specifying a vhost returns the targeted vhost exchanges", context do
    other_vhost = "other_vhost"
    add_vhost other_vhost
    on_exit(fn ->
      delete_vhost other_vhost
    end)
    declare_exchange("test_exchange_1", @vhost)
    declare_exchange("test_exchange_2", other_vhost)
    capture_io(fn ->
      non_default_exchanges1 = ListExchangesCommand.run(["name"], context[:opts])
                               |> without_default_exchanges

      non_default_exchanges2 = ListExchangesCommand.run(["name"], %{context[:opts] | :vhost => other_vhost})
                               |> without_default_exchanges

      assert non_default_exchanges1 == [[name: "test_exchange_1"]]
      assert non_default_exchanges2 == [[name: "test_exchange_2"]]
      end)
  end

  defp without_default_exchanges(xs) do
    Enum.filter(xs,
                fn(x) ->
                  not Enum.member?(default_exchange_names(), x[:name])
                end)
  end

end