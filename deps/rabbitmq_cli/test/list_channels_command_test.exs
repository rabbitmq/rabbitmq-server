defmodule ListChannelsCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @user "guest"
  @default_timeout :infinity

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    close_all_connections(get_rabbit_hostname)

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname)
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname,
        timeout: context[:test_timeout] || @default_timeout
      }
    }
  end

  test "return bad_info_key on a single bad arg", context do
    capture_io(fn ->
      assert ListChannelsCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListChannelsCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListChannelsCommand.run(["quack", "pid"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert ListChannelsCommand.run(["user", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert ListChannelsCommand.run(["user", "oink", "pid"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert ListChannelsCommand.run([], context[:opts]) ==
        [{:badrpc, {:timeout, 0.0}}]
    end)
  end

  test "default channel info keys are pid, user, consumer_count, and messages_unacknowledged", context do
    close_all_connections(get_rabbit_hostname)
    capture_io(fn ->
      with_channel("/", fn(_channel) ->
        channels = ListChannelsCommand.run([], context[:opts])
        chan = Enum.at(channels, 0)
        assert Keyword.keys(chan) == ~w(pid user consumer_count messages_unacknowledged)a
        assert [user: "guest", consumer_count: 0, messages_unacknowledged: 0] == Keyword.delete(chan, :pid)
      end)
    end)
  end

  test "multiple channels on multiple connections", context do
    close_all_connections(get_rabbit_hostname)
    capture_io(fn ->
      with_channel("/", fn(_channel1) ->
        with_channel("/", fn(_channel2) ->
          channels = ListChannelsCommand.run(["pid", "user", "connection"], context[:opts])
          chan1 = Enum.at(channels, 0)
          chan2 = Enum.at(channels, 1)
          assert Keyword.keys(chan1) == ~w(pid user connection)a
          assert Keyword.keys(chan2) == ~w(pid user connection)a
          assert "guest" == chan1[:user]
          assert "guest" == chan2[:user]
          assert chan1[:pid] !== chan2[:pid]
        end)
      end)
    end)
  end

  test "multiple channels on single connection", context do
    close_all_connections(get_rabbit_hostname)
    capture_io(fn ->
      with_connection("/", fn(conn) ->
        {:ok, _} = AMQP.Channel.open(conn)
        {:ok, _} = AMQP.Channel.open(conn)
        channels = ListChannelsCommand.run(["pid", "user", "connection"], context[:opts])
        chan1 = Enum.at(channels, 0)
        chan2 = Enum.at(channels, 1)                                          
        assert Keyword.keys(chan1) == ~w(pid user connection)a
        assert Keyword.keys(chan2) == ~w(pid user connection)a
        assert "guest" == chan1[:user]
        assert "guest" == chan2[:user]
        assert chan1[:pid] !== chan2[:pid]
      end)
    end)
  end

  test "info keys order is preserved", context do
    close_all_connections(get_rabbit_hostname)
    capture_io(fn ->
      with_channel("/", fn(_channel) ->
        channels = ListChannelsCommand.run(~w(connection vhost name pid number user), context[:opts])
        chan     = Enum.at(channels, 0)
        assert Keyword.keys(chan) == ~w(connection vhost name pid number user)a
      end)
    end)
  end
end