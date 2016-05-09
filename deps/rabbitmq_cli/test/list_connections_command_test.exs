defmodule ListConnectionsCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

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
      assert ListConnectionsCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListConnectionsCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListConnectionsCommand.run(["quack", "peer_host"], context[:opts]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert ListConnectionsCommand.run(["user", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert ListConnectionsCommand.run(["user", "oink", "peer_host"], context[:opts]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert ListConnectionsCommand.run([], context[:opts]) ==
        [{:badrpc, :timeout}]
    end)
  end

  test "no connections by default", context do
    capture_io(fn ->
      assert [] == ListConnectionsCommand.run([], context[:opts])
    end)
  end

  test "user, peer_host, peer_port and state by default", context do
    capture_io(fn ->
      with_connection("/", fn(conn) ->
        conns = ListConnectionsCommand.run([], context[:opts])
        assert Enum.map(conns, &Keyword.keys/1) == [[:user, :peer_host, :peer_port, :state]]
      end)
    end)
  end

  test "filter single key", context do
    capture_io(fn ->
      with_connection("/", fn(conn) ->
        conns = ListConnectionsCommand.run(["name"], context[:opts])
        assert Enum.map(conns, &Keyword.keys/1) == [[:name]]
      end)
    end)
  end

  test "show connection vhost", context do
    vhost = "custom_vhost"
    add_vhost vhost
    set_permissions @user, vhost, [".*", ".*", ".*"]
    on_exit(fn ->
      delete_vhost @vhost
    end)
    capture_io(fn ->
      with_connection(vhost, fn(conn) ->
        conns = ListConnectionsCommand.run(["vhost"], context[:opts])
        assert conns == [[vhost: vhost]]
      end)
    end)
  end


end