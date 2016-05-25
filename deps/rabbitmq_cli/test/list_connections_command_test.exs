defmodule ListConnectionsCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @user "guest"
  @default_timeout 15000

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
      assert run_command_to_list(ListConnectionsCommand, [["quack"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack]}}
    end)
  end

  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert run_command_to_list(ListConnectionsCommand, [["quack", "oink"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack, :oink]}}
    end)
  end

  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert run_command_to_list(ListConnectionsCommand, [["quack", "peer_host"], context[:opts]]) ==
        {:error, {:bad_info_key, [:quack]}}
      assert run_command_to_list(ListConnectionsCommand, [["user", "oink"], context[:opts]]) ==
        {:error, {:bad_info_key, [:oink]}}
      assert run_command_to_list(ListConnectionsCommand, [["user", "oink", "peer_host"], context[:opts]]) ==
        {:error, {:bad_info_key, [:oink]}}
    end)
  end

  @tag test_timeout: 0
  test "zero timeout causes command to return badrpc", context do
    capture_io(fn ->
      assert run_command_to_list(ListConnectionsCommand, [[], context[:opts]]) ==
        [{:badrpc, {:timeout, 0.0}}]
    end)
  end

  test "user, peer_host, peer_port and state by default", context do
    vhost = "/"
    capture_io(fn ->
      with_connection(vhost, fn(_conn) ->
        conns = run_command_to_list(ListConnectionsCommand, [[], context[:opts]])
        assert Enum.any?(conns, fn(conn) -> conn[:state] != nil end)
      end)
    end)
  end

  test "filter single key", context do
    vhost = "/"
    capture_io(fn ->
      with_connection(vhost, fn(_conn) ->
        conns = run_command_to_list(ListConnectionsCommand, [["name"], context[:opts]])
        assert (Enum.map(conns, &Keyword.keys/1) |> Enum.uniq) == [[:name]]
        assert Enum.any?(conns, fn(conn) -> conn[:name] != nil end)
      end)
    end)
  end

  test "show connection vhost", context do
    vhost = "custom_vhost"
    add_vhost vhost
    set_permissions @user, vhost, [".*", ".*", ".*"]
    on_exit(fn ->
      delete_vhost vhost
    end)
    capture_io(fn ->
      with_connection(vhost, fn(_conn) ->
        conns = run_command_to_list(ListConnectionsCommand, [["vhost"], context[:opts]])
        assert (Enum.map(conns, &Keyword.keys/1) |> Enum.uniq) == [[:vhost]]
        assert Enum.any?(conns, fn(conn) -> conn[:vhost] == vhost end)
      end)
    end)
  end


end