defmodule ListConnectionsCommandTest do
  use ExUnit.Case, async: false
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

  test "merge_defaults: user, peer_host, peer_port and state by default" do
    assert ListConnectionsCommand.merge_defaults([], %{}) == {~w(user peer_host peer_port state), %{}}
  end

  test "validate: returns bad_info_key on a single bad arg", context do
    assert ListConnectionsCommand.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: multiple bad args return a list of bad info key values", context do
    assert ListConnectionsCommand.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack, :oink]}}
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert ListConnectionsCommand.validate(["quack", "peer_host"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert ListConnectionsCommand.validate(["user", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert ListConnectionsCommand.validate(["user", "oink", "peer_host"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: zero timeout causes command to return badrpc", context do
    assert run_command_to_list(ListConnectionsCommand, [["name"], context[:opts]]) ==
      [{:badrpc, {:timeout, 0.0}}]
  end

  test "run: filter single key", context do
    vhost = "/"
    with_connection(vhost, fn(_conn) ->
      conns = run_command_to_list(ListConnectionsCommand, [["name"], context[:opts]])
      assert (Enum.map(conns, &Keyword.keys/1) |> Enum.uniq) == [[:name]]
      assert Enum.any?(conns, fn(conn) -> conn[:name] != nil end)
    end)
  end

  test "run: show connection vhost", context do
    vhost = "custom_vhost"
    add_vhost vhost
    set_permissions @user, vhost, [".*", ".*", ".*"]
    on_exit(fn ->
      delete_vhost vhost
    end)
    with_connection(vhost, fn(_conn) ->
      conns = run_command_to_list(ListConnectionsCommand, [["vhost"], context[:opts]])
      assert (Enum.map(conns, &Keyword.keys/1) |> Enum.uniq) == [[:vhost]]
      assert Enum.any?(conns, fn(conn) -> conn[:vhost] == vhost end)
    end)
  end


end
