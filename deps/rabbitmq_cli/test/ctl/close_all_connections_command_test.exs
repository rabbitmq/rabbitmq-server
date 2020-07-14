## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule CloseAllConnectionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Ctl.RpcStream
  @helpers RabbitMQ.CLI.Core.Helpers
  @command RabbitMQ.CLI.Ctl.Commands.CloseAllConnectionsCommand

  @vhost "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname())
    end)

    :ok
  end

  setup context do
    node_name = get_rabbit_hostname()
    close_all_connections(node_name)
    await_no_client_connections(node_name, 5_000)

    {:ok, context}
  end

  test "validate: with an invalid number of arguments returns an arg count error", context do
    assert @command.validate(["random", "explanation"], context[:opts]) == {:validation_failure, :too_many_args}
    assert @command.validate([], context[:opts]) == {:validation_failure, :not_enough_args}
  end

  test "validate: with the correct number of arguments returns ok", context do
    assert @command.validate(["explanation"], context[:opts]) == :ok
  end

  test "run: a close connections request in an existing vhost with all defaults closes all connections", context do
    with_connection(@vhost, fn(_) ->
      node = @helpers.normalise_node(context[:node], :shortnames)
      nodes = @helpers.nodes_in_cluster(node)
      [[vhost: @vhost]] = fetch_connection_vhosts(node, nodes)
      opts = %{node: node, vhost: @vhost, global: false, per_connection_delay: 0, limit: 0}
      assert {:ok, "Closed 1 connections"} == @command.run(["test"], opts)

      await_no_client_connections(node, 5_000)
      assert fetch_connection_vhosts(node, nodes) == []
    end)
  end

  test "run: close a limited number of connections in an existing vhost closes a subset of connections", context do
    with_connections([@vhost, @vhost, @vhost], fn(_) ->
      node = @helpers.normalise_node(context[:node], :shortnames)
      nodes = @helpers.nodes_in_cluster(node)
      [[vhost: @vhost], [vhost: @vhost], [vhost: @vhost]] = fetch_connection_vhosts(node, nodes)
      opts = %{node: node, vhost: @vhost, global: false, per_connection_delay: 0, limit: 2}
      assert {:ok, "Closed 2 connections"} == @command.run(["test"], opts)
      Process.sleep(100)
      assert fetch_connection_vhosts(node, nodes) == [[vhost: @vhost]]
    end)
  end

  test "run: a close connections request for a non-existing vhost does nothing", context do
    with_connection(@vhost, fn(_) ->
      node = @helpers.normalise_node(context[:node], :shortnames)
      nodes = @helpers.nodes_in_cluster(node)
      [[vhost: @vhost]] = fetch_connection_vhosts(node, nodes)
      opts = %{node: node, vhost: "non_existent-9288737", global: false, per_connection_delay: 0, limit: 0}
      assert {:ok, "Closed 0 connections"} == @command.run(["test"], opts)
      assert fetch_connection_vhosts(node, nodes) == [[vhost: @vhost]]
    end)
  end

  test "run: a close connections request to an existing node with --global (all vhosts)", context do
    with_connection(@vhost, fn(_) ->
      node = @helpers.normalise_node(context[:node], :shortnames)
      nodes = @helpers.nodes_in_cluster(node)
      [[vhost: @vhost]] = fetch_connection_vhosts(node, nodes)
      opts = %{node: node, global: true, per_connection_delay: 0, limit: 0}
      assert {:ok, "Closed 1 connections"} == @command.run(["test"], opts)
      await_no_client_connections(node, 5_000)
      assert fetch_connection_vhosts(node, nodes) == []
    end)
  end

  test "run: a close_all_connections request to non-existent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, vhost: @vhost, global: true, per_connection_delay: 0, limit: 0, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["test"], opts))
  end

  test "banner for vhost option", context do
    node = @helpers.normalise_node(context[:node], :shortnames)
    opts = %{node: node, vhost: "burrow", global: false, per_connection_delay: 0, limit: 0}
    s = @command.banner(["some reason"], opts)
    assert s =~ ~r/Closing all connections in vhost burrow/
    assert s =~ ~r/some reason/
  end

  test "banner for vhost option with limit", context do
    node = @helpers.normalise_node(context[:node], :shortnames)
    opts = %{node: node, vhost: "burrow", global: false, per_connection_delay: 0, limit: 2}
    s = @command.banner(["some reason"], opts)
    assert s =~ ~r/Closing 2 connections in vhost burrow/
    assert s =~ ~r/some reason/
  end

  test "banner for global option" do
    opts = %{node: :test@localhost, vhost: "burrow", global: true, per_connection_delay: 0, limit: 0}
    s = @command.banner(["some reason"], opts)
    assert s =~ ~r/Closing all connections to node test@localhost/
    assert s =~ ~r/some reason/
  end

  defp fetch_connection_vhosts(node, nodes) do
    fetch_connection_vhosts(node, nodes, 50)
  end

  defp fetch_connection_vhosts(node, nodes, retries) do
      stream = RpcStream.receive_list_items(node,
                                            :rabbit_networking,
                                            :emit_connection_info_all,
                                            [nodes, [:vhost]],
                                            :infinity,
                                            [:vhost],
                                            Kernel.length(nodes))
      xs = Enum.to_list(stream)

      case {xs, retries} do
        {xs, 0} ->
          xs
        {[], n} when n >= 0 ->
          Process.sleep(10)
          fetch_connection_vhosts(node, nodes, retries - 1)
        _ ->
          xs
      end
  end
end
